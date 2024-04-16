// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package partitionedhashjoin

import (
	"github.com/pingcap/tidb/pkg/util/hack"
	"hash/fnv"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
)

const SizeOfNextPtr = int(unsafe.Sizeof(uintptr(0)))
const SizeOfLengthField = int(unsafe.Sizeof(uint64(1)))
const sizeOfUInt64 = int(unsafe.Sizeof(uint64(1)))
const sizeOfFloat64 = int(unsafe.Sizeof(float64(1)))
const usedFlagMask = uint32(1) << 31

type rowTableSegment struct {
	/*
	   The row storage used in hash join, the layout is
	   |---------------------|-----------------|----------------------|-------------------------------|
	              |                   |                   |                           |
	              V                   V                   V                           V
	        next_row_ptr          null_map     serialized_key/key_length           row_data
	   next_row_ptr: the ptr to link to the next row, used in hash table build, it will make all the rows of the same hash value a linked list
	   null_map(optional): null_map actually includes two parts: the null_flag for each column in current row, the used_flag which is used in
	                       right semi/outer join. This field is optional, if all the column from build side is not null and used_flag is not used
	                       this field is not needed.
	   serialized_key/key_length(optional): if the join key is inlined, and the key has variable length, this field is used to record the key length
	                       of current row, if the join key is not inlined, this field is the serialized representation of the join keys, used to quick
	                       join key compare during probe stage. This field is optional, for join keys that can be inlined in the row_data(for example,
	                       join key with one integer) and has fixed length, this field is not needed.
	   row_data: the data for all the columns of current row
	   The columns in row_data is variable length. For elements that has fixed length(e.g. int64), it will be saved directly, for elements has a
	   variable length(e.g. string related elements), it will first save the size followed by the raw data. Since the row_data is variable length,
	   it is designed to access the column data in order. In order to avoid random access of the column data in row_data, the column order in the
	   row_data will be adjusted to fit the usage order, more specifically the column order will be
	   * join key is inlined + have other conditions: join keys, column used in other condition, rest columns that will be used as join output
	   * join key is inlined + no other conditions: join keys, rest columns that will be used as join output
	   * join key is not inlined + have other conditions: columns used in other condition, rest columns that will be used as join output
	   * join key is not inlined + no other conditions: columns that will be used as join output
	*/
	rawData         []byte    // the chunk of memory to save the row data
	hashValues      []uint64  // the hash value of each rows
	rowLocations    []uintptr // the start address of each row
	validJoinKeyPos []int     // the pos of rows that need to be inserted into hash table, used in hash table build
}

const MAX_ROW_TABLE_SEGMENT_SIZE = 1024

func newRowTableSegment() *rowTableSegment {
	return &rowTableSegment{
		// TODO: @XuHuaiyu if joinKeyIsInlined, the cap of rawData can be calculated
		rawData:         make([]byte, 0),
		hashValues:      make([]uint64, 0, MAX_ROW_TABLE_SEGMENT_SIZE),
		rowLocations:    make([]uintptr, 0, MAX_ROW_TABLE_SEGMENT_SIZE),
		validJoinKeyPos: make([]int, 0, MAX_ROW_TABLE_SEGMENT_SIZE),
	}
}

func (rts *rowTableSegment) rowCount() int64 {
	return int64(len(rts.rowLocations))
}

func (rts *rowTableSegment) validKeyCount() uint64 {
	return uint64(len(rts.validJoinKeyPos))
}

func setNextRowAddress(rowStart uintptr, nextRowAddress uintptr) {
	*(*unsafe.Pointer)(unsafe.Pointer(rowStart)) = unsafe.Pointer(nextRowAddress)
}

func getNextRowAddress(rowStart uintptr) uintptr {
	return uintptr(*(*unsafe.Pointer)(unsafe.Pointer(rowStart)))
}

type JoinTableMeta struct {
	// if the row has fixed length
	isFixedLength bool
	// the row length if the row is fixed length
	rowLength int
	// if the join keys has fixed length
	isJoinKeysFixedLength bool
	// the join keys length if it is fixed length
	joinKeysLength int
	// is the join key inlined in the row data, the join key can be inlined if and only if
	// 1. keyProb.canBeInlined returns true for all the keys
	// 2. there is no duplicate join keys
	isJoinKeysInlined bool
	// the length of null map, the null map include null bit for each column in the row and the used flag for right semi/outer join
	nullMapLength int
	// the column order in row layout, as described above, the save column order maybe different from the column order in build schema
	// for example, the build schema maybe [col1, col2, col3], and the column order in row maybe [col2, col1, col3], then this array
	// is [1, 0, 2]
	rowColumnsOrder []int
	// the column size of each column, -1 mean variable column, the order is the same as rowColumnsOrder
	columnsSize []int
	// the serialize mode for each key
	serializeModes []codec.SerializeMode
	// the first n columns in row is used for other condition, if a join has other condition, we only need to extract
	// first n columns from the RowTable to evaluate other condition
	columnCountNeededForOtherCondition int
	// total column numbers for build side chunk, this is used to construct the chunk if there is join other condition
	totalColumnNumber int
	// column index offset in null map, will be 1 when if there is usedFlag and 0 if there is no usedFlag
	colOffsetInNullMap int
	// keyMode is the key mode, it can be OneInt/FixedSerializedKey/VariableSerializedKey
	keyMode keyMode
	// offset to rowData, -1 for variable length, non-inlined key
	rowDataOffset int
}

func (meta *JoinTableMeta) getSerializedKeyLength(rowStart uintptr) uint64 {
	return *(*uint64)(unsafe.Add(unsafe.Pointer(rowStart), SizeOfNextPtr+meta.nullMapLength))
}

func (meta *JoinTableMeta) getKeyBytes(rowStart uintptr) []byte {
	switch meta.keyMode {
	case OneInt64:
		return hack.GetBytesFromPtr(unsafe.Add(unsafe.Pointer(rowStart), meta.nullMapLength+SizeOfNextPtr), sizeOfUInt64)
	case FixedSerializedKey:
		return hack.GetBytesFromPtr(unsafe.Add(unsafe.Pointer(rowStart), meta.nullMapLength+SizeOfNextPtr), meta.joinKeysLength)
	case VariableSerializedKey:
		return hack.GetBytesFromPtr(unsafe.Add(unsafe.Pointer(rowStart), meta.nullMapLength+SizeOfNextPtr+SizeOfLengthField), int(meta.getSerializedKeyLength(rowStart)))
	default:
		panic("unknown key match type")
	}
}

func (meta *JoinTableMeta) advanceToRowData(rowStart uintptr) uintptr {
	if meta.rowDataOffset == -1 {
		// variable length, non-inlined key
		return uintptr(unsafe.Add(unsafe.Pointer(rowStart), SizeOfNextPtr+meta.nullMapLength+SizeOfLengthField+int(meta.getSerializedKeyLength(rowStart))))
	} else {
		return uintptr(unsafe.Add(unsafe.Pointer(rowStart), meta.rowDataOffset))
	}
}

func (meta *JoinTableMeta) isColumnNull(rowStart uintptr, columnIndex int) bool {
	byteIndex := (columnIndex + meta.colOffsetInNullMap) / 8
	bitIndex := (columnIndex + meta.colOffsetInNullMap) % 8
	return *(*uint8)(unsafe.Add(unsafe.Pointer(rowStart), SizeOfNextPtr+byteIndex))&(uint8(1)<<(7-bitIndex)) != uint8(0)
}

func (meta *JoinTableMeta) setUsedFlag(rowStart uintptr) {
	addr := (*uint32)(unsafe.Add(unsafe.Pointer(rowStart), SizeOfNextPtr))
	value := atomic.LoadUint32(addr)
	value |= usedFlagMask
	atomic.StoreUint32(addr, value)
}

func (meta *JoinTableMeta) isCurrentRowUsed(rowStart uintptr) bool {
	return (*(*uint32)(unsafe.Add(unsafe.Pointer(rowStart), SizeOfNextPtr)) & usedFlagMask) == usedFlagMask
}

type rowTable struct {
	meta     *JoinTableMeta
	segments []*rowTableSegment
}

// used for test
func (rt *rowTable) getRowStart(rowIndex int) uintptr {
	for segIndex := 0; segIndex < len(rt.segments); segIndex++ {
		if rowIndex >= len(rt.segments[segIndex].rowLocations) {
			rowIndex -= len(rt.segments[segIndex].rowLocations)
		} else {
			return rt.segments[segIndex].rowLocations[rowIndex]
		}
	}
	return 0
}

func (rt *rowTable) getValidJoinKeyPos(rowIndex int) int {
	startOffset := 0
	for segIndex := 0; segIndex < len(rt.segments); segIndex++ {
		if rowIndex >= len(rt.segments[segIndex].validJoinKeyPos) {
			rowIndex -= len(rt.segments[segIndex].validJoinKeyPos)
			startOffset += len(rt.segments[segIndex].rowLocations)
		} else {
			return startOffset + rt.segments[segIndex].validJoinKeyPos[rowIndex]
		}
	}
	return -1
}

type keyProp struct {
	canBeInlined        bool
	keyLength           int
	isStringRelatedType bool
	isKeyUInt64         bool
}

func getKeyProp(tp *types.FieldType) *keyProp {
	switch tp.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear,
		mysql.TypeDuration:
		return &keyProp{canBeInlined: true, keyLength: chunk.GetFixedLen(tp), isStringRelatedType: false, isKeyUInt64: true}
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		collator := collate.GetCollator(tp.GetCollate())
		return &keyProp{canBeInlined: collate.CanUseRawMemAsKey(collator), keyLength: chunk.VarElemLen, isStringRelatedType: true, isKeyUInt64: false}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		// date related type will use uint64 as serialized key
		return &keyProp{canBeInlined: false, keyLength: sizeOfUInt64, isStringRelatedType: false, isKeyUInt64: true}
	case mysql.TypeFloat:
		// float will use float64 as serialized key
		return &keyProp{canBeInlined: false, keyLength: sizeOfFloat64, isStringRelatedType: false, isKeyUInt64: false}
	case mysql.TypeNewDecimal:
		// Although decimal is fixed length, but its key is not fixed length
		return &keyProp{canBeInlined: false, keyLength: chunk.VarElemLen, isStringRelatedType: false, isKeyUInt64: false}
	case mysql.TypeEnum:
		if mysql.HasEnumSetAsIntFlag(tp.GetFlag()) {
			return &keyProp{canBeInlined: false, keyLength: sizeOfUInt64, isStringRelatedType: false, isKeyUInt64: true}
		}
		return &keyProp{canBeInlined: false, keyLength: chunk.VarElemLen, isStringRelatedType: false, isKeyUInt64: false}
	case mysql.TypeBit:
		return &keyProp{canBeInlined: false, keyLength: sizeOfUInt64, isStringRelatedType: false, isKeyUInt64: true}
	default:
		keyLength := chunk.GetFixedLen(tp)
		return &keyProp{canBeInlined: false, keyLength: keyLength, isStringRelatedType: false, isKeyUInt64: false}
	}
}

// buildKeyIndex is the build key column index based on buildSchema, should not be nil
// otherConditionColIndex is the column index that will be used in other condition, if no other condition, will be nil
// columnsNeedConvertToRow is the column index that need to be converted to row, should not be nil
// needUsedFlag is true for outer/semi join that use outer to build
func newTableMeta(buildKeyIndex []int, buildTypes, buildKeyTypes, probeKeyTypes []*types.FieldType, columnsUsedByOtherCondition []int, outputColumns []int, needUsedFlag bool) *JoinTableMeta {
	meta := &JoinTableMeta{}
	meta.isFixedLength = true
	meta.rowLength = 0
	meta.totalColumnNumber = len(buildTypes)

	columnsNeedToBeSaved := make(map[int]struct{}, len(buildTypes))
	updateMeta := func(index int) {
		if _, ok := columnsNeedToBeSaved[index]; !ok {
			columnsNeedToBeSaved[index] = struct{}{}
			length := chunk.GetFixedLen(buildTypes[index])
			if length == chunk.VarElemLen {
				meta.isFixedLength = false
			} else {
				meta.rowLength += length
			}
		}
	}
	if outputColumns == nil {
		// outputColumns = nil means all the column is needed
		for index := range buildTypes {
			updateMeta(index)
		}
	} else {
		for _, index := range outputColumns {
			updateMeta(index)
		}
		if columnsUsedByOtherCondition != nil {
			for _, index := range columnsUsedByOtherCondition {
				updateMeta(index)
			}
		}
	}

	meta.isJoinKeysFixedLength = true
	meta.joinKeysLength = 0
	meta.isJoinKeysInlined = true
	keyIndexMap := make(map[int]struct{})
	meta.serializeModes = make([]codec.SerializeMode, 0, len(buildKeyIndex))
	isAllKeyInteger := true
	hasFixedSizeKeyColumn := false
	for index, keyIndex := range buildKeyIndex {
		keyType := buildKeyTypes[index]
		prop := getKeyProp(keyType)
		if prop.keyLength != chunk.VarElemLen {
			meta.joinKeysLength += prop.keyLength
			hasFixedSizeKeyColumn = true
		} else {
			meta.isJoinKeysFixedLength = false
		}
		if !prop.canBeInlined {
			meta.isJoinKeysInlined = false
		}
		if mysql.IsIntegerType(keyType.GetType()) {
			buildUnsigned := mysql.HasUnsignedFlag(keyType.GetFlag())
			probeUnsigned := mysql.HasUnsignedFlag(probeKeyTypes[index].GetFlag())
			if (buildUnsigned && !probeUnsigned) || (probeUnsigned && !buildUnsigned) {
				meta.serializeModes = append(meta.serializeModes, codec.NeedSignFlag)
				meta.isJoinKeysInlined = false
				if meta.isJoinKeysFixedLength {
					// an extra sign flag is needed in this case
					meta.joinKeysLength += 1
				}
			} else {
				meta.serializeModes = append(meta.serializeModes, codec.Normal)
			}
		} else {
			if !prop.isKeyUInt64 {
				isAllKeyInteger = false
			}
			if meta.isJoinKeysInlined && prop.isStringRelatedType {
				meta.serializeModes = append(meta.serializeModes, codec.KeepStringLength)
			} else {
				meta.serializeModes = append(meta.serializeModes, codec.Normal)
			}
		}
		keyIndexMap[keyIndex] = struct{}{}
	}
	if !meta.isJoinKeysFixedLength {
		meta.joinKeysLength = -1
	}
	if len(buildKeyIndex) != len(keyIndexMap) {
		// has duplicated key, can not be inlined
		meta.isJoinKeysInlined = false
	}
	if !meta.isJoinKeysInlined {
		for i := 0; i < len(buildKeyIndex); i++ {
			if meta.serializeModes[i] == codec.KeepStringLength {
				meta.serializeModes[i] = codec.Normal
			}
		}
	} else {
		for _, index := range buildKeyIndex {
			updateMeta(index)
		}
	}
	if !meta.isFixedLength {
		meta.rowLength = 0
	}
	// construct the column order
	meta.rowColumnsOrder = make([]int, 0, len(columnsNeedToBeSaved))
	meta.columnsSize = make([]int, 0, len(columnsNeedToBeSaved))
	usedColumnMap := make(map[int]struct{}, len(columnsNeedToBeSaved))

	updateColumnOrder := func(index int) {
		if _, ok := usedColumnMap[index]; !ok {
			meta.rowColumnsOrder = append(meta.rowColumnsOrder, index)
			meta.columnsSize = append(meta.columnsSize, chunk.GetFixedLen(buildTypes[index]))
			usedColumnMap[index] = struct{}{}
		}
	}
	if meta.isJoinKeysInlined {
		// if join key is inlined, the join key will be the first columns
		for _, index := range buildKeyIndex {
			updateColumnOrder(index)
		}
	}
	meta.columnCountNeededForOtherCondition = 0
	if len(columnsUsedByOtherCondition) > 0 {
		// if join has other condition, the columns used by other condition is appended to row layout after the key
		for _, index := range columnsUsedByOtherCondition {
			updateColumnOrder(index)
		}
		meta.columnCountNeededForOtherCondition = len(usedColumnMap)
	}
	if outputColumns == nil {
		// outputColumns = nil means all the column is needed
		for index := range buildTypes {
			updateColumnOrder(index)
		}
	} else {
		for _, index := range outputColumns {
			updateColumnOrder(index)
		}
	}
	if isAllKeyInteger && len(buildKeyIndex) == 1 && meta.serializeModes[0] != codec.NeedSignFlag {
		meta.keyMode = OneInt64
	} else {
		if meta.isJoinKeysFixedLength {
			meta.keyMode = FixedSerializedKey
		} else {
			meta.keyMode = VariableSerializedKey
		}
	}
	if needUsedFlag {
		meta.colOffsetInNullMap = 1
		// the total row length should be larger than 4 byte since the smallest unit of atomic.LoadXXX is UInt32
		if len(columnsNeedToBeSaved) > 0 {
			// the smallest length of a column is 4 byte, so the total row length is enough
			meta.nullMapLength = (len(columnsNeedToBeSaved) + 1 + 7) / 8
		} else {
			// if no columns need to be converted to row format, then the key is not inlined
			// 1. if any of the key columns is fixed length, then the row length is larger than 4 bytes(since the smallest length of a fixed length column is 4 bytes)
			// 2. if all the key columns are variable length, there is no guarantee that the row length is larger than 4 byte, the nullmap should be 4 bytes alignment
			if hasFixedSizeKeyColumn {
				meta.nullMapLength = (len(columnsNeedToBeSaved) + 1 + 7) / 8
			} else {
				meta.nullMapLength = ((len(columnsNeedToBeSaved) + 1 + 31) / 32) * 4
			}
		}
	} else {
		meta.colOffsetInNullMap = 0
		meta.nullMapLength = (len(columnsNeedToBeSaved) + 7) / 8
	}
	meta.rowDataOffset = -1
	if meta.isJoinKeysInlined {
		if meta.isJoinKeysFixedLength {
			meta.rowDataOffset = SizeOfNextPtr + meta.nullMapLength
		} else {
			meta.rowDataOffset = SizeOfNextPtr + meta.nullMapLength + SizeOfLengthField
		}
	} else {
		if meta.isJoinKeysFixedLength {
			meta.rowDataOffset = SizeOfNextPtr + meta.nullMapLength + meta.joinKeysLength
		}
	}
	return meta
}

type rowTableBuilder struct {
	buildKeyIndex    []int
	buildSchema      *expression.Schema
	rowColumnsOrder  []int
	columnsSize      []int
	needUsedFlag     bool
	hasNullableKey   bool
	hasFilter        bool
	keepFilteredRows bool

	serializedKeyVectorBuffer [][]byte
	partIdxVector             []int
	selRows                   []int
	usedRows                  []int
	hashValue                 []uint64
	// filterVector and nullKeyVector is indexed by physical row index because the return vector of VectorizedFilter is based on physical row index
	filterVector  []bool // if there is filter before probe, filterVector saves the filter result
	nullKeyVector []bool // nullKeyVector[i] = true if any of the key is null

	crrntSizeOfRowTable []int64
	// store the start position of each row in the rawData,
	// we'll use this temp array to get the address of each row at the end
	startPosInRawData [][]uint64
}

func createRowTableBuilder(buildKeyIndex []int, buildSchema *expression.Schema, meta *JoinTableMeta, partitionNumber int, hasNullableKey bool, hasFilter bool, keepFilteredRows bool) *rowTableBuilder {
	builder := &rowTableBuilder{
		buildKeyIndex:       buildKeyIndex,
		buildSchema:         buildSchema,
		rowColumnsOrder:     meta.rowColumnsOrder,
		columnsSize:         meta.columnsSize,
		crrntSizeOfRowTable: make([]int64, partitionNumber),
		startPosInRawData:   make([][]uint64, partitionNumber),
		hasNullableKey:      hasNullableKey,
		hasFilter:           hasFilter,
		keepFilteredRows:    keepFilteredRows,
	}
	builder.initBuffer()
	return builder
}

func (b *rowTableBuilder) initBuffer() {
	b.serializedKeyVectorBuffer = make([][]byte, chunk.InitialCapacity)
	b.partIdxVector = make([]int, 0, chunk.InitialCapacity)
	b.hashValue = make([]uint64, 0, chunk.InitialCapacity)
	if b.hasFilter {
		b.filterVector = make([]bool, 0, chunk.InitialCapacity)
	}
	if b.hasNullableKey {
		b.nullKeyVector = make([]bool, 0, chunk.InitialCapacity)
		for i := 0; i < chunk.InitialCapacity; i++ {
			b.nullKeyVector = append(b.nullKeyVector, false)
		}
	}
	b.selRows = make([]int, 0, chunk.InitialCapacity)
	for i := 0; i < chunk.InitialCapacity; i++ {
		b.selRows = append(b.selRows, i)
	}
}

func (b *rowTableBuilder) processOneChunk(chk *chunk.Chunk, typeCtx types.Context, hashJoinCtx *PartitionedHashJoinCtx, rowTables []*rowTable) error {
	b.ResetBuffer(chk)
	var err error
	if b.hasFilter {
		b.filterVector, err = expression.VectorizedFilter(hashJoinCtx.SessCtx.GetExprCtx().GetEvalCtx(), hashJoinCtx.SessCtx.GetSessionVars().EnableVectorizedExpression, hashJoinCtx.BuildFilter, chunk.NewIterator4Chunk(chk), b.filterVector)
		if err != nil {
			return err
		}
	}
	// split partition
	for index, colIdx := range b.buildKeyIndex {
		err := codec.SerializeKeys(typeCtx, chk, b.buildSchema.Columns[colIdx].RetType, colIdx, b.usedRows, b.filterVector, b.nullKeyVector, hashJoinCtx.hashTableMeta.serializeModes[index], b.serializedKeyVectorBuffer)
		if err != nil {
			return err
		}
	}

	h := fnv.New64()
	fakePartIndex := 0
	for logicalRowIndex, physicalRowIndex := range b.usedRows {
		if (b.filterVector != nil && !b.filterVector[physicalRowIndex]) || (b.nullKeyVector != nil && b.nullKeyVector[physicalRowIndex]) {
			b.hashValue[logicalRowIndex] = uint64(fakePartIndex)
			b.partIdxVector[logicalRowIndex] = fakePartIndex
			fakePartIndex = (fakePartIndex + 1) % hashJoinCtx.PartitionNumber
			continue
		}
		h.Write(b.serializedKeyVectorBuffer[logicalRowIndex])
		hash := h.Sum64()
		b.hashValue[logicalRowIndex] = hash
		b.partIdxVector[logicalRowIndex] = int(hash % uint64(hashJoinCtx.PartitionNumber))
		h.Reset()
	}

	// 2. build rowtable
	b.appendToRowTable(chk, rowTables, hashJoinCtx.hashTableMeta)
	return nil
}

func resizeSlice[T int | uint64 | bool](s []T, newSize int) []T {
	if cap(s) >= newSize {
		s = s[:newSize]
	} else {
		s = make([]T, newSize)
	}
	return s
}

func (b *rowTableBuilder) ResetBuffer(chk *chunk.Chunk) {
	b.usedRows = chk.Sel()
	logicalRows := chk.NumRows()
	physicalRows := chk.Column(0).Rows()

	if b.usedRows == nil {
		b.selRows = resizeSlice(b.selRows, logicalRows)
		for i := 0; i < logicalRows; i++ {
			b.selRows[i] = i
		}
		b.usedRows = b.selRows
	}
	b.partIdxVector = resizeSlice(b.partIdxVector, logicalRows)
	b.hashValue = resizeSlice(b.hashValue, logicalRows)
	if b.hasFilter {
		b.filterVector = resizeSlice(b.filterVector, physicalRows)
	}
	if b.hasNullableKey {
		b.nullKeyVector = resizeSlice(b.nullKeyVector, physicalRows)
		for i := 0; i < physicalRows; i++ {
			b.nullKeyVector[i] = false
		}
	}
	if cap(b.serializedKeyVectorBuffer) >= logicalRows {
		b.serializedKeyVectorBuffer = b.serializedKeyVectorBuffer[:logicalRows]
		for i := 0; i < logicalRows; i++ {
			b.serializedKeyVectorBuffer[i] = b.serializedKeyVectorBuffer[i][:0]
		}
	} else {
		b.serializedKeyVectorBuffer = make([][]byte, logicalRows)
	}
}

func newRowTable(meta *JoinTableMeta) *rowTable {
	return &rowTable{
		meta:     meta,
		segments: make([]*rowTableSegment, 0),
	}
}

func (builder *rowTableBuilder) appendRemainingRowLocations(rowTables []*rowTable) {
	for partId := 0; partId < len(rowTables); partId++ {
		startPosInRawData := builder.startPosInRawData[partId]
		if len(startPosInRawData) > 0 {
			seg := rowTables[partId].segments[len(rowTables[partId].segments)-1]
			for _, pos := range startPosInRawData {
				seg.rowLocations = append(seg.rowLocations, uintptr(unsafe.Pointer(&seg.rawData[pos])))
			}
			builder.startPosInRawData[partId] = builder.startPosInRawData[partId][:]
		}
	}
}

func (builder *rowTableBuilder) appendToRowTable(chk *chunk.Chunk, rowTables []*rowTable, rowTableMeta *JoinTableMeta) {
	fakeAddrByte := make([]byte, 8)
	for logicalRowIndex, physicalRowIndex := range builder.usedRows {
		needInsertedToHashTable := (!builder.hasFilter || builder.filterVector[physicalRowIndex]) && (!builder.hasNullableKey || !builder.nullKeyVector[physicalRowIndex])
		if !needInsertedToHashTable && !builder.keepFilteredRows {
			continue
		}
		var (
			row     = chk.GetRow(logicalRowIndex)
			partIdx = builder.partIdxVector[logicalRowIndex]
			seg     *rowTableSegment
		)
		if rowTables[partIdx] == nil {
			rowTables[partIdx] = newRowTable(rowTableMeta)
			seg = newRowTableSegment()
			rowTables[partIdx].segments = append(rowTables[partIdx].segments, seg)
			builder.startPosInRawData[partIdx] = builder.startPosInRawData[partIdx][:0]
		} else {
			seg = rowTables[partIdx].segments[len(rowTables[partIdx].segments)-1]
			if builder.crrntSizeOfRowTable[partIdx] >= MAX_ROW_TABLE_SEGMENT_SIZE {
				for _, pos := range builder.startPosInRawData[partIdx] {
					seg.rowLocations = append(seg.rowLocations, uintptr(unsafe.Pointer(&seg.rawData[pos])))
				}
				builder.crrntSizeOfRowTable[partIdx] = 0
				builder.startPosInRawData[partIdx] = builder.startPosInRawData[partIdx][:0]
				seg = newRowTableSegment()
				rowTables[partIdx].segments = append(rowTables[partIdx].segments, seg)
			}
		}
		if needInsertedToHashTable {
			seg.validJoinKeyPos = append(seg.validJoinKeyPos, len(seg.hashValues))
		}
		seg.hashValues = append(seg.hashValues, builder.hashValue[logicalRowIndex])
		builder.startPosInRawData[partIdx] = append(builder.startPosInRawData[partIdx], uint64(len(seg.rawData)))
		// next_row_ptr
		seg.rawData = append(seg.rawData, fakeAddrByte...)
		if len := rowTableMeta.nullMapLength; len > 0 {
			tmp := make([]byte, len)
			seg.rawData = append(seg.rawData, tmp...)
		}
		length := uint64(0)
		// if join_key is not fixed length: `key_length` need to be written in rawData
		if !rowTableMeta.isJoinKeysFixedLength {
			length = uint64(len(builder.serializedKeyVectorBuffer[logicalRowIndex]))
			seg.rawData = append(seg.rawData, unsafe.Slice((*byte)(unsafe.Pointer(&length)), SizeOfLengthField)...)
		}
		if !rowTableMeta.isJoinKeysInlined {
			// if join_key is not inlined: `serialized_key` need to be written in rawData
			seg.rawData = append(seg.rawData, builder.serializedKeyVectorBuffer[logicalRowIndex]...)
		}

		for index, colIdx := range builder.rowColumnsOrder {
			if builder.columnsSize[index] > 0 {
				// fixed size
				seg.rawData = append(seg.rawData, row.GetRaw(colIdx)...)
			} else {
				// length, raw_data
				raw := row.GetRaw(colIdx)
				length = uint64(len(raw))
				seg.rawData = append(seg.rawData, unsafe.Slice((*byte)(unsafe.Pointer(&length)), SizeOfLengthField)...)
				seg.rawData = append(seg.rawData, raw...)
			}
		}
		builder.crrntSizeOfRowTable[partIdx]++
	}
}

func (rt *rowTable) merge(other *rowTable) {
	rt.segments = append(rt.segments, other.segments...)
}

func (rt *rowTable) rowCount() uint64 {
	ret := uint64(0)
	for _, s := range rt.segments {
		ret += uint64(s.rowCount())
	}
	return ret
}

func (rt *rowTable) validKeyCount() uint64 {
	ret := uint64(0)
	for _, s := range rt.segments {
		ret += s.validKeyCount()
	}
	return ret
}
