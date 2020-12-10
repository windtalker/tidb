// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"strconv"
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
)

var (
	_ PhysicalPlan = &PhysicalSelection{}
	_ PhysicalPlan = &PhysicalProjection{}
	_ PhysicalPlan = &PhysicalTopN{}
	_ PhysicalPlan = &PhysicalMaxOneRow{}
	_ PhysicalPlan = &PhysicalTableDual{}
	_ PhysicalPlan = &PhysicalUnionAll{}
	_ PhysicalPlan = &PhysicalSort{}
	_ PhysicalPlan = &NominalSort{}
	_ PhysicalPlan = &PhysicalLock{}
	_ PhysicalPlan = &PhysicalLimit{}
	_ PhysicalPlan = &PhysicalIndexScan{}
	_ PhysicalPlan = &PhysicalTableScan{}
	_ PhysicalPlan = &PhysicalTableReader{}
	_ PhysicalPlan = &PhysicalIndexReader{}
	_ PhysicalPlan = &PhysicalIndexLookUpReader{}
	_ PhysicalPlan = &PhysicalIndexMergeReader{}
	_ PhysicalPlan = &PhysicalHashAgg{}
	_ PhysicalPlan = &PhysicalStreamAgg{}
	_ PhysicalPlan = &PhysicalApply{}
	_ PhysicalPlan = &PhysicalIndexJoin{}
	_ PhysicalPlan = &PhysicalBroadCastJoin{}
	_ PhysicalPlan = &PhysicalHashJoin{}
	_ PhysicalPlan = &PhysicalMergeJoin{}
	_ PhysicalPlan = &PhysicalUnionScan{}
	_ PhysicalPlan = &PhysicalWindow{}
	_ PhysicalPlan = &PhysicalShuffle{}
	_ PhysicalPlan = &PhysicalShuffleDataSourceStub{}
	_ PhysicalPlan = &BatchPointGetPlan{}
)

// PhysicalTableReader is the table reader in tidb.
type PhysicalTableReader struct {
	physicalSchemaProducer

	// TablePlans flats the tablePlan to construct executor pb.
	TablePlans []PhysicalPlan
	tablePlan  PhysicalPlan

	// StoreType indicates table read from which type of store.
	StoreType kv.StoreType

	IsCommonHandle bool

	// Used by partition table.
	PartitionInfo PartitionInfo
}

// PartitionInfo indicates partition helper info in physical plan.
type PartitionInfo struct {
	PruningConds   []expression.Expression
	PartitionNames []model.CIStr
	Columns        []*expression.Column
	ColumnNames    types.NameSlice
}

// GetTablePlan exports the tablePlan.
func (p *PhysicalTableReader) GetTablePlan() PhysicalPlan {
	return p.tablePlan
}

// GetTableScan exports the tableScan that contained in tablePlan.
func (p *PhysicalTableReader) GetTableScan() *PhysicalTableScan {
	curPlan := p.tablePlan
	for {
		chCnt := len(curPlan.Children())
		if chCnt == 0 {
			return curPlan.(*PhysicalTableScan)
		} else if chCnt == 1 {
			curPlan = curPlan.Children()[0]
		} else {
			join := curPlan.(*PhysicalBroadCastJoin)
			curPlan = join.children[1-join.globalChildIndex]
		}
	}
}

// GetPhysicalTableReader returns PhysicalTableReader for logical TiKVSingleGather.
func (sg *TiKVSingleGather) GetPhysicalTableReader(schema *expression.Schema, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalTableReader {
	reader := PhysicalTableReader{}.Init(sg.ctx, sg.blockOffset)
	reader.PartitionInfo = PartitionInfo{
		PruningConds:   sg.Source.allConds,
		PartitionNames: sg.Source.partitionNames,
		Columns:        sg.Source.TblCols,
		ColumnNames:    sg.Source.names,
	}
	reader.stats = stats
	reader.SetSchema(schema)
	reader.childrenReqProps = props
	return reader
}

// GetPhysicalIndexReader returns PhysicalIndexReader for logical TiKVSingleGather.
func (sg *TiKVSingleGather) GetPhysicalIndexReader(schema *expression.Schema, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalIndexReader {
	reader := PhysicalIndexReader{}.Init(sg.ctx, sg.blockOffset)
	reader.stats = stats
	reader.SetSchema(schema)
	reader.childrenReqProps = props
	return reader
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalTableReader) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalTableReader)
	base, err := p.physicalSchemaProducer.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	cloned.StoreType = p.StoreType
	cloned.IsCommonHandle = p.IsCommonHandle
	if cloned.tablePlan, err = p.tablePlan.Clone(); err != nil {
		return nil, err
	}
	if cloned.TablePlans, err = clonePhysicalPlan(p.TablePlans); err != nil {
		return nil, err
	}
	return cloned, nil
}

// SetChildren overrides PhysicalPlan SetChildren interface.
func (p *PhysicalTableReader) SetChildren(children ...PhysicalPlan) {
	p.tablePlan = children[0]
	p.TablePlans = flattenPushDownPlan(p.tablePlan)
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (p *PhysicalTableReader) ExtractCorrelatedCols() (corCols []*expression.CorrelatedColumn) {
	for _, child := range p.TablePlans {
		corCols = append(corCols, ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	return corCols
}

// PhysicalIndexReader is the index reader in tidb.
type PhysicalIndexReader struct {
	physicalSchemaProducer

	// IndexPlans flats the indexPlan to construct executor pb.
	IndexPlans []PhysicalPlan
	indexPlan  PhysicalPlan

	// OutputColumns represents the columns that index reader should return.
	OutputColumns []*expression.Column

	// Used by partition table.
	PartitionInfo PartitionInfo
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalIndexReader) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalIndexReader)
	base, err := p.physicalSchemaProducer.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	if cloned.indexPlan, err = p.indexPlan.Clone(); err != nil {
		return nil, err
	}
	if cloned.IndexPlans, err = clonePhysicalPlan(p.IndexPlans); err != nil {
		return nil, err
	}
	cloned.OutputColumns = cloneCols(p.OutputColumns)
	return cloned, err
}

// SetSchema overrides PhysicalPlan SetSchema interface.
func (p *PhysicalIndexReader) SetSchema(_ *expression.Schema) {
	if p.indexPlan != nil {
		p.IndexPlans = flattenPushDownPlan(p.indexPlan)
		switch p.indexPlan.(type) {
		case *PhysicalHashAgg, *PhysicalStreamAgg:
			p.schema = p.indexPlan.Schema()
		default:
			is := p.IndexPlans[0].(*PhysicalIndexScan)
			p.schema = is.dataSourceSchema
		}
		p.OutputColumns = p.schema.Clone().Columns
	}
}

// SetChildren overrides PhysicalPlan SetChildren interface.
func (p *PhysicalIndexReader) SetChildren(children ...PhysicalPlan) {
	p.indexPlan = children[0]
	p.SetSchema(nil)
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (p *PhysicalIndexReader) ExtractCorrelatedCols() (corCols []*expression.CorrelatedColumn) {
	for _, child := range p.IndexPlans {
		corCols = append(corCols, ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	return corCols
}

// PushedDownLimit is the limit operator pushed down into PhysicalIndexLookUpReader.
type PushedDownLimit struct {
	Offset uint64
	Count  uint64
}

// Clone clones this pushed-down list.
func (p *PushedDownLimit) Clone() *PushedDownLimit {
	cloned := new(PushedDownLimit)
	*cloned = *p
	return cloned
}

// PhysicalIndexLookUpReader is the index look up reader in tidb. It's used in case of double reading.
type PhysicalIndexLookUpReader struct {
	physicalSchemaProducer

	// IndexPlans flats the indexPlan to construct executor pb.
	IndexPlans []PhysicalPlan
	// TablePlans flats the tablePlan to construct executor pb.
	TablePlans []PhysicalPlan
	indexPlan  PhysicalPlan
	tablePlan  PhysicalPlan

	ExtraHandleCol *expression.Column
	// PushedLimit is used to avoid unnecessary table scan tasks of IndexLookUpReader.
	PushedLimit *PushedDownLimit

	CommonHandleCols []*expression.Column

	// Used by partition table.
	PartitionInfo PartitionInfo
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalIndexLookUpReader) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalIndexLookUpReader)
	base, err := p.physicalSchemaProducer.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	if cloned.IndexPlans, err = clonePhysicalPlan(p.IndexPlans); err != nil {
		return nil, err
	}
	if cloned.TablePlans, err = clonePhysicalPlan(p.TablePlans); err != nil {
		return nil, err
	}
	if cloned.indexPlan, err = p.indexPlan.Clone(); err != nil {
		return nil, err
	}
	if cloned.tablePlan, err = p.tablePlan.Clone(); err != nil {
		return nil, err
	}
	if p.ExtraHandleCol != nil {
		cloned.ExtraHandleCol = p.ExtraHandleCol.Clone().(*expression.Column)
	}
	if p.PushedLimit != nil {
		cloned.PushedLimit = p.PushedLimit.Clone()
	}
	return cloned, nil
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (p *PhysicalIndexLookUpReader) ExtractCorrelatedCols() (corCols []*expression.CorrelatedColumn) {
	for _, child := range p.TablePlans {
		corCols = append(corCols, ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	for _, child := range p.IndexPlans {
		corCols = append(corCols, ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	return corCols
}

// PhysicalIndexMergeReader is the reader using multiple indexes in tidb.
type PhysicalIndexMergeReader struct {
	physicalSchemaProducer

	// PartialPlans flats the partialPlans to construct executor pb.
	PartialPlans [][]PhysicalPlan
	// TablePlans flats the tablePlan to construct executor pb.
	TablePlans []PhysicalPlan
	// partialPlans are the partial plans that have not been flatted. The type of each element is permitted PhysicalIndexScan or PhysicalTableScan.
	partialPlans []PhysicalPlan
	// tablePlan is a PhysicalTableScan to get the table tuples. Current, it must be not nil.
	tablePlan PhysicalPlan

	// Used by partition table.
	PartitionInfo PartitionInfo
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (p *PhysicalIndexMergeReader) ExtractCorrelatedCols() (corCols []*expression.CorrelatedColumn) {
	for _, child := range p.TablePlans {
		corCols = append(corCols, ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	for _, child := range p.partialPlans {
		corCols = append(corCols, ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	for _, PartialPlan := range p.PartialPlans {
		for _, child := range PartialPlan {
			corCols = append(corCols, ExtractCorrelatedCols4PhysicalPlan(child)...)
		}
	}
	return corCols
}

// PhysicalIndexScan represents an index scan plan.
type PhysicalIndexScan struct {
	physicalSchemaProducer

	// AccessCondition is used to calculate range.
	AccessCondition []expression.Expression

	Table      *model.TableInfo
	Index      *model.IndexInfo
	IdxCols    []*expression.Column
	IdxColLens []int
	Ranges     []*ranger.Range
	Columns    []*model.ColumnInfo
	DBName     model.CIStr

	TableAsName *model.CIStr

	// dataSourceSchema is the original schema of DataSource. The schema of index scan in KV and index reader in TiDB
	// will be different. The schema of index scan will decode all columns of index but the TiDB only need some of them.
	dataSourceSchema *expression.Schema

	// Hist is the histogram when the query was issued.
	// It is used for query feedback.
	Hist *statistics.Histogram

	rangeInfo string

	// The index scan may be on a partition.
	physicalTableID int64

	GenExprs map[model.TableColumnID]expression.Expression

	isPartition bool
	Desc        bool
	KeepOrder   bool
	// DoubleRead means if the index executor will read kv two times.
	// If the query requires the columns that don't belong to index, DoubleRead will be true.
	DoubleRead bool

	NeedCommonHandle bool
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalIndexScan) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalIndexScan)
	*cloned = *p
	base, err := p.physicalSchemaProducer.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	cloned.AccessCondition = cloneExprs(p.AccessCondition)
	if p.Table != nil {
		cloned.Table = p.Table.Clone()
	}
	if p.Index != nil {
		cloned.Index = p.Index.Clone()
	}
	cloned.IdxCols = cloneCols(p.IdxCols)
	cloned.IdxColLens = make([]int, len(p.IdxColLens))
	copy(cloned.IdxColLens, p.IdxColLens)
	cloned.Ranges = cloneRanges(p.Ranges)
	cloned.Columns = cloneColInfos(p.Columns)
	if p.dataSourceSchema != nil {
		cloned.dataSourceSchema = p.dataSourceSchema.Clone()
	}
	if p.Hist != nil {
		cloned.Hist = p.Hist.Copy()
	}
	return cloned, nil
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (p *PhysicalIndexScan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.AccessCondition))
	for _, expr := range p.AccessCondition {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	return corCols
}

// PhysicalMemTable reads memory table.
type PhysicalMemTable struct {
	physicalSchemaProducer

	DBName         model.CIStr
	Table          *model.TableInfo
	Columns        []*model.ColumnInfo
	Extractor      MemTablePredicateExtractor
	QueryTimeRange QueryTimeRange
}

// PhysicalTableScan represents a table scan plan.
type PhysicalTableScan struct {
	physicalSchemaProducer

	// AccessCondition is used to calculate range.
	AccessCondition []expression.Expression
	filterCondition []expression.Expression

	Table   *model.TableInfo
	Columns []*model.ColumnInfo
	DBName  model.CIStr
	Ranges  []*ranger.Range
	PkCols  []*expression.Column

	TableAsName *model.CIStr

	// Hist is the histogram when the query was issued.
	// It is used for query feedback.
	Hist *statistics.Histogram

	physicalTableID int64

	rangeDecidedBy []*expression.Column

	// HandleIdx is the index of handle, which is only used for admin check table.
	HandleIdx  []int
	HandleCols HandleCols

	StoreType kv.StoreType

	IsGlobalRead bool

	// The table scan may be a partition, rather than a real table.
	isPartition bool
	// KeepOrder is true, if sort data by scanning pkcol,
	KeepOrder bool
	Desc      bool

	isChildOfIndexLookUp bool
}

// Clone implements PhysicalPlan interface.
func (ts *PhysicalTableScan) Clone() (PhysicalPlan, error) {
	clonedScan := new(PhysicalTableScan)
	*clonedScan = *ts
	prod, err := ts.physicalSchemaProducer.cloneWithSelf(clonedScan)
	if err != nil {
		return nil, err
	}
	clonedScan.physicalSchemaProducer = *prod
	clonedScan.AccessCondition = cloneExprs(ts.AccessCondition)
	clonedScan.filterCondition = cloneExprs(ts.filterCondition)
	if ts.Table != nil {
		clonedScan.Table = ts.Table.Clone()
	}
	clonedScan.Columns = cloneColInfos(ts.Columns)
	clonedScan.Ranges = cloneRanges(ts.Ranges)
	clonedScan.PkCols = cloneCols(ts.PkCols)
	clonedScan.TableAsName = ts.TableAsName
	if ts.Hist != nil {
		clonedScan.Hist = ts.Hist.Copy()
	}
	clonedScan.rangeDecidedBy = cloneCols(ts.rangeDecidedBy)
	return clonedScan, nil
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (ts *PhysicalTableScan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(ts.AccessCondition)+len(ts.filterCondition))
	for _, expr := range ts.AccessCondition {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	for _, expr := range ts.filterCondition {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	return corCols
}

// IsPartition returns true and partition ID if it's actually a partition.
func (ts *PhysicalTableScan) IsPartition() (bool, int64) {
	return ts.isPartition, ts.physicalTableID
}

// ExpandVirtualColumn expands the virtual column's dependent columns to ts's schema and column.
func ExpandVirtualColumn(columns []*model.ColumnInfo, schema *expression.Schema,
	colsInfo []*model.ColumnInfo) []*model.ColumnInfo {
	copyColumn := make([]*model.ColumnInfo, len(columns))
	copy(copyColumn, columns)
	var extraColumn *expression.Column
	var extraColumnModel *model.ColumnInfo
	if schema.Columns[len(schema.Columns)-1].ID == model.ExtraHandleID {
		extraColumn = schema.Columns[len(schema.Columns)-1]
		extraColumnModel = copyColumn[len(copyColumn)-1]
		schema.Columns = schema.Columns[:len(schema.Columns)-1]
		copyColumn = copyColumn[:len(copyColumn)-1]
	}
	schemaColumns := schema.Columns
	for _, col := range schemaColumns {
		if col.VirtualExpr == nil {
			continue
		}

		baseCols := expression.ExtractDependentColumns(col.VirtualExpr)
		for _, baseCol := range baseCols {
			if !schema.Contains(baseCol) {
				schema.Columns = append(schema.Columns, baseCol)
				copyColumn = append(copyColumn, FindColumnInfoByID(colsInfo, baseCol.ID))
			}
		}
	}
	if extraColumn != nil {
		schema.Columns = append(schema.Columns, extraColumn)
		copyColumn = append(copyColumn, extraColumnModel)
	}
	return copyColumn
}

//SetIsChildOfIndexLookUp is to set the bool if is a child of IndexLookUpReader
func (ts *PhysicalTableScan) SetIsChildOfIndexLookUp(isIsChildOfIndexLookUp bool) {
	ts.isChildOfIndexLookUp = isIsChildOfIndexLookUp
}

// PhysicalProjection is the physical operator of projection.
type PhysicalProjection struct {
	physicalSchemaProducer

	Exprs                []expression.Expression
	CalculateNoDelay     bool
	AvoidColumnEvaluator bool
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalProjection) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalProjection)
	*cloned = *p
	base, err := p.basePhysicalPlan.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalPlan = *base
	cloned.Exprs = cloneExprs(p.Exprs)
	return cloned, err
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (p *PhysicalProjection) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.Exprs))
	for _, expr := range p.Exprs {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	return corCols
}

// ToCuraJson implements PhysicalPlan interface.
func (p *PhysicalProjection) ToCuraJson(jsonPlan []byte) ([]byte, error) {
	jsonPlan = append(jsonPlan, []byte("{\"rel_op\": \"Project\", \"exprs\": ")...)
	var err error = nil
	jsonPlan, err = ExprsToCuraJson(p.Exprs, jsonPlan)
	if err != nil {
		return jsonPlan, err
	}
	jsonPlan = append(jsonPlan, '}')
	return jsonPlan, nil
}

// PhysicalTopN is the physical operator of topN.
type PhysicalTopN struct {
	basePhysicalPlan

	ByItems []*util.ByItems
	Offset  uint64
	Count   uint64
}

// Clone implements PhysicalPlan interface.
func (lt *PhysicalTopN) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalTopN)
	*cloned = *lt
	base, err := lt.basePhysicalPlan.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalPlan = *base
	cloned.ByItems = make([]*util.ByItems, 0, len(lt.ByItems))
	for _, it := range lt.ByItems {
		cloned.ByItems = append(cloned.ByItems, it.Clone())
	}
	return cloned, nil
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (lt *PhysicalTopN) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(lt.ByItems))
	for _, item := range lt.ByItems {
		corCols = append(corCols, expression.ExtractCorColumns(item.Expr)...)
	}
	return corCols
}

func (lt *PhysicalTopN) ToCuraJson(jsonPlan []byte) ([]byte, error) {
	jsonPlan = append(jsonPlan, []byte("{\"rel_op\": \"Sort\", \"sort_infos\": [")...)
	var err error = nil
	for idx, o := range lt.ByItems {
		if idx != 0 {
			jsonPlan = append(jsonPlan, ',')
		}
		jsonPlan = append(jsonPlan, []byte("{\"expr\": ")...)
		jsonPlan, err = ExprToCuraJson(o.Expr, jsonPlan)
		if err != nil {
			return nil, err
		}
		jsonPlan = append(jsonPlan, []byte(", \"order\": ")...)
		if o.Desc {
			jsonPlan = append(jsonPlan, []byte("\"DESC\", \"null_order\": \"NULL_FIRST\"}")...)
		} else {
			jsonPlan = append(jsonPlan, []byte("\"ASC\", \"null_order\": \"NULL_FIRST\"}")...)
		}
	}
	jsonPlan = append(jsonPlan, []byte("]}, ")...)
	jsonPlan = append(jsonPlan, []byte("{\"rel_op\": \"Limit\", \"n\": ")...)
	jsonPlan = append(jsonPlan, []byte(strconv.FormatUint(lt.Count, 10))...)
	jsonPlan = append(jsonPlan, []byte("}")...)
	return jsonPlan, nil
}

// PhysicalApply represents apply plan, only used for subquery.
type PhysicalApply struct {
	PhysicalHashJoin

	CanUseCache bool
	Concurrency int
	OuterSchema []*expression.CorrelatedColumn
}

// Clone implements PhysicalPlan interface.
func (la *PhysicalApply) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalApply)
	base, err := la.PhysicalHashJoin.Clone()
	if err != nil {
		return nil, err
	}
	hj := base.(*PhysicalHashJoin)
	cloned.PhysicalHashJoin = *hj
	cloned.CanUseCache = la.CanUseCache
	cloned.Concurrency = la.Concurrency
	for _, col := range la.OuterSchema {
		cloned.OuterSchema = append(cloned.OuterSchema, col.Clone().(*expression.CorrelatedColumn))
	}
	return cloned, nil
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (la *PhysicalApply) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := la.PhysicalHashJoin.ExtractCorrelatedCols()
	for i := len(corCols) - 1; i >= 0; i-- {
		if la.children[0].Schema().Contains(&corCols[i].Column) {
			corCols = append(corCols[:i], corCols[i+1:]...)
		}
	}
	return corCols
}

type basePhysicalJoin struct {
	physicalSchemaProducer

	JoinType JoinType

	LeftConditions  expression.CNFExprs
	RightConditions expression.CNFExprs
	OtherConditions expression.CNFExprs

	InnerChildIdx int
	OuterJoinKeys []*expression.Column
	InnerJoinKeys []*expression.Column
	LeftJoinKeys  []*expression.Column
	RightJoinKeys []*expression.Column
	IsNullEQ      []bool
	DefaultValues []types.Datum
}

func (p *basePhysicalJoin) cloneWithSelf(newSelf PhysicalPlan) (*basePhysicalJoin, error) {
	cloned := new(basePhysicalJoin)
	base, err := p.physicalSchemaProducer.cloneWithSelf(newSelf)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	cloned.JoinType = p.JoinType
	cloned.LeftConditions = cloneExprs(p.LeftConditions)
	cloned.RightConditions = cloneExprs(p.RightConditions)
	cloned.OtherConditions = cloneExprs(p.OtherConditions)
	cloned.InnerChildIdx = p.InnerChildIdx
	cloned.OuterJoinKeys = cloneCols(p.OuterJoinKeys)
	cloned.InnerJoinKeys = cloneCols(p.InnerJoinKeys)
	cloned.LeftJoinKeys = cloneCols(p.LeftJoinKeys)
	cloned.RightJoinKeys = cloneCols(p.RightJoinKeys)
	for _, d := range p.DefaultValues {
		cloned.DefaultValues = append(cloned.DefaultValues, *d.Clone())
	}
	return cloned, nil
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (p *basePhysicalJoin) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))
	for _, fun := range p.LeftConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.RightConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.OtherConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	return corCols
}

// PhysicalHashJoin represents hash join implementation of LogicalJoin.
type PhysicalHashJoin struct {
	basePhysicalJoin

	Concurrency     uint
	EqualConditions []*expression.ScalarFunction

	// use the outer table to build a hash table when the outer table is smaller.
	UseOuterToBuild bool
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalHashJoin) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalHashJoin)
	base, err := p.basePhysicalJoin.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalJoin = *base
	cloned.Concurrency = p.Concurrency
	cloned.UseOuterToBuild = p.UseOuterToBuild
	for _, c := range p.EqualConditions {
		cloned.EqualConditions = append(cloned.EqualConditions, c.Clone().(*expression.ScalarFunction))
	}
	return cloned, nil
}

func TypesToCuraJson(types []*types.FieldType, jsonPlan []byte) ([]byte, error) {
	if len(types) == 1 {
		return TypeToCuraJson(types[0], jsonPlan)
	}
	var err error = nil
	jsonPlan = append(jsonPlan, '[')
	for idx, t := range types {
		if idx != 0 {
			jsonPlan = append(jsonPlan, ',')
		}
		jsonPlan, err = TypeToCuraJson(t, jsonPlan)
		if err != nil {
			return jsonPlan, err
		}
	}
	jsonPlan = append(jsonPlan, ']')
	return jsonPlan, nil
}

func TypeToCuraJson(t *types.FieldType, jsonPlan []byte) ([]byte, error) {
	jsonPlan = append(jsonPlan, []byte("{\"type\": ")...)
	switch t.Tp {
	case mysql.TypeLong, mysql.TypeLonglong:
		if t.Flag&mysql.UnsignedFlag == mysql.UnsignedFlag {
			jsonPlan = append(jsonPlan, []byte("\"UINT64\"")...)
		} else {
			jsonPlan = append(jsonPlan, []byte("\"INT64\"")...)
		}
	case mysql.TypeString, mysql.TypeVarchar:
		jsonPlan = append(jsonPlan, []byte("\"STRING\"")...)
	case mysql.TypeFloat, mysql.TypeDouble:
		jsonPlan = append(jsonPlan, []byte("\"FLOAT64\"")...)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		jsonPlan = append(jsonPlan, []byte("\"UINT64\"")...)
	default:
		return jsonPlan, errors.New("Type not supported by Cura")
	}
	jsonPlan = append(jsonPlan, []byte(", \"nullable\": ")...)
	if t.Flag&mysql.NotNullFlag == mysql.NotNullFlag {
		jsonPlan = append(jsonPlan, []byte("false")...)
	} else {
		jsonPlan = append(jsonPlan, []byte("true")...)
	}
	jsonPlan = append(jsonPlan, []byte("}")...)
	return jsonPlan, nil
}

func ExprsToCuraJson(exprs expression.CNFExprs, jsonPlan []byte) ([]byte, error) {
	if len(exprs) == 1 {
		return ExprToCuraJson(exprs[0], jsonPlan)
	}
	var err error = nil
	jsonPlan = append(jsonPlan, '[')
	for idx, expr := range exprs {
		if idx != 0 {
			jsonPlan = append(jsonPlan, ',')
		}
		jsonPlan, err = ExprToCuraJson(expr, jsonPlan)
		if err != nil {
			return jsonPlan, err
		}
	}
	jsonPlan = append(jsonPlan, ']')
	return jsonPlan, nil
}

func ExprToCuraJson(expr expression.Expression, jsonPlan []byte) ([]byte, error) {
	return ExprToCuraJsonInternal(expr, jsonPlan, false)
}
func ExprToCuraJsonInternal(expr expression.Expression, jsonPlan []byte, forSelection bool) ([]byte, error) {
	jsonPlan = append(jsonPlan, '{')
	var err error = nil
	returnBool := false
	switch x := expr.(type) {
	case *expression.ScalarFunction:
		if len(x.GetArgs()) == 1 {
			return jsonPlan, errors.New("Cura not supported expr")
		} else if len(x.GetArgs()) == 2 {
			jsonPlan = append(jsonPlan, []byte("\"binary_op\": ")...)
			name := x.FuncName
			switch strings.ToLower(name.L) {
			case "+":
				jsonPlan = append(jsonPlan, []byte("\"ADD\"")...)
			case "-", "minus":
				jsonPlan = append(jsonPlan, []byte("\"SUB\"")...)
			case "*", "mul":
				jsonPlan = append(jsonPlan, []byte("\"MUL\"")...)
			case "/":
				jsonPlan = append(jsonPlan, []byte("\"DIV\"")...)
			case "=":
				jsonPlan = append(jsonPlan, []byte("\"EQUAL\"")...)
				if forSelection {
					returnBool = true
				}
			case "!=":
				jsonPlan = append(jsonPlan, []byte("\"NOT_EQUAL\"")...)
				if forSelection {
					returnBool = true
				}
			case "<", "lt":
				jsonPlan = append(jsonPlan, []byte("\"LESS\"")...)
				if forSelection {
					returnBool = true
				}
			case ">", "gt":
				jsonPlan = append(jsonPlan, []byte("\"GREATER\"")...)
				if forSelection {
					returnBool = true
				}
			case "<=", "le":
				jsonPlan = append(jsonPlan, []byte("\"LESS_EQUAL\"")...)
				if forSelection {
					returnBool = true
				}
			case ">=", "ge":
				jsonPlan = append(jsonPlan, []byte("\"CREATER_EQUAL\"")...)
				if forSelection {
					returnBool = true
				}
			case "%":
				jsonPlan = append(jsonPlan, []byte("\"PMOD\"")...)
			case "and":
				jsonPlan = append(jsonPlan, []byte("\"LOGICAL_AND\"")...)
				if forSelection {
					returnBool = true
				}
			case "or":
				jsonPlan = append(jsonPlan, []byte("\"LOGICAL_OR\"")...)
				if forSelection {
					returnBool = true
				}
			default:
				return jsonPlan, errors.New("Cura not supported expr")
			}
			jsonPlan = append(jsonPlan, []byte(", \"operands\": ")...)
			jsonPlan, err = ExprsToCuraJson(x.GetArgs(), jsonPlan)
			if err != nil {
				return jsonPlan, err
			}
			jsonPlan = append(jsonPlan, []byte(", \"type\": ")...)
			if returnBool {
				jsonPlan = append(jsonPlan, []byte("{\"type\": ")...)
				jsonPlan = append(jsonPlan, []byte("\"BOOL8\"")...)
				jsonPlan = append(jsonPlan, []byte(", \"nullable\": ")...)
				if x.RetType.Flag&mysql.NotNullFlag == mysql.NotNullFlag {
					jsonPlan = append(jsonPlan, []byte("false")...)
				} else {
					jsonPlan = append(jsonPlan, []byte("true")...)
				}
				jsonPlan = append(jsonPlan, []byte("}}")...)
			} else {
				jsonPlan, err = TypeToCuraJson(x.RetType, jsonPlan)
				if err != nil {
					return jsonPlan, err
				}
				jsonPlan = append(jsonPlan, '}')
			}
			return jsonPlan, nil
		} else {
			return jsonPlan, errors.New("Cura not supported expr")
		}
	case *expression.Column:
		jsonPlan = append(jsonPlan, []byte("\"col_ref\": ")...)
		jsonPlan = append(jsonPlan, []byte(strconv.Itoa(x.Index))...)
	case *expression.Constant:
		jsonPlan = append(jsonPlan, []byte("\"type\":")...)
		switch x.RetType.Tp {
		case mysql.TypeLonglong:
			jsonPlan = append(jsonPlan, []byte("\"INT64\", \"literal\": ")...)
			jsonPlan = append(jsonPlan, []byte(strconv.FormatInt(x.Value.GetInt64(), 10))...)
		case mysql.TypeDouble, mysql.TypeFloat:
			jsonPlan = append(jsonPlan, []byte("\"FLOAT64\", \"literal\": ")...)
			jsonPlan = append(jsonPlan, []byte(strconv.FormatFloat(x.Value.GetFloat64(), 'f', -1, 64))...)
		case mysql.TypeString:
			jsonPlan = append(jsonPlan, []byte("\"STRING\", \"literal\": ")...)
			jsonPlan = append(jsonPlan, []byte(x.Value.GetString())...)
		default:
			return jsonPlan, errors.New("Cura not supported expr")
		}
	default:
		return jsonPlan, errors.New("Cura not supported expr")
	}
	jsonPlan = append(jsonPlan, '}')
	return jsonPlan, nil
}

func joinEqualConditionToCuraJson(leftKey *expression.Column, rightKey *expression.Column, leftOffset int, jsonPlan []byte) ([]byte, bool, error) {
	jsonPlan = append(jsonPlan, []byte("{\"binary_op\": \"EQUAL\", \"operands\": ")...)
	operands := make([]expression.Expression, 2, 2)
	operands[0] = leftKey
	operands[1] = rightKey
	var err error = nil

	jsonPlan = append(jsonPlan, '[')
	jsonPlan, err = ExprToCuraJson(leftKey, jsonPlan)
	if err != nil {
		return jsonPlan, false, err
	}
	jsonPlan = append(jsonPlan, ',')
	jsonPlan = append(jsonPlan, '{')
	jsonPlan = append(jsonPlan, []byte("\"col_ref\": ")...)
	jsonPlan = append(jsonPlan, []byte(strconv.Itoa(rightKey.Index+leftOffset))...)
	jsonPlan = append(jsonPlan, '}')
	jsonPlan = append(jsonPlan, ']')

	jsonPlan = append(jsonPlan, []byte(", \"type\": ")...)
	jsonPlan = append(jsonPlan, []byte("{\"type\": ")...)
	jsonPlan = append(jsonPlan, []byte("\"INT64\"")...)
	jsonPlan = append(jsonPlan, []byte(", \"nullable\": ")...)
	var nullable bool = false
	if leftKey.RetType.Flag&mysql.NotNullFlag == mysql.NotNullFlag && rightKey.RetType.Flag&mysql.NotNullFlag == mysql.NotNullFlag {
		jsonPlan = append(jsonPlan, []byte("false")...)
		nullable = false
	} else {
		jsonPlan = append(jsonPlan, []byte("true")...)
		nullable = true
	}
	jsonPlan = append(jsonPlan, []byte("}}")...)
	return jsonPlan, nullable, nil
}

func joinEqualConditionsToCuraJson(leftKeys []*expression.Column, rightKeys []*expression.Column, index, leftOffset int, jsonPlan []byte) ([]byte, bool, error) {
	var err error = nil
	var nullable bool = false
	if index == len(rightKeys)-2 {
		// last one
		jsonPlan = append(jsonPlan, []byte("{\"binary_op\": \"LOGICAL_AND\", \"operands\": [")...)
		var leftNullable bool = false
		var rightNullable bool = false
		jsonPlan, leftNullable, err = joinEqualConditionToCuraJson(leftKeys[index], rightKeys[index], leftOffset, jsonPlan)
		if err != nil {
			return jsonPlan, false, err
		}
		jsonPlan = append(jsonPlan, ',')
		jsonPlan, rightNullable, err = joinEqualConditionToCuraJson(leftKeys[index+1], rightKeys[index+1], leftOffset, jsonPlan)
		if err != nil {
			return jsonPlan, false, err
		}
		jsonPlan = append(jsonPlan, []byte("], \"type\": {\"type\": \"INT64\", \"nullable\": ")...)
		if leftNullable || rightNullable {
			jsonPlan = append(jsonPlan, []byte("false")...)
			nullable = false
		} else {
			jsonPlan = append(jsonPlan, []byte("true")...)
			nullable = true
		}
		jsonPlan = append(jsonPlan, []byte("}}")...)
	} else {
		jsonPlan = append(jsonPlan, []byte("{\"binary_op\": \"LOGICAL_AND\", \"operands\": [")...)
		var leftNullable bool = false
		var rightNullable bool = false
		jsonPlan, leftNullable, err = joinEqualConditionToCuraJson(leftKeys[index], rightKeys[index], leftOffset, jsonPlan)
		if err != nil {
			return jsonPlan, false, err
		}
		jsonPlan = append(jsonPlan, ',')
		jsonPlan, rightNullable, err = joinEqualConditionsToCuraJson(leftKeys, rightKeys, index+1, leftOffset, jsonPlan)
		if err != nil {
			return jsonPlan, false, err
		}
		jsonPlan = append(jsonPlan, []byte("], \"type\": {\"type\": \"INT64\", \"nullable\": ")...)
		if leftNullable || rightNullable {
			jsonPlan = append(jsonPlan, []byte("false")...)
			nullable = false
		} else {
			jsonPlan = append(jsonPlan, []byte("true")...)
			nullable = true
		}
		jsonPlan = append(jsonPlan, []byte("}}")...)
	}
	return jsonPlan, nullable, nil
}

func (p *PhysicalHashJoin) ToCuraJson(jsonPlan []byte) ([]byte, error) {
	if len(p.LeftConditions) > 0 || len(p.RightConditions) > 0 || len(p.OtherConditions) > 0 {
		return nil, errors.New("Cura not supported join")
	}
	if p.JoinType != InnerJoin {
		return nil, errors.New("Cura not supported join")
	}
	jsonPlan = append(jsonPlan, []byte("{\"rel_op\": \"HashJoin\",\"type\": \"INNER\", \"condition\":")...)
	var err error = nil
	if len(p.LeftJoinKeys) == 1 {
		jsonPlan, _, err = joinEqualConditionToCuraJson(p.LeftJoinKeys[0], p.RightJoinKeys[0], len(p.children[0].Schema().Columns), jsonPlan)
		if err != nil {
			return jsonPlan, err
		}
	} else {
		jsonPlan, _, err = joinEqualConditionsToCuraJson(p.LeftJoinKeys, p.RightJoinKeys, 0, len(p.children[0].Schema().Columns), jsonPlan)
		if err != nil {
			return jsonPlan, err
		}
	}
	if p.JoinType == InnerJoin {
		if p.InnerChildIdx == 0 {
			jsonPlan = append(jsonPlan, []byte(",\"build_side\": \"LEFT\"")...)
		} else {
			jsonPlan = append(jsonPlan, []byte(",\"build_side\": \"RIGHT\"")...)
		}
	}
	jsonPlan = append(jsonPlan, '}')
	if len(p.schema.Columns) != len(p.children[0].Schema().Columns)+len(p.children[1].Schema().Columns) {
		// need to add extra projection
		jsonPlan = append(jsonPlan, []byte(", {\"rel_op\": \"Project\", \"exprs\": [")...)
		for i, col := range p.schema.Columns {
			index := 0
			found := false
			for idx, leftCol := range p.children[0].Schema().Columns {
				if leftCol.UniqueID == col.UniqueID {
					found = true
					index = index + idx
				}
			}
			if !found {
				index = len(p.children[0].Schema().Columns)
				for idx, rightCol := range p.children[1].Schema().Columns {
					if rightCol.UniqueID == col.UniqueID {
						found = true
						index = index + idx
					}
				}
			}
			if !found {
				return nil, errors.New("Could not found join output column from input columns")
			}
			if i != 0 {
				jsonPlan = append(jsonPlan, ',')
			}
			jsonPlan = append(jsonPlan, []byte("{\"col_ref\": ")...)
			jsonPlan = append(jsonPlan, []byte(strconv.Itoa(index))...)
			jsonPlan = append(jsonPlan, '}')
		}
		if len(p.schema.Columns) == 0 {
			// make at least one column is selected
			jsonPlan = append(jsonPlan, []byte("{\"col_ref\": ")...)
			jsonPlan = append(jsonPlan, []byte(strconv.Itoa(0))...)
			jsonPlan = append(jsonPlan, '}')
		}
		jsonPlan = append(jsonPlan, []byte("]}")...)
	}
	return jsonPlan, nil
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (p *PhysicalHashJoin) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.EqualConditions)+len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))
	for _, fun := range p.EqualConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.LeftConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.RightConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.OtherConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	return corCols
}

// NewPhysicalHashJoin creates a new PhysicalHashJoin from LogicalJoin.
func NewPhysicalHashJoin(p *LogicalJoin, innerIdx int, useOuterToBuild bool, newStats *property.StatsInfo, prop ...*property.PhysicalProperty) *PhysicalHashJoin {
	leftJoinKeys, rightJoinKeys, isNullEQ, _ := p.GetJoinKeys()
	baseJoin := basePhysicalJoin{
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		LeftJoinKeys:    leftJoinKeys,
		RightJoinKeys:   rightJoinKeys,
		IsNullEQ:        isNullEQ,
		JoinType:        p.JoinType,
		DefaultValues:   p.DefaultValues,
		InnerChildIdx:   innerIdx,
	}
	hashJoin := PhysicalHashJoin{
		basePhysicalJoin: baseJoin,
		EqualConditions:  p.EqualConditions,
		Concurrency:      uint(p.ctx.GetSessionVars().HashJoinConcurrency()),
		UseOuterToBuild:  useOuterToBuild,
	}.Init(p.ctx, newStats, p.blockOffset, prop...)
	return hashJoin
}

// PhysicalIndexJoin represents the plan of index look up join.
type PhysicalIndexJoin struct {
	basePhysicalJoin

	outerSchema *expression.Schema
	innerTask   task

	// Ranges stores the IndexRanges when the inner plan is index scan.
	Ranges []*ranger.Range
	// KeyOff2IdxOff maps the offsets in join key to the offsets in the index.
	KeyOff2IdxOff []int
	// IdxColLens stores the length of each index column.
	IdxColLens []int
	// CompareFilters stores the filters for last column if those filters need to be evaluated during execution.
	// e.g. select * from t, t1 where t.a = t1.a and t.b > t1.b and t.b < t1.b+10
	//      If there's index(t.a, t.b). All the filters can be used to construct index range but t.b > t1.b and t.b < t1.b=10
	//      need to be evaluated after we fetch the data of t1.
	// This struct stores them and evaluate them to ranges.
	CompareFilters *ColWithCmpFuncManager
}

// PhysicalIndexMergeJoin represents the plan of index look up merge join.
type PhysicalIndexMergeJoin struct {
	PhysicalIndexJoin

	// KeyOff2KeyOffOrderByIdx maps the offsets in join keys to the offsets in join keys order by index.
	KeyOff2KeyOffOrderByIdx []int
	// CompareFuncs store the compare functions for outer join keys and inner join key.
	CompareFuncs []expression.CompareFunc
	// OuterCompareFuncs store the compare functions for outer join keys and outer join
	// keys, it's for outer rows sort's convenience.
	OuterCompareFuncs []expression.CompareFunc
	// NeedOuterSort means whether outer rows should be sorted to build range.
	NeedOuterSort bool
	// Desc means whether inner child keep desc order.
	Desc bool
}

// PhysicalIndexHashJoin represents the plan of index look up hash join.
type PhysicalIndexHashJoin struct {
	PhysicalIndexJoin
	// KeepOuterOrder indicates whether keeping the output result order as the
	// outer side.
	KeepOuterOrder bool
}

// PhysicalMergeJoin represents merge join implementation of LogicalJoin.
type PhysicalMergeJoin struct {
	basePhysicalJoin

	CompareFuncs []expression.CompareFunc
	// Desc means whether inner child keep desc order.
	Desc bool
}

/*
type CuraPlan struct {
	basePhysicalPlan
	originalPlan       PhysicalPlan
	idToChildPlans map[int]PhysicalPlan
}
*/

// PhysicalBroadCastJoin only works for TiFlash Engine, which broadcast the small table to every replica of probe side of tables.
type PhysicalBroadCastJoin struct {
	basePhysicalJoin
	globalChildIndex int
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalMergeJoin) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalMergeJoin)
	base, err := p.basePhysicalJoin.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalJoin = *base
	for _, cf := range p.CompareFuncs {
		cloned.CompareFuncs = append(cloned.CompareFuncs, cf)
	}
	cloned.Desc = p.Desc
	return cloned, nil
}

// PhysicalLock is the physical operator of lock, which is used for `select ... for update` clause.
type PhysicalLock struct {
	basePhysicalPlan

	Lock *ast.SelectLockInfo

	TblID2Handle     map[int64][]HandleCols
	PartitionedTable []table.PartitionedTable
}

// PhysicalLimit is the physical operator of Limit.
type PhysicalLimit struct {
	basePhysicalPlan

	Offset uint64
	Count  uint64
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalLimit) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalLimit)
	*cloned = *p
	base, err := p.basePhysicalPlan.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalPlan = *base
	return cloned, nil
}

func (p *PhysicalLimit) ToCuraJson(jsonPlan []byte) ([]byte, error) {
	jsonPlan = append(jsonPlan, []byte("{\"rel_op\": \"Limit\", \"n\": ")...)
	jsonPlan = append(jsonPlan, []byte(strconv.FormatUint(p.Count, 10))...)
	jsonPlan = append(jsonPlan, []byte("}")...)
	return jsonPlan, nil
}

// PhysicalUnionAll is the physical operator of UnionAll.
type PhysicalUnionAll struct {
	physicalSchemaProducer
}

type basePhysicalAgg struct {
	physicalSchemaProducer

	AggFuncs     []*aggregation.AggFuncDesc
	GroupByItems []expression.Expression
}

func (p *basePhysicalAgg) cloneWithSelf(newSelf PhysicalPlan) (*basePhysicalAgg, error) {
	cloned := new(basePhysicalAgg)
	base, err := p.physicalSchemaProducer.cloneWithSelf(newSelf)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	for _, aggDesc := range p.AggFuncs {
		cloned.AggFuncs = append(cloned.AggFuncs, aggDesc.Clone())
	}
	cloned.GroupByItems = cloneExprs(p.GroupByItems)
	return cloned, nil
}

func (p *basePhysicalAgg) numDistinctFunc() (num int) {
	for _, fun := range p.AggFuncs {
		if fun.HasDistinct {
			num++
		}
	}
	return
}

func (p *basePhysicalAgg) getAggFuncCostFactor() (factor float64) {
	factor = 0.0
	for _, agg := range p.AggFuncs {
		if fac, ok := aggFuncFactor[agg.Name]; ok {
			factor += fac
		} else {
			factor += aggFuncFactor["default"]
		}
	}
	if factor == 0 {
		factor = 1.0
	}
	return
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (p *basePhysicalAgg) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.GroupByItems)+len(p.AggFuncs))
	for _, expr := range p.GroupByItems {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	for _, fun := range p.AggFuncs {
		for _, arg := range fun.Args {
			corCols = append(corCols, expression.ExtractCorColumns(arg)...)
		}
	}
	return corCols
}

// PhysicalHashAgg is hash operator of aggregate.
type PhysicalHashAgg struct {
	basePhysicalAgg
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalHashAgg) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalHashAgg)
	base, err := p.basePhysicalAgg.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalAgg = *base
	return cloned, nil
}

func (p *PhysicalHashAgg) ToCuraJson(jsonPlan []byte) ([]byte, error) {
	var err error = nil
	hasConstantColumnInAggFunc := false
	//hasNonColumnExprInAggFunc := false
	for _, agg := range p.AggFuncs {
		for _, expr := range agg.Args {
			if _, isConst := expr.(*expression.Constant); isConst {
				if strings.ToLower(agg.Name) != "count" {
					return nil, errors.New("const args for non-count agg func")
				}
				hasConstantColumnInAggFunc = true
				break
			}
		}
	}
	constColumnIndex := -1
	if hasConstantColumnInAggFunc {
		for idx, col := range p.children[0].Schema().Columns {
			if col.RetType.Flag&mysql.NotNullFlag == mysql.NotNullFlag {
				constColumnIndex = idx
				break
			}
		}
	}
	if constColumnIndex == -1 && hasConstantColumnInAggFunc {
		// add extra project
		jsonPlan = append(jsonPlan, []byte("{\"rel_op\": \"Project\", \"exprs\": [")...)
		for i := range p.children[0].Schema().Columns {
			if i != 0 {
				jsonPlan = append(jsonPlan, ',')
			}
			jsonPlan = append(jsonPlan, []byte("{\"col_ref\": ")...)
			jsonPlan = append(jsonPlan, []byte(strconv.Itoa(i))...)
			jsonPlan = append(jsonPlan, '}')
		}
		if len(p.children[0].Schema().Columns) > 0 {
			jsonPlan = append(jsonPlan, ',')
		}
		jsonPlan = append(jsonPlan, []byte("{\"type\": \"UINT64\", \"literal\": ")...)
		jsonPlan = append(jsonPlan, []byte(strconv.Itoa(42))...)
		jsonPlan = append(jsonPlan, '}')
		jsonPlan = append(jsonPlan, []byte("]}, ")...)
		constColumnIndex = len(p.children[0].Schema().Columns)
	}
	jsonPlan = append(jsonPlan, []byte("{\"rel_op\": \"Aggregate\", \"groups\": ")...)
	if len(p.GroupByItems) == 0 {
		jsonPlan = append(jsonPlan, []byte("[]")...)
	} else if len(p.GroupByItems) == 1 {
		jsonPlan = append(jsonPlan, '[')
		jsonPlan, err = ExprToCuraJson(p.GroupByItems[0], jsonPlan)
		jsonPlan = append(jsonPlan, ']')
	} else {
		jsonPlan, err = ExprsToCuraJson(p.GroupByItems, jsonPlan)
	}
	if err != nil {
		return jsonPlan, err
	}
	jsonPlan = append(jsonPlan, []byte(", \"aggs\":[")...)
	for idx, agg := range p.AggFuncs {
		if idx != 0 {
			jsonPlan = append(jsonPlan, ',')
		}
		curaFunName := ""
		jsonPlan = append(jsonPlan, []byte("{\"agg\":")...)
		switch strings.ToLower(agg.Name) {
		case "count":
			jsonPlan = append(jsonPlan, []byte("\"COUNT_VALID\", \"operands\":")...)
			if len(agg.Args) == 1 {
				jsonPlan = append(jsonPlan, '[')
				if _, isConst := agg.Args[0].(*expression.Constant); isConst {
					jsonPlan = append(jsonPlan, []byte("{\"col_ref\": ")...)
					jsonPlan = append(jsonPlan, []byte(strconv.Itoa(constColumnIndex))...)
					jsonPlan = append(jsonPlan, '}')
				} else {
					jsonPlan, err = ExprToCuraJson(agg.Args[0], jsonPlan)
				}
				jsonPlan = append(jsonPlan, ']')
			} else {
				jsonPlan, err = ExprsToCuraJson(agg.Args, jsonPlan)
			}
			if err != nil {
				return jsonPlan, err
			}
			jsonPlan = append(jsonPlan, []byte(",\"type\":")...)
			jsonPlan, err = TypeToCuraJson(agg.RetTp, jsonPlan)
		case "firstrow":
			if len(agg.Args) > 1 {
				return jsonPlan, errors.New("first row only support 1 arg")
			}
			jsonPlan = append(jsonPlan, []byte("\"NTH_ELEMENT\", \"operands\":")...)
			jsonPlan = append(jsonPlan, '[')
			jsonPlan, err = ExprToCuraJson(agg.Args[0], jsonPlan)
			if err != nil {
				return jsonPlan, err
			}
			jsonPlan = append(jsonPlan, ']')
			jsonPlan = append(jsonPlan, []byte(",\"n\": 0")...)
			jsonPlan = append(jsonPlan, []byte(",\"type\":")...)
			jsonPlan, err = TypeToCuraJson(agg.RetTp, jsonPlan)
		case "avg":
			// avg is very special, if current agg is final agg, then need to rewrite is to sum and count
			// otherwise, push avg to cura is ok
			if len(agg.Args) == 2 {
				jsonPlan = append(jsonPlan, []byte("\"COUNT_VALID\", \"operands\":")...)
				jsonPlan = append(jsonPlan, '[')
				jsonPlan, err = ExprToCuraJson(agg.Args[0], jsonPlan)
				if err != nil {
					return jsonPlan, err
				}
				jsonPlan = append(jsonPlan, ']')
				jsonPlan = append(jsonPlan, []byte(",\"type\":")...)
				jsonPlan, err = TypeToCuraJson(agg.Args[0].GetType(), jsonPlan)
				jsonPlan = append(jsonPlan, []byte("},")...)
				jsonPlan = append(jsonPlan, []byte("{\"agg\":")...)
				jsonPlan = append(jsonPlan, []byte("\"SUM\", \"operands\":")...)
				jsonPlan = append(jsonPlan, '[')
				jsonPlan, err = ExprToCuraJson(agg.Args[1], jsonPlan)
				if err != nil {
					return jsonPlan, err
				}
				jsonPlan = append(jsonPlan, ']')
				jsonPlan = append(jsonPlan, []byte(",\"type\":")...)
				jsonPlan, err = TypeToCuraJson(agg.Args[0].GetType(), jsonPlan)
			} else if len(agg.Args) == 1 {
				jsonPlan = append(jsonPlan, []byte("\"MEAN\", \"operands\":")...)
				jsonPlan = append(jsonPlan, '[')
				jsonPlan, err = ExprToCuraJson(agg.Args[0], jsonPlan)
				if err != nil {
					return jsonPlan, err
				}
				jsonPlan = append(jsonPlan, ']')
				jsonPlan = append(jsonPlan, []byte(",\"type\":")...)
				jsonPlan, err = TypeToCuraJson(agg.RetTp, jsonPlan)
			} else {
				return jsonPlan, errors.New("first row only support 1 or 2 arg")
			}
		case "sum":
			curaFunName = "SUM"
			fallthrough
		case "min":
			if len(curaFunName) == 0 {
				curaFunName = "MIN"
			}
			fallthrough
		case "max":
			if len(curaFunName) == 0 {
				curaFunName = "MAX"
			}
			jsonPlan = append(jsonPlan, []byte("\""+curaFunName+"\", \"operands\":")...)
			if len(agg.Args) == 1 {
				jsonPlan = append(jsonPlan, '[')
				jsonPlan, err = ExprToCuraJson(agg.Args[0], jsonPlan)
				jsonPlan = append(jsonPlan, ']')
			} else {
				jsonPlan, err = ExprsToCuraJson(agg.Args, jsonPlan)
			}
			if err != nil {
				return jsonPlan, err
			}
			jsonPlan = append(jsonPlan, []byte(",\"type\":")...)
			jsonPlan, err = TypeToCuraJson(agg.RetTp, jsonPlan)
		default:
			return jsonPlan, errors.New("Cura not supported agg")
		}
		jsonPlan = append(jsonPlan, '}')
	}
	jsonPlan = append(jsonPlan, []byte("]}, ")...)
	// add extra project since the output schema for aggregation in cura and tidb is different
	jsonPlan = append(jsonPlan, []byte("{\"rel_op\": \"Project\", \"exprs\": [")...)
	aggFuncNum := len(p.AggFuncs)
	gbyColNum := len(p.GroupByItems)
	curaOutputColumn := gbyColNum
	for i := 0; i < aggFuncNum; i++ {
		if i != 0 {
			jsonPlan = append(jsonPlan, ',')
		}
		if strings.ToLower(p.AggFuncs[i].Name) == "avg" && len(p.AggFuncs[i].Args) == 2 {
			//
			jsonPlan = append(jsonPlan, []byte("{\"binary_op\": \"DIV\", \"operands\": [")...)
			jsonPlan = append(jsonPlan, []byte("{\"col_ref\": ")...)
			jsonPlan = append(jsonPlan, []byte(strconv.Itoa(curaOutputColumn))...)
			jsonPlan = append(jsonPlan, []byte("},")...)
			curaOutputColumn++
			jsonPlan = append(jsonPlan, []byte("{\"col_ref\": ")...)
			jsonPlan = append(jsonPlan, []byte(strconv.Itoa(curaOutputColumn))...)
			jsonPlan = append(jsonPlan, []byte("}], \"type\": ")...)
			jsonPlan, err = TypeToCuraJson(p.AggFuncs[i].RetTp, jsonPlan)
			if err != nil {
				return jsonPlan, err
			}
			jsonPlan = append(jsonPlan, '}')
			curaOutputColumn++
		} else {
			jsonPlan = append(jsonPlan, []byte("{\"col_ref\": ")...)
			jsonPlan = append(jsonPlan, []byte(strconv.Itoa(curaOutputColumn))...)
			jsonPlan = append(jsonPlan, '}')
			curaOutputColumn++
		}
	}
	jsonPlan = append(jsonPlan, []byte("]}")...)
	return jsonPlan, err
}

// NewPhysicalHashAgg creates a new PhysicalHashAgg from a LogicalAggregation.
func NewPhysicalHashAgg(la *LogicalAggregation, newStats *property.StatsInfo, prop *property.PhysicalProperty) *PhysicalHashAgg {
	agg := basePhysicalAgg{
		GroupByItems: la.GroupByItems,
		AggFuncs:     la.AggFuncs,
	}.initForHash(la.ctx, newStats, la.blockOffset, prop)
	return agg
}

// PhysicalStreamAgg is stream operator of aggregate.
type PhysicalStreamAgg struct {
	basePhysicalAgg
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalStreamAgg) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalStreamAgg)
	base, err := p.basePhysicalAgg.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalAgg = *base
	return cloned, nil
}

// PhysicalSort is the physical operator of sort, which implements a memory sort.
type PhysicalSort struct {
	basePhysicalPlan

	ByItems []*util.ByItems
}

// Clone implements PhysicalPlan interface.
func (ls *PhysicalSort) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalSort)
	base, err := ls.basePhysicalPlan.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalPlan = *base
	for _, it := range ls.ByItems {
		cloned.ByItems = append(cloned.ByItems, it.Clone())
	}
	return cloned, nil
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (ls *PhysicalSort) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(ls.ByItems))
	for _, item := range ls.ByItems {
		corCols = append(corCols, expression.ExtractCorColumns(item.Expr)...)
	}
	return corCols
}

func (ls *PhysicalSort) ToCuraJson(jsonPlan []byte) ([]byte, error) {
	jsonPlan = append(jsonPlan, []byte("{\"rel_op\": \"Sort\", \"sort_infos\": [")...)
	var err error = nil
	for idx, o := range ls.ByItems {
		if idx != 0 {
			jsonPlan = append(jsonPlan, ',')
		}
		jsonPlan = append(jsonPlan, []byte("{\"expr\": ")...)
		jsonPlan, err = ExprToCuraJson(o.Expr, jsonPlan)
		if err != nil {
			return nil, err
		}
		jsonPlan = append(jsonPlan, []byte(", \"order\": ")...)
		if o.Desc {
			jsonPlan = append(jsonPlan, []byte("\"DESC\", \"null_order\": \"NULL_FIRST\"}")...)
		} else {
			jsonPlan = append(jsonPlan, []byte("\"ASC\", \"null_order\": \"NULL_FIRST\"}")...)
		}
	}
	jsonPlan = append(jsonPlan, []byte("]}")...)
	return jsonPlan, nil
}

// NominalSort asks sort properties for its child. It is a fake operator that will not
// appear in final physical operator tree. It will be eliminated or converted to Projection.
type NominalSort struct {
	basePhysicalPlan

	// These two fields are used to switch ScalarFunctions to Constants. For these
	// NominalSorts, we need to converted to Projections check if the ScalarFunctions
	// are out of bounds. (issue #11653)
	ByItems    []*util.ByItems
	OnlyColumn bool
}

// PhysicalUnionScan represents a union scan operator.
type PhysicalUnionScan struct {
	basePhysicalPlan

	Conditions []expression.Expression

	HandleCols HandleCols
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (p *PhysicalUnionScan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0)
	for _, cond := range p.Conditions {
		corCols = append(corCols, expression.ExtractCorColumns(cond)...)
	}
	return corCols
}

// IsPartition returns true and partition ID if it works on a partition.
func (p *PhysicalIndexScan) IsPartition() (bool, int64) {
	return p.isPartition, p.physicalTableID
}

// IsPointGetByUniqueKey checks whether is a point get by unique key.
func (p *PhysicalIndexScan) IsPointGetByUniqueKey(sc *stmtctx.StatementContext) bool {
	return len(p.Ranges) == 1 &&
		p.Index.Unique &&
		len(p.Ranges[0].LowVal) == len(p.Index.Columns) &&
		p.Ranges[0].IsPoint(sc)
}

// PhysicalSelection represents a filter.
type PhysicalSelection struct {
	basePhysicalPlan

	Conditions []expression.Expression
}

// Clone implements PhysicalPlan interface.
func (p *PhysicalSelection) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalSelection)
	base, err := p.basePhysicalPlan.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalPlan = *base
	cloned.Conditions = cloneExprs(p.Conditions)
	return cloned, nil
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (p *PhysicalSelection) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.Conditions))
	for _, cond := range p.Conditions {
		corCols = append(corCols, expression.ExtractCorColumns(cond)...)
	}
	return corCols
}

func selectionConditionsToJsonPlan(conditions []expression.Expression, index int, jsonPlan []byte) ([]byte, bool, error) {
	var err error = nil
	nullable := false
	if index == len(conditions)-2 {
		// last one
		jsonPlan = append(jsonPlan, []byte("{\"binary_op\": \"LOGICAL_AND\", \"operands\": [")...)
		leftNullable := conditions[index].GetType().Flag&mysql.NotNullFlag != mysql.NotNullFlag
		rightNullable := conditions[index+1].GetType().Flag&mysql.NotNullFlag != mysql.NotNullFlag
		jsonPlan, err = ExprToCuraJsonInternal(conditions[index], jsonPlan, true)
		if err != nil {
			return nil, false, err
		}
		jsonPlan = append(jsonPlan, ',')
		jsonPlan, err = ExprToCuraJsonInternal(conditions[index+1], jsonPlan, true)
		if err != nil {
			return nil, false, err
		}
		jsonPlan = append(jsonPlan, []byte("], \"type\": {\"type\": \"BOOL8\", \"nullable\": ")...)
		if leftNullable || rightNullable {
			jsonPlan = append(jsonPlan, []byte("false")...)
			nullable = false
		} else {
			jsonPlan = append(jsonPlan, []byte("true")...)
			nullable = true
		}
	} else {
		jsonPlan = append(jsonPlan, []byte("{\"binary_op\": \"LOGICAL_AND\", \"operands\": [")...)
		leftNullable := conditions[index].GetType().Flag&mysql.NotNullFlag != mysql.NotNullFlag
		rightNullable := false
		jsonPlan, err = ExprToCuraJsonInternal(conditions[index], jsonPlan, true)
		if err != nil {
			return nil, false, err
		}
		jsonPlan = append(jsonPlan, ',')
		jsonPlan, rightNullable, err = selectionConditionsToJsonPlan(conditions, index+1, jsonPlan)
		if err != nil {
			return jsonPlan, false, err
		}
		jsonPlan = append(jsonPlan, []byte("], \"type\": {\"type\": \"BOOL8\", \"nullable\": ")...)
		if leftNullable || rightNullable {
			jsonPlan = append(jsonPlan, []byte("false")...)
			nullable = false
		} else {
			jsonPlan = append(jsonPlan, []byte("true")...)
			nullable = true
		}
		jsonPlan = append(jsonPlan, []byte("}}")...)
	}
	return jsonPlan, nullable, nil
}

// ToCuraJson implements PhysicalPlan interface.
func (p *PhysicalSelection) ToCuraJson(jsonPlan []byte) ([]byte, error) {
	jsonPlan = append(jsonPlan, []byte("{\"rel_op\": \"Filter\", \"condition\": ")...)
	var err error = nil
	if len(p.Conditions) == 1 {
		jsonPlan, err = ExprToCuraJsonInternal(p.Conditions[0], jsonPlan, true)
		if err != nil {
			return nil, err
		}
	} else {
		jsonPlan, _, err = selectionConditionsToJsonPlan(p.Conditions, 0, jsonPlan)
		if err != nil {
			return nil, err
		}
	}
	jsonPlan = append(jsonPlan, '}')
	return jsonPlan, nil
}

// PhysicalMaxOneRow is the physical operator of maxOneRow.
type PhysicalMaxOneRow struct {
	basePhysicalPlan
}

// PhysicalTableDual is the physical operator of dual.
type PhysicalTableDual struct {
	physicalSchemaProducer

	RowCount int

	// names is used for OutputNames() method. Dual may be inited when building point get plan.
	// So it needs to hold names for itself.
	names []*types.FieldName
}

// OutputNames returns the outputting names of each column.
func (p *PhysicalTableDual) OutputNames() types.NameSlice {
	return p.names
}

// SetOutputNames sets the outputting name by the given slice.
func (p *PhysicalTableDual) SetOutputNames(names types.NameSlice) {
	p.names = names
}

// PhysicalWindow is the physical operator of window function.
type PhysicalWindow struct {
	physicalSchemaProducer

	WindowFuncDescs []*aggregation.WindowFuncDesc
	PartitionBy     []property.Item
	OrderBy         []property.Item
	Frame           *WindowFrame
}

// ExtractCorrelatedCols implements PhysicalPlan interface.
func (p *PhysicalWindow) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.WindowFuncDescs))
	for _, windowFunc := range p.WindowFuncDescs {
		for _, arg := range windowFunc.Args {
			corCols = append(corCols, expression.ExtractCorColumns(arg)...)
		}
	}
	if p.Frame != nil {
		if p.Frame.Start != nil {
			for _, expr := range p.Frame.Start.CalcFuncs {
				corCols = append(corCols, expression.ExtractCorColumns(expr)...)
			}
		}
		if p.Frame.End != nil {
			for _, expr := range p.Frame.End.CalcFuncs {
				corCols = append(corCols, expression.ExtractCorColumns(expr)...)
			}
		}
	}
	return corCols
}

// PhysicalShuffle represents a shuffle plan.
// `Tail` and `DataSource` are the last plan within and the first plan following the "shuffle", respectively,
//  to build the child executors chain.
// Take `Window` operator for example:
//  Shuffle -> Window -> Sort -> DataSource, will be separated into:
//    ==> Shuffle: for main thread
//    ==> Window -> Sort(:Tail) -> shuffleWorker: for workers
//    ==> DataSource: for `fetchDataAndSplit` thread
type PhysicalShuffle struct {
	basePhysicalPlan

	Concurrency int
	Tail        PhysicalPlan
	DataSource  PhysicalPlan

	SplitterType PartitionSplitterType
	HashByItems  []expression.Expression
}

// PartitionSplitterType is the type of `Shuffle` executor splitter, which splits data source into partitions.
type PartitionSplitterType int

const (
	// PartitionHashSplitterType is the splitter splits by hash.
	PartitionHashSplitterType = iota
)

// PhysicalShuffleDataSourceStub represents a data source stub of `PhysicalShuffle`,
// and actually, is executed by `executor.shuffleWorker`.
type PhysicalShuffleDataSourceStub struct {
	physicalSchemaProducer

	// Worker points to `executor.shuffleWorker`.
	Worker unsafe.Pointer
}

// CollectPlanStatsVersion uses to collect the statistics version of the plan.
func CollectPlanStatsVersion(plan PhysicalPlan, statsInfos map[string]uint64) map[string]uint64 {
	for _, child := range plan.Children() {
		statsInfos = CollectPlanStatsVersion(child, statsInfos)
	}
	switch copPlan := plan.(type) {
	case *PhysicalTableReader:
		statsInfos = CollectPlanStatsVersion(copPlan.tablePlan, statsInfos)
	case *PhysicalIndexReader:
		statsInfos = CollectPlanStatsVersion(copPlan.indexPlan, statsInfos)
	case *PhysicalIndexLookUpReader:
		// For index loop up, only the indexPlan is necessary,
		// because they use the same stats and we do not set the stats info for tablePlan.
		statsInfos = CollectPlanStatsVersion(copPlan.indexPlan, statsInfos)
	case *PhysicalIndexScan:
		statsInfos[copPlan.Table.Name.O] = copPlan.stats.StatsVersion
	case *PhysicalTableScan:
		statsInfos[copPlan.Table.Name.O] = copPlan.stats.StatsVersion
	}

	return statsInfos
}

// PhysicalShow represents a show plan.
type PhysicalShow struct {
	physicalSchemaProducer

	ShowContents
}

// PhysicalShowDDLJobs is for showing DDL job list.
type PhysicalShowDDLJobs struct {
	physicalSchemaProducer

	JobNumber int64
}

// BuildMergeJoinPlan builds a PhysicalMergeJoin from the given fields. Currently, it is only used for test purpose.
func BuildMergeJoinPlan(ctx sessionctx.Context, joinType JoinType, leftKeys, rightKeys []*expression.Column) *PhysicalMergeJoin {
	baseJoin := basePhysicalJoin{
		JoinType:      joinType,
		DefaultValues: []types.Datum{types.NewDatum(1), types.NewDatum(1)},
		LeftJoinKeys:  leftKeys,
		RightJoinKeys: rightKeys,
	}
	return PhysicalMergeJoin{basePhysicalJoin: baseJoin}.Init(ctx, nil, 0)
}

// SafeClone clones this PhysicalPlan and handles its panic.
func SafeClone(v PhysicalPlan) (_ PhysicalPlan, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("%v", r)
		}
	}()
	return v.Clone()
}
