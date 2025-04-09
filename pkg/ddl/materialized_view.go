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

package ddl

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// some constants for materialized view and materialized view log
const (
	// MVLogNamePrefix is the prefix of materialized view log name, the mv log name is MVLogNamePrefix + base table name.
	MVLogNamePrefix         = "__mv_log_"
	MVLogDMLTypeInsert      = 1
	MVLogDMLTypeUpdate      = 2
	MVLogDMLTypeDelete      = 3
	MVLogIsNewValueOldValue = 0
	MVLogIsNewValueNewValue = 1
)

var (
	// MVLogDMLTypeCol is the column of materialized view log which records the DML type.
	MVLogDMLTypeColName = ast.NewCIStr("__mv_log_dml_type")
	MVLogDMLTypeColType *types.FieldType
	// MVLogIsNewValueCol is the column of materialized view log which records is current row the new value.
	MVLogIsNewValueColName = ast.NewCIStr("__mv_log_is_new_value")
	MVLogIsNewValueColType *types.FieldType
	// MVLogIdInTxnCol is the column name of materialized view log which records the row id inside txn.
	// For all the rows in the same txn, the value of this column is increasing from 0.
	// For rows in different txn, the value is not relevant.
	MVLogIdInTxnColName = ast.NewCIStr("__mv_log_id_in_txn")
	MVLogIdInTxnColType *types.FieldType
	// MVLogExtraHandleCol is the extra handle col for base table without primary key or primary key is not handle
	MVLogExtraHandleColName = ast.NewCIStr("__mv_log_tidb_rowid")
	ExtraHandleColType      *types.FieldType
)

func init() {
	// 1: insert, 2: update, 3: delete
	flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeTiny)
	MVLogDMLTypeColType = types.NewFieldType(mysql.TypeTiny)
	MVLogDMLTypeColType.AddFlag(mysql.NotNullFlag)
	MVLogDMLTypeColType.SetFlen(flen)
	MVLogDMLTypeColType.SetDecimal(decimal)
	MVLogDMLTypeColType.SetCharset(charset.CharsetBin)
	MVLogDMLTypeColType.SetCollate(charset.CollationBin)
	// 0: old value, 1: new value
	MVLogIsNewValueColType = types.NewFieldType(mysql.TypeTiny)
	MVLogIsNewValueColType.AddFlag(mysql.NotNullFlag)
	MVLogIsNewValueColType.SetFlen(flen)
	MVLogIsNewValueColType.SetDecimal(decimal)
	MVLogIsNewValueColType.SetCharset(charset.CharsetBin)
	MVLogIsNewValueColType.SetCollate(charset.CollationBin)

	flen, decimal = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
	MVLogIdInTxnColType = types.NewFieldType(mysql.TypeLonglong)
	MVLogIdInTxnColType.AddFlag(mysql.NotNullFlag)
	MVLogIdInTxnColType.AddFlag(mysql.UnsignedFlag)
	MVLogIdInTxnColType.SetFlen(flen)
	MVLogIdInTxnColType.SetDecimal(decimal)
	MVLogIdInTxnColType.SetCharset(charset.CharsetBin)
	MVLogIdInTxnColType.SetCollate(charset.CollationBin)

	ExtraHandleColType = types.NewFieldType(mysql.TypeLonglong)
	ExtraHandleColType.SetFlag(mysql.NotNullFlag)
	ExtraHandleColType.SetFlen(flen)
	ExtraHandleColType.SetDecimal(decimal)
	ExtraHandleColType.SetCharset(charset.CharsetBin)
	ExtraHandleColType.SetCollate(charset.CollationBin)
}

func buildColumnsForMVLog(baseTblInfo *model.TableInfo, refCols []ast.CIStr) ([]*table.Column, []*model.IndexColumn, error) {
	// the colums of materialized view log is (dml_type tinyint, is_new_value tinyint, id_in_txn longlong, s.RefCols...)
	offset := 0
	cols := make([]*table.Column, 0, len(refCols)+3)
	colsInBaseTable := make([]*model.IndexColumn, 0, len(refCols))
	cols = append(cols, &table.Column{
		ColumnInfo: &model.ColumnInfo{
			Name:      MVLogDMLTypeColName,
			FieldType: *MVLogDMLTypeColType,
			Offset:    offset,
			State:     model.StatePublic,
			Version:   model.CurrLatestColumnInfoVersion,
		},
	})
	offset++
	cols = append(cols, &table.Column{
		ColumnInfo: &model.ColumnInfo{
			Name:      MVLogIsNewValueColName,
			FieldType: *MVLogIsNewValueColType,
			Offset:    offset,
			State:     model.StatePublic,
			Version:   model.CurrLatestColumnInfoVersion,
		},
	})
	offset++
	cols = append(cols, &table.Column{
		ColumnInfo: &model.ColumnInfo{
			Name:      MVLogIdInTxnColName,
			FieldType: *MVLogIdInTxnColType,
			Offset:    offset,
			State:     model.StatePublic,
			Version:   model.CurrLatestColumnInfoVersion,
		},
	})
	offset++

	checkExtraHandle := !baseTblInfo.PKIsHandle && !baseTblInfo.IsCommonHandle
	for _, colName := range refCols {
		if checkExtraHandle && colName.L == model.ExtraHandleName.L {
			cols = append(cols, &table.Column{
				ColumnInfo: &model.ColumnInfo{
					Name:      MVLogExtraHandleColName,
					FieldType: *ExtraHandleColType,
					Offset:    offset,
					State:     model.StatePublic,
					Version:   model.CurrLatestColumnInfoVersion,
				},
			})
			colsInBaseTable = append(colsInBaseTable, &model.IndexColumn{
				Name: colName,
				// -1 means extra handle col
				Offset: -1,
			})
			offset++
			continue
		}
		col := findColumnByName(colName.L, baseTblInfo)
		if col == nil {
			return nil, nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(colName.O, baseTblInfo.Name.O)
		}
		if col.IsGenerated() || col.Hidden {
			return nil, nil, dbterror.ErrWrongUsage.GenWithStackByArgs(colName, "Materialized View Log on generated/hidden column")
		}
		// constuct FieldType for mv log
		mvColFieldType := col.FieldType
		// delete all the unused flags
		mvColFieldType.DelFlag(mysql.AutoIncrementFlag)
		mvColFieldType.DelFlag(mysql.OnUpdateNowFlag)
		mvColFieldType.DelFlag(mysql.PriKeyFlag)
		mvColFieldType.DelFlag(mysql.UniqueKeyFlag)
		mvColFieldType.DelFlag(mysql.UniqueFlag)
		cols = append(cols, &table.Column{
			ColumnInfo: &model.ColumnInfo{
				Name:      colName,
				FieldType: mvColFieldType,
				Offset:    offset,
				State:     model.StatePublic,
				Version:   model.CurrLatestColumnInfoVersion,
			},
		})
		colsInBaseTable = append(colsInBaseTable, &model.IndexColumn{
			Name: colName,
			// Offset is the offset of the column in the base table
			Offset: col.Offset,
		})
		offset++
	}
	return cols, colsInBaseTable, nil
}

// BuildTableInfoForMVLog builds model.TableInfo for CreateMVLog statement
func BuildTableInfoForMVLog(ctx *metabuild.Context, s *ast.CreateMVLogStmt, baseTblInfo *model.TableInfo, placementPolicyRef *model.PolicyRefInfo) (*model.TableInfo, error) {
	// the colation of materialized view log is the same as the base table
	tableCharset, tableCollate := baseTblInfo.Charset, baseTblInfo.Collate

	cols, refColInfos, err := buildColumnsForMVLog(baseTblInfo, s.RefCols)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var tbInfo *model.TableInfo
	MVLogName := ast.NewCIStr(MVLogNamePrefix + baseTblInfo.Name.O)
	tbInfo, err = BuildTableInfo(ctx, MVLogName, cols, nil, tableCharset, tableCollate)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if tbInfo.TempTableType == model.TempTableNone && tbInfo.PlacementPolicyRef == nil && placementPolicyRef != nil {
		// Set the defaults from Schema. Note: they are mutual exclusive!
		tbInfo.PlacementPolicyRef = placementPolicyRef
	}

	// setup the MVLogInfo
	tbInfo.MVLogInfo = &model.MVLogInfo{
		BaseTableID:        baseTblInfo.ID,
		ColumnsInBaseTable: refColInfos,
	}

	// todo if base table is partition table, the mv log should also be partition table
	return tbInfo, nil
}

func createMView(jobCtx *jobContext, job *model.Job, args *model.CreateTableArgs) (int64, error) {
	schemaID := job.SchemaID
	tbInfo := args.TableInfo
	tbInfo.State = model.StateNone
	// check mv not exists
	err := checkTableNotExists(jobCtx.infoCache, schemaID, tbInfo.Name.L)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return 0, errors.Trace(err)
	}
	// check base table exists
	for _, tbl := range tbInfo.MView.BaseTableNames {
		_, err := jobCtx.infoCache.GetLatest().TableByName(context.Background(), tbl[0], tbl[1])
		if err != nil {
			job.State = model.JobStateCancelled
			return 0, errors.Trace(err)
		}
	}

	metaMut := jobCtx.metaMut
	if tbInfo.State != model.StateNone {
		// invalid state, cancel this job
		job.State = model.JobStateCancelled
		return 0, errors.New("unexpected ddl state")
	}
	tbInfo.State = model.StatePublic
	tbInfo.UpdateTS = metaMut.StartTS
	err = checkTableInfoValid(tbInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}
	// update base table info

	return 0, errors.New("create materialized view is not supported yet")
}
func createMVLog(jobCtx *jobContext, job *model.Job, args *model.CreateTableArgs) (int64, error) {
	schemaID := job.SchemaID
	tbInfo := args.TableInfo
	tbInfo.State = model.StateNone
	err := checkTableNotExists(jobCtx.infoCache, schemaID, tbInfo.Name.L)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return 0, errors.Trace(err)
	}

	metaMut := jobCtx.metaMut

	if tbInfo.State != model.StateNone {
		// invalid state, cancel this job
		job.State = model.JobStateCancelled
		return 0, errors.New("unexpected ddl state")
	}

	tbInfo.State = model.StatePublic
	tbInfo.UpdateTS = metaMut.StartTS
	err = checkTableInfoValid(tbInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}
	// update base table info
	is := jobCtx.infoCache.GetLatest()
	baseTable, exist := is.TableByID(context.Background(), tbInfo.MVLogInfo.BaseTableID)
	if !exist {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(tbInfo.Name.O))
	}
	baseTableInfo := baseTable.Meta()
	if baseTableInfo.MVLogID != 0 {
		job.State = model.JobStateCancelled
		return 0, errors.New("base table already has a materialized view log")
	}
	baseTableInfo.MVLogID = tbInfo.ID
	// create mv log and update base table info
	err = jobCtx.metaMut.CreateTableOrView(schemaID, tbInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}
	jobCtx.metaMut.UpdateTable(schemaID, baseTableInfo)

	affectedTblInfo := make([]*model.TableInfo, 0, 2)
	affectedTblInfo = append(affectedTblInfo, tbInfo)
	affectedTblInfo = append(affectedTblInfo, baseTableInfo)
	affectedSchemaAndTables := make([]schemaIDAndTableInfo, 0, 2)
	affectedSchemaAndTables = append(affectedSchemaAndTables, schemaIDAndTableInfo{schemaID: schemaID, tblInfo: tbInfo})
	affectedSchemaAndTables = append(affectedSchemaAndTables, schemaIDAndTableInfo{schemaID: schemaID, tblInfo: baseTableInfo})
	ver, err := updateSchemaVersion(jobCtx, job, affectedSchemaAndTables...)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}
	job.FinishMultipleTableJob(model.JobStateDone, model.StatePublic, ver, affectedTblInfo)
	return ver, nil
}

func (w *worker) onCreateMVLog(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	failpoint.Inject("mockExceedErrorLimit", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ver, errors.New("mock do job error"))
		}
	})

	args, err := model.GetCreateTableArgs(job)
	if err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args
	return createMVLog(jobCtx, job, args)
}
