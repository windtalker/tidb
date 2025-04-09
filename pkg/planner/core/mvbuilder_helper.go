// Copyright 2015 PingCAP, Inc.
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

package core

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

type mvCheckerHelper struct {
	meetSelection   bool
	meetProjection  bool
	meetAggregation bool
	outputSchema    *expression.Schema
	baseTableNames  [][2]ast.CIStr
}

func isFunctionSupportedInMV(funcName string) bool {
	if funcName == "plus" || funcName == "gt" || funcName == "ge" || funcName == "le" || funcName == "lt" || funcName == "eq" || funcName == "ne" {
		return true
	}
	return false
}

func isAggFunctionSupportedInMV(funcName string) bool {
	// in tidb, the group by col is converted to firstrow(col), so we need to support firstrow
	if funcName == "sum" || funcName == "count" || funcName == "firstrow" {
		return true
	}
	return false
}

func checkExprSupportedInMV(expr expression.Expression) error {
	switch x := expr.(type) {
	case *expression.Column:
		return nil
	case *expression.Constant:
		return nil
	case *expression.ScalarFunction:
		if !isFunctionSupportedInMV(x.FuncName.L) {
			return errors.New("scalar function: " + x.FuncName.L + " not supported in mv")
		}
		for _, arg := range x.GetArgs() {
			if err := checkExprSupportedInMV(arg); err != nil {
				return err
			}
		}
		return nil
	default:
		return errors.New("Only column/constant/scalar function is supported in mv")
	}
}

func checkMVPlan(p base.LogicalPlan, checkerHelper *mvCheckerHelper) error {
	// currently only two pattern of plan is supported
	// tablescan => [filter] => projection
	// tablescan => [filter] => aggregation => projection
	switch x := p.(type) {
	case *logicalop.LogicalProjection:
		if checkerHelper.meetProjection {
			return errors.New("duplicate projection")
		}
		if checkerHelper.meetSelection || checkerHelper.meetAggregation {
			return errors.New("subquery not supported")
		}
		checkerHelper.meetProjection = true
		for _, expr := range x.Exprs {
			// currently, only column reference is supported
			if err := checkExprSupportedInMV(expr); err != nil {
				return err
			}
		}
		return checkMVPlan(x.Children()[0], checkerHelper)
	case *logicalop.LogicalSelection:
		if checkerHelper.meetSelection {
			return errors.New("duplicate selection")
		}
		checkerHelper.meetSelection = true
		for _, expr := range x.Conditions {
			if err := checkExprSupportedInMV(expr); err != nil {
				return err
			}
		}
		return checkMVPlan(x.Children()[0], checkerHelper)
	case *logicalop.LogicalAggregation:
		if checkerHelper.meetAggregation {
			return errors.New("duplicate aggregation")
		}
		if checkerHelper.meetSelection {
			return errors.New("having/subquery not supported")
		}
		checkerHelper.meetAggregation = true
		count_not_null_col_id := int64(-1)
		for _, agg := range x.AggFuncs {
			name := strings.ToLower(agg.Name)
			if !isAggFunctionSupportedInMV(name) {
				return errors.New("only sum/count/firstrow is supported in aggregation")
			}
			for index, aggArg := range agg.Args {
				switch x := aggArg.(type) {
				case *expression.Constant:
					if name == "count" && !x.Value.IsNull() {
						count_not_null_col_id = p.Schema().Columns[index].UniqueID
					}
				case *expression.Column:
					if name == "count" && x.RetType.GetFlag()&mysql.NotNullFlag == mysql.NotNullFlag {
						count_not_null_col_id = p.Schema().Columns[index].UniqueID
					}
				default:
					return errors.New("only column/constant is supported in aggregation function")
				}
			}
		}
		if count_not_null_col_id == -1 {
			return errors.New("count(not_null) is required in aggregation")
		}
		mustHaveColumnIds := make(map[int64]bool)
		mustHaveColumnIds[count_not_null_col_id] = true
		uniqueKeys := make([]*expression.Column, 0, len(x.GroupByItems))
		containsNullableGroupByColumn := false
		for _, expr := range x.GroupByItems {
			switch col := expr.(type) {
			case *expression.Column:
				mustHaveColumnIds[col.UniqueID] = true
				uniqueKeys = append(uniqueKeys, col)
				if col.RetType.GetFlag()&mysql.NotNullFlag != mysql.NotNullFlag {
					containsNullableGroupByColumn = true
				}
			default:
				return errors.New("only column is supported in group by")
			}
		}
		selectedColCount := 0
		for _, col := range checkerHelper.outputSchema.Columns {
			if val, ok := mustHaveColumnIds[col.UniqueID]; ok {
				if !val {
					return errors.New("all group by column and count(not_null) must not be duplicated")
				} else {
					mustHaveColumnIds[col.UniqueID] = false
					selectedColCount++
				}
			}
		}
		if selectedColCount != len(x.GroupByItems)+1 {
			return errors.New("all group by column and count(not_null) must be selected")
		}
		if len(uniqueKeys) != 0 {
			if containsNullableGroupByColumn {
				checkerHelper.outputSchema.NullableUK = append(checkerHelper.outputSchema.NullableUK, uniqueKeys)
			} else {
				checkerHelper.outputSchema.PKOrUK = append(checkerHelper.outputSchema.PKOrUK, uniqueKeys)
			}
		}
		// for aggregation, all group by column and count(*) must be selected, and all group by column should be marked as pk or unique key(if contains null)
		return checkMVPlan(x.Children()[0], checkerHelper)
	case *logicalop.DataSource:
		checkerHelper.baseTableNames = append(checkerHelper.baseTableNames, [2]ast.CIStr{x.DBName, x.TableInfo.Name})
		if !checkerHelper.meetAggregation {
			// for data source, if there is no aggregation, then the pk must be selected as output
			var pkCols []*expression.Column
			pkUniqueID := make(map[int64]bool)
			for _, indexInfo := range x.Table.Meta().Indices {
				if indexInfo.Primary {
					pkCols = make([]*expression.Column, 0, len(indexInfo.Columns))
					outputCols := x.Schema().Columns
					for _, col := range indexInfo.Columns {
						colID := x.TableInfo.Columns[col.Offset].ID
						for _, outputCol := range outputCols {
							if outputCol.ID == colID {
								pkCols = append(pkCols, outputCol)
								pkUniqueID[outputCol.UniqueID] = true
								break
							}
						}
					}
					if len(pkCols) != len(indexInfo.Columns) {
						return errors.New("pk must be selected as output")
					}
					break
				}
			}
			if pkCols == nil {
				// no pk col, use _tidb_rowid
				for _, col := range x.Schema().Columns {
					if col.ID == model.ExtraHandleID {
						pkCols = []*expression.Column{col}
						pkUniqueID[col.UniqueID] = true
						break
					}
				}
				if pkCols == nil {
					return errors.New("pk must be selected as output")
				}
			}
			selectedColCount := 0
			for _, col := range checkerHelper.outputSchema.Columns {
				if val, ok := pkUniqueID[col.UniqueID]; ok {
					if !val {
						return errors.New("all pk must be selected as output")
					} else {
						pkUniqueID[col.UniqueID] = false
						selectedColCount++
					}
				}
			}
			if selectedColCount != len(pkCols) || selectedColCount == 0 {
				return errors.New("all pk must be selected as output")
			}
			// setup the pk for mv
			checkerHelper.outputSchema.PKOrUK = append(checkerHelper.outputSchema.PKOrUK, pkCols)
		}
		return nil
	default:
		return errors.New("unsupported plan in mv")
	}
}

func checkMVPlanAndGenerateMVSchema(p base.LogicalPlan) (*expression.Schema, [][2]ast.CIStr, error) {
	// copy the schema since it will be modified during the check
	outputSchema := &(*p.Schema())
	outputSchema.NullableUK = outputSchema.NullableUK[:0]
	outputSchema.PKOrUK = outputSchema.PKOrUK[:0]
	checkHelper := &mvCheckerHelper{
		outputSchema: outputSchema,
	}
	err := checkMVPlan(p, checkHelper)
	if err != nil {
		return nil, nil, err
	}
	return checkHelper.outputSchema, checkHelper.baseTableNames, nil
}
