// Copyright 2026 PingCAP, Inc.
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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hint"
)

// InferMaterializedViewSchema derives SELECT output types using a private table
// override without executing the query. The caller applies the MV definition settings.
func InferMaterializedViewSchema(
	ctx context.Context,
	sctx sessionctx.Context,
	is infoschema.InfoSchema,
	sel *ast.SelectStmt,
	baseInfo *model.TableInfo,
) ([]*types.FieldType, error) {
	db, ok := infoschema.SchemaByTable(is, baseInfo)
	if !ok {
		return nil, errors.New("materialized view schema inference: base table schema not found")
	}
	candidate := baseInfo.Clone()
	candidate.DBID = db.ID
	allocators := autoid.NewAllocatorsFromTblInfo(is.GetAutoIDRequirement(), db.ID, candidate)
	tbl, err := table.TableFromMeta(allocators, candidate)
	if err != nil {
		return nil, err
	}
	// This wrapper belongs only to this inference call. Do not attach candidate
	// metadata to the transaction's own MDL tables or the global InfoSchema.
	planIS := &infoschema.SessionExtendedInfoSchema{InfoSchema: is}
	if err := planIS.UpdateTableInfo(db, tbl); err != nil {
		return nil, err
	}
	node := resolve.NewNodeW(sel)
	if err := Preprocess(ctx, sctx, node,
		WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: planIS}),
		useProvidedInfoSchemaAsIs,
	); err != nil {
		return nil, err
	}
	savedBlockNames := sctx.GetSessionVars().PlannerSelectBlockAsName.Load()
	defer sctx.GetSessionVars().PlannerSelectBlockAsName.Store(savedBlockNames)
	builder, _ := NewPlanBuilder(planBuilderOptUseProvidedInfoSchemaAsIs{}).Init(sctx.GetPlanCtx(), planIS, hint.NewQBHintHandler(nil))
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, err
	}
	logical, ok := p.(base.LogicalPlan)
	if !ok {
		return nil, errors.New("materialized view schema inference: expected a logical SELECT plan")
	}
	result := make([]*types.FieldType, logical.Schema().Len())
	for i, col := range logical.Schema().Columns {
		result[i] = col.RetType.Clone()
	}
	return result, nil
}
