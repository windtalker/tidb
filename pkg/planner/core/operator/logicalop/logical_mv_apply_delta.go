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

package logicalop

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalMVApplyDelta represents applying delta results into a MV table.
type LogicalMVApplyDelta struct {
	LogicalSchemaProducer `hash64-equals:"true"`

	TargetTable  table.Table
	TargetInfo   *model.TableInfo `hash64-equals:"true"`
	TargetDBName ast.CIStr

	// GroupByItems and AggFuncs describe how to merge delta results.
	GroupByItems []expression.Expression    `hash64-equals:"true" shallow-ref:"true"`
	AggFuncs     []*aggregation.AggFuncDesc `hash64-equals:"true" shallow-ref:"true"`

	// OpColumnName is the name of op column in mv log.
	OpColumnName string
}

// Init initializes LogicalMVApplyDelta.
func (p LogicalMVApplyDelta) Init(ctx base.PlanContext, offset int) *LogicalMVApplyDelta {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeMVApplyDelta, &p, offset)
	return &p
}

// ExplainInfo implements base.Plan interface.
func (p *LogicalMVApplyDelta) ExplainInfo() string {
	if p.TargetInfo == nil {
		return "mv:"
	}
	if p.TargetDBName.L != "" {
		return fmt.Sprintf("mv:%s.%s, gby:%d, agg:%d", p.TargetDBName.O, p.TargetInfo.Name.O, len(p.GroupByItems), len(p.AggFuncs))
	}
	return fmt.Sprintf("mv:%s, gby:%d, agg:%d", p.TargetInfo.Name.O, len(p.GroupByItems), len(p.AggFuncs))
}
