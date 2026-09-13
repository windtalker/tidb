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

package executor

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestRestoreRefreshMaterializedViewStmtForSlowLog(t *testing.T) {
	stmt := &ast.RefreshMaterializedViewImplementStmt{
		RefreshStmt: &ast.RefreshMaterializedViewStmt{
			ViewName:     &ast.TableName{Name: ast.NewCIStr("mv")},
			Type:         ast.RefreshMaterializedViewTypeComplete,
			CompleteType: ast.RefreshMaterializedViewCompleteTypeInPlace,
		},
	}

	require.Equal(t,
		"IMPLEMENT FOR REFRESH MATERIALIZED VIEW `mv` COMPLETE IN PLACE USING TIMESTAMP",
		restoreStmtTextForSlowLogWhenEmptySQL(stmt),
	)
	require.Empty(t, restoreStmtTextForSlowLogWhenEmptySQL(&ast.RefreshMaterializedViewImplementStmt{}))
}

func TestBuildMVRefreshOutOfPlaceShadowTableInfoSetsSource(t *testing.T) {
	shadow, err := buildMVRefreshOutOfPlaceShadowTableInfo(
		ast.NewCIStr("test"),
		"__mv_shadow_42",
		&model.TableInfo{
			ID:               42,
			Name:             ast.NewCIStr("mv"),
			MaterializedView: &model.MaterializedViewInfo{},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, shadow.MaterializedViewShadow)
	require.Equal(t, int64(42), shadow.MaterializedViewShadow.SourceMViewID)
}
