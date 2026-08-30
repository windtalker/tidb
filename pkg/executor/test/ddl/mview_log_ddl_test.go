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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	metamodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
)

func mustExecInternal(t *testing.T, tk *testkit.TestKit, sql string) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMVMaintenance)
	vars := tk.Session().GetSessionVars()
	origMaint := vars.InMaterializedViewMaintenance
	origRestr := vars.InRestrictedSQL
	vars.InMaterializedViewMaintenance = true
	vars.InRestrictedSQL = true
	defer func() {
		vars.InMaterializedViewMaintenance = origMaint
		vars.InRestrictedSQL = origRestr
	}()
	rs, err := tk.Session().ExecuteInternal(ctx, sql)
	require.NoError(t, err)
	require.Nil(t, rs)
}

func TestCreateMaterializedViewLogBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	expectedSQLMode := tk.Session().GetSessionVars().SQLMode

	tk.MustExec("create materialized view log on t (a) purge start with cast('2026-01-02 03:04:05' as datetime) next cast('2026-01-02 03:14:05' as datetime) alert rows 1234")

	// Physical table created.
	tk.MustQuery("select count(*) from information_schema.tables where table_schema='test' and table_name='$mlog$t'").Check(testkit.Rows("1"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t"))
	require.NoError(t, err)

	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.Equal(t, mlogTable.Meta().ID, baseTable.Meta().MaterializedViewBase.MLogID)
	// Lock row for PURGE MATERIALIZED VIEW LOG should be inserted on CREATE MATERIALIZED VIEW LOG success.
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = %d", mlogTable.Meta().ID)).
		Check(testkit.Rows("1"))

	mlogInfo := mlogTable.Meta().MaterializedViewLog
	require.NotNil(t, mlogInfo)
	require.Equal(t, baseTable.Meta().ID, mlogInfo.BaseTableID)
	require.Equal(t, []ast.CIStr{ast.NewCIStr("a")}, mlogInfo.Columns)
	require.Equal(t, "DEFERRED", mlogInfo.PurgeMethod)
	require.Equal(t, "CAST('2026-01-02 03:04:05' AS DATETIME)", mlogInfo.PurgeStartWith)
	require.Equal(t, "CAST('2026-01-02 03:14:05' AS DATETIME)", mlogInfo.PurgeNext)
	require.NotNil(t, mlogInfo.LogAccumulationAlertRows)
	require.Equal(t, uint64(1234), *mlogInfo.LogAccumulationAlertRows)
	require.Equal(t, expectedSQLMode, mlogInfo.DefinitionSQLMode)

	// Meta columns should exist on the log table.
	dmlTypeColName := ast.NewCIStr("_MLOG$_DML_TYPE")
	oldNewColName := ast.NewCIStr("_MLOG$_OLD_NEW")

	var hasDMLType, hasOldNew bool
	for _, c := range mlogTable.Meta().Columns {
		if c.Name.L == dmlTypeColName.L {
			hasDMLType = true
		}
		if c.Name.L == oldNewColName.L {
			hasOldNew = true
			require.Equal(t, mysql.TypeTiny, c.FieldType.GetType())
		}
	}
	require.True(t, hasDMLType)
	require.True(t, hasOldNew)

	// Duplicated MV LOG should fail (same derived table name).
	tk.MustGetErrMsg("create materialized view log on t (a)", "[schema:1050]Table 'test.$mlog$t' already exists")
}

func TestCreateMaterializedViewLogPreservesTextColumnTypes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_text_types (id bigint not null primary key, c_tiny tinytext, c_text text, c_medium mediumtext, c_long longtext)")
	tk.MustExec("create materialized view log on t_text_types (id, c_tiny, c_text, c_medium, c_long)")

	showCreate := tk.MustQuery("show create table `$mlog$t_text_types`").Rows()[0][1].(string)
	require.Contains(t, showCreate, "  `c_tiny` tinytext DEFAULT NULL")
	require.Contains(t, showCreate, "  `c_text` text DEFAULT NULL")
	require.Contains(t, showCreate, "  `c_medium` mediumtext DEFAULT NULL")
	require.Contains(t, showCreate, "  `c_long` longtext DEFAULT NULL")
}

// TestAlterMaterializedViewLogAddColumnBasic verifies that ADD COLUMN updates
// mlog metadata, gives existing physical rows the mlog defaults, and rolls
// back a cancelled multi-column DDL atomically.

func TestAlterMaterializedViewLogAddColumnPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_add_mlog_priv (a int, b int)")
	tk.MustExec("create materialized view log on t_add_mlog_priv (a)")
	tk.MustExec("create user 'u_add_mlog_no_select'@'%'")
	tk.MustExec("create user 'u_add_mlog_ok'@'%'")
	defer tk.MustExec("drop user 'u_add_mlog_no_select'@'%'")
	defer tk.MustExec("drop user 'u_add_mlog_ok'@'%'")

	tk.MustExec("grant alter on test.`$mlog$t_add_mlog_priv` to 'u_add_mlog_no_select'@'%'")
	tkNoSelect := testkit.NewTestKit(t, store)
	require.NoError(t, tkNoSelect.Session().Auth(&auth.UserIdentity{Username: "u_add_mlog_no_select", Hostname: "%"}, nil, nil, nil))
	err := tkNoSelect.ExecToErr("alter materialized view log on test.t_add_mlog_priv add column (b)")
	require.ErrorContains(t, err, "SELECT command denied")

	tk.MustExec("grant alter on test.`$mlog$t_add_mlog_priv` to 'u_add_mlog_ok'@'%'")
	tk.MustExec("grant select on test.t_add_mlog_priv to 'u_add_mlog_ok'@'%'")
	tkOK := testkit.NewTestKit(t, store)
	require.NoError(t, tkOK.Session().Auth(&auth.UserIdentity{Username: "u_add_mlog_ok", Hostname: "%"}, nil, nil, nil))
	tkOK.MustExec("alter materialized view log on test.t_add_mlog_priv add column (b)")
}

// TestAlterMaterializedViewLogAddColumnSupportsNewMaterializedView verifies
// that a fast-refresh materialized view can be created and refreshed after its
// referenced base column is added to the materialized view log.

func TestCreateMaterializedViewLogPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_create_mlog_priv (a int)")
	tk.MustExec("create user 'u_create_mlog_no_create'@'%'")
	tk.MustExec("create user 'u_create_mlog_no_select'@'%'")
	tk.MustExec("create user 'u_create_mlog_table_create'@'%'")
	tk.MustExec("create user 'u_create_mlog_ok'@'%'")
	defer tk.MustExec("drop user 'u_create_mlog_no_create'@'%'")
	defer tk.MustExec("drop user 'u_create_mlog_no_select'@'%'")
	defer tk.MustExec("drop user 'u_create_mlog_table_create'@'%'")
	defer tk.MustExec("drop user 'u_create_mlog_ok'@'%'")

	tk.MustExec("grant select on test.t_create_mlog_priv to 'u_create_mlog_no_create'@'%'")
	tkNoCreate := testkit.NewTestKit(t, store)
	require.NoError(t, tkNoCreate.Session().Auth(&auth.UserIdentity{Username: "u_create_mlog_no_create", Hostname: "%"}, nil, nil, nil))
	err := tkNoCreate.ExecToErr("create materialized view log on test.t_create_mlog_priv (a)")
	require.ErrorContains(t, err, "CREATE MATERIALIZED VIEW LOG command denied")
	require.ErrorContains(t, err, "t_create_mlog_priv")
	require.NotContains(t, err.Error(), "$mlog$")

	tk.MustExec("grant create view on test.* to 'u_create_mlog_no_select'@'%'")
	tkNoSelect := testkit.NewTestKit(t, store)
	require.NoError(t, tkNoSelect.Session().Auth(&auth.UserIdentity{Username: "u_create_mlog_no_select", Hostname: "%"}, nil, nil, nil))
	err = tkNoSelect.ExecToErr("create materialized view log on test.t_create_mlog_priv (a)")
	require.ErrorContains(t, err, "SELECT command denied")

	tk.MustExec("grant create view on test.* to 'u_create_mlog_ok'@'%'")
	tk.MustExec("grant select on test.t_create_mlog_priv to 'u_create_mlog_ok'@'%'")
	tkOK := testkit.NewTestKit(t, store)
	require.NoError(t, tkOK.Session().Auth(&auth.UserIdentity{Username: "u_create_mlog_ok", Hostname: "%"}, nil, nil, nil))
	tkOK.MustExec("create materialized view log on test.t_create_mlog_priv (a)")

	tk.MustExec("grant create view on test.t_create_mlog_priv to 'u_create_mlog_table_create'@'%'")
	tk.MustExec("grant select on test.t_create_mlog_priv to 'u_create_mlog_table_create'@'%'")
	tkTableCreate := testkit.NewTestKit(t, store)
	require.NoError(t, tkTableCreate.Session().Auth(&auth.UserIdentity{Username: "u_create_mlog_table_create", Hostname: "%"}, nil, nil, nil))
	err = tkTableCreate.ExecToErr("create materialized view log on test.t_create_mlog_priv (a)")
	require.ErrorContains(t, err, "CREATE MATERIALIZED VIEW LOG command denied")
	require.ErrorContains(t, err, "t_create_mlog_priv")
	require.NotContains(t, err.Error(), "$mlog$")
}

func TestCreateMaterializedViewLogPreSplitOptions(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	originSplit := atomic.LoadUint32(&ddl.EnableSplitTableRegion)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	defer atomic.StoreUint32(&ddl.EnableSplitTableRegion, originSplit)
	tk.MustExec("set @@session.tidb_scatter_region='table'")
	tk.MustExec("create table t_mlog_presplit (a int, b int)")

	tk.MustExec("create materialized view log on t_mlog_presplit (a) shard_row_id_bits = 2 pre_split_regions = 2 purge next date_add(now(), interval 1 hour)")

	showCreate := tk.MustQuery("show create table `$mlog$t_mlog_presplit`").Rows()[0][1].(string)
	require.Contains(t, showCreate, "SHARD_ROW_ID_BITS=2")
	require.Contains(t, showCreate, "PRE_SPLIT_REGIONS=2")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_mlog_presplit"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), mlogTable.Meta().ShardRowIDBits)
	require.Equal(t, uint64(2), mlogTable.Meta().PreSplitRegions)

	regions := tk.MustQuery("show table `$mlog$t_mlog_presplit` regions").Rows()
	regionNames := make([]string, 0, len(regions))
	for _, row := range regions {
		regionNames = append(regionNames, fmt.Sprint(row[1]))
	}
	require.Contains(t, regionNames, fmt.Sprintf("t_%d_r_2305843009213693952", mlogTable.Meta().ID))
	require.Contains(t, regionNames, fmt.Sprintf("t_%d_r_4611686018427387904", mlogTable.Meta().ID))
	require.Contains(t, regionNames, fmt.Sprintf("t_%d_r_6917529027641081856", mlogTable.Meta().ID))
}

func TestCreateMaterializedViewLogPurgeExprTypeValidation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")

	err := tk.ExecToErr("create materialized view log on t (a) purge immediate")
	require.Truef(t, dbterror.ErrGeneralUnsupportedDDL.Equal(err), "err %v", err)
	require.ErrorContains(t, err, "PURGE IMMEDIATE is not supported for CREATE MATERIALIZED VIEW LOG")

	err = tk.ExecToErr("create materialized view log on t (a) purge start with 1 next date_add(now(), interval 1 hour)")
	require.ErrorContains(t, err, "PURGE START WITH expression must return DATETIME/TIMESTAMP")

	err = tk.ExecToErr("create materialized view log on t (a) purge next 600")
	require.ErrorContains(t, err, "PURGE NEXT expression must return DATETIME/TIMESTAMP")

	tk.MustExec("create materialized view log on t (a) purge start with now() next date_add(now(), interval 1 hour)")
}

func TestCreateMaterializedViewLogAccumulationAlert(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_alert_default (a int)")
	tk.MustExec("create table t_alert_zero (a int)")
	tk.MustExec("create table t_alert_custom (a int)")
	tk.MustExec("create table t_alert_negative (a int)")

	err := tk.ExecToErr("create materialized view log on t_alert_negative (a) alert rows -1")
	require.ErrorContains(t, err, "invalid ALERT ROWS value: -1 (must be non-negative)")

	tk.MustExec("create materialized view log on t_alert_default (a)")
	tk.MustExec("create materialized view log on t_alert_zero (a) alert rows 0")
	tk.MustExec("create materialized view log on t_alert_custom (a) alert rows 2048")

	getMLogInfo := func(baseTable string) *metamodel.MaterializedViewLogInfo {
		is := dom.InfoSchema()
		mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$"+baseTable))
		require.NoError(t, err)
		require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
		return mlogTable.Meta().MaterializedViewLog
	}

	defaultInfo := getMLogInfo("t_alert_default")
	require.Nil(t, defaultInfo.LogAccumulationAlertRows)
	defaultRows, defaultEnabled := defaultInfo.EffectiveLogAccumulationAlertRows()
	require.False(t, defaultEnabled)
	require.Equal(t, uint64(0), defaultRows)

	zeroInfo := getMLogInfo("t_alert_zero")
	require.NotNil(t, zeroInfo.LogAccumulationAlertRows)
	require.Equal(t, uint64(0), *zeroInfo.LogAccumulationAlertRows)
	zeroRows, zeroEnabled := zeroInfo.EffectiveLogAccumulationAlertRows()
	require.False(t, zeroEnabled)
	require.Equal(t, uint64(0), zeroRows)

	customInfo := getMLogInfo("t_alert_custom")
	require.NotNil(t, customInfo.LogAccumulationAlertRows)
	require.Equal(t, uint64(2048), *customInfo.LogAccumulationAlertRows)
	customRows, customEnabled := customInfo.EffectiveLogAccumulationAlertRows()
	require.True(t, customEnabled)
	require.Equal(t, uint64(2048), customRows)
}

func TestCreateMaterializedViewLogPurgeInfoNextUnixSecondsDerivation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	getMLogID := func(baseTable string) int64 {
		is := dom.InfoSchema()
		mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$"+baseTable))
		require.NoError(t, err)
		return mlogTable.Meta().ID
	}

	// START WITH and NEXT both present, START WITH is not near-now: NEXT_PURGE_UNIX_SECONDS should use START WITH.
	tk.MustExec("create table t_purge_start_only (a int)")
	tk.MustExec("create materialized view log on t_purge_start_only (a) purge start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute)")
	mlogStartOnlyID := getMLogID("t_purge_start_only")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS is not null, NEXT_PURGE_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 30 minute), NEXT_PURGE_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 2 hour) from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogStartOnlyID,
	)).Check(testkit.Rows("1 1 1"))

	// NEXT only: NEXT_PURGE_UNIX_SECONDS should use evaluated NEXT.
	tk.MustExec("create table t_purge_next_only (a int)")
	tk.MustExec("create materialized view log on t_purge_next_only (a) purge next date_add(now(), interval 20 minute)")
	mlogNextOnlyID := getMLogID("t_purge_next_only")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS is not null, NEXT_PURGE_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 10 minute), NEXT_PURGE_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 1 hour) from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogNextOnlyID,
	)).Check(testkit.Rows("1 1 1"))

	// Neither START WITH nor NEXT: NEXT_PURGE_UNIX_SECONDS should stay unchanged (create path: NULL).
	tk.MustExec("create table t_purge_no_schedule (a int)")
	tk.MustExec("create materialized view log on t_purge_no_schedule (a)")
	mlogNoScheduleID := getMLogID("t_purge_no_schedule")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS is null from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogNoScheduleID,
	)).Check(testkit.Rows("1"))

	// START WITH near-now and NEXT present: NEXT_PURGE_UNIX_SECONDS should use NEXT.
	tk.MustExec("create table t_purge_near_now (a int)")
	tk.MustExec("create materialized view log on t_purge_near_now (a) purge start with now() next date_add(now(), interval 40 minute)")
	mlogNearNowID := getMLogID("t_purge_near_now")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS is not null, NEXT_PURGE_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 20 minute), NEXT_PURGE_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 2 hour) from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogNearNowID,
	)).Check(testkit.Rows("1 1 1"))
}

func TestCreateMaterializedViewLogPurgeInfoNextUnixSecondsUsesScheduleTimeZone(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+08:00'")

	getMLogID := func(baseTable string) int64 {
		is := dom.InfoSchema()
		mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$"+baseTable))
		require.NoError(t, err)
		return mlogTable.Meta().ID
	}

	tk.MustExec("create table t_purge_schedule_next (a int)")
	tk.MustExec("create materialized view log on t_purge_schedule_next (a) purge next cast('2030-01-02 10:00:00' as datetime)")
	mlogNextID := getMLogID("t_purge_schedule_next")

	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS = 1893549600, "+
			"NEXT_PURGE_UNIX_SECONDS = 1893578400 "+
			"from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogNextID,
	)).Check(testkit.Rows("1 0"))

	// START WITH and NEXT use the session timezone captured when the schedule is defined.
	tk.MustExec("create table t_purge_schedule_start (a int)")
	tk.MustExec("create materialized view log on t_purge_schedule_start (a) purge start with cast('2030-01-02 10:00:00' as datetime) next cast('2030-01-03 10:00:00' as datetime)")
	mlogStartID := getMLogID("t_purge_schedule_start")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS = 1893549600, "+
			"NEXT_PURGE_UNIX_SECONDS = 1893636000 "+
			"from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogStartID,
	)).Check(testkit.Rows("1 0"))
}

func TestAlterMaterializedViewLogPurgeScheduleTimeZone(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a) purge next cast('2030-01-01 10:00:00' as datetime)")

	getMLogInfo := func() *metamodel.MaterializedViewLogInfo {
		is := dom.InfoSchema()
		mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t"))
		require.NoError(t, err)
		require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
		return mlogTable.Meta().MaterializedViewLog
	}

	getMLogID := func() int64 {
		is := dom.InfoSchema()
		mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t"))
		require.NoError(t, err)
		return mlogTable.Meta().ID
	}

	initialTimeZone := getMLogInfo().PurgeScheduleTimeZone
	require.Equal(t, 0, initialTimeZone.Offset)

	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec("alter materialized view log on t purge")
	info := getMLogInfo()
	require.Equal(t, initialTimeZone.Name, info.PurgeScheduleTimeZone.Name)
	require.Equal(t, initialTimeZone.Offset, info.PurgeScheduleTimeZone.Offset)
	require.Empty(t, info.PurgeNext)

	tk.MustExec("alter materialized view log on t purge next cast('2030-01-02 10:00:00' as datetime)")
	info = getMLogInfo()
	require.Equal(t, 8*60*60, info.PurgeScheduleTimeZone.Offset)
	tk.MustQuery("select NEXT_PURGE_UNIX_SECONDS = 1893549600 from mysql.tidb_mlog_purge_info where MLOG_ID = " + strconv.FormatInt(getMLogID(), 10)).
		Check(testkit.Rows("1"))
}

func TestAlterMaterializedViewLogPurgeExprTypeValidation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create materialized view log on t (a) purge next date_add(now(), interval 1 hour)")

	err := tk.ExecToErr("alter materialized view log on t purge start with 1 next date_add(now(), interval 1 hour)")
	require.ErrorContains(t, err, "PURGE START WITH expression must return DATETIME/TIMESTAMP")

	err = tk.ExecToErr("alter materialized view log on t purge next 300")
	require.ErrorContains(t, err, "PURGE NEXT expression must return DATETIME/TIMESTAMP")

	tk.MustExec("alter materialized view log on t purge start with now() next date_add(now(), interval 1 hour)")
}

func TestAlterMaterializedViewLogPurgeUpdatesMetaAndNextUnixSeconds(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 2 hour)")

	getMLogMeta := func() (int64, string, string, string) {
		is := dom.InfoSchema()
		mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t"))
		require.NoError(t, err)
		require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
		return mlogTable.Meta().ID,
			mlogTable.Meta().MaterializedViewLog.PurgeMethod,
			mlogTable.Meta().MaterializedViewLog.PurgeStartWith,
			mlogTable.Meta().MaterializedViewLog.PurgeNext
	}

	mlogID, purgeMethod, purgeStartWith, purgeNext := getMLogMeta()
	require.Equal(t, "DEFERRED", purgeMethod)
	require.Equal(t, "", purgeStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 2 HOUR)", purgeNext)

	tk.MustExec("alter materialized view log on t purge start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute)")
	_, purgeMethod, purgeStartWith, purgeNext = getMLogMeta()
	require.Equal(t, "DEFERRED", purgeMethod)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 40 MINUTE)", purgeStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 20 MINUTE)", purgeNext)
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS is not null, NEXT_PURGE_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 30 minute), NEXT_PURGE_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 2 hour) from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogID,
	)).Check(testkit.Rows("1 1 1"))

	tk.MustExec("alter materialized view log on t purge next date_add(now(), interval 25 minute)")
	_, purgeMethod, purgeStartWith, purgeNext = getMLogMeta()
	require.Equal(t, "DEFERRED", purgeMethod)
	require.Equal(t, "", purgeStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", purgeNext)
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS is not null, NEXT_PURGE_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 15 minute), NEXT_PURGE_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 1 hour) from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogID,
	)).Check(testkit.Rows("1 1 1"))

	tk.MustExec("alter materialized view log on t purge")
	_, purgeMethod, purgeStartWith, purgeNext = getMLogMeta()
	require.Equal(t, "DEFERRED", purgeMethod)
	require.Equal(t, "", purgeStartWith)
	require.Equal(t, "", purgeNext)
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS is null from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogID,
	)).Check(testkit.Rows("1"))

	err := tk.ExecToErr("alter materialized view log on t purge immediate")
	require.ErrorContains(t, err, "PURGE IMMEDIATE is not supported for ALTER MATERIALIZED VIEW LOG")
	// meta is unchanged
	_, purgeMethod, purgeStartWith, purgeNext = getMLogMeta()
	require.Equal(t, "DEFERRED", purgeMethod)
	require.Equal(t, "", purgeStartWith)
	require.Equal(t, "", purgeNext)

	tk.MustExec("drop materialized view log on t")
}

func TestAlterMaterializedViewLogPurgeUpdatesNextUnixSecondsWithMLogAlterPrivilege(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 2 hour)")
	tk.MustExec("create user 'mv_alter_purge_u'@'%' identified by ''")
	tk.MustExec("create user 'mv_alter_purge_select_u'@'%' identified by ''")
	defer tk.MustExec("drop user 'mv_alter_purge_u'@'%'")
	defer tk.MustExec("drop user 'mv_alter_purge_select_u'@'%'")
	tk.MustExec("grant alter on test.`$mlog$t` to 'mv_alter_purge_u'@'%'")
	tk.MustExec("grant select on test.t to 'mv_alter_purge_select_u'@'%'")

	getMLogMeta := func() (int64, string, string, string) {
		is := dom.InfoSchema()
		mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t"))
		require.NoError(t, err)
		require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
		return mlogTable.Meta().ID,
			mlogTable.Meta().MaterializedViewLog.PurgeMethod,
			mlogTable.Meta().MaterializedViewLog.PurgeStartWith,
			mlogTable.Meta().MaterializedViewLog.PurgeNext
	}

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "mv_alter_purge_u", Hostname: "%"}, nil, nil, nil))
	tkUser.MustExec("alter materialized view log on test.t purge next date_add(now(), interval 25 minute)")

	tkSelectUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkSelectUser.Session().Auth(&auth.UserIdentity{Username: "mv_alter_purge_select_u", Hostname: "%"}, nil, nil, nil))
	err := tkSelectUser.ExecToErr("alter materialized view log on test.t purge next date_add(now(), interval 30 minute)")
	require.ErrorContains(t, err, "ALTER command denied")

	mlogID, purgeMethod, purgeStartWith, purgeNext := getMLogMeta()
	require.Equal(t, "DEFERRED", purgeMethod)
	require.Equal(t, "", purgeStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", purgeNext)
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS is not null, NEXT_PURGE_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 15 minute), NEXT_PURGE_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 1 hour) from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		mlogID,
	)).Check(testkit.Rows("1 1 1"))
}

func TestCreateMaterializedViewLogMetaColumnNameConflict(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_conflict (`_MLOG$_DML_TYPE` int, a int)")
	tk.MustGetErrCode("create materialized view log on t_conflict (`_MLOG$_DML_TYPE`, a)", errno.ErrDupFieldName)
}

func TestCreateMaterializedViewLogRejectNonBaseObject(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create view v as select a from t")
	tk.MustExec("create sequence s")
	tk.MustExec("create global temporary table gt (a int) on commit delete rows")
	tk.MustExec("create materialized view log on t (a)")
	tk.MustExec("create table t_mv_base (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_mv_base (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, cnt) refresh fast as select a, count(1) from t_mv_base group by a")

	err := tk.ExecToErr("create materialized view log on v (a)")
	require.Error(t, err)
	require.Equal(t, dbterror.ErrWrongObject.GenWithStackByArgs("test", "v", "BASE TABLE").Error(), err.Error())

	err = tk.ExecToErr("create materialized view log on s (a)")
	require.Error(t, err)
	require.Equal(t, dbterror.ErrWrongObject.GenWithStackByArgs("test", "s", "BASE TABLE").Error(), err.Error())

	err = tk.ExecToErr("create materialized view log on gt (a)")
	require.Equal(t, dbterror.ErrWrongObject.GenWithStackByArgs("test", "gt", "BASE TABLE").Error(), err.Error())

	err = tk.ExecToErr("create materialized view log on mysql.user (User)")
	require.Equal(t, dbterror.ErrWrongObject.GenWithStackByArgs("mysql", "user", "BASE TABLE").Error(), err.Error())

	err = tk.ExecToErr("create materialized view log on information_schema.tables (TABLE_SCHEMA)")
	require.Equal(t, dbterror.ErrWrongObject.GenWithStackByArgs("information_schema", "tables", "BASE TABLE").Error(), err.Error())

	tk.MustExec("drop materialized view log on t")
	tk.MustExec("drop table t")
	tk.MustExec("create temporary table t (a int)")
	err = tk.ExecToErr("create materialized view log on t (a)")
	require.Equal(t, dbterror.ErrWrongObject.GenWithStackByArgs("test", "t", "BASE TABLE").Error(), err.Error())
	err = tk.ExecToErr("create materialized view log on mv (a, cnt)")
	require.Error(t, err)
	require.Equal(t, dbterror.ErrWrongObject.GenWithStackByArgs("test", "mv", "BASE TABLE").Error(), err.Error())

	tk.MustExec("drop table t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a)")
	err = tk.ExecToErr("create materialized view log on `$mlog$t` (a)")
	require.Error(t, err)
	require.Equal(t, dbterror.ErrWrongObject.GenWithStackByArgs("test", "$mlog$t", "BASE TABLE").Error(), err.Error())
}

func TestCreateMaterializedViewLogNameLengthByRune(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	maxBaseNameLen := mysql.MaxTableNameLength - len([]rune(metamodel.MaterializedViewLogTableNamePrefix))
	maxName := strings.Repeat("表", maxBaseNameLen)
	maxMLogName := metamodel.MaterializedViewLogTableName(ast.NewCIStr(maxName)).O
	require.Equal(t, mysql.MaxTableNameLength, len([]rune(maxMLogName)))
	tk.MustExec(fmt.Sprintf("create table `%s` (a int)", maxName))
	tk.MustExec(fmt.Sprintf("create materialized view log on `%s` (a)", maxName))
	tk.MustQuery(fmt.Sprintf("select count(*) from information_schema.tables where table_schema='test' and table_name='%s'", maxMLogName)).Check(testkit.Rows("1"))

	tooLongName := strings.Repeat("表", maxBaseNameLen+1)
	require.Equal(t, maxMLogName, metamodel.MaterializedViewLogTableName(ast.NewCIStr(tooLongName)).O)
	tk.MustExec(fmt.Sprintf("create table `%s` (a int)", tooLongName))
	tk.MustGetErrCode(fmt.Sprintf("create materialized view log on `%s` (a)", tooLongName), errno.ErrTableExists)

	tk.MustExec(fmt.Sprintf("drop materialized view log on `%s`", maxName))
	tk.MustExec(fmt.Sprintf("create materialized view log on `%s` (a)", tooLongName))
	tk.MustQuery(fmt.Sprintf("select count(*) from information_schema.tables where table_schema='test' and table_name='%s'", maxMLogName)).Check(testkit.Rows("1"))
}

func TestTruncateMaterializedViewRelatedTablesRejected(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_truncate_mv (a int not null, b int)")
	tk.MustExec("create materialized view log on t_truncate_mv (a, b)")

	err := tk.ExecToErr("truncate table t_truncate_mv")
	require.ErrorContains(t, err, "TRUNCATE TABLE on base table with materialized view log")

	err = tk.ExecToErr("truncate table `$mlog$t_truncate_mv`")
	require.ErrorContains(t, err, "TRUNCATE TABLE on materialized view log table")

	tk.MustExec("create materialized view mv_truncate_mv (a, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, count(1) from t_truncate_mv group by a")

	err = tk.ExecToErr("truncate table mv_truncate_mv")
	require.ErrorContains(t, err, "TRUNCATE TABLE on materialized view table")

	err = tk.ExecToErr("truncate table `$mlog$t_truncate_mv`")
	require.ErrorContains(t, err, "TRUNCATE TABLE on materialized view log table")

	err = tk.ExecToErr("truncate table t_truncate_mv")
	require.ErrorContains(t, err, "TRUNCATE TABLE on base table with materialized view log")
}

func TestMaterializedViewRelatedTablesDDLRejected(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_ddl_mv (a int not null, b int)")
	tk.MustExec("create materialized view log on t_ddl_mv (a, b)")

	err := tk.ExecToErr("drop table t_ddl_mv")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view log")
	err = tk.ExecToErr("rename table t_ddl_mv to t_ddl_mv2")
	require.ErrorContains(t, err, "RENAME TABLE on base table with materialized view log")
	err = tk.ExecToErr("drop table `$mlog$t_ddl_mv`")
	require.ErrorContains(t, err, "DROP TABLE on materialized view log table")
	err = tk.ExecToErr("rename table `$mlog$t_ddl_mv` to `$mlog$t_ddl_mv2`")
	require.ErrorContains(t, err, "RENAME TABLE on materialized view log table")

	tk.MustExec("create materialized view mv_ddl_mv (a, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, count(1) from t_ddl_mv group by a")

	tk.MustExec("alter table t_ddl_mv add column c int")
	err = tk.ExecToErr("alter table t_ddl_mv modify column a bigint")
	require.ErrorContains(t, err, "does not support changing charset/collation/nullability of group keys")
	err = tk.ExecToErr("drop table t_ddl_mv")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view dependencies")
	err = tk.ExecToErr("rename table t_ddl_mv to t_ddl_mv2")
	require.ErrorContains(t, err, "RENAME TABLE on base table with materialized view dependencies")

	// Restricted MODIFY/CHANGE COLUMN should be allowed at ALTER TABLE entry, but still rejected on reorg/renaming.
	tk.MustExec("alter table t_ddl_mv modify column b bigint")
	err = tk.ExecToErr("alter table t_ddl_mv modify column b smallint")
	require.ErrorContains(t, err, "only supports no-reorg compatible type changes")
	err = tk.ExecToErr("alter table t_ddl_mv change column b b2 bigint")
	require.ErrorContains(t, err, "does not support renaming")

	err = tk.ExecToErr("alter table mv_ddl_mv add column x int")
	require.ErrorContains(t, err, "ALTER TABLE on materialized view table")
	err = tk.ExecToErr("drop table mv_ddl_mv")
	require.ErrorContains(t, err, "DROP TABLE on materialized view table")
	err = tk.ExecToErr("rename table mv_ddl_mv to mv_ddl_mv2")
	require.ErrorContains(t, err, "RENAME TABLE on materialized view table")

	err = tk.ExecToErr("alter table `$mlog$t_ddl_mv` set tiflash replica 1")
	if err != nil {
		require.NotContains(t, err.Error(), "ALTER TABLE on materialized view log table")
	}
	err = tk.ExecToErr("alter table `$mlog$t_ddl_mv` add index idx_mlog_b(b)")
	require.ErrorContains(t, err, "ALTER TABLE on materialized view log table")
	err = tk.ExecToErr("alter table `$mlog$t_ddl_mv` add column c int")
	require.ErrorContains(t, err, "ALTER TABLE on materialized view log table")
	err = tk.ExecToErr("create index idx_mlog_b_create on `$mlog$t_ddl_mv`(b)")
	require.ErrorContains(t, err, "CREATE INDEX on materialized view log table")
	err = tk.ExecToErr("create vector index idx_mlog_vec_create on `$mlog$t_ddl_mv` ((vec_cosine_distance(b))) using hnsw")
	require.ErrorContains(t, err, "CREATE INDEX on materialized view log table")
	err = tk.ExecToErr("drop index idx_mlog_b_create on `$mlog$t_ddl_mv`")
	require.ErrorContains(t, err, "DROP INDEX on materialized view log table")
}

func TestCreateVectorIndexOnMaterializedViewLogTableRejected(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomainWithSchemaLease(t, 100*time.Millisecond, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mlog_vec (id int, v vector(3))")
	tk.MustExec("create materialized view log on t_mlog_vec (v) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("alter table `$mlog$t_mlog_vec` set tiflash replica 1")

	err := tk.ExecToErr("create vector index idx_mlog_vec on `$mlog$t_mlog_vec`((vec_cosine_distance(v))) USING HNSW")
	require.Truef(t, dbterror.ErrGeneralUnsupportedDDL.Equal(err), "err %v", err)
	require.ErrorContains(t, err, "CREATE INDEX on materialized view log table")
}

func TestTruncateOrdinaryTableStillWorks(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_normal_truncate (a int)")
	tk.MustExec("insert into t_normal_truncate values (1), (2)")
	tk.MustExec("truncate table t_normal_truncate")
	tk.MustQuery("select count(*) from t_normal_truncate").Check(testkit.Rows("0"))
}

func TestDropMaterializedViewLogBeforeBaseTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_drop_seq (a int)")
	tk.MustExec("create materialized view log on t_drop_seq (a)")
	tk.MustExec("drop materialized view log on t_drop_seq")
	tk.MustExec("drop table if exists t_drop_seq")
}

func TestDropMaterializedViewLogRemovesPurgeState(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_drop_mlog_purge_state (a int)")
	tk.MustExec("create materialized view log on t_drop_mlog_purge_state (a)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_drop_mlog_purge_state"))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("1"))

	tk.MustExec("drop materialized view log on t_drop_mlog_purge_state")
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID = %d", mlogID)).
		Check(testkit.Rows("0"))
}

func TestAlterMaterializedViewLogAddColumnBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_add_mlog_col (id int not null, n int not null, s varchar(10) not null, d date not null, note text not null, untouched int)")
	tk.MustExec("create materialized view log on t_add_mlog_col (id)")
	mustExecInternal(t, tk, "insert into `$mlog$t_add_mlog_col` (id, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW`) values (1, 'I', 1)")

	tk.MustExec("alter materialized view log on t_add_mlog_col add column (n, s, d, note)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_add_mlog_col"))
	require.NoError(t, err)
	mlogInfo := mlogTable.Meta().MaterializedViewLog
	require.NotNil(t, mlogInfo)
	require.Equal(t, []ast.CIStr{
		ast.NewCIStr("id"),
		ast.NewCIStr("n"),
		ast.NewCIStr("s"),
		ast.NewCIStr("d"),
		ast.NewCIStr("note"),
	}, mlogInfo.Columns)

	colNames := make([]string, 0, len(mlogTable.Meta().Columns))
	colByName := make(map[string]*metamodel.ColumnInfo, len(mlogTable.Meta().Columns))
	for _, col := range mlogTable.Meta().Columns {
		colNames = append(colNames, col.Name.O)
		colByName[col.Name.L] = col
	}
	require.Equal(t, []string{"id", "n", "s", "d", "note", "_MLOG$_DML_TYPE", "_MLOG$_OLD_NEW"}, colNames)
	for _, name := range []string{"n", "s", "d", "note"} {
		require.True(t, mysql.HasNotNullFlag(colByName[name].GetFlag()))
	}
	require.Equal(t, "0", fmt.Sprint(colByName["n"].GetOriginDefaultValue()))
	require.Equal(t, " ", fmt.Sprint(colByName["s"].GetOriginDefaultValue()))
	require.Equal(t, "0000-00-00", fmt.Sprint(colByName["d"].GetOriginDefaultValue()))
	require.Equal(t, " ", fmt.Sprint(colByName["note"].GetOriginDefaultValue()))
	require.Equal(t, mysql.TypeBlob, colByName["note"].GetType())

	tk.MustQuery("select n, hex(s), cast(d as char), hex(note), `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$t_add_mlog_col`").
		Check(testkit.Rows("0 20 0000-00-00 20 I 1"))

	showCreate := tk.MustQuery("show create materialized view log on t_add_mlog_col").Rows()[0][1].(string)
	require.Contains(t, showCreate, "CREATE MATERIALIZED VIEW LOG ON `t_add_mlog_col` (`id`, `n`, `s`, `d`, `note`)")

	tk.MustExec("create table t_add_mlog_atomic (id int, b int, c int)")
	tk.MustExec("create materialized view log on t_add_mlog_atomic (id)")
	cancelTK := testkit.NewTestKit(t, store)
	cancelTK.MustExec("use test")
	cancelTriggered := atomic.Bool{}
	cancelDone := make(chan error, 1)
	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/onJobUpdated", func(job *metamodel.Job) {
		if !cancelTriggered.CompareAndSwap(false, true) {
			return
		}
		if job.Type != metamodel.ActionMultiSchemaChange ||
			job.SchemaName != "test" ||
			job.TableName != "$mlog$t_add_mlog_atomic" ||
			job.MultiSchemaInfo == nil ||
			len(job.MultiSchemaInfo.SubJobs) != 2 ||
			job.MultiSchemaInfo.SubJobs[1].SchemaState != metamodel.StateWriteReorganization {
			cancelTriggered.Store(false)
			return
		}
		errs, err := ddl.CancelJobs(context.Background(), cancelTK.Session(), []int64{job.ID})
		if len(errs) > 0 && errs[0] != nil {
			cancelDone <- errs[0]
			return
		}
		cancelDone <- err
	}))
	err = tk.ExecToErr("alter materialized view log on t_add_mlog_atomic add column (b, c)")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/onJobUpdated"))
	require.ErrorContains(t, err, "Cancelled DDL job")
	select {
	case cancelErr := <-cancelDone:
		require.NoError(t, cancelErr)
	default:
		require.FailNow(t, "expected mlog multi-column add cancellation")
	}
	showCreate = tk.MustQuery("show create materialized view log on t_add_mlog_atomic").Rows()[0][1].(string)
	require.Contains(t, showCreate, "CREATE MATERIALIZED VIEW LOG ON `t_add_mlog_atomic` (`id`)")
	require.NotContains(t, showCreate, "`b`")
	require.NotContains(t, showCreate, "`c`")
}

// TestAlterMaterializedViewLogAddColumnDefaultSemantics verifies the default
// values used for existing mlog rows when ADD COLUMN tracks nullable columns,
// enum/set columns, and not-null string columns.

func TestAlterMaterializedViewLogAddColumnDefaultSemantics(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// This table covers the mlog backfill defaults that differ from normal AddColumn:
	// nullable columns should keep NULL for old mlog rows, enum/set should use the
	// regular TiDB default semantics, and not-null string columns should use a
	// single-space placeholder required by materialized view log history rows.
	tk.MustExec("create table t_add_mlog_defaults (" +
		"id int," +
		"nullable_varchar varchar(10)," +
		"nullable_text text," +
		"nn_enum enum('a','b') not null," +
		"nn_set set('x','y') not null," +
		"nullable_enum enum('a','b')," +
		"nullable_set set('x','y')," +
		"nn_varchar varchar(10) not null," +
		"nn_text text not null)")
	tk.MustExec("create materialized view log on t_add_mlog_defaults (id)")
	mustExecInternal(t, tk, "insert into `$mlog$t_add_mlog_defaults` (id, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW`) values (1, 'I', 1)")

	tk.MustExec("alter materialized view log on t_add_mlog_defaults add column (" +
		"nullable_varchar, nullable_text, nn_enum, nn_set, nullable_enum, nullable_set, nn_varchar, nn_text)")

	// The existing INSERT log row is historical data. It should not read current
	// base-table values for newly tracked columns; it should only expose the
	// metadata default chosen for old mlog rows.
	tk.MustQuery("select " +
		"nullable_varchar is null, nullable_text is null, " +
		"cast(nn_enum as char), cast(nn_set as char), " +
		"nullable_enum is null, nullable_set is null, " +
		"hex(nn_varchar), hex(nn_text), `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` " +
		"from `$mlog$t_add_mlog_defaults`").
		Check(testkit.Rows("1 1 a  1 1 20 20 I 1"))
}

// TestAlterMaterializedViewLogAddColumnRejectsInvalidColumns verifies that ADD
// COLUMN rejects duplicate tracked columns, duplicate names in one statement,
// missing base columns, and reserved mlog metadata columns.

func TestAlterMaterializedViewLogAddColumnRejectsInvalidColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_add_mlog_invalid (a int, b int, c int)")
	tk.MustExec("create materialized view log on t_add_mlog_invalid (a)")

	tk.MustGetErrCode("alter materialized view log on t_add_mlog_invalid add column (a)", errno.ErrDupFieldName)
	tk.MustGetErrCode("alter materialized view log on t_add_mlog_invalid add column (b, b)", errno.ErrDupFieldName)
	tk.MustGetErrCode("alter materialized view log on t_add_mlog_invalid add column (missing_col)", errno.ErrBadField)
	tk.MustGetErrCode("alter materialized view log on t_add_mlog_invalid add column (`_MLOG$_DML_TYPE`)", errno.ErrDupFieldName)
	tk.MustGetErrMsg(
		"alter materialized view log on t_add_mlog_invalid add column (b), add column (c)",
		"[ddl:8200]Unsupported ALTER MATERIALIZED VIEW LOG with multiple ADD COLUMN actions",
	)

	tk.MustExec("create table t_add_mlog_unsupported (id int, b blob, j json, g1 int, g2 int as (g1 + 1) stored)")
	tk.MustExec("create materialized view log on t_add_mlog_unsupported (id)")

	err := tk.ExecToErr("alter materialized view log on t_add_mlog_unsupported add column (b)")
	require.ErrorContains(t, err, "ALTER MATERIALIZED VIEW LOG does not support BLOB column b")

	err = tk.ExecToErr("alter materialized view log on t_add_mlog_unsupported add column (j)")
	require.ErrorContains(t, err, "ALTER MATERIALIZED VIEW LOG does not support JSON column j")

	tk.MustExec("alter materialized view log on t_add_mlog_unsupported add column (g2)")
}

func TestCreateMaterializedViewLogRejectUnsupportedColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_tinyblob (id bigint not null primary key, b tinyblob null)")
	tk.MustExec("create table t_blob (id bigint not null primary key, b blob null)")
	tk.MustExec("create table t_mediumblob (id bigint not null primary key, b mediumblob null)")
	tk.MustExec("create table t_longblob (id bigint not null primary key, b longblob null)")
	for _, tbl := range []string{"t_tinyblob", "t_blob", "t_mediumblob", "t_longblob"} {
		err := tk.ExecToErr(fmt.Sprintf("create materialized view log on %s (id, b)", tbl))
		require.ErrorContains(t, err, "CREATE MATERIALIZED VIEW LOG does not support BLOB column b")
	}

	tk.MustExec("create table t_text_ok (id bigint not null primary key, c1 tinytext null, c2 text null, c3 mediumtext null, c4 longtext null)")
	tk.MustExec("create materialized view log on t_text_ok (id, c1, c2, c3, c4)")

	tk.MustExec("create table t_json (id bigint not null primary key, j json null)")
	err := tk.ExecToErr("create materialized view log on t_json (id, j)")
	require.ErrorContains(t, err, "CREATE MATERIALIZED VIEW LOG does not support JSON column j")

	tk.MustExec("create table t_gen (id bigint not null primary key, g1 int not null, g_virtual int as (g1 + 1) virtual, g_stored int as (g1 + 2) stored)")
	tk.MustExec("create materialized view log on t_gen (id, g_virtual, g_stored)")
	tk.MustExec("create table t_untracked_unsupported (id bigint not null primary key, b blob null, j json null, g int as (id + 1) stored)")
	tk.MustExec("create materialized view log on t_untracked_unsupported (id)")
}

func TestAlterMaterializedViewLogAddColumnSupportsNewMaterializedView(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_add_mlog_mv (a int not null, b int not null, c int not null)")
	tk.MustExec("insert into t_add_mlog_mv values (1, 10, 100), (1, 20, 200), (2, 30, 300)")
	tk.MustExec("create materialized view log on t_add_mlog_mv (a, b)")

	err := tk.ExecToErr("create materialized view mv_add_mlog_col_before (a, s, cnt) refresh fast as select a, sum(c), count(1) from t_add_mlog_mv group by a")
	require.ErrorContains(t, err, "materialized view log does not contain column c")

	tk.MustExec("alter materialized view log on t_add_mlog_mv add column (c)")
	tk.MustExec("create materialized view mv_add_mlog_col_after (a, s, cnt) refresh fast as select a, sum(c), count(1) from t_add_mlog_mv group by a")
	tk.MustQuery("select a, s, cnt from mv_add_mlog_col_after order by a").Check(testkit.Rows(
		"1 300 2",
		"2 300 1",
	))

}

func TestDropMaterializedViewLogPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_mlog_priv (a int)")
	tk.MustExec("create materialized view log on t_drop_mlog_priv (a)")
	tk.MustExec("create user 'u_drop_mlog_select'@'%'")
	tk.MustExec("create user 'u_drop_mlog_ok'@'%'")
	defer tk.MustExec("drop user 'u_drop_mlog_select'@'%'")
	defer tk.MustExec("drop user 'u_drop_mlog_ok'@'%'")
	tk.MustExec("grant select on test.t_drop_mlog_priv to 'u_drop_mlog_select'@'%'")
	tk.MustExec("grant drop on test.`$mlog$t_drop_mlog_priv` to 'u_drop_mlog_ok'@'%'")

	tkSelect := testkit.NewTestKit(t, store)
	require.NoError(t, tkSelect.Session().Auth(&auth.UserIdentity{Username: "u_drop_mlog_select", Hostname: "%"}, nil, nil, nil))
	err := tkSelect.ExecToErr("drop materialized view log on test.t_drop_mlog_priv")
	require.ErrorContains(t, err, "DROP command denied")

	tkDrop := testkit.NewTestKit(t, store)
	require.NoError(t, tkDrop.Session().Auth(&auth.UserIdentity{Username: "u_drop_mlog_ok", Hostname: "%"}, nil, nil, nil))
	tkDrop.MustExec("drop materialized view log on test.t_drop_mlog_priv")
}

func TestAlterMaterializedViewLogPurgeBestEffortInfoUpdateWarning(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tkLock := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tkLock.MustExec("use test")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 2 hour)")

	getMLogMeta := func() (int64, string, string, string) {
		is := dom.InfoSchema()
		mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t"))
		require.NoError(t, err)
		require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
		return mlogTable.Meta().ID,
			mlogTable.Meta().MaterializedViewLog.PurgeMethod,
			mlogTable.Meta().MaterializedViewLog.PurgeStartWith,
			mlogTable.Meta().MaterializedViewLog.PurgeNext
	}

	mlogID, purgeMethod, purgeStartWith, purgeNext := getMLogMeta()
	require.Equal(t, "DEFERRED", purgeMethod)
	require.Equal(t, "", purgeStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 2 HOUR)", purgeNext)

	const expectedNextUnixSeconds int64 = 1_925_089_445
	tk.MustExec(fmt.Sprintf(
		"update mysql.tidb_mlog_purge_info set NEXT_PURGE_UNIX_SECONDS = %d where MLOG_ID = %d",
		expectedNextUnixSeconds,
		mlogID,
	))
	tkLock.MustExec("begin pessimistic")
	defer tkLock.MustExec("rollback")
	tkLock.MustExec(fmt.Sprintf(
		"update mysql.tidb_mlog_purge_info set NEXT_PURGE_UNIX_SECONDS = NEXT_PURGE_UNIX_SECONDS where MLOG_ID = %d",
		mlogID,
	))

	tk.MustExec("alter materialized view log on t purge next date_add(now(), interval 25 minute)")
	tk.MustQuery("show warnings").CheckContain(
		"alter materialized view log purge: metadata updated but failed to update mysql.tidb_mlog_purge_info.NEXT_PURGE_UNIX_SECONDS within 10s due to row lock contention",
	)

	_, purgeMethod, purgeStartWith, purgeNext = getMLogMeta()
	require.Equal(t, "DEFERRED", purgeMethod)
	require.Equal(t, "", purgeStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", purgeNext)
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_PURGE_UNIX_SECONDS = %d from mysql.tidb_mlog_purge_info where MLOG_ID = %d",
		expectedNextUnixSeconds,
		mlogID,
	)).Check(testkit.Rows("1"))
}

func TestCreateMaterializedViewLogUpdatesPlacementBundle(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create placement policy mlog_p followers=1")
	tk.MustExec("alter database test placement policy mlog_p")
	tk.MustExec("create table t_placement (a int)")
	tk.MustExec("create materialized view log on t_placement (a)")

	tk.MustQuery("show placement for table `$mlog$t_placement`").CheckContain("TABLE test.$mlog$t_placement")
	tk.MustQuery("show placement for table `$mlog$t_placement`").CheckContain("FOLLOWERS=1")
}

func TestCreateMaterializedViewLogAllowsGeneratedColumns(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("CREATE TABLE t_gen (" +
		"id BIGINT NOT NULL PRIMARY KEY," +
		"base BIGINT NOT NULL," +
		"gv BIGINT AS (base + 1) VIRTUAL," +
		"gs BIGINT AS (base + 2) STORED" +
		")")
	tk.MustExec("CREATE MATERIALIZED VIEW LOG ON t_gen (id, gv, gs)")

	tk.MustQuery("select column_name from information_schema.columns where table_schema='test' and table_name='$mlog$t_gen' order by ordinal_position").
		Check(testkit.Rows("id", "gv", "gs", "_MLOG$_DML_TYPE", "_MLOG$_OLD_NEW"))

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_gen"))
	require.NoError(t, err)
	require.NotNil(t, mlogTable.Meta().MaterializedViewLog)
	require.Equal(t, []ast.CIStr{ast.NewCIStr("id"), ast.NewCIStr("gv"), ast.NewCIStr("gs")}, mlogTable.Meta().MaterializedViewLog.Columns)
}
