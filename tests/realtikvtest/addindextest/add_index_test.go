// Copyright 2022 PingCAP, Inc.
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

package addindextest

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func init() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Path = "127.0.0.1:2379"
	})
}

func initTest(t *testing.T) *suiteContext {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindex;")
	tk.MustExec("create database addindex;")
	tk.MustExec("use addindex;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)

	ctx := newSuiteContext(t, tk, store)
	createTable(tk)
	insertRows(tk)
	initWorkloadParams(ctx)
	return ctx
}

func TestCreateNonUniqueIndex(t *testing.T) {
	var colIDs = [][]int{
		{1, 4, 7, 10, 13, 16, 19, 22, 25},
		{2, 5, 8, 11, 14, 17, 20, 23, 26},
		{3, 6, 9, 12, 15, 18, 21, 24, 27},
	}
	ctx := initTest(t)
	testOneColFrame(ctx, colIDs, addIndexNonUnique)
}

func TestCreateUniqueIndex(t *testing.T) {
	var colIDs [][]int = [][]int{
		{1, 6, 7, 8, 11, 13, 15, 16, 18, 19, 22, 26},
		{2, 9, 11, 17},
		{3, 12, 25},
	}
	ctx := initTest(t)
	testOneColFrame(ctx, colIDs, addIndexUnique)
}

func TestCreatePrimaryKey(t *testing.T) {
	ctx := initTest(t)
	testOneIndexFrame(ctx, 0, addIndexPK)
}

func TestCreateGenColIndex(t *testing.T) {
	ctx := initTest(t)
	testOneIndexFrame(ctx, 29, addIndexGenCol)
}

func TestCreateMultiColsIndex(t *testing.T) {
	var coliIDs = [][]int{
		{1, 4, 7},
		{2, 5},
		{3, 6, 9},
	}
	var coljIDs = [][]int{
		{16, 19},
		{14, 17, 20},
		{18, 21},
	}

	if *FullMode {
		coliIDs = [][]int{
			{1, 4, 7, 10, 13},
			{2, 5, 8, 11},
			{3, 6, 9, 12, 15},
		}
		coljIDs = [][]int{
			{16, 19, 22, 25},
			{14, 17, 20, 23, 26},
			{18, 21, 24, 27},
		}
	}
	ctx := initTest(t)
	testTwoColsFrame(ctx, coliIDs, coljIDs, addIndexMultiCols)
}

func TestAddForeignKeyWithAutoCreateIndex(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists fk_index;")
	tk.MustExec("create database fk_index;")
	tk.MustExec("use fk_index;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=1;`)
	tk.MustExec("create table employee (id bigint auto_increment key, pid bigint)")
	tk.MustExec("insert into employee (id) values (1),(2),(3),(4),(5),(6),(7),(8)")
	for i := 0; i < 14; i++ {
		tk.MustExec("insert into employee (pid) select pid from employee")
	}
	tk.MustExec("update employee set pid=id-1 where id>1")
	tk.MustQuery("select count(*) from employee").Check(testkit.Rows("131072"))
	tk.MustExec("alter table employee add foreign key fk_1(pid) references employee(id)")
	tk.MustExec("alter table employee drop foreign key fk_1")
	tk.MustExec("alter table employee drop index fk_1")
	tk.MustExec("update employee set pid=0 where id=1")
	tk.MustGetErrMsg("alter table employee add foreign key fk_1(pid) references employee(id)",
		"[ddl:1452]Cannot add or update a child row: a foreign key constraint fails (`fk_index`.`employee`, CONSTRAINT `fk_1` FOREIGN KEY (`pid`) REFERENCES `employee` (`id`))")
	tk.MustExec("update employee set pid=null where id=1")
	tk.MustExec("insert into employee (pid) select pid from employee")
	tk.MustExec("update employee set pid=id-1 where id>1 and pid is null")
	tk.MustExec("alter table employee add foreign key fk_1(pid) references employee(id)")
}

func TestAddIndexDistBasic(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	if store.Name() != "TiKV" {
		t.Skip("TiKV store only")
	}

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test;")
	tk.MustExec("create database test;")
	tk.MustExec("use test;")
	tk.MustExec(`set global tidb_enable_dist_task=1;`)

	tk.MustExec("create table t(a bigint auto_random primary key) partition by hash(a) partitions 8;")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("split table t between (3) and (8646911284551352360) regions 50;")
	tk.MustExec("alter table t add index idx(a);")
	tk.MustExec("admin check index t idx;")
	tk.MustExec(`set global tidb_enable_dist_task=0;`)
}

func TestAddIndexDistCancel(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	if store.Name() != "TiKV" {
		t.Skip("TiKV store only")
	}

	tk := testkit.NewTestKit(t, store)
	tk1 := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test;")
	tk.MustExec("create database test;")
	tk.MustExec("use test;")
	tk.MustExec(`set global tidb_enable_dist_task=1;`)

	tk.MustExec("create table t(a bigint auto_random primary key) partition by hash(a) partitions 8;")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("split table t between (3) and (8646911284551352360) regions 50;")

	ddl.MockDMLExecutionAddIndexSubTaskFinish = func() {
		row := tk1.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
		require.Equal(t, 1, len(row))
		jobID := row[0][0].(string)
		tk1.MustExec("admin cancel ddl jobs " + jobID)
	}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockDMLExecutionAddIndexSubTaskFinish", "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockDMLExecutionAddIndexSubTaskFinish"))
	}()

	require.Error(t, tk.ExecToErr("alter table t add index idx(a);"))
	tk.MustExec("admin check table t;")
	tk.MustExec("alter table t add index idx2(a);")
	tk.MustExec("admin check table t;")

	tk.MustExec(`set global tidb_enable_dist_task=0;`)
}

func TestIssue51162(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE tl (
	 col_42 json NOT NULL,
	 col_43 tinyint(1) DEFAULT NULL,
	 col_44 char(168) CHARACTER SET gbk COLLATE gbk_bin DEFAULT NULL,
	 col_45 json DEFAULT NULL,
	 col_46 text COLLATE utf8mb4_unicode_ci NOT NULL,
	 col_47 char(43) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'xW2YNb99pse4)',
	 col_48 time NOT NULL DEFAULT '12:31:25',
	 PRIMARY KEY (col_47,col_46(2)) /*T![clustered_index] CLUSTERED */
	  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`)

	tk.MustExec(`INSERT INTO tl VALUES
	('[\"1\"]',0,'1','[1]','Wxup81','1','10:14:20');`)

	tk.MustExec("alter table tl add index idx_16(`col_48`,(cast(`col_45` as signed array)),`col_46`(5));")
	tk.MustExec("admin check table tl")
}

func TestAddUKWithSmallIntHandles(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists small;")
	tk.MustExec("create database small;")
	tk.MustExec("use small;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=1;`)
	tk.MustExec("create table t (a bigint, b int, primary key (a) clustered)")
	tk.MustExec("insert into t values (-9223372036854775808, 1),(-9223372036854775807, 1)")
	tk.MustContainErrMsg("alter table t add unique index uk(b)", "Duplicate entry '1' for key 't.uk'")
}
