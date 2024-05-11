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

package join

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// GetHashJoinOptSetSQL returns the set sql used to switch hash join v1 and v2
func GetHashJoinOptSetSQL() []string {
	return []string{"set tidb_opt_use_hash_join_v2 = 0", "set tidb_opt_use_hash_join_v2 = 1"}
}

// hashjoinWorkerResult stores the result of join workers,
// `src` is for Chunk reuse: the main goroutine will get the join result chunk `chk`,
// and push `chk` into `src` after processing, join worker goroutines get the empty chunk from `src`
// and push new data into this chunk.
type hashjoinWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

type hashJoinCtxBase struct {
	SessCtx        sessionctx.Context
	ChunkAllocPool chunk.Allocator
	// Concurrency is the number of partition, build and join workers.
	Concurrency  uint
	joinResultCh chan *hashjoinWorkerResult
	// closeCh add a lock for closing executor.
	closeCh       chan struct{}
	finished      atomic.Bool
	IsNullEQ      []bool
	buildFinished chan error
	JoinType      plannercore.JoinType
	IsNullAware   bool
	memTracker    *memory.Tracker // track memory usage.
	diskTracker   *disk.Tracker   // track disk usage.
}

type probeSideTupleFetcherBase struct {
	ProbeSideExec      exec.Executor
	probeChkResourceCh chan *probeChkResource
	probeResultChs     []chan *chunk.Chunk
	requiredRows       int64
}

type isBuildSideEmpty func() bool

func wait4BuildSide(isBuildEmpty isBuildSideEmpty, canSkipIfBuildEmpty bool, hashJoinCtx *hashJoinCtxBase) (skipProbe bool, err error) {
	select {
	case <-hashJoinCtx.closeCh:
		return true, nil
	case err := <-hashJoinCtx.buildFinished:
		if err != nil {
			return false, err
		}
	}
	if isBuildEmpty() && canSkipIfBuildEmpty {
		return true, nil
	}
	return false, nil
}

type probeWorkerBase struct {
	WorkerID           uint
	probeChkResourceCh chan *probeChkResource
	joinChkResourceCh  chan *chunk.Chunk
	probeResultCh      chan *chunk.Chunk
}

type buildWorkerBase struct {
	BuildSideExec  exec.Executor
	BuildKeyColIdx []int
}

// fetchBuildSideRows fetches all rows from build side executor, and append them
// to e.buildSideResult.
func (w *buildWorkerBase) fetchBuildSideRows(ctx context.Context, hashJoinCtx *hashJoinCtxBase, chkCh chan<- *chunk.Chunk, errCh chan<- error, doneCh <-chan struct{}) {
	defer close(chkCh)
	var err error
	failpoint.Inject("issue30289", func(val failpoint.Value) {
		if val.(bool) {
			err = errors.Errorf("issue30289 build return error")
			errCh <- errors.Trace(err)
			return
		}
	})
	failpoint.Inject("issue42662_1", func(val failpoint.Value) {
		if val.(bool) {
			if hashJoinCtx.SessCtx.GetSessionVars().ConnectionID != 0 {
				// consume 170MB memory, this sql should be tracked into MemoryTop1Tracker
				hashJoinCtx.memTracker.Consume(170 * 1024 * 1024)
			}
			return
		}
	})
	sessVars := hashJoinCtx.SessCtx.GetSessionVars()
	failpoint.Inject("issue51998", func(val failpoint.Value) {
		if val.(bool) {
			time.Sleep(2 * time.Second)
		}
	})
	for {
		if hashJoinCtx.finished.Load() {
			return
		}
		chk := hashJoinCtx.ChunkAllocPool.Alloc(w.BuildSideExec.RetFieldTypes(), sessVars.MaxChunkSize, sessVars.MaxChunkSize)
		err = exec.Next(ctx, w.BuildSideExec, chk)
		failpoint.Inject("issue51998", func(val failpoint.Value) {
			if val.(bool) {
				err = errors.Errorf("issue51998 build return error")
			}
		})
		if err != nil {
			errCh <- errors.Trace(err)
			return
		}
		failpoint.Inject("errorFetchBuildSideRowsMockOOMPanic", nil)
		failpoint.Inject("ConsumeRandomPanic", nil)
		if chk.NumRows() == 0 {
			return
		}
		select {
		case <-doneCh:
			return
		case <-hashJoinCtx.closeCh:
			return
		case chkCh <- chk:
		}
	}
}

// probeChkResource stores the result of the join probe side fetch worker,
// `dest` is for Chunk reuse: after join workers process the probe side chunk which is read from `dest`,
// they'll store the used chunk as `chk`, and then the probe side fetch worker will put new data into `chk` and write `chk` into dest.
type probeChkResource struct {
	chk  *chunk.Chunk
	dest chan<- *chunk.Chunk
}

type hashJoinRuntimeStats struct {
	fetchAndBuildHashTable time.Duration
	hashStat               hashStatistic
	fetchAndProbe          int64
	probe                  int64
	concurrent             int
	maxFetchAndProbe       int64
}

func (e *hashJoinRuntimeStats) setMaxFetchAndProbeTime(t int64) {
	for {
		value := atomic.LoadInt64(&e.maxFetchAndProbe)
		if t <= value {
			return
		}
		if atomic.CompareAndSwapInt64(&e.maxFetchAndProbe, value, t) {
			return
		}
	}
}

// Tp implements the RuntimeStats interface.
func (*hashJoinRuntimeStats) Tp() int {
	return execdetails.TpHashJoinRuntimeStats
}

func (e *hashJoinRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	if e.fetchAndBuildHashTable > 0 {
		buf.WriteString("build_hash_table:{total:")
		buf.WriteString(execdetails.FormatDuration(e.fetchAndBuildHashTable))
		buf.WriteString(", fetch:")
		buf.WriteString(execdetails.FormatDuration(e.fetchAndBuildHashTable - e.hashStat.buildTableElapse))
		buf.WriteString(", build:")
		buf.WriteString(execdetails.FormatDuration(e.hashStat.buildTableElapse))
		buf.WriteString("}")
	}
	if e.probe > 0 {
		buf.WriteString(", probe:{concurrency:")
		buf.WriteString(strconv.Itoa(e.concurrent))
		buf.WriteString(", total:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.fetchAndProbe)))
		buf.WriteString(", max:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(atomic.LoadInt64(&e.maxFetchAndProbe))))
		buf.WriteString(", probe:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.probe)))
		// fetch time is the time wait fetch result from its child executor,
		// wait time is the time wait its parent executor to fetch the joined result
		buf.WriteString(", fetch and wait:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.fetchAndProbe - e.probe)))
		if e.hashStat.probeCollision > 0 {
			buf.WriteString(", probe_collision:")
			buf.WriteString(strconv.FormatInt(e.hashStat.probeCollision, 10))
		}
		buf.WriteString("}")
	}
	return buf.String()
}

func (e *hashJoinRuntimeStats) Clone() execdetails.RuntimeStats {
	return &hashJoinRuntimeStats{
		fetchAndBuildHashTable: e.fetchAndBuildHashTable,
		hashStat:               e.hashStat,
		fetchAndProbe:          e.fetchAndProbe,
		probe:                  e.probe,
		concurrent:             e.concurrent,
		maxFetchAndProbe:       e.maxFetchAndProbe,
	}
}

func (e *hashJoinRuntimeStats) Merge(rs execdetails.RuntimeStats) {
	tmp, ok := rs.(*hashJoinRuntimeStats)
	if !ok {
		return
	}
	e.fetchAndBuildHashTable += tmp.fetchAndBuildHashTable
	e.hashStat.buildTableElapse += tmp.hashStat.buildTableElapse
	e.hashStat.probeCollision += tmp.hashStat.probeCollision
	e.fetchAndProbe += tmp.fetchAndProbe
	e.probe += tmp.probe
	if e.maxFetchAndProbe < tmp.maxFetchAndProbe {
		e.maxFetchAndProbe = tmp.maxFetchAndProbe
	}
}

type hashStatistic struct {
	// NOTE: probeCollision may be accessed from multiple goroutines concurrently.
	probeCollision   int64
	buildTableElapse time.Duration
}

func (s *hashStatistic) String() string {
	return fmt.Sprintf("probe_collision:%v, build:%v", s.probeCollision, execdetails.FormatDuration(s.buildTableElapse))
}
