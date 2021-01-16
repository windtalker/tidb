// Copyright 2019 PingCAP, Inc.
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

package tikv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/dgraph-io/ristretto"
	uuid2 "github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/atomic"
)

type coprCache struct {
	cache                   *ristretto.Cache
	admissionMaxRanges      int
	admissionMaxSize        int
	admissionMinProcessTime time.Duration
	fileIndex               *atomic.Int64
	rw                      sync.RWMutex
}

type coprCacheValue struct {
	Key               []byte `json:"key"`
	Data              []byte `json:"data"`
	TimeStamp         uint64 `json:"timestamp"`
	RegionID          uint64 `json:"region_id"`
	RegionDataVersion uint64 `json:"region_data_version"`
}

func (v *coprCacheValue) String() string {
	return fmt.Sprintf("{ Ts = %d, RegionID = %d, RegionDataVersion = %d, len(Data) = %d }",
		v.TimeStamp,
		v.RegionID,
		v.RegionDataVersion,
		len(v.Data))
}

const coprCacheValueSize = int(unsafe.Sizeof(coprCacheValue{}))

func (v *coprCacheValue) Len() int {
	return coprCacheValueSize + len(v.Key) + len(v.Data)
}

func newCoprCache(config *config.CoprocessorCache) (*coprCache, error) {
	if config == nil || !config.Enable {
		return nil, nil
	}
	capacityInBytes := int64(config.CapacityMB * 1024.0 * 1024.0)
	if capacityInBytes == 0 {
		return nil, errors.New("Capacity must be > 0 to enable the cache")
	}
	maxEntityInBytes := int64(config.AdmissionMaxResultMB * 1024.0 * 1024.0)
	if maxEntityInBytes == 0 {
		return nil, errors.New("AdmissionMaxResultMB must be > 0 to enable the cache")
	}
	estimatedEntities := capacityInBytes / maxEntityInBytes * 2
	if estimatedEntities < 10 {
		estimatedEntities = 10
	}
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: estimatedEntities * 10,
		MaxCost:     capacityInBytes,
		BufferItems: 64,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	c := coprCache{
		cache:                   cache,
		admissionMaxRanges:      int(config.AdmissionMaxRanges),
		admissionMaxSize:        int(maxEntityInBytes),
		admissionMinProcessTime: time.Duration(config.AdmissionMinProcessMs) * time.Millisecond,
		fileIndex:               atomic.NewInt64(0),
	}
	return &c, nil
}

func coprCacheBuildKey(copReq *coprocessor.Request) ([]byte, error) {
	// Calculate amount of space to allocate
	if copReq.Tp > math.MaxUint8 {
		return nil, errors.New("Request Tp too big")
	}
	if len(copReq.Data) > math.MaxUint32 {
		return nil, errors.New("Cache data too big")
	}
	totalLength := 1 + 4 + len(copReq.Data)
	for _, r := range copReq.Ranges {
		if len(r.Start) > math.MaxUint16 {
			return nil, errors.New("Cache start key too big")
		}
		if len(r.End) > math.MaxUint16 {
			return nil, errors.New("Cache end key too big")
		}
		totalLength += 2 + len(r.Start) + 2 + len(r.End)
	}

	key := make([]byte, totalLength)

	// 1 byte Tp
	key[0] = uint8(copReq.Tp)
	dest := 1

	// 4 bytes Data len
	binary.LittleEndian.PutUint32(key[dest:], uint32(len(copReq.Data)))
	dest += 4

	// N bytes Data
	copy(key[dest:], copReq.Data)
	dest += len(copReq.Data)

	for _, r := range copReq.Ranges {
		// 2 bytes Key len
		binary.LittleEndian.PutUint16(key[dest:], uint16(len(r.Start)))
		dest += 2

		// N bytes Key
		copy(key[dest:], r.Start)
		dest += len(r.Start)

		// 2 bytes Key len
		binary.LittleEndian.PutUint16(key[dest:], uint16(len(r.End)))
		dest += 2

		// N bytes Key
		copy(key[dest:], r.End)
		dest += len(r.End)
	}

	return key, nil
}

// Get gets a cache item according to cache key.
func (c *coprCache) Get(key []byte) *coprCacheValue {
	if c == nil {
		return nil
	}
	c.rw.RLock()
	defer c.rw.RUnlock()
	value, hit := c.cache.Get(key)
	if !hit {
		return nil
	}
	typedValue := value.(*coprCacheValue)
	// ristretto does not handle hash collision, so check the key equality after getting a value.
	if !bytes.Equal(typedValue.Key, key) {
		return nil
	}
	return typedValue
}

// CheckRequestAdmission checks whether a response item is worth caching.
func (c *coprCache) CheckRequestAdmission(ranges int) bool {
	if c == nil {
		return false
	}
	if c.admissionMaxRanges != 0 && ranges > c.admissionMaxRanges {
		return false
	}
	return true
}

// CheckResponseAdmission checks whether a response item is worth caching.
func (c *coprCache) CheckResponseAdmission(dataSize int, processTime time.Duration) bool {
	if c == nil {
		return false
	}
	if dataSize == 0 || dataSize > c.admissionMaxSize {
		return false
	}
	if processTime < c.admissionMinProcessTime {
		return false
	}
	return true
}

func (c *coprCache) loadSingleFile(path string, file string) {
	names := strings.Split(file, "_")
	if len(names) != 5 {
		logutil.BgLogger().Warn("failed to load cop cache from " + file + ": file name not valid")
		return
	}
	cacheValue := &coprCacheValue{}
	var err error
	cacheValue.TimeStamp, err = strconv.ParseUint(names[0], 10, 64)
	if err != nil {
		logutil.BgLogger().Warn("failed to load cop cache from " + file + ": can not extract ts from filename")
		return
	}
	cacheValue.RegionID, err = strconv.ParseUint(names[1], 10, 64)
	if err != nil {
		logutil.BgLogger().Warn("failed to load cop cache from " + file + ": can not extract region id from filename")
		return
	}
	cacheValue.RegionDataVersion, err = strconv.ParseUint(names[2], 10, 64)
	if err != nil {
		logutil.BgLogger().Warn("failed to load cop cache from " + file + ": can not extract region data version from filename")
		return
	}
	cacheValue.Key, err = ioutil.ReadFile(filepath.Join(path, file))
	if err != nil {
		logutil.BgLogger().Warn("failed to load cop cache from " + file + ": can not load key")
		return
	}
	valueFileName := names[0] + "_" + names[1] + "_" + names[2] + "_" + names[3] + "_value"
	cacheValue.Data, err = ioutil.ReadFile(filepath.Join(path, valueFileName))
	if err != nil {
		logutil.BgLogger().Warn("failed to load cop cache from " + file + ": can not load value")
		return
	}
	c.Set(cacheValue.Key, cacheValue, "")
}

func (c *coprCache) LoadFromFile(loadPath string, loadConcurrency uint64) error {
	if c == nil {
		return nil
	}
	c.rw.Lock()
	c.cache.Clear()
	c.rw.Unlock()
	if loadConcurrency <= 0 || loadConcurrency >= 20 {
		loadConcurrency = 15
	}

	stat, err := os.Stat(loadPath)
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		return errors.New(loadPath + " is not a dir")
	}
	rd, err := ioutil.ReadDir(loadPath)
	if err != nil {
		return err
	}
	fileNum := len(rd)
	filePerThread := fileNum / int(loadConcurrency)
	remainingFile := fileNum % int(loadConcurrency)
	var wg sync.WaitGroup
	for index := 0; index < int(loadConcurrency); index++ {
		wg.Add(1)
		go func(indexValue int) {
			defer wg.Done()
			extraFile := false
			if remainingFile > indexValue {
				extraFile = true
			}
			for i := filePerThread * indexValue; i < filePerThread*(indexValue+1); i++ {
				file := rd[i]
				if strings.HasSuffix(file.Name(), "_key") {
					c.loadSingleFile(loadPath, file.Name())
				}
			}
			if extraFile {
				file := rd[len(rd)-1-indexValue]
				if strings.HasSuffix(file.Name(), "_key") {
					c.loadSingleFile(loadPath, file.Name())
				}
			}
		}(index)
	}
	wg.Wait()
	return nil
}

// Set inserts an item to the cache.
// It is recommended to call `CheckRequestAdmission` and `CheckResponseAdmission` before inserting
// the item to the cache.
func (c *coprCache) Set(key []byte, value *coprCacheValue, diskName string) bool {
	if c == nil {
		return false
	}
	c.rw.RLock()
	defer c.rw.RUnlock()
	// Always ensure that the `Key` in `value` is the `key` we received.
	value.Key = key
	if len(diskName) > 0 {
		if uid, err := uuid2.NewUUID(); err == nil {
			uuidString := uid.String()
			fileNamePrefix := strconv.FormatUint(value.TimeStamp, 10) + "_" + strconv.FormatUint(value.RegionID, 10) + "_" + strconv.FormatUint(value.RegionDataVersion, 10) + "_" + uuidString
			keyName := fileNamePrefix + "_key"
			e := ioutil.WriteFile(filepath.Join(diskName, keyName), value.Key, 0644)
			if e != nil {
				logutil.BgLogger().Warn("failed to save cop key to file")
			}
			valueName := fileNamePrefix + "_value"
			e = ioutil.WriteFile(filepath.Join(diskName, valueName), value.Data, 0644)
			if e != nil {
				logutil.BgLogger().Warn("failed to save cop key to file")
			}
		} else {
			logutil.BgLogger().Warn("failed to save cop cache to file because uuid is not generated")
		}
	}
	return c.cache.Set(key, value, int64(value.Len()))
}
