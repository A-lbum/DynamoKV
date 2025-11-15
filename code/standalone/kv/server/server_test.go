package server

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/llllleeeewwwiis/standalone/kv/config"
	"github.com/llllleeeewwwiis/standalone/kv/storage"
	"github.com/llllleeeewwwiis/standalone/kv/storage/standalone_storage"
	"github.com/llllleeeewwwiis/standalone/proto/pkg/rawkv"
	"github.com/llllleeeewwwiis/standalone/util/engine_util"
	"github.com/stretchr/testify/assert"
)

func Set(s *standalone_storage.StandAloneStorage, cf string, key []byte, value []byte) error {
	return s.Write([]storage.Modify{
		{
			Data: storage.Put{
				Cf:    cf,
				Key:   key,
				Value: value,
			},
		},
	})
}

func Get(s *standalone_storage.StandAloneStorage, cf string, key []byte) ([]byte, error) {
	reader, err := s.Reader()
	if err != nil {
		return nil, err
	}
	return reader.GetCF(cf, key)
}

func Iter(s *standalone_storage.StandAloneStorage, cf string) (engine_util.DBIterator, error) {
	reader, err := s.Reader()
	if err != nil {
		return nil, err
	}
	return reader.IterCF(cf), nil
}

func cleanUpTestData(conf *config.Config) error {
	if conf != nil {
		return os.RemoveAll(conf.DBPath)
	}
	return nil
}

func TestRawGet1(t *testing.T) {
	conf := config.NewTestConfig()
	s := standalone_storage.NewStandAloneStorage(conf)
	s.Start()
	server := NewServer(s)
	defer cleanUpTestData(conf)
	defer s.Stop()

	cf := engine_util.CfDefault
	Set(s, cf, []byte{99}, []byte{42})

	req := &rawkv.RawGetRequest{
		Key: []byte{99},
		Cf:  cf,
	}
	resp, err := server.RawGet(nil, req)
	assert.Nil(t, err)
	assert.Equal(t, []byte{42}, resp.Value)
}

func TestRawGetNotFound1(t *testing.T) {
	conf := config.NewTestConfig()
	s := standalone_storage.NewStandAloneStorage(conf)
	s.Start()
	server := NewServer(s)
	defer cleanUpTestData(conf)
	defer s.Stop()

	cf := engine_util.CfDefault
	req := &rawkv.RawGetRequest{
		Key: []byte{99},
		Cf:  cf,
	}
	resp, err := server.RawGet(nil, req)
	assert.Nil(t, err)
	assert.True(t, resp.NotFound)
}

func TestRawPut1(t *testing.T) {
	conf := config.NewTestConfig()
	s := standalone_storage.NewStandAloneStorage(conf)
	s.Start()
	server := NewServer(s)
	defer cleanUpTestData(conf)
	defer s.Stop()

	cf := engine_util.CfDefault
	req := &rawkv.RawPutRequest{
		Key:   []byte{99},
		Value: []byte{42},
		Cf:    cf,
	}

	_, err := server.RawPut(nil, req)
	assert.Nil(t, err)

	got, err := Get(s, cf, []byte{99})
	assert.Nil(t, err)
	assert.Equal(t, []byte{42}, got)
}

func TestRawGetAfterRawPut1(t *testing.T) {
	conf := config.NewTestConfig()
	s := standalone_storage.NewStandAloneStorage(conf)
	s.Start()
	server := NewServer(s)
	defer cleanUpTestData(conf)
	defer s.Stop()

	put1 := &rawkv.RawPutRequest{
		Key:   []byte{99},
		Value: []byte{42},
		Cf:    engine_util.CfDefault,
	}
	_, err := server.RawPut(nil, put1)
	assert.Nil(t, err)

	put2 := &rawkv.RawPutRequest{
		Key:   []byte{99},
		Value: []byte{44},
		Cf:    engine_util.CfWrite,
	}
	_, err = server.RawPut(nil, put2)
	assert.Nil(t, err)

	get1 := &rawkv.RawGetRequest{
		Key: []byte{99},
		Cf:  engine_util.CfDefault,
	}
	resp, err := server.RawGet(nil, get1)
	assert.Nil(t, err)
	assert.Equal(t, []byte{42}, resp.Value)

	get2 := &rawkv.RawGetRequest{
		Key: []byte{99},
		Cf:  engine_util.CfWrite,
	}
	resp, err = server.RawGet(nil, get2)
	assert.Nil(t, err)
	assert.Equal(t, []byte{44}, resp.Value)
}

func TestRawGetAfterRawDelete1(t *testing.T) {
	conf := config.NewTestConfig()
	s := standalone_storage.NewStandAloneStorage(conf)
	s.Start()
	server := NewServer(s)
	defer cleanUpTestData(conf)
	defer s.Stop()

	cf := engine_util.CfDefault
	assert.Nil(t, Set(s, cf, []byte{99}, []byte{42}))

	delete := &rawkv.RawDeleteRequest{
		Key: []byte{99},
		Cf:  cf,
	}
	get := &rawkv.RawGetRequest{
		Key: []byte{99},
		Cf:  cf,
	}

	_, err := server.RawDelete(nil, delete)
	assert.Nil(t, err)

	resp, err := server.RawGet(nil, get)
	assert.Nil(t, err)
	assert.True(t, resp.NotFound)
}

func TestRawDelete1(t *testing.T) {
	conf := config.NewTestConfig()
	s := standalone_storage.NewStandAloneStorage(conf)
	s.Start()
	server := NewServer(s)
	defer cleanUpTestData(conf)
	defer s.Stop()

	cf := engine_util.CfDefault
	assert.Nil(t, Set(s, cf, []byte{99}, []byte{42}))

	req := &rawkv.RawDeleteRequest{
		Key: []byte{99},
		Cf:  cf,
	}

	_, err := server.RawDelete(nil, req)
	assert.Nil(t, err)

	val, err := Get(s, cf, []byte{99})
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte(nil), val)
}

func TestRawScan1(t *testing.T) {
	conf := config.NewTestConfig()
	s := standalone_storage.NewStandAloneStorage(conf)
	s.Start()
	server := NewServer(s)
	defer cleanUpTestData(conf)
	defer s.Stop()

	cf := engine_util.CfDefault

	Set(s, cf, []byte{1}, []byte{233, 1})
	Set(s, cf, []byte{2}, []byte{233, 2})
	Set(s, cf, []byte{3}, []byte{233, 3})
	Set(s, cf, []byte{4}, []byte{233, 4})
	Set(s, cf, []byte{5}, []byte{233, 5})

	req := &rawkv.RawScanRequest{
		StartKey: []byte{1},
		Limit:    3,
		Cf:       cf,
	}

	resp, err := server.RawScan(nil, req)
	assert.Nil(t, err)

	assert.Equal(t, 3, len(resp.Kvs))
	expectedKeys := [][]byte{{1}, {2}, {3}}
	for i, kv := range resp.Kvs {
		assert.Equal(t, expectedKeys[i], kv.Key)
		assert.Equal(t, append([]byte{233}, expectedKeys[i]...), kv.Value)
	}
}

func TestRawScanAfterRawPut1(t *testing.T) {
	conf := config.NewTestConfig()
	s := standalone_storage.NewStandAloneStorage(conf)
	s.Start()
	server := NewServer(s)
	defer cleanUpTestData(conf)
	defer s.Stop()

	cf := engine_util.CfDefault
	assert.Nil(t, Set(s, cf, []byte{1}, []byte{233, 1}))
	assert.Nil(t, Set(s, cf, []byte{2}, []byte{233, 2}))
	assert.Nil(t, Set(s, cf, []byte{3}, []byte{233, 3}))
	assert.Nil(t, Set(s, cf, []byte{4}, []byte{233, 4}))

	put := &rawkv.RawPutRequest{
		Key:   []byte{5},
		Value: []byte{233, 5},
		Cf:    cf,
	}

	scan := &rawkv.RawScanRequest{
		StartKey: []byte{1},
		Limit:    10,
		Cf:       cf,
	}

	expectedKeys := [][]byte{{1}, {2}, {3}, {4}, {5}}

	_, err := server.RawPut(nil, put)
	assert.Nil(t, err)

	resp, err := server.RawScan(nil, scan)
	assert.Nil(t, err)
	assert.Equal(t, len(expectedKeys), len(resp.Kvs))
	for i, kv := range resp.Kvs {
		assert.Equal(t, expectedKeys[i], kv.Key)
		assert.Equal(t, append([]byte{233}, expectedKeys[i]...), kv.Value)
	}
}

func TestRawScanAfterRawDelete1(t *testing.T) {
	conf := config.NewTestConfig()
	s := standalone_storage.NewStandAloneStorage(conf)
	s.Start()
	server := NewServer(s)
	defer cleanUpTestData(conf)
	defer s.Stop()

	cf := engine_util.CfDefault
	assert.Nil(t, Set(s, cf, []byte{1}, []byte{233, 1}))
	assert.Nil(t, Set(s, cf, []byte{2}, []byte{233, 2}))
	assert.Nil(t, Set(s, cf, []byte{3}, []byte{233, 3}))
	assert.Nil(t, Set(s, cf, []byte{4}, []byte{233, 4}))

	delete := &rawkv.RawDeleteRequest{
		Key: []byte{3},
		Cf:  cf,
	}

	scan := &rawkv.RawScanRequest{
		StartKey: []byte{1},
		Limit:    10,
		Cf:       cf,
	}

	expectedKeys := [][]byte{{1}, {2}, {4}}

	_, err := server.RawDelete(nil, delete)
	assert.Nil(t, err)

	resp, err := server.RawScan(nil, scan)
	assert.Nil(t, err)
	assert.Equal(t, len(expectedKeys), len(resp.Kvs))
	for i, kv := range resp.Kvs {
		assert.Equal(t, expectedKeys[i], kv.Key)
		assert.Equal(t, append([]byte{233}, expectedKeys[i]...), kv.Value)
	}
}

func TestIterWithRawDelete1(t *testing.T) {
	conf := config.NewTestConfig()
	s := standalone_storage.NewStandAloneStorage(conf)
	s.Start()
	server := NewServer(s)
	defer cleanUpTestData(conf)
	defer s.Stop()

	cf := engine_util.CfDefault
	assert.Nil(t, Set(s, cf, []byte{1}, []byte{233, 1}))
	assert.Nil(t, Set(s, cf, []byte{2}, []byte{233, 2}))
	assert.Nil(t, Set(s, cf, []byte{3}, []byte{233, 3}))
	assert.Nil(t, Set(s, cf, []byte{4}, []byte{233, 4}))

	it, err := Iter(s, cf)
	assert.Nil(t, err)
	defer it.Close()

	delete := &rawkv.RawDeleteRequest{
		Key: []byte{3},
		Cf:  cf,
	}
	_, err = server.RawDelete(nil, delete)
	assert.Nil(t, err)

	expectedKeys := [][]byte{{1}, {2}, {3}, {4}}
	i := 0
	for it.Seek([]byte{1}); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		assert.Equal(t, expectedKeys[i], key)
		i++
	}
}

func TestRawPutGetDelete1(t *testing.T) {
	conf := config.NewTestConfig()
	s := standalone_storage.NewStandAloneStorage(conf)
	s.Start()
	server := NewServer(s)
	defer cleanUpTestData(conf)
	defer s.Stop()

	cf := engine_util.CfDefault
	key := []byte("testkey")
	value := []byte("testvalue")

	// 1. Test PUT
	putReq := &rawkv.RawPutRequest{
		Key:   key,
		Value: value,
		Cf:    cf,
	}
	_, err := server.RawPut(nil, putReq)
	assert.Nil(t, err)
	t.Logf("PUT operation succeeded for key=%s", key)

	// 2. Test GET
	getReq := &rawkv.RawGetRequest{
		Key: key,
		Cf:  cf,
	}
	getResp, err := server.RawGet(nil, getReq)
	assert.Nil(t, err)
	assert.Equal(t, value, getResp.Value)
	t.Logf("GET operation returned expected value for key=%s", key)

	// 3. Test DELETE
	delReq := &rawkv.RawDeleteRequest{
		Key: key,
		Cf:  cf,
	}
	_, err = server.RawDelete(nil, delReq)
	assert.Nil(t, err)
	t.Logf("DELETE operation succeeded for key=%s", key)

	// 4. Confirm GET after DELETE
	getRespAfterDel, err := server.RawGet(nil, getReq)
	assert.Nil(t, err)
	assert.True(t, getRespAfterDel.NotFound)
	t.Logf("GET after DELETE confirmed key=%s is not found", key)
}

func TestConcurrentPutGetDelete1(t *testing.T) {
	conf := config.NewTestConfig()
	s := standalone_storage.NewStandAloneStorage(conf)
	s.Start()
	server := NewServer(s)
	defer cleanUpTestData(conf)
	defer s.Stop()

	cf := engine_util.CfDefault
	numKeys := 50
	var wg sync.WaitGroup

	// 并发 PUT
	for i := 0; i < numKeys; i++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key-%d", k))
			value := []byte(fmt.Sprintf("val-%d", k))
			req := &rawkv.RawPutRequest{Key: key, Value: value, Cf: cf}
			_, err := server.RawPut(nil, req)
			assert.Nil(t, err)
			t.Logf("PUT %s = %s", key, value)
		}(i)
	}
	wg.Wait()

	// 并发 GET
	for i := 0; i < numKeys; i++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key-%d", k))
			req := &rawkv.RawGetRequest{Key: key, Cf: cf}
			resp, err := server.RawGet(nil, req)
			assert.Nil(t, err)
			assert.Equal(t, []byte(fmt.Sprintf("val-%d", k)), resp.Value)
			t.Logf("GET %s = %s", key, resp.Value)
		}(i)
	}
	wg.Wait()

	// 并发 DELETE
	for i := 0; i < numKeys; i++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key-%d", k))
			req := &rawkv.RawDeleteRequest{Key: key, Cf: cf}
			_, err := server.RawDelete(nil, req)
			assert.Nil(t, err)
			t.Logf("DELETE %s", key)
		}(i)
	}
	wg.Wait()

	// 检查 DELETE 是否成功
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		req := &rawkv.RawGetRequest{Key: key, Cf: cf}
		resp, err := server.RawGet(nil, req)
		assert.Nil(t, err)
		assert.True(t, resp.NotFound)
	}
}

func TestBasicPerformanceScaling1(t *testing.T) {
	conf := config.NewTestConfig()
	s := standalone_storage.NewStandAloneStorage(conf)
	s.Start()
	server := NewServer(s)
	defer cleanUpTestData(conf)
	defer s.Stop()

	cf := engine_util.CfDefault
	nValues := []int{100, 1000, 10000, 100000, 1000000} // 不同规模
	fmt.Printf("| n | PUT (ms) | GET (ms) | DELETE (ms) |\n")
	fmt.Printf("|---|----------|----------|-------------|\n")

	for _, n := range nValues {
		// 生成测试数据
		keys := make([][]byte, n)
		values := make([][]byte, n)
		for i := 0; i < n; i++ {
			keys[i] = []byte(fmt.Sprintf("key-%d", i))
			values[i] = []byte(fmt.Sprintf("val-%d", i))
		}

		// PUT
		start := time.Now()
		for i := 0; i < n; i++ {
			req := &rawkv.RawPutRequest{Key: keys[i], Value: values[i], Cf: cf}
			_, err := server.RawPut(nil, req)
			assert.Nil(t, err)
		}
		putTime := time.Since(start).Milliseconds()

		// GET
		start = time.Now()
		for i := 0; i < n; i++ {
			req := &rawkv.RawGetRequest{Key: keys[i], Cf: cf}
			resp, err := server.RawGet(nil, req)
			assert.Nil(t, err)
			assert.Equal(t, values[i], resp.Value)
		}
		getTime := time.Since(start).Milliseconds()

		// DELETE
		start = time.Now()
		for i := 0; i < n; i++ {
			req := &rawkv.RawDeleteRequest{Key: keys[i], Cf: cf}
			_, err := server.RawDelete(nil, req)
			assert.Nil(t, err)
		}
		delTime := time.Since(start).Milliseconds()

		// 输出表格行
		fmt.Printf("| %d | %d | %d | %d |\n", n, putTime, getTime, delTime)
	}
}
