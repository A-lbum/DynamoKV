package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

type StandAloneStorage struct {
	db  *badger.DB
	txn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	return &StandAloneStorage{db: db}
}

func (s *StandAloneStorage) Start() error { return nil }
func (s *StandAloneStorage) Stop() error  { return s.db.Close() }

// 截图
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.db.NewTransaction(false) // 只读事务，用于迭代器
	return &StandAloneStorageReader{
		db:  s.db,
		txn: txn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, m := range batch {
			switch data := m.Data.(type) {

			// ---------------- 写入 ----------------
			case storage.Put:
				if err := engine_util.PutCFWithTxn(txn, data.Cf, data.Key, data.Value); err != nil {
					return err
				}

			// ---------------- 删除 ----------------
			case storage.Delete:
				// fmt.Println("standalone_storage.go delete called")
				err := engine_util.DeleteCFWithTxn(txn, data.Cf, data.Key)
				// fmt.Println("err:", err)
				if err != nil && err != badger.ErrKeyNotFound {
					return err
				}
			default:
				continue // 不返回错误，直接跳过
			}
		}
		return nil
	})
}

// ---------------- Reader ----------------
// ... StandAloneStorage 部分不变 ...

// ---------------- Reader ----------------
type StandAloneStorageReader struct {
	db  *badger.DB
	txn *badger.Txn
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	// 修复：使用 r.txn 来从快照中读取
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil // 对于 Get，未找到不应返回 error，而是 (nil, nil)
	}
	return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	// 修复：必须 Discard 事务以释放资源
	r.txn.Discard()
}
