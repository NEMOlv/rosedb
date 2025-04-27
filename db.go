package rosedb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/gofrs/flock"
	"github.com/robfig/cron/v3"
	"github.com/rosedblabs/rosedb/v2/index"
	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/rosedblabs/wal"
)

// 文件锁标识与文件后缀
const (
	fileLockName       = "FLOCK"
	dataFileNameSuffix = ".SEG"
	hintFileNameSuffix = ".HINT"
	mergeFinNameSuffix = ".MERGEFIN"
)

// DB represents a ROSEDB database instance.
// It is built on the bitcask model, which is a log-structured storage.
// It uses WAL to write data, and uses an in-memory index to store the key
// and the position of the data in the WAL,
// the index will be rebuilt when the database is opened.
//
// The main advantage of ROSEDB is that it is very fast to write, read, and delete data.
// Because it only needs one disk IO to complete a single operation.
//
// But since we should store all keys and their positions(index) in memory,
// our total data size is limited by the memory size.
//
// So if your memory can almost hold all the keys, ROSEDB is the perfect storage engine for you.

// DB 表示 ROSEDB 数据库实例。
// 它基于 bitcask 模型构建，是一种日志结构存储。
// 它使用 WAL 来写入数据，并使用内存中的索引来存储键和数据在 WAL 中的位置，索引将在数据库打开时重建。

// ROSEDB 的主要优点是写入、读取和删除数据的速度非常快。
// 因为它只需要一次磁盘 IO 就能完成一次操作。

// 但是，由于我们要把所有的键和它们的位置（索引）都存储在内存中，所以我们的总数据量会受到内存大小的限制。

// 因此，如果你的内存几乎可以容纳所有的键，那么 ROSEDB 就是最适合你的存储引擎。
type DB struct {
	// data files are a sets of segment files in WAL.
	// 数据文件是 WAL 中的一组段文件
	dataFiles *wal.WAL
	// hint file is used to store the key and the position for fast startup.
	// hint 文件用于存储key和数据位置，以便于启动数据库时快速重建索引
	hintFile *wal.WAL
	// 内存索引
	index index.Indexer
	// 配置选项
	options Options
	// 文件锁
	fileLock *flock.Flock
	// 读写锁
	mu sync.RWMutex
	// 关闭标识
	closed bool
	// indicate if the database is merging
	// 合并过程标识
	mergeRunning uint32
	// batch池
	batchPool sync.Pool
	// 数据池
	recordPool sync.Pool
	// 编码头部
	encodeHeader []byte
	// user consume channel for watch events
	// 用户消费通道来观察事件
	watchCh chan *Event
	watcher *Watcher
	// the location to which DeleteExpiredKeys executes.
	// 执行删除过期key的位置：用来判断删除到哪里了
	expiredCursorKey []byte
	// cron scheduler for auto merge task
	// 自动合并任务的 cron 调度程序
	cronScheduler *cron.Cron
}

// Stat represents the statistics of the database.
// Stat 表示数据库的统计数据。
type Stat struct {
	// Total number of keys
	// key的总数
	KeysNum int

	// Total disk size of database directory
	// 数据库目录的磁盘总大小
	DiskSize int64
}

// Open a database with the specified options.
// If the database directory does not exist, it will be created automatically.
//
// Multiple processes can not use the same database directory at the same time,
// otherwise it will return ErrDatabaseIsUsing.
//
// It will open the wal files in the database directory and load the index from them.
// Return the DB instance, or an error if any.

// Open 函数
// 使用指定选项打开数据库。
// 如果数据库目录不存在，将自动创建。
// 多个进程不能同时使用同一数据库目录，否则将返回 ErrDatabaseIsUsing。
// 它将打开数据库目录中的WAL文件，并从中加载索引。
// 返回数据库实例或错误。
func Open(options Options) (*DB, error) {
	// check options
	// 检查选项
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// create data directory if not exist
	// 如果目录不存在则新建一个数据库目录
	if _, err := os.Stat(options.DirPath); err != nil {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// create file lock, prevent multiple processes from using the same database directory
	// 创建文件锁，防止多个进程使用同一数据库目录
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// load merge files if exists
	// 加载merge文件
	if err = loadMergeFiles(options.DirPath); err != nil {
		return nil, err
	}

	// init DB instance
	// 初始化数据库实例
	db := &DB{
		index:        index.NewIndexer(),
		options:      options,
		fileLock:     fileLock,
		batchPool:    sync.Pool{New: newBatch},
		recordPool:   sync.Pool{New: newRecord},
		encodeHeader: make([]byte, maxLogRecordHeaderSize),
	}

	// open data files
	// 打开数据文件
	if db.dataFiles, err = db.openWalFiles(); err != nil {
		return nil, err
	}

	// load index
	// 加载索引
	if err = db.loadIndex(); err != nil {
		return nil, err
	}

	// enable watch
	if options.WatchQueueSize > 0 {
		db.watchCh = make(chan *Event, 100)
		db.watcher = NewWatcher(options.WatchQueueSize)
		// run a goroutine to synchronize event information
		go db.watcher.sendEvent(db.watchCh)
	}

	// enable auto merge task
	if len(options.AutoMergeCronExpr) > 0 {
		db.cronScheduler = cron.New(
			cron.WithParser(
				cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour |
					cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
			),
		)
		_, err = db.cronScheduler.AddFunc(options.AutoMergeCronExpr, func() {
			// maybe we should deal with different errors with different logic,
			// but a background task can't omit its error.
			// after auto merge, we should close and reopen the db.
			_ = db.Merge(true)
		})
		if err != nil {
			return nil, err
		}
		db.cronScheduler.Start()
	}

	return db, nil
}

// open data files from WAL
// 从WAL中打开数据文件
func (db *DB) openWalFiles() (*wal.WAL, error) {
	walFiles, err := wal.Open(wal.Options{
		DirPath:        db.options.DirPath,
		SegmentSize:    db.options.SegmentSize,
		SegmentFileExt: dataFileNameSuffix,
		Sync:           db.options.Sync,
		BytesPerSync:   db.options.BytesPerSync,
	})
	if err != nil {
		return nil, err
	}
	return walFiles, nil
}

// 加载索引
func (db *DB) loadIndex() error {
	// load index from hint file
	// 从hint文件中加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return err
	}
	// load index from data files
	// 从数据文件中加载索引
	if err := db.loadIndexFromWAL(); err != nil {
		return err
	}
	return nil
}

// Close the database, close all data files and release file lock.
// Set the closed flag to true.
// The DB instance cannot be used after closing.
// 关闭数据库：
//
//		   关闭所有数据文件并释放文件锁
//	    将closed标识设为true
//
// 数据库实例在关闭后无法使用
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭数据文件
	if err := db.closeFiles(); err != nil {
		return err
	}

	// release file lock
	// 释放文件锁
	if err := db.fileLock.Unlock(); err != nil {
		return err
	}

	// close watch channel
	// 关闭监控通道
	if db.options.WatchQueueSize > 0 {
		close(db.watchCh)
	}

	// close auto merge cron scheduler
	// 关闭自动合并corn调度
	if db.cronScheduler != nil {
		db.cronScheduler.Stop()
	}

	// closed标识设为true
	db.closed = true
	return nil
}

// closeFiles close all data files and hint file
// closeFiles：关闭所有数据文件和hint索引文件
func (db *DB) closeFiles() error {
	// close wal
	// 关闭WAL
	if err := db.dataFiles.Close(); err != nil {
		return err
	}

	// close hint file if exists
	// 关闭hint索引文件
	if db.hintFile != nil {
		if err := db.hintFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync all data files to the underlying storage.
// Sync：所有数据文件存储到磁盘中
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.dataFiles.Sync()
}

// Stat returns the statistics of the database.
// Stat：返回数据库统计信息
func (db *DB) Stat() *Stat {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 获取数据库目录大小
	diskSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("rosedb: get database directory size error: %v", err))
	}

	return &Stat{
		KeysNum:  db.index.Size(),
		DiskSize: diskSize,
	}
}

// Put a key-value pair into the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Put operation.
// Put：将键值对写入数据库
// 实际上，这回打开一个新批次并自动提交
// 你可以认为该批次只有一个Put操作
func (db *DB) Put(key []byte, value []byte) error {
	// 从batch池中获取一个batch
	batch := db.batchPool.Get().(*Batch)

	// 后处理：清理并返还batch
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()

	// This is a single put operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	// 这是一个独立的put操作，我们可以将Sync设置为fasle
	// 因为数据将会被写入WAL文件，而WAL文件会根据DB配置将数据同步到磁盘
	batch.init(false, false, db)
	if err := batch.Put(key, value); err != nil {
		// 如果Put操作失败，则回滚put操作
		_ = batch.Rollback()
		return err
	}

	// 提交put操作
	return batch.Commit()
}

// PutWithTTL a key-value pair into the database, with a ttl.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one PutWithTTL operation.
// PutWithTTL：将带有生命周期(过期时间)的键值对写入数据库
func (db *DB) PutWithTTL(key []byte, value []byte, ttl time.Duration) error {
	// 从batch池中获取一个batch
	batch := db.batchPool.Get().(*Batch)

	// 后处理：清理并返还batch
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// This is a single put operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch.init(false, false, db)

	if err := batch.PutWithTTL(key, value, ttl); err != nil {
		_ = batch.Rollback()
		return err
	}
	return batch.Commit()
}

// Get the value of the specified key from the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Get operation.
// Get：从数据库中获取key对应的数据
// 实际上，这会打开一个新批次并自动提交
// 你可以认为该批次只有一个Get操作
func (db *DB) Get(key []byte) ([]byte, error) {
	// 从batch池中获取一个batch
	batch := db.batchPool.Get().(*Batch)
	batch.init(true, false, db)

	// 后处理：提交Get操作，清理并返还batch
	defer func() {
		_ = batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.Get(key)
}

// Delete the specified key from the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Delete operation.
// Delete：从数据库中删除指定key及其对应数据
// 实际上，这会打开一个新批次并自动提交
// 你可以认为该批次只有一个Delete操作
func (db *DB) Delete(key []byte) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// This is a single delete operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch.init(false, false, db)
	if err := batch.Delete(key); err != nil {
		_ = batch.Rollback()
		return err
	}
	return batch.Commit()
}

// Exist checks if the specified key exists in the database.
// Actually, it will open a new batch and commit it.
// You can think the batch has only one Exist operation.
// Exist：检查指定key在数据库中是否存在
// 实际上，这回打开一个新批次并提交
// 你可以认为该批次只有一个Exist操作
func (db *DB) Exist(key []byte) (bool, error) {
	batch := db.batchPool.Get().(*Batch)
	batch.init(true, false, db)
	defer func() {
		_ = batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.Exist(key)
}

// Expire sets the ttl of the key.
// Expire：为key设置生命周期(过期时间)
func (db *DB) Expire(key []byte, ttl time.Duration) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// This is a single expire operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch.init(false, false, db)
	if err := batch.Expire(key, ttl); err != nil {
		_ = batch.Rollback()
		return err
	}
	return batch.Commit()
}

// TTL get the ttl of the key.
// TTL：获取key的生命周期(过期时间)
func (db *DB) TTL(key []byte) (time.Duration, error) {
	batch := db.batchPool.Get().(*Batch)
	batch.init(true, false, db)
	defer func() {
		_ = batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.TTL(key)
}

// Persist removes the ttl of the key.
// If the key does not exist or expired, it will return ErrKeyNotFound.
// Persist(持久化)：移除key的生命周期(过期时间)
// 如果key不存在或过期了，该函数会返回ErrKeyNotFound
func (db *DB) Persist(key []byte) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	// This is a single persist operation, we can set Sync to false.
	// Because the data will be written to the WAL,
	// and the WAL file will be synced to disk according to the DB options.
	batch.init(false, false, db)
	if err := batch.Persist(key); err != nil {
		_ = batch.Rollback()
		return err
	}
	return batch.Commit()
}

// Watch：返回观察通道
func (db *DB) Watch() (<-chan *Event, error) {
	if db.options.WatchQueueSize <= 0 {
		return nil, ErrWatchDisabled
	}
	return db.watchCh, nil
}

// Ascend calls handleFn for each key/value pair in the db in ascending order.
// Ascend 按升序为数据库中的每个键/值对调用 handleFn。
// 相当于提供了一个遍历数据的处理操作
func (db *DB) Ascend(handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.Ascend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, err
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// AscendRange calls handleFn for each key/value pair in the db within the range [startKey, endKey] in ascending order.
func (db *DB) AscendRange(startKey, endKey []byte, handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.AscendRange(startKey, endKey, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// AscendGreaterOrEqual calls handleFn for each key/value pair in the db with keys greater than or equal to the given key.
func (db *DB) AscendGreaterOrEqual(key []byte, handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.AscendGreaterOrEqual(key, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// AscendKeys calls handleFn for each key in the db in ascending order.
// Since our expiry time is stored in the value, if you want to filter expired keys,
// you need to set parameter filterExpired to true. But the performance will be affected.
// Because we need to read the value of each key to determine if it is expired.
func (db *DB) AscendKeys(pattern []byte, filterExpired bool, handleFn func(k []byte) (bool, error)) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var reg *regexp.Regexp
	if len(pattern) > 0 {
		var err error
		reg, err = regexp.Compile(string(pattern))
		if err != nil {
			return err
		}
	}

	db.index.Ascend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		if reg != nil && !reg.Match(key) {
			return true, nil
		}
		if filterExpired {
			chunk, err := db.dataFiles.Read(pos)
			if err != nil {
				return false, err
			}
			if value := db.checkValue(chunk); value == nil {
				return true, nil
			}
		}
		return handleFn(key)
	})
	return nil
}

// AscendKeysRange calls handleFn for keys within a range in the db in ascending order.
// Since our expiry time is stored in the value, if you want to filter expired keys,
// you need to set parameter filterExpired to true. But the performance will be affected.
// Because we need to read the value of each key to determine if it is expired.
func (db *DB) AscendKeysRange(startKey, endKey, pattern []byte, filterExpired bool, handleFn func(k []byte) (bool, error)) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var reg *regexp.Regexp
	if len(pattern) > 0 {
		var err error
		reg, err = regexp.Compile(string(pattern))
		if err != nil {
			return err
		}
	}

	db.index.AscendRange(startKey, endKey, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		if reg != nil && !reg.Match(key) {
			return true, nil
		}
		if filterExpired {
			chunk, err := db.dataFiles.Read(pos)
			if err != nil {
				return false, err
			}
			if value := db.checkValue(chunk); value == nil {
				return true, nil
			}
		}
		return handleFn(key)
	})
	return nil
}

// Descend calls handleFn for each key/value pair in the db in descending order.
func (db *DB) Descend(handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.Descend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// DescendRange calls handleFn for each key/value pair in the db within the range [startKey, endKey] in descending order.
func (db *DB) DescendRange(startKey, endKey []byte, handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.DescendRange(startKey, endKey, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// DescendLessOrEqual calls handleFn for each key/value pair in the db with keys less than or equal to the given key.
func (db *DB) DescendLessOrEqual(key []byte, handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.DescendLessOrEqual(key, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// DescendKeys calls handleFn for each key in the db in descending order.
// Since our expiry time is stored in the value, if you want to filter expired keys,
// you need to set parameter filterExpired to true. But the performance will be affected.
// Because we need to read the value of each key to determine if it is expired.
func (db *DB) DescendKeys(pattern []byte, filterExpired bool, handleFn func(k []byte) (bool, error)) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var reg *regexp.Regexp
	if len(pattern) > 0 {
		var err error
		reg, err = regexp.Compile(string(pattern))
		if err != nil {
			return err
		}
	}

	db.index.Descend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		if reg != nil && !reg.Match(key) {
			return true, nil
		}
		if filterExpired {
			chunk, err := db.dataFiles.Read(pos)
			if err != nil {
				return false, err
			}
			if value := db.checkValue(chunk); value == nil {
				return true, nil
			}
		}
		return handleFn(key)
	})
	return nil
}

// DescendKeysRange calls handleFn for keys within a range in the db in descending order.
// Since our expiry time is stored in the value, if you want to filter expired keys,
// you need to set parameter filterExpired to true. But the performance will be affected.
// Because we need to read the value of each key to determine if it is expired.
func (db *DB) DescendKeysRange(startKey, endKey, pattern []byte, filterExpired bool, handleFn func(k []byte) (bool, error)) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var reg *regexp.Regexp
	if len(pattern) > 0 {
		var err error
		reg, err = regexp.Compile(string(pattern))
		if err != nil {
			return err
		}
	}

	db.index.DescendRange(startKey, endKey, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		if reg != nil && !reg.Match(key) {
			return true, nil
		}
		if filterExpired {
			chunk, err := db.dataFiles.Read(pos)
			if err != nil {
				return false, err
			}
			if value := db.checkValue(chunk); value == nil {
				return true, nil
			}
		}
		return handleFn(key)
	})
	return nil
}

// checkValue：检查Key对应的Value是否有效，有效则返回Value，无效则返回nil
func (db *DB) checkValue(chunk []byte) []byte {
	record := decodeLogRecord(chunk)
	now := time.Now().UnixNano()
	if record.Type != LogRecordDeleted && !record.IsExpired(now) {
		return record.Value
	}
	return nil
}

// checkOptions 检查配置项
func checkOptions(options Options) error {
	// 如果数据库目录为空，返回数据库目录为空的错误
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	// 如果段文件大小小于0，返回数据库 数据文件大小必须大于0的错误
	if options.SegmentSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}

	if len(options.AutoMergeCronExpr) > 0 {
		if _, err := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor).
			Parse(options.AutoMergeCronExpr); err != nil {
			return fmt.Errorf("database auto merge cron expression is invalid, err: %s", err)
		}
	}

	return nil
}

// loadIndexFromWAL loads index from WAL.
// It will iterate over all the WAL files and read data
// from them to rebuild the index.

// loadIndexFromWAL 从WAL文件中加载索引
// 迭代所有的WAL文件并从中读取数据以重建索引
func (db *DB) loadIndexFromWAL() error {
	// 从DirPath中获取合并完成标识ID
	mergeFinSegmentId, err := getMergeFinSegmentId(db.options.DirPath)
	if err != nil {
		return err
	}

	// 创建索引记录
	indexRecords := make(map[uint64][]*IndexRecord)
	// 获取当前时间
	now := time.Now().UnixNano()

	// get a reader for WAL
	// 获取一个WAL reader
	reader := db.dataFiles.NewReader()
	// todo: 不知道在干什么
	db.dataFiles.SetIsStartupTraversal(true)
	// 循环读取数据
	for {
		// if the current segment id is less than the mergeFinSegmentId,
		// we can skip this segment because it has been merged,
		// and we can load index from the hint file directly.
		// 如果当前分段 id 小于 mergeFinSegmentId，我们可以跳过该分段，因为它已经被合并，
		// 我们可以直接从提示文件加载索引。
		if reader.CurrentSegmentId() <= mergeFinSegmentId {
			reader.SkipCurrentSegment()
			continue
		}

		// 获取数据块（一条记录）和位置
		chunk, position, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// decode and get log record
		// 对块数据进行解码
		record := decodeLogRecord(chunk)

		// if we get the end of a batch,
		// all records in this batch are ready to be indexed.
		// 如果我们得到的一个批次结束了，那么这个批次中的所有记录都可以被索引了。
		if record.Type == LogRecordBatchFinished {
			// 使用解码出Key中的batchId
			// batchId是使用雪花算法生成的
			batchId, err := snowflake.ParseBytes(record.Key)
			if err != nil {
				return err
			}
			for _, idxRecord := range indexRecords[uint64(batchId)] {
				if idxRecord.recordType == LogRecordNormal {
					db.index.Put(idxRecord.key, idxRecord.position)
				}
				if idxRecord.recordType == LogRecordDeleted {
					db.index.Delete(idxRecord.key)
				}
			}
			// delete indexRecords according to batchId after indexing
			// 在写入内存索引后，根据batchID从indexRecords map中删除对应的indexRecords
			delete(indexRecords, uint64(batchId))
		} else if record.Type == LogRecordNormal && record.BatchId == mergeFinishedBatchID {
			// if the record is a normal record and the batch id is 0,
			// it means that the record is involved in the merge operation.
			// so put the record into index directly.
			// 如果记录类型是普通记录且批次 ID 为 0，表示该记录参与了合并操作
			// 因此，请直接将记录放入索引
			db.index.Put(record.Key, position)
		} else {
			// expired records should not be indexed
			// 过期记录不应该被索引
			if record.IsExpired(now) {
				db.index.Delete(record.Key)
				continue
			}
			// put the record into the temporary indexRecords
			// 将记录放入临时indexRecords数组
			indexRecords[record.BatchId] = append(indexRecords[record.BatchId],
				&IndexRecord{
					key:        record.Key,
					recordType: record.Type,
					position:   position,
				})
		}
	}
	db.dataFiles.SetIsStartupTraversal(false)
	return nil
}

// DeleteExpiredKeys scan the entire index in ascending order to delete expired keys.
// It is a time-consuming operation, so we need to specify a timeout
// to prevent the DB from being unavailable for a long time.
// DeleteExpiredKeys：以升序扫描整个索引，删除过期Key
// 这是一个耗时的操作，因此我们需要指定一个超时时间，以防止数据库长时间不可用。
func (db *DB) DeleteExpiredKeys(timeout time.Duration) error {
	// set timeout
	// 设置超时时间
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	done := make(chan struct{}, 1)

	var innerErr error
	now := time.Now().UnixNano()
	go func(ctx context.Context) {
		db.mu.Lock()
		defer db.mu.Unlock()
		for {
			// select 100 keys from the db.index
			// 从数据库索引中选择100个key
			positions := make([]*wal.ChunkPosition, 0, 100)
			db.index.AscendGreaterOrEqual(db.expiredCursorKey, func(k []byte, pos *wal.ChunkPosition) (bool, error) {
				positions = append(positions, pos)
				if len(positions) >= 100 {
					return false, nil
				}
				return true, nil
			})

			// If keys in the db.index has been traversed, len(positions) will be 0.
			// 如果数据库索引中的键已被遍历，则 len(positions) 将为 0。
			if len(positions) == 0 {
				db.expiredCursorKey = nil
				done <- struct{}{}
				return
			}

			// delete from index if the key is expired.
			// 如果key过期了，从索引中删除
			for _, pos := range positions {
				chunk, err := db.dataFiles.Read(pos)
				if err != nil {
					innerErr = err
					done <- struct{}{}
					return
				}
				record := decodeLogRecord(chunk)
				if record.IsExpired(now) {
					db.index.Delete(record.Key)
				}
				db.expiredCursorKey = record.Key
			}
		}
	}(ctx)

	select {
	case <-ctx.Done():
		return innerErr
	case <-done:
		return innerErr
	}
}
