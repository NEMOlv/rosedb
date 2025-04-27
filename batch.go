package rosedb

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/rosedblabs/rosedb/v2/utils"
	"github.com/valyala/bytebufferpool"
)

// Batch is a batch operations of the database.
// If readonly is true, you can only get data from the batch by Get method.
// An error will be returned if you try to use Put or Delete method.
//
// If readonly is false, you can use Put and Delete method to write data to the batch.
// The data will be written to the database permanently after you call Commit method.
//
// NB. There can only one write batch and multi read-only batches at the same time.
// And the db method is not allowed to use before the batch commit/rollback.
// So a typical usage of Batch is like:
//
// batch := db.NewBatch(rosedb.DefaultBatchOptions)
// batch.Put/batch.Get (and other methods)
// /* 1. a new write batch is not allowed */
// /* 2. invoke DB method is not allowed, like db.Put */
// batch.Commit() or batch.Rollback()
//
// Batch is not a transaction, it does not guarantee isolation.
// But it can guarantee atomicity, consistency and durability(if the Sync options is true).
//
// You must call Commit or Rollback method after using the batch,
// otherwise the DB will be locked in an unexpected way.

// Batch 是一个对数据库的批处理操作
// 如果readonly是true，你只可以用Get方法从batch中获取数据，如果你尝试使用Put或Delete方法将会返回错误
//
// 如果readonly是false，你可以使用Put和Delete方法向batch写入数据
// 数据将会在你调用Commti方法后写入数据库
//
// 注意：同一时间只能有一个写入批次和多个只读批次
// 并且db方法在batch提交/回滚前不被允许使用
//
// 因此，批处理的典型用法如下：
// batch := db.NewBatch(rosedb.DefaultBatchOptions)
// batch.Put/batch.Get (and other methods)
// /* 1. a new write batch is not allowed */
// /* 2. invoke DB method is not allowed, like db.Put */
// batch.Commit() or batch.Rollback()
//
// 批处理不是事务，它不保证隔离性
// 但它可以保证原子性、一致性和持久性（如果同步选项为 true）。
//
// 你必须在使用batch后调用Commit或Rollback方法，否则数据库将会被意外锁定。

// Batch结构体：用于执行批处理操作
type Batch struct {
	// 数据库实例
	db *DB
	// save the data to be written
	// 保存将要写入的数据
	pendingWrites []*LogRecord
	// map record hash key to index, fast lookup to pendingWrites
	// 将记录哈希键映射到索引，快速查找待写内容
	pendingWritesMap map[uint64][]int
	// 批处理配置项
	options BatchOptions
	// 读写锁
	mu sync.RWMutex
	// whether the batch has been committed
	//  标识batch是否提交
	committed bool
	// whether the batch has been rollbacked
	// 标识batch是否回滚
	rollbacked bool
	// 批次ID，由雪花算法提供
	batchId *snowflake.Node
	// 缓冲池
	buffers []*bytebufferpool.ByteBuffer
}

// NewBatch creates a new Batch instance.
// NewBatch 创建一个新的Batch实例
func (db *DB) NewBatch(options BatchOptions) *Batch {
	// batch实例
	batch := &Batch{
		db:         db,
		options:    options,
		committed:  false,
		rollbacked: false,
	}

	// 如果是写操作，用雪花算法给batchId赋值
	if !options.ReadOnly {
		node, err := snowflake.NewNode(1)
		if err != nil {
			panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
		}
		batch.batchId = node
	}

	// 为数据库实例加锁，读操作加可重入锁，写操作加不可重入锁
	batch.lock()
	return batch
}

// newBatch：创建Batch实例
func newBatch() interface{} {
	node, err := snowflake.NewNode(1)
	if err != nil {
		panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
	}
	return &Batch{
		options: DefaultBatchOptions,
		batchId: node,
	}
}

// newRecord：创建LogRecord实例
func newRecord() interface{} {
	return &LogRecord{}
}

// init：初始化batch实例
// 主要是为batch池中的batch提供的
func (b *Batch) init(rdonly, sync bool, db *DB) {
	b.options.ReadOnly = rdonly
	b.options.Sync = sync
	b.db = db
	b.lock()
}

// reset：重置batch实例
func (b *Batch) reset() {
	b.db = nil
	b.pendingWrites = b.pendingWrites[:0]
	b.pendingWritesMap = nil
	b.committed = false
	b.rollbacked = false
	// put all buffers back to the pool
	// 将所有缓存返回缓冲池
	for _, buf := range b.buffers {
		bytebufferpool.Put(buf)
	}
	b.buffers = b.buffers[:0]
}

// lock：给数据库加锁，如果是读操作加可重复锁，否则加不可重入锁
func (b *Batch) lock() {
	if b.options.ReadOnly {
		b.db.mu.RLock()
	} else {
		b.db.mu.Lock()
	}
}

// unlock：给数据库解锁
func (b *Batch) unlock() {
	if b.options.ReadOnly {
		b.db.mu.RUnlock()
	} else {
		b.db.mu.Unlock()
	}
}

// Put adds a key-value pair to the batch for writing.
// Put 将键值对添加到批次中，以便写入。
func (b *Batch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	// 批处理加锁
	b.mu.Lock()
	// write to pendingWrites
	// 写入待写数组
	var record = b.lookupPendingWrites(key)
	if record == nil {
		// if the key does not exist in pendingWrites, write a new record
		// the record will be put back to the pool when the batch is committed or rollbacked
		// 如果key在pendingWrites中不存在，写一个新的record
		// 该record在提交或回滚后会被放回缓冲池
		record = b.db.recordPool.Get().(*LogRecord)
		b.appendPendingWrites(key, record)
	}

	// appendPendingWrites传入的是record指针，因此后续再传值，pendingWrites亦然能获取相关数据
	record.Key, record.Value = key, value
	record.Type, record.Expire = LogRecordNormal, 0

	// 批处理解锁
	b.mu.Unlock()

	return nil
}

// PutWithTTL adds a key-value pair with ttl to the batch for writing.
// PutWithTTL 将带 生命周期(过期时间)的键值对添加到批次中，以便写入。
func (b *Batch) PutWithTTL(key []byte, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	// write to pendingWrites
	var record = b.lookupPendingWrites(key)
	if record == nil {
		// if the key does not exist in pendingWrites, write a new record
		// the record will be put back to the pool when the batch is committed or rollbacked
		record = b.db.recordPool.Get().(*LogRecord)
		b.appendPendingWrites(key, record)
	}

	record.Key, record.Value = key, value
	record.Type, record.Expire = LogRecordNormal, time.Now().Add(ttl).UnixNano()
	b.mu.Unlock()

	return nil
}

// Get retrieves the value associated with a given key from the batch.
// Get 从批次中检索给定Key对应的值。
func (b *Batch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	if b.db.closed {
		return nil, ErrDBClosed
	}

	now := time.Now().UnixNano()
	// todo：get key/value from pendingWrites
	// get from pendingWrites
	// 从代写数组中获取
	b.mu.RLock()
	var record = b.lookupPendingWrites(key)
	b.mu.RUnlock()

	// if the record is in pendingWrites, return the value directly
	// 如果record在代写数组中，直接返回值
	if record != nil {
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			return nil, ErrKeyNotFound
		}
		return record.Value, nil
	}

	// get key/value from data file
	// 从数据文件中获取键值对
	// 通过内存索引获取chunk位置
	chunkPosition := b.db.index.Get(key)
	// 如果chunk位置为空，说明没有这个key，返回ErrKeyNotFound
	if chunkPosition == nil {
		return nil, ErrKeyNotFound
	}
	// 根据chunk位置读出对应的chunk
	chunk, err := b.db.dataFiles.Read(chunkPosition)
	if err != nil {
		return nil, err
	}

	// check if the record is deleted or expired
	// 检查record是否已经被删除或过期
	record = decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted {
		panic("Deleted data cannot exist in the index")
	}
	if record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return nil, ErrKeyNotFound
	}
	// 返回值
	return record.Value, nil
}

// Delete marks a key for deletion in the batch.
// Delete：在batch中标识指定的key已经被删除
func (b *Batch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	// only need key and type when deleting a value.
	// 删除数据时，只需要key和记录类型
	var exist bool
	var record = b.lookupPendingWrites(key)
	if record != nil {
		record.Type = LogRecordDeleted
		record.Value = nil
		record.Expire = 0
		exist = true
	}

	// 如果代写数组中不存在，则新建一个del record
	if !exist {
		record = &LogRecord{
			Key:  key,
			Type: LogRecordDeleted,
		}
		b.appendPendingWrites(key, record)
	}
	b.mu.Unlock()

	return nil
}

// Exist checks if the key exists in the database.
// Exist 检查数据库中是否存在该key
func (b *Batch) Exist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	if b.db.closed {
		return false, ErrDBClosed
	}

	now := time.Now().UnixNano()
	// check if the key exists in pendingWrites
	// 检查key是否存在于待写数组
	b.mu.RLock()
	var record = b.lookupPendingWrites(key)
	b.mu.RUnlock()

	// 如果存在，则判断是否该Key是否被删除或过期，将判断结果返回
	if record != nil {
		return record.Type != LogRecordDeleted && !record.IsExpired(now), nil
	}

	// check if the key exists in index
	// 检查key是否存在于内存索引中
	position := b.db.index.Get(key)
	if position == nil {
		return false, nil
	}

	// check if the record is deleted or expired
	// 检查记录是否被删除或过期
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return false, err
	}

	record = decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted || record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return false, nil
	}
	return true, nil
}

// Expire sets the ttl of the key.
// Expire 为key设置生命周期(过期时间)
func (b *Batch) Expire(key []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	var record = b.lookupPendingWrites(key)

	// if the key exists in pendingWrites, update the expiry time directly
	// 如果key在待写数组中存在，直接更新该key的生命周期(过期时间)
	if record != nil {
		// return key not found if the record is deleted or expired
		// 如果记录被删除或过期，则返回ErrKeyNotFound
		if record.Type == LogRecordDeleted || record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
		record.Expire = time.Now().Add(ttl).UnixNano()
		return nil
	}

	// if the key does not exist in pendingWrites, get the value from wal
	// 如果key在待写数组中不存在，从WAL文件中获取值
	position := b.db.index.Get(key)
	if position == nil {
		return ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return err
	}

	now := time.Now()
	record = decodeLogRecord(chunk)
	// if the record is deleted or expired, we can assume that the key does not exist,
	// and delete the key from the index
	// 如果记录被删除或过期，我们可以假设该key不存在，并从内存索引中删除该key
	if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
		b.db.index.Delete(key)
		return ErrKeyNotFound
	}
	// now we get the value from wal, update the expiry time
	// and rewrite the record to pendingWrites
	// 现在我们从WAL文件中获取了值，更新过期时间(生命周期)并将该记录再次写入待写数组
	record.Expire = now.Add(ttl).UnixNano()
	b.appendPendingWrites(key, record)

	return nil
}

// TTL returns the ttl of the key.
// TTL 返回key的生命周期(过期时间)
func (b *Batch) TTL(key []byte) (time.Duration, error) {
	if len(key) == 0 {
		return -1, ErrKeyIsEmpty
	}
	if b.db.closed {
		return -1, ErrDBClosed
	}

	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()

	var record = b.lookupPendingWrites(key)
	if record != nil {
		// record没有生命周期(过期时间)，返回-1
		if record.Expire == 0 {
			return -1, nil
		}
		// return key not found if the record is deleted or expired
		//如果记录被删除或过期，返回ErrKeyNotFound
		if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
			return -1, ErrKeyNotFound
		}
		// now we get the valid expiry time, we can calculate the ttl
		// TTL = 过期时间-当前时间
		return time.Duration(record.Expire - now.UnixNano()), nil
	}

	// if the key does not exist in pendingWrites, get the value from wal
	// 如果在待写数组中不存在key，从WAL文件中获取值
	position := b.db.index.Get(key)
	if position == nil {
		return -1, ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return -1, err
	}

	// return key not found if the record is deleted or expired
	// 如果记录被删除或过期，返回ErrKeyNotFound
	record = decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted {
		return -1, ErrKeyNotFound
	}
	if record.IsExpired(now.UnixNano()) {
		b.db.index.Delete(key)
		return -1, ErrKeyNotFound
	}

	// now we get the valid expiry time, we can calculate the ttl
	// 现在我们获得了有效过期时间，我们可以计算TTL
	if record.Expire > 0 {
		return time.Duration(record.Expire - now.UnixNano()), nil
	}

	return -1, nil
}

// Persist removes the ttl of the key.
// Persist：去除key的存活时间
func (b *Batch) Persist(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// if the key exists in pendingWrites, update the expiry time directly
	// 如果在待写数组中存在key，直接更新过期时间
	var record = b.lookupPendingWrites(key)
	if record != nil {
		if record.Type == LogRecordDeleted && record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
		record.Expire = 0
		return nil
	}

	// check if the key exists in index
	// 检查key是否存在于内存索引中
	position := b.db.index.Get(key)
	if position == nil {
		return ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return err
	}

	record = decodeLogRecord(chunk)
	now := time.Now().UnixNano()
	// check if the record is deleted or expired
	// 检查记录是否被删除或过期
	if record.Type == LogRecordDeleted || record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return ErrKeyNotFound
	}
	// if the expiration time is 0, it means that the key has no expiration time,
	// so we can return directly
	// 如果过期时间为0，代表key没有过期时间，所以我们可以直接返回
	if record.Expire == 0 {
		return nil
	}

	// todo：set the expiration time to 0, and rewrite the record to PendingWrites
	// set the expiration time to 0, and rewrite the record to wal
	// 将过期时间设为0，并且将记录重新写入待写文件
	record.Expire = 0
	b.appendPendingWrites(key, record)

	return nil
}

// Commit commits the batch, if the batch is readonly or empty, it will return directly.
//
// It will iterate the pendingWrites and write the data to the database,
// then write a record to indicate the end of the batch to guarantee atomicity.
// Finally, it will write the index.

// Commit：提交批处理，如果是只读批次或批次为空，则直接返回
// 它将遍历待写文件，并将数据写入数据库，然后写入一条批处理结束记录，以保证原子性
// 最后，它将写入内存
func (b *Batch) Commit() error {
	// 此处释放数据库实例锁的原因是，创建或初始化时已对数据库加锁
	// 此时要先把数据库的锁释放掉，才能让其他进程使用数据库
	defer b.unlock()
	if b.db.closed {
		return ErrDBClosed
	}

	if b.options.ReadOnly || len(b.pendingWrites) == 0 {
		return nil
	}

	// 为数据库实例加锁
	b.mu.Lock()
	defer b.mu.Unlock()

	// check if committed or rollbacked
	// 检查是否提交或回滚
	if b.committed {
		return ErrBatchCommitted
	}
	if b.rollbacked {
		return ErrBatchRollbacked
	}

	// 生成batchId
	batchId := b.batchId.Generate()
	// 获取当前时间
	now := time.Now().UnixNano()
	// write to wal buffer
	// 写入WAL缓存
	// 每次都要从缓冲池中获取一个缓存变量的原因是什么？此处已经加锁，按理说是单线程
	for _, record := range b.pendingWrites {
		buf := bytebufferpool.Get()
		b.buffers = append(b.buffers, buf)
		record.BatchId = uint64(batchId)
		encRecord := encodeLogRecord(record, b.db.encodeHeader, buf)
		b.db.dataFiles.PendingWrites(encRecord)
	}

	// write a record to indicate the end of the batch
	// 写入一条标识批处理结束的记录
	buf := bytebufferpool.Get()
	b.buffers = append(b.buffers, buf)
	endRecord := encodeLogRecord(&LogRecord{
		Key:  batchId.Bytes(),
		Type: LogRecordBatchFinished,
	}, b.db.encodeHeader, buf)
	b.db.dataFiles.PendingWrites(endRecord)

	// write to wal file
	// 写入WAL文件
	chunkPositions, err := b.db.dataFiles.WriteAll()
	if err != nil {
		b.db.dataFiles.ClearPendingWrites()
		return err
	}
	if len(chunkPositions) != len(b.pendingWrites)+1 {
		panic("chunk positions length is not equal to pending writes length")
	}

	// flush wal if necessary
	// 如有必要，刷写WAL文件
	if b.options.Sync && !b.db.options.Sync {
		if err := b.db.dataFiles.Sync(); err != nil {
			return err
		}
	}

	// write to index
	// 写入内存索引
	for i, record := range b.pendingWrites {
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			b.db.index.Delete(record.Key)
		} else {
			b.db.index.Put(record.Key, chunkPositions[i])
		}

		if b.db.options.WatchQueueSize > 0 {
			e := &Event{Key: record.Key, Value: record.Value, BatchId: record.BatchId}
			if record.Type == LogRecordDeleted {
				e.Action = WatchActionDelete
			} else {
				e.Action = WatchActionPut
			}
			b.db.watcher.putEvent(e)
		}
		// put the record back to the pool
		b.db.recordPool.Put(record)
	}

	b.committed = true
	return nil
}

// Rollback discards an uncommitted batch instance.
// the discard operation will clear the buffered data and release the lock.

// Rollback：丢弃未提交的批处理实例
// 该丢弃操作会清空缓存数据并释放锁
func (b *Batch) Rollback() error {
	defer b.unlock()

	if b.db.closed {
		return ErrDBClosed
	}

	if b.committed {
		return ErrBatchCommitted
	}
	if b.rollbacked {
		return ErrBatchRollbacked
	}

	for _, buf := range b.buffers {
		bytebufferpool.Put(buf)
	}

	if !b.options.ReadOnly {
		// clear pendingWrites
		// 清理待写数组
		for _, record := range b.pendingWrites {
			b.db.recordPool.Put(record)
		}
		b.pendingWrites = b.pendingWrites[:0]
		for key := range b.pendingWritesMap {
			delete(b.pendingWritesMap, key)
		}
	}

	b.rollbacked = true
	return nil
}

// lookupPendingWrites if the key exists in pendingWrites, update the value directly
// lookupPendingWrites 如果key在待写数组中存在，直接更新值
func (b *Batch) lookupPendingWrites(key []byte) *LogRecord {
	// 如果待写数组为空，直接返回
	if len(b.pendingWritesMap) == 0 {
		return nil
	}

	// 获取key的hashKey
	hashKey := utils.MemHash(key)
	// 从pendingWritesMap的某一hashKey下存储的下标中，寻找和指定key相同的记录
	for _, entry := range b.pendingWritesMap[hashKey] {
		// 如果下标中取出的key和要寻找的key相等则返回
		if bytes.Equal(b.pendingWrites[entry].Key, key) {
			return b.pendingWrites[entry]
		}
	}
	return nil
}

// add new record to pendingWrites and pendingWritesMap.
// 将新record添加进pendingWrites和pendingWritesMap
func (b *Batch) appendPendingWrites(key []byte, record *LogRecord) {
	b.pendingWrites = append(b.pendingWrites, record)
	if b.pendingWritesMap == nil {
		b.pendingWritesMap = make(map[uint64][]int)
	}
	hashKey := utils.MemHash(key)
	// 构建hashKey和pendingWrites下标的关系
	// pendingWritesMap的每个hashKey中，存储了一系列的的pendingWrites下标
	b.pendingWritesMap[hashKey] = append(b.pendingWritesMap[hashKey], len(b.pendingWrites)-1)
}
