package rosedb

import (
	"bytes"
	"log"
	"time"

	"github.com/rosedblabs/rosedb/v2/index"
)

// Item represents a key-value pair in the database.
// Item代表了数据库中的键值对
type Item struct {
	Key   []byte
	Value []byte
}

// Iterator represents a database-level iterator that
// provides methods to traverse over the key/value pairs in the database.
// It wraps the index iterator and adds functionality to
// retrieve the actual values from the database.
//
// Iterator 代表数据库级别的迭代器，它提供了在数据库中遍历键值对的方法
// 它封装了索引迭代器，并添加了从数据库检索实际值的功能。
type Iterator struct {
	// index iterator for traversing keys
	// 用于遍历key的索引迭代器
	indexIter index.IndexIterator
	// database instance for retrieving values
	// 用于检索值的数据库实例
	db *DB
	// user-defined configuration options
	// 用户自定义的迭代器配置项
	options IteratorOptions
	// stores the last error encountered during iteration
	// 存储迭代过程中遇到的最后一个错误
	lastError error
}

// NewIterator initializes and returns a new database iterator with the specified options.
// The iterator is automatically positioned at the first valid entry.
//
// NewIterator 初始化并返回一个携带指定配置项的数据库迭代器
// 该迭代器自动指向了第一个有效的元素
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	iterator := &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   opts,
	}
	_ = iterator.skipToNext()
	return iterator
}

// Rewind repositions the iterator to its initial state based on the iteration order.
// After repositioning, it automatically skips any invalid or expired entries.
func (it *Iterator) Rewind() {
	if it.db == nil || it.indexIter == nil {
		return
	}
	it.indexIter.Rewind()
}

// Seek positions the iterator at a specific key in the database.
// After seeking, it automatically skips any invalid or expired entries.
func (it *Iterator) Seek(key []byte) {
	if it.db == nil || it.indexIter == nil {
		return
	}
	it.indexIter.Seek(key)
}

// Next advances the iterator to the next valid entry in the database.
func (it *Iterator) Next() {
	if it.db == nil || it.indexIter == nil {
		return
	}
	it.indexIter.Next()
	_ = it.skipToNext()
}

// Valid checks if the iterator is currently positioned at a valid entry.
func (it *Iterator) Valid() bool {
	if it.db == nil || it.indexIter == nil {
		return false
	}
	return it.indexIter.Valid()
}

// Item retrieves the current key-value pair as an Item.
func (it *Iterator) Item() *Item {
	if it.db == nil || it.indexIter == nil || !it.Valid() {
		return nil
	}

	record := it.skipToNext()
	if record == nil {
		return nil
	}
	return &Item{
		Key:   record.Key,
		Value: record.Value,
	}
}

// Close releases all resources associated with the iterator.
func (it *Iterator) Close() {
	if it.db == nil || it.indexIter == nil {
		return
	}

	it.indexIter.Close()
	it.indexIter = nil
	it.db = nil
}

// Err returns the last error encountered during iteration.
func (it *Iterator) Err() error {
	return it.lastError
}

// skipToNext advances the iterator to the next valid entry that satisfies all conditions:
// - Matches the prefix filter if one is specified
// - Has not expired
// - Has not been marked for deletion
// Returns the LogRecord of the valid entry or an error if no valid entry is found.
//
// skipToNext 将迭代器前进到满足所有条件的下一个有效条目：
//   - 如果指定了前缀过滤器，则匹配该过滤器
//   - Key未过期
//   - Key未标记为删除
//
// 返回有效条目的日志记录，如果未找到有效条目，则返回错误信息。
func (it *Iterator) skipToNext() *LogRecord {
	prefixLen := len(it.options.Prefix)

	for it.indexIter.Valid() {
		key := it.indexIter.Key()
		// Check prefix condition if prefix is specified
		// 如果指定了前缀，则检查前缀条件
		// 如果前缀长度大于key的长度或前缀不匹配，则前进到下一个条目
		if prefixLen > 0 {
			if prefixLen > len(key) || !bytes.Equal(it.options.Prefix, key[:prefixLen]) {
				it.indexIter.Next()
				continue
			}
		}

		// 如果pos为空则前进到下一个条目
		position := it.indexIter.Value()
		if position == nil {
			it.indexIter.Next()
			continue
		}

		// read the record from data file
		// 从数据文件中读取记录
		chunk, err := it.db.dataFiles.Read(position)
		if err != nil {
			it.lastError = err
			if !it.options.ContinueOnError {
				it.Close()
				return nil
			}
			log.Printf("Error reading data file at key %q: %v", key, err)
			it.indexIter.Next()
			continue
		}

		// Skip if record is deleted or expired
		// 如果记录被删除或过期，则跳过该记录
		record := decodeLogRecord(chunk)
		now := time.Now().UnixNano()
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			it.indexIter.Next()
			continue
		}

		return record
	}
	return nil
}
