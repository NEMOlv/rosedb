package rosedb

import (
	"encoding/binary"
	"github.com/rosedblabs/wal"
	"github.com/valyala/bytebufferpool"
)

// LogRecordType is the type of the log record.
// LogRecordType 是 LogRecord 的类型
type LogRecordType = byte

const (
	// LogRecordNormal is the normal log record type.
	// PUT类型
	LogRecordNormal LogRecordType = iota
	// LogRecordDeleted is the deleted log record type.
	// DELETE类型
	LogRecordDeleted
	// LogRecordBatchFinished is the batch finished log record type.
	// 批量写入完成类型/ 事务写入完成类型
	LogRecordBatchFinished
)

// type batchId keySize valueSize expire
//
//	1  +  10  +   5   +   5   +    10  = 31
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + binary.MaxVarintLen64*2 + 1

// LogRecord is the log record of the key/value pair.
// It contains the key, the value, the record type and the batch id
// It will be encoded to byte slice and written to the wal.
// LogRecord是键值对(Key-Value)的日志记录
// LogRecord包括：键(key)、值(value)、类型(Type)、批次ID(BatchId)、过期时间(Expire)
// LogRecord会在编码为字节切片后写入WAL文件
type LogRecord struct {
	Key     []byte
	Value   []byte
	Type    LogRecordType
	BatchId uint64
	Expire  int64
}

// IsExpired checks whether the log record is expired.
// IsExpired方法：检查LogRecord是否过期
func (lr *LogRecord) IsExpired(now int64) bool {
	return lr.Expire > 0 && lr.Expire <= now
}

// IndexRecord is the index record of the key.
// It contains the key, the record type and the position of the record in the wal.
// Only used in start up to rebuild the index.
// IndexRecord 是 key的索引记录
// IndexRecord包括：键(key)、类型(recordType)、位置(position)
// IndexRecord仅在重建索引时使用
type IndexRecord struct {
	key        []byte
	recordType LogRecordType
	position   *wal.ChunkPosition
}

// +-------------+-------------+-------------+--------------+---------------+---------+--------------+
// |    type     |  batch id   |   key size  |   value size |     expire    |  key    |      value   |
// +-------------+-------------+-------------+--------------+---------------+--------+--------------+
//
//	1 byte	      varint(max 10) varint(max 5)  varint(max 5) varint(max 10)  varint      varint

// encodeLogRecord函数：将logRecord编码成字节数组
func encodeLogRecord(logRecord *LogRecord, header []byte, buf *bytebufferpool.ByteBuffer) []byte {
	// type
	header[0] = logRecord.Type
	var index = 1

	// batch id
	index += binary.PutUvarint(header[index:], logRecord.BatchId)
	// key size
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	// value size
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	// expire
	index += binary.PutVarint(header[index:], logRecord.Expire)

	// 缓冲写入ByteBuffer
	// copy header
	_, _ = buf.Write(header[:index])
	// copy key
	_, _ = buf.Write(logRecord.Key)
	// copy value
	_, _ = buf.Write(logRecord.Value)

	// 返回LogRecord编码后的字节数组
	return buf.Bytes()
}

// decodeLogRecord decodes the log record from the given byte slice.
// decodeLogRecord函数：从字节数组中解码出LogRecord
func decodeLogRecord(buf []byte) *LogRecord {
	// 解码出LogRecordType
	recordType := buf[0]
	var index uint32 = 1

	// batch id
	batchId, n := binary.Uvarint(buf[index:])
	index += uint32(n)

	// key size
	keySize, n := binary.Varint(buf[index:])
	index += uint32(n)

	// value size
	valueSize, n := binary.Varint(buf[index:])
	index += uint32(n)

	// expire
	expire, n := binary.Varint(buf[index:])
	index += uint32(n)

	// key
	key := make([]byte, keySize)
	copy(key[:], buf[index:index+uint32(keySize)])
	index += uint32(keySize)

	// value
	value := make([]byte, valueSize)
	copy(value[:], buf[index:index+uint32(valueSize)])

	return &LogRecord{
		Key:     key,
		Value:   value,
		Expire:  expire,
		BatchId: batchId,
		Type:    recordType,
	}
}

// SegmentId BlockNumber ChunkOffset ChunkSize
//
//	5     +     5     +    10     +    5      =    25
//
// see binary.MaxVarintLen64 and binary.MaxVarintLen32
const maxHintRecordHeaderSize = binary.MaxVarintLen32*3 + binary.MaxVarintLen64

// encodeHintRecord函数:将HintRecord编码为字节数组
func encodeHintRecord(key []byte, pos *wal.ChunkPosition) []byte {
	buf := make([]byte, maxHintRecordHeaderSize)
	var idx = 0

	// SegmentId
	idx += binary.PutUvarint(buf[idx:], uint64(pos.SegmentId))
	// BlockNumber
	idx += binary.PutUvarint(buf[idx:], uint64(pos.BlockNumber))
	// ChunkOffset
	idx += binary.PutUvarint(buf[idx:], uint64(pos.ChunkOffset))
	// ChunkSize
	idx += binary.PutUvarint(buf[idx:], uint64(pos.ChunkSize))

	// key
	result := make([]byte, idx+len(key))
	copy(result, buf[:idx])
	copy(result[idx:], key)
	return result
}

// decodeHintRecord函数:将字节数组解码为HintRecord
func decodeHintRecord(buf []byte) ([]byte, *wal.ChunkPosition) {
	var idx = 0
	// SegmentId
	segmentId, n := binary.Uvarint(buf[idx:])
	idx += n
	// BlockNumber
	blockNumber, n := binary.Uvarint(buf[idx:])
	idx += n
	// ChunkOffset
	chunkOffset, n := binary.Uvarint(buf[idx:])
	idx += n
	// ChunkSize
	chunkSize, n := binary.Uvarint(buf[idx:])
	idx += n
	// Key
	key := buf[idx:]

	return key, &wal.ChunkPosition{
		SegmentId:   wal.SegmentID(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}

// encodeMergeFinRecord函数：将MergeFinRecord编码为字节数组
func encodeMergeFinRecord(segmentId wal.SegmentID) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, segmentId)
	return buf
}
