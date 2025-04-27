package rosedb

import (
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Options specifies the options for opening a database.
// Options结构体：定义了创建数据库实例的配置选项
type Options struct {
	// DirPath specifies the directory path where the WAL segment files will be stored.
	// DirPath：存储WAL段文件的目录路径
	DirPath string

	// SegmentSize specifies the maximum size of each segment file in bytes.
	// SegmentSize：每个段文件的最大字节大小
	SegmentSize int64

	// Sync is whether to synchronize writes through os buffer cache and down onto the actual disk.
	// Setting sync is required for durability of a single write operation, but also results in slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (machine does not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.
	//

	// Sync：指是否同步写入到缓冲区缓存和实际磁盘。
	// 设置同步是单次写入操作持久性的需要，但也会导致写入速度变慢。
	// 如果设置为 false，并且机器崩溃，那么最近的一些写入操作可能会丢失。
	// 请注意，如果只是进程崩溃（机器没有崩溃），则不会丢失任何写入数据。
	// 换句话说，同步为假与写入系统调用的语义相同。Sync 为 true 意味着写入后会执行 fsync。
	Sync bool

	// BytesPerSync specifies the number of bytes to write before calling fsync.
	// BytesPerSync：调用 fsync 之前要写入的字节数
	BytesPerSync uint32

	// WatchQueueSize the cache length of the watch queue.
	// if the size greater than 0, which means enable the watch.
	// WatchQueueSize：观察队列的缓存长度，如果该长度大于0，意味着开启观察
	WatchQueueSize uint64

	// AutoMergeEnable enable the auto merge.
	// auto merge will be triggered when cron expr is satisfied.
	// cron expression follows the standard cron expression.
	// e.g. "0 0 * * *" means merge at 00:00:00 every day.
	// it also supports seconds optionally.
	// when enable the second field, the cron expression will be like this: "0/10 * * * * *" (every 10 seconds).
	// when auto merge is enabled, the db will be closed and reopened after merge done.
	// do not set this shecule too frequently, it will affect the performance.
	// refer to https://en.wikipedia.org/wiki/Cron

	// AutoMergeEnable：启用自动合并，当 cron 表达式满足要求时，将触发自动合并。
	// cron 表达式：遵循标准cron 表达式，它可以支持秒级选择
	// 比如："0 0 * * *" 代表 在每天00:00:00执行合并。
	// 当启用第二个字段后，cron 表达式将如下所示： “0/10 * * * *”（每 10 秒一次）。
	// 启用自动合并后，数据库将被关闭，并在合并完成后重新打开。
	// 不要频繁设置该参数，否则会影响性能。
	AutoMergeCronExpr string
}

// BatchOptions specifies the options for creating a batch.
// BatchOptions结构体：定义了创建批次的配置选项
type BatchOptions struct {
	// Sync has the same semantics as Options.Sync.
	// Sync：指是否同步写入到缓冲区缓存和实际磁盘
	Sync bool

	// ReadOnly specifies whether the batch is read only.
	// ReadOnly：决定batch是否是只读的
	ReadOnly bool
}

// IteratorOptions defines configuration options for creating a new iterator.
// IteratorOptions结构体：定义了创建新迭代器的配置选项
type IteratorOptions struct {
	// Prefix specifies a key prefix for filtering. If set, the iterator will only
	// traverse keys that start with this prefix. Default is empty (no filtering).
	// Prefix：定义了用于过滤的key前缀。如果设定了该选项，迭代器只会获取key中包含该前缀的数据。
	// 默认为空，代表不进行过滤
	Prefix []byte

	// Reverse determines the traversal order. If true, the iterator will traverse
	// in descending order. Default is false (ascending order).
	// Reverse：定义了遍历顺序。如果为 “true”，迭代器将按降序遍历，默认为 false（升序）。
	Reverse bool

	// ContinueOnError determines how the iterator handles errors during iteration.
	// If true, the iterator will log errors and continue to the next entry.
	// If false, the iterator will stop and become invalid when an error occurs.
	// ContinueOnError：决定迭代器在迭代过程中如何处理错误。
	// 如果为 true，迭代器将记录错误并继续下一个条目。
	// 如果为 false，迭代器将停止并在发生错误时失效。
	ContinueOnError bool
}

// 字节大小常量定义
const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

// 数据库默认选项
var DefaultOptions = Options{
	DirPath:           tempDBDir(),
	SegmentSize:       1 * GB,
	Sync:              false,
	BytesPerSync:      0,
	WatchQueueSize:    0,
	AutoMergeCronExpr: "",
}

// batch默认选项
var DefaultBatchOptions = BatchOptions{
	Sync:     true,
	ReadOnly: false,
}

// 迭代器默认选项
var DefaultIteratorOptions = IteratorOptions{
	Prefix:          nil,
	Reverse:         false,
	ContinueOnError: false,
}

// 随机数据库名后缀
var nameRand = rand.NewSource(time.Now().UnixNano())

// 临时数据库目录
func tempDBDir() string {
	return filepath.Join(os.TempDir(), "rosedb-temp"+strconv.Itoa(int(nameRand.Int63())))
}
