package rosedb

import (
	"sync"
	"time"
)

type WatchActionType = byte

const (
	WatchActionPut WatchActionType = iota
	WatchActionDelete
)

// Event is the event that occurs when the database is modified.
// It is used to synchronize the watch of the database.
//
// Event 是数据库被修改时发生的事件，它用于同步数据库的监视
type Event struct {
	Action  WatchActionType
	Key     []byte
	Value   []byte
	BatchId uint64
}

// Watcher temporarily stores event information,
// as it is generated until it is synchronized to DB's watch.
//
// If the event is overflow, It will remove the oldest data,
// even if event hasn't been read yet.
//
// 监视器会临时存储事件信息，直到事件信息同步到 DB 的监视器中。
// 如果事件溢出，它将删除最旧的数据，即使事件尚未被读取。
type Watcher struct {
	queue eventQueue
	mu    sync.RWMutex
}

// NewWatcher 创建一个监视器
func NewWatcher(capacity uint64) *Watcher {
	return &Watcher{
		queue: eventQueue{
			Events:   make([]*Event, capacity),
			Capacity: capacity,
		},
	}
}

// putEvent 添加事件
func (w *Watcher) putEvent(e *Event) {
	w.mu.Lock()
	w.queue.push(e)
	if w.queue.isFull() {
		w.queue.frontTakeAStep()
	}
	w.mu.Unlock()
}

// getEvent if queue is empty, it will return nil.
// getEvent 获取事件，如果队列为空将返回nil
func (w *Watcher) getEvent() *Event {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.queue.isEmpty() {
		return nil
	}
	return w.queue.pop()
}

// sendEvent send events to DB's watch
// sendEvent 发送事件，将事件发送给数据库监视器
func (w *Watcher) sendEvent(c chan *Event) {
	for {
		event := w.getEvent()
		if event == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		c <- event
	}
}

// 事件队列（环形）
type eventQueue struct {
	// 事件数组
	Events []*Event
	// 事件队列容量
	Capacity uint64
	// read point
	// 读下标
	Front uint64
	// write point
	// 写下标
	Back uint64
}

// push 添加事件
func (eq *eventQueue) push(e *Event) {
	eq.Events[eq.Back] = e
	eq.Back = (eq.Back + 1) % eq.Capacity
}

// pop 删除事件
func (eq *eventQueue) pop() *Event {
	e := eq.Events[eq.Front]
	eq.frontTakeAStep()
	return e
}

// isFull 判断队列是否已满
func (eq *eventQueue) isFull() bool {
	return (eq.Back+1)%eq.Capacity == eq.Front
}

// isEmpty 判断队列是否为空
func (eq *eventQueue) isEmpty() bool {
	return eq.Back == eq.Front
}

func (eq *eventQueue) frontTakeAStep() {
	eq.Front = (eq.Front + 1) % eq.Capacity
}
