package chunkenc

import (
	"sync/atomic"
	"unsafe"
)

type sample struct {
	t int64
	v []byte
}

type element struct {
	// Next and previous pointers in the doubly-linked list of elements.
	// To simplify the implementation, internally a list l is implemented
	// as a ring, such that &l.root is both the next element of the last
	// list element (l.Back()) and the previous element of the first list
	// element (l.Front()).
	next, prev *element

	// The list to which this element belongs.
	list *sampleList

	// The value stored with this element.
	Value sample
}

// Next returns the next list element or nil.
func (e *element) Next() *element {
	p := (*element)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&e.next))))

	if e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

func (e *element) nextElem() *element {
	p := e.next

	if e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

// Prev returns the previous list element or nil.
func (e *element) Prev() *element {
	p := (*element)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&e.prev))))

	if e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

func (e *element) prevElem() *element {
	p := e.prev

	if e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

// sampleList represents a doubly linked list.
// The zero value for sampleList is an empty list ready to use.
type sampleList struct {
	root element // sentinel list element, only &root, root.prev, and root.next are used
}

// Init initializes or clears list l.
func (l *sampleList) Init() *sampleList {
	l.root.next = &l.root
	l.root.prev = &l.root
	return l
}

// New returns an initialized list.
func newList() *sampleList { return new(sampleList).Init() }

// Front returns the first element of list l or nil if the list is empty.
func (l *sampleList) First() (e *element) {
	p := (*element)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.root.next))))

	if p == &l.root {
		return nil
	}
	return p
}

func (l *sampleList) firstElem() (e *element) {
	if l.root.next == &l.root {
		return nil
	}
	return l.root.next
}

// Back returns the last element of list l or nil if the list is empty.
func (l *sampleList) Last() *element {
	p := (*element)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.root.prev))))

	if p == &l.root {
		return nil
	}
	return p
}

func (l *sampleList) lastElem() *element {
	if l.root.prev == &l.root {
		return nil
	}
	return l.root.prev
}

// insert inserts e after at, increments l.len, and returns e.
func (l *sampleList) insertAt(e, at *element) *element {
	e.list = l

	n := at.next

	e.prev = at
	e.next = n

	n.prev = e  //atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&n.prev)), unsafe.Pointer(e))
	at.next = e //atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&at.next)), unsafe.Pointer(e))

	return e
}

func (l *sampleList) Insert(s sample) *element {
	for e := l.lastElem(); e != nil; e = e.prevElem() {
		if e.Value.t <= s.t {
			return l.insertAt(&element{Value: s}, e)
		}
	}
	return l.insertAt(&element{Value: s}, &l.root)
}
