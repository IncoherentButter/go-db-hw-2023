package godb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"unsafe"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	// TODO: some code goes here
	Hdr    Header
	Desc   *TupleDesc
	PageNo int
	File   *HeapFile
	Tuples []*Tuple
	Dirty  bool
}

type RecordId struct {
	PageNo int
	SlotNo int
}

type Header struct {
	NumSlots     int32
	NumUsedSlots int32
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	// TODO: some code goes here
	bytesPerTuple := 0

	for _, e := range desc.Fields {
		if e.Ftype == StringType {
			bytesPerTuple += ((int)(unsafe.Sizeof(byte('a')))) * StringLength
		} else {
			bytesPerTuple += int(unsafe.Sizeof(int64(0)))
		}
	}

	remPageSize := PageSize - 8
	numSlots := int32(remPageSize / bytesPerTuple)

	return &heapPage{
		Hdr:    Header{NumSlots: numSlots, NumUsedSlots: 0},
		Desc:   desc,
		PageNo: pageNo,
		File:   f,
		Tuples: []*Tuple{},
		Dirty:  false} //replace me
}

func (h *heapPage) getNumSlots() int {
	// TODO: some code goes here
	bytesPerTuple := 0

	for _, e := range h.Desc.Fields {
		if e.Ftype == StringType {
			bytesPerTuple += ((int)(unsafe.Sizeof(byte('a')))) * StringLength
		} else {
			bytesPerTuple += int(unsafe.Sizeof(int64(0)))
		}
	}

	remPageSize := PageSize - 8
	numSlots := remPageSize / bytesPerTuple

	return numSlots
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	// TODO: some code goes here
	rid := RecordId{PageNo: h.PageNo, SlotNo: 0}
	inserted := false

	if h.Hdr.NumUsedSlots >= h.Hdr.NumSlots {
		return rid, errors.New("no free slots")
	}

	for i := range h.Tuples {
		if h.Tuples[i].Rid == "nil" {
			h.Tuples[i] = t
			rid.SlotNo = i
			h.Tuples[i].Rid = rid
			h.Hdr.NumUsedSlots += 1
			inserted = true
			break
		}
	}

	if !inserted {
		h.Tuples = append(h.Tuples, t)
		rid.SlotNo = len(h.Tuples) - 1
		h.Tuples[rid.SlotNo].Rid = rid
		h.Hdr.NumUsedSlots += 1
	}

	return rid, nil //replace me
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {
	// TODO: some code goes here
	deleted := false

	for i := range h.Tuples {
		if h.Tuples[i].Rid == rid {
			h.Tuples[i].Rid = "nil"
			h.Hdr.NumUsedSlots -= 1
			deleted = true
		}
	}

	if !deleted {
		return errors.New("invalid slot")
	}

	return nil //replace me
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	// TODO: some code goes here
	return h.Dirty
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	// TODO: some code goes here
	h.Dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() *DBFile {
	// TODO: some code goes here
	var f DBFile = p.File
	return &f
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	// TODO: some code goes here
	b := new(bytes.Buffer)

	err := binary.Write(b, binary.LittleEndian, h.Hdr)

	for _, e := range h.Tuples {
		e.writeTo(b)
	}

	for b.Len() < PageSize {
		binary.Write(b, binary.LittleEndian, []byte("0"))
	}

	return b, err //replace me

}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	var hdr Header
	err := binary.Read(buf, binary.LittleEndian, &hdr)
	i := 0

	if err != nil {
		panic(err)
	}
	h.Hdr = hdr

	tupleslice := []*Tuple{}

	for {
		tup, err := readTupleFrom(buf, h.Desc)
		tup.Rid = RecordId{PageNo: h.PageNo, SlotNo: i}

		if err == io.EOF || i >= int(hdr.NumUsedSlots) {
			break
		} else if err != nil {
			panic(err)
		}

		tupleslice = append(tupleslice, tup)
		i += 1
	}

	h.Tuples = tupleslice

	return nil //replace me
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	// TODO: some code goes here
	i := 0

	return func() (*Tuple, error) {
		for i < len(p.Tuples) && p.Tuples[i].Rid == "nil" {
			i += 1
		}

		if i >= len(p.Tuples) {
			return nil, nil
		}

		tuple := p.Tuples[i]
		i += 1

		tuple.Rid = RecordId{PageNo: p.PageNo, SlotNo: p.getNumSlots()}

		return tuple, nil
	} //replace me
}
