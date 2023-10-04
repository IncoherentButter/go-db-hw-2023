package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	// "unsafe"
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
	desc     *TupleDesc
	pageNo   int
	file     *HeapFile
	tuples   []*Tuple
	dirtyBit bool
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	// TODO: some code goes here
	fmt.Printf("-----\nnewHeapPage: desc = %v; pageNo = %v\n", desc, pageNo)
	return &heapPage{
		desc:     desc,
		pageNo:   pageNo,
		file:     f,
		tuples:   make([]*Tuple, 0),
		dirtyBit: false,
	}
}

func (h *heapPage) getNumSlots() int {
	// TODO: some code goes here
	// fmt.Printf("-----\nheap_page.getNumSlots(): HeapPage h = %v\n", h)
	remPageSize := PageSize - 8 // bytes after header
	// fmt.Printf("heap_page.getNumSlots(): remPageSize = %v\n", remPageSize)
	bytesPerTuple := h.desc.size()
	// fmt.Printf("heap_page.getNumSlots(): bytesPerTuple = %v\n", bytesPerTuple)
	numSlots := remPageSize / bytesPerTuple //integer division will round down
	
	// var int64sizeof int = int(unsafe.Sizeof(int64(0)))
	// fmt.Printf("heap_page.getNumSlots() StringLength = %v\n", StringLength)
	// fmt.Printf("heap_page.getNumSlots() int64sizeof = %v\n", int64sizeof)
	// fmt.Printf("heap_page.getNumSlots(): numSlots = %v\n------", numSlots)

	return numSlots
}

// bytesPerTuple := len(*h.desc.Fields)

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	// TODO: some code goes here
	numSlots := h.getNumSlots()
	tuplesInHeapPage := len(h.tuples)
	if tuplesInHeapPage >= numSlots {
		return nil, GoDBError{
			code:      PageFullError,
			errString: "No slots in heap page to add tuples",
		}
	}

	for i := 0; i < numSlots; i++ {
		if i >= tuplesInHeapPage || h.tuples[i] == nil {
			// If there are still available slots & all earlier slots are
			// non-nil, append a new tuple. If a slot is nil, replace it
			if i >= tuplesInHeapPage {
				h.tuples = append(h.tuples, t)
			} else {
				h.tuples[i] = t
			}
			rid := &heapRecordId{
				pageNumber: h.pageNo,
				slotNumber: i,
			}
			t.Rid = rid
			return rid, nil

		}
	}
	return nil, GoDBError{
		code:      PageFullError,
		errString: "No slots in heap page to add tuples",
	}
	// for ndx, currentTuple := range h.tuples {
	// 	if currentTuple == nil{
	// 		t.recordID = recordID{PageId: h.pageNo, TupleNumber: ndx}
	// 		h.tuples[ndx] = t
	// 		return t.recordID, nil
	// 	}
	// }
	// return recordID{}, fmt.Errorf("page has no free slots for another tuple") //replace me
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {
	// TODO: some code goes here

	heapRid := rid.(*heapRecordId)
	// ensure that the record id is actually in current page
	if heapRid.getPageNumber() != h.pageNo {
		return fmt.Errorf("recordID page number doesn't match heap page number")
	}

	slotNumber, isValidRID := heapRid.getSlotNumber()
	if !isValidRID {
		return fmt.Errorf("slot number invalid")
	}
	// validate that slot number is in valid range
	// if slotNumber < 0 || slotNumber >= len(h.tuples){
	// 	return fmt.Errorf("slot number is out of range")
	// }

	if slotNumber < len(h.tuples) && h.tuples[slotNumber] != nil {
		// set tuple to nil
		h.tuples[slotNumber] = nil
	} else {
		return fmt.Errorf("no tuple at that slot number that we can delete")
	}

	// if h.tuples[slotNumber] == nil {
	// 	return fmt.Errorf("slot is already nil, has no tuple to delete")
	// }

	// // set slotNumber value to nil
	// h.tuples[slotNumber] = nil
	return nil
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	// TODO: some code goes here
	return h.dirtyBit
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	// TODO: some code goes here
	h.dirtyBit = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() *DBFile {
	// TODO: some code goes here
	// page := newHeapPage(p.desc, p.pageNo, p.file)
	file := &HeapFile{
		bufPool:   p.file.bufPool,
		fileName:  p.file.fileName,
		tupleDesc: p.file.tupleDesc,
		numPages:  p.file.numPages,
	}
	var dbFile DBFile = file
	return &dbFile
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	// TODO: some code goes here

	// create buffer to hold data
	buffer := new(bytes.Buffer)
	numSlots := h.getNumSlots()

	// write in the total number of slots
	if totalSlotsWriteErr := binary.Write(buffer, binary.LittleEndian, int32(numSlots)); totalSlotsWriteErr != nil {
		return nil, totalSlotsWriteErr
	}

	// write in the number of occupied slots
	usedSlots := len(h.tuples)
	if usedWritesErr := binary.Write(buffer, binary.LittleEndian, int32(usedSlots)); usedWritesErr != nil {
		return nil, usedWritesErr
	}

	for _, tuple := range h.tuples {
		if tupleWriteErr := tuple.writeTo(buffer); tupleWriteErr != nil {
			return nil, tupleWriteErr
		}
		if tuple != nil {
			usedSlots++
		}
	}
	return buffer, nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	// TODO: some code goes here
	var totalSlots int32
	var usedSlots int32

	if err := binary.Read(buf, binary.LittleEndian, &totalSlots); err != nil {
		return err
	}

	if err2 := binary.Read(buf, binary.LittleEndian, &usedSlots); err2 != nil {
		return err2
	}

	h.tuples = make([]*Tuple, totalSlots)

	for i := int32(0); i < usedSlots; i++ {
		tuple, tupleReadErr := readTupleFrom(buf, h.desc)
		if tupleReadErr != nil {
			return tupleReadErr
		}
		h.tuples[i] = tuple
	}
	return nil

	// return nil //replace me
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	// TODO: some code goes here
	ndx := 0
	iteratorFunc := func() (*Tuple, error) {
		// if we've passed all tuples, return nil
		if ndx >= len(p.tuples) {
			return nil, nil
		}

		// tuple for current index
		tuple := p.tuples[ndx]

		if tuple == nil {return nil, nil}

		// set RID for tuple
		tuple.Rid = &heapRecordId{
			pageNumber: p.pageNo,
			slotNumber: ndx,
		}

		ndx++
		return tuple, nil

		// for ndx < len(p.tuples) {
		// 	currentTup := p.tuples[ndx]
		// 	ndx++
		// 	if currentTup != nil {
		// 		return currentTup, nil // if current tuple of iterator is non-null, return it
		// 	}
		// }
	}
	return iteratorFunc
}
