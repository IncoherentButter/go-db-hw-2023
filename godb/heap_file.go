package godb

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// HeapFile is an unordered collection of tuples Internally, it is arranged as a
// set of heapPage objects
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	// TODO: some code goes here
	// HeapFile should include the fields below;  you may want to add
	// additional fields
	bufPool *BufferPool
	sync.Mutex

	file      *os.File
	fileName  string
	tupleDesc *TupleDesc
	numPages  int
}

type heapRecordId struct {
	pageNumber int
	slotNumber int
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fileName string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	// TODO: some code goes here
	file, openFileErr := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if openFileErr != nil {
		return nil, openFileErr
	}
	// defer file.Close()

	fileInfo, getFileInfoErr := file.Stat()
	if getFileInfoErr != nil {
		return nil, getFileInfoErr
	}

	fileInfoSize := fileInfo.Size()
	numPages := int(fileInfoSize / int64(PageSize))

	return &HeapFile{
		bufPool:   bp,
		fileName:  fileName,
		file:      file,
		tupleDesc: td,
		numPages:  numPages,
	}, nil
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	// TODO: some code goes here
	return f.numPages
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] is implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		f.insertTuple(&newT, tid)

		// hack to force dirty pages to disk
		// because CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2
		for j := 0; j < f.NumPages(); j++ {
			pg, err := bp.GetPage(f, j, tid, 0)
			if pg == nil || err != nil {
				fmt.Println("page nil or error", err)
				break
			}
			if (*pg).isDirty() {
				(*f).flushPage(pg)
				(*pg).setDirty(false)
			}

		}

		//commit frequently, to avoid all pages in BP being full
		//todo fix
		bp.CommitTransaction(tid)
	}
	return nil
}

// Read the specified page number from the HeapFile on disk.  This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to the
// appropriate offset, read the bytes in, and construct a [heapPage] object, using
// the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (*Page, error) {
	// TODO: some code goes here
	file, fileOpenErr := os.Open(f.fileName)
	if fileOpenErr != nil {
		return nil, fileOpenErr
	}	
	defer file.Close()
	fileInfo, getFileInfoErr := file.Stat()
	if getFileInfoErr != nil {
			return nil, fmt.Errorf("error getting file info:\n%v", getFileInfoErr)
	}
	if fileInfo.Size() == 0 {
			return nil, fmt.Errorf("File is empty")
	}

	// find offset of desired page
	offset := int64(pageNo) * int64(PageSize)


	heapPage := newHeapPage(f.tupleDesc, pageNo, f)
	// read file bytes into buffer
	buffer := make([]byte, PageSize)
	_, fileReadErr := file.ReadAt(buffer, offset)
	if fileReadErr != nil {
		return nil, fileReadErr
	}
	bytesBuffer := bytes.NewBuffer(buffer)

	// read page in from buffer
	bufferInitErr := heapPage.initFromBuffer(bytesBuffer)
	if bufferInitErr != nil {
		return nil, bufferInitErr
	}

	var page Page = heapPage
	return &page, nil
}

// Method to force the specified page back to the backing file at the appropriate
// location.  This will be called by BufferPool when it wants to evict a page.
// The Page object should store information about its offset on disk (e.g.,
// that it is the ith page in the heap file), so you can determine where to write it
// back.
func (f *HeapFile) flushPage(page *Page) error {
	// TODO: some code goes here
	// seek to desired writing offset in Backing File
	var actualPage Page = *page
	heapPage := actualPage.(*heapPage)

	// create buffer to serialize tuples from the page into
	buffer := new(bytes.Buffer)

	// serialize the page content by tuple to the buffer
	for _, tuple := range heapPage.tuples {
		if tuple != nil {
			tupleWritingErr := tuple.writeTo(buffer)
			if tupleWritingErr != nil {
				return tupleWritingErr
			}
		}
	}

	// write the buffer to the file

	offset := int64(heapPage.pageNo * PageSize)
	_, bufferWriteError := f.file.WriteAt(buffer.Bytes(), offset)
	if bufferWriteError != nil {
		return bufferWriteError
	}

	// mark page as non-dirty since it's been flushed
	heapPage.setDirty(false)
	return nil
}

// Add the tuple to the HeapFile.  This method should search through pages in
// the heap file, looking for empty slots and adding the tuple in the first
// empty slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile.  We will
// add support for concurrent modifications in lab 3.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	// TODO: some code goes here
	
	if f.numPages == 0 {
		brandNewHeapPage := newHeapPage(f.tupleDesc, f.numPages, f)
		_, brandNewHeapPageTupleInsertError := brandNewHeapPage.insertTuple(t)
		if brandNewHeapPageTupleInsertError != nil{
			return brandNewHeapPageTupleInsertError
		}
		brandNewHeapPage.setDirty(true)

		var page Page = brandNewHeapPage
		if brandNewHeapPage.isDirty(){
			if flushNewPageErr := f.flushPage(&page); flushNewPageErr != nil {
				return flushNewPageErr
			}
			f.numPages++
		}
	} else {
		for i := 0; i < f.numPages; i++ {
			// try to get page from Buffer Pool
			pagePtr, getPageFromBufferPoolError := f.bufPool.GetPage(f, i, tid, 0) // 0 = read perms
			if getPageFromBufferPoolError != nil {
				return getPageFromBufferPoolError
			}
			if pagePtr == nil {
				continue
			}
	
	
	
			page := *pagePtr
			if heapPage, isHeapPage := page.(*heapPage); isHeapPage {
				_, heapPageTupleInsertError := heapPage.insertTuple(t)
				// if tuple insertion is error-free, we're done
				if heapPageTupleInsertError == nil {
					// check that insertion was successful
					buf := make([]byte, PageSize) // Assuming PageSize is the size of a heapPage
					offset := int64(i) * int64(PageSize)
					_, fileReadErr := f.file.ReadAt(buf, offset)
					if fileReadErr != nil {
						return fmt.Errorf("ERROR checking insertion: %v", fileReadErr)
					}
					return nil
				} else {
					continue
					// return fmt.Errorf("error inserting tuple")
				}
			} else {
				return fmt.Errorf("Error casting page to heapPage: \n%v", isHeapPage)
			}
		}
		// at this line, no pages could hold the tuple
		// make a new heap page and insert tuple there
		newHeapPage := newHeapPage(f.tupleDesc, f.numPages, f)
		
		_, insertTupleNewHeapPageError := newHeapPage.insertTuple(t)
		if insertTupleNewHeapPageError != nil {
			return insertTupleNewHeapPageError
		}
		newHeapPage.setDirty(true)
	

		heapPage := &heapPage{
			desc:     newHeapPage.desc,
			pageNo:   newHeapPage.pageNo,
			file:     newHeapPage.file,
			tuples:   newHeapPage.tuples,
			dirtyBit: newHeapPage.dirtyBit,
		}
		var page Page = heapPage
		if newHeapPage.isDirty(){
			if flushPageErr := f.flushPage(&page); flushPageErr != nil {
				return flushPageErr
			}
			f.numPages++
		}
	}
	
	// iterate over pages in file
	

	return nil
}

// Remove the provided tuple from the HeapFile.  This method should use the
// [Tuple.Rid] field of t to determine which tuple to remove.
// This method is only called with tuples that are read from storage via the
// [Iterator] method, so you can so you can supply the value of the Rid
// for tuples as they are read via [Iterator].  Note that Rid is an empty interface,
// so you can supply any object you wish.  You will likely want to identify the
// heap page and slot within the page that the tuple came from.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	// TODO: some code goes here

	// get RID
	rid, isValidRID := t.Rid.(*heapRecordId)
	if !isValidRID {
		return fmt.Errorf("error: tuple Rid couldn't cast to rid")
	}

	// get page number & slot number based on RID
	pageNumber := rid.getPageNumber()
	_, isValidSlotNumber := rid.getSlotNumber()
	if !isValidSlotNumber {
		return fmt.Errorf("error: invalid tuple slot number")
	}

	pagePtr, err := f.bufPool.GetPage(f, pageNumber, tid, WritePerm)
	if err != nil {
		return fmt.Errorf("error getting page from buffer pool")
	}
	page := *pagePtr

	// cast page to heapPage
	heapPage, isCastableToHeapPage := page.(*heapPage)
	if !isCastableToHeapPage {
		return fmt.Errorf("error casting Page to HeapPage")
	}

	deleteTupleError := heapPage.deleteTuple(rid)
	if deleteTupleError != nil {
		return deleteTupleError
	}


	heapPage.setDirty(true)
	return nil
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {

	// TODO: some code goes here
	return &heapHash{
		FileName: f.fileName,
		PageNo:   pgNo,
	}
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return f.tupleDesc

}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	currentPage := 0
	currentSlot := -1
	iteratorFunc := func() (*Tuple, error) {
		if currentPage >= f.numPages {
			return nil, fmt.Errorf("end of file after all pages have been read\n")
		}

		for {
			currentSlot++ //inc slot number

			if currentPage >= f.numPages {
				return nil, nil
			}
			page, getPageFromBufferPoolError := f.bufPool.GetPage(f, currentPage, tid, 0)
			if getPageFromBufferPoolError != nil {
				return nil, getPageFromBufferPoolError
			}

			heapPage, isHeapPage := (*page).(*heapPage)
			if !isHeapPage {
				return nil, fmt.Errorf("error casting Page to HeapPage")
			}

			// If the current slot is beyond this page's slots, go to the next page
			if currentSlot >= heapPage.getNumSlots() {
				currentPage++
				currentSlot = -1
				continue
			}

			tuple := heapPage.tuples[currentSlot]
			if tuple != nil {
				tuple.Rid = &heapRecordId{
					pageNumber: currentPage,
					slotNumber: currentSlot,
				}
				return tuple, nil
			}
		}
	}
	return iteratorFunc, nil
}

func (h *heapRecordId) getPageNumber() int {
	return h.pageNumber
}
func (h *heapRecordId) getSlotNumber() (int, bool) {
	isValidSlotNumber := true // change later
	return h.slotNumber, isValidSlotNumber
}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}
