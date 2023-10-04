package godb

import (
	"fmt"
	// "golang.org/x/text/currency"
)

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	// TODO: some code goes here
	frames      []*Frame             //frames in BufferPool
	pageIndexMap   map[string]int     //map page key to frame index

	pageNodeMap map[string]*PageNode //map page key to PageNode

	capacity    int                  //max num of pages in BufferPool (number of frames)
	currentSize int                  //current num of pages occupying frames in BufferPool
	order       *DoublyLinkedList    //ordering that supports LRU structure
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) *BufferPool {
	// TODO: some code goes here
	newFrames := make([]*Frame, numPages)
	newPageIndexMap := make(map[string]int)
	newPageNodeMap := make(map[string]*PageNode)
	return &BufferPool{
		frames:      newFrames,
		pageIndexMap: newPageIndexMap,
		pageNodeMap: newPageNodeMap,
		capacity:    numPages,
		currentSize: 0,
		order:       &DoublyLinkedList{},
	}
}

type Frame struct {
	page *Page
}

type DoublyLinkedList struct {
	head *PageNode
	tail *PageNode
}

func (dll *DoublyLinkedList) MoveToFront(newHeadNode *PageNode) {
	if newHeadNode == dll.head {
		return
	}
	// make the head node come after new head node
	newHeadNode.next = dll.head
	newHeadNode.prev = nil
	dll.head.prev = newHeadNode
	// close the gap left by moving the new head node
	if newHeadNode == dll.tail {
		newHeadNode.prev.next = nil
		dll.tail = newHeadNode.prev
	} else {
		newHeadNode.prev.next = newHeadNode.next
		newHeadNode.next.prev = newHeadNode.prev
	}

	// set new head node
	dll.head = newHeadNode
}

// append a new node to the DLL. It goes to the head in accordance with
// LRU policy
func (dll *DoublyLinkedList) Append(newNode *PageNode) {
	if dll.tail == nil{ // case where list is empty
		dll.head = newNode
		dll.tail = newNode
		return 
	}
	newNode.next = dll.head
	newNode.prev = nil 
	dll.head.prev = newNode
	dll.head = newNode
}

func (dll *DoublyLinkedList) Remove(node *PageNode){
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		dll.head = node.next
	}
	if node.next != nil{
		node.next.prev = node.prev
	} else {
		dll.tail = node.prev
	}
	node.next = nil
	node.prev = nil
}
// handle eviction logic for the Buffer Pool's DLL
// while maintaining LRU logic
func (dll *DoublyLinkedList) Evict() (*PageNode, bool, int) {
	isAllDirty := false
	// if tail is nil, the buffer pool is empty, evict nothing and note that BP is empty
	if dll.tail == nil {
		return nil, isAllDirty, 0
	}

	currentNode := dll.tail 
	for currentNode != nil {
		if !currentNode.IsDirty(){
			// if prev and next both nil, buffer pool is 1-page
			if currentNode.prev == nil && currentNode.next == nil{
				dll.head = nil 
				dll.tail = nil 
			} else if currentNode.prev == nil { // if evicting the head
				dll.head = currentNode.next
				dll.head.prev = nil 
			} else if currentNode.next == nil { // if evicting the tail
				dll.tail = currentNode.prev 
				dll.tail.next = nil 
			} else { // evicting a node that is neither head nor tail
				currentNode.prev.next = currentNode.next
				currentNode.next.prev = currentNode.prev
			}
			currentNode.prev = nil
			currentNode.next = nil
			return currentNode, isAllDirty, currentNode.frameNdx
		}
		currentNode = currentNode.prev 
	}
	isAllDirty = true 
	return nil, isAllDirty, -1 // if this line is reached, all pages are dirty
}

type PageNode struct {
	key      string
	frameNdx int 
	isDirty bool 
	// file	*DBFile
	// when page is HeapPage: TupleDesc is unique, HeapFile 
	page     *Page 
	prev     *PageNode
	next     *PageNode
}

// type heapPage struct {
// 	// TODO: some code goes here
// 	desc     *TupleDesc
// 	pageNo   int
// 	file     *HeapFile
// 	tuples   []*Tuple
// 	dirtyBit bool
// }

func (pageNode *PageNode) IsDirty() bool {
	return pageNode.isDirty
}
func (pageNode *PageNode) SetDirty(isDirty bool) {
	pageNode.isDirty = isDirty
	page := *pageNode.page
	page.setDirty(isDirty)
}
func (pageNode *PageNode) getFile() *DBFile{
	page := *pageNode.page
	return page.getFile()
}
// evict a page from the BufferPool and the corresponding DLL
func (bp *BufferPool) evictPage() (*Page, bool, int){
	// get LRU pagenode 
	evictedPageNode, isAllDirty, evictedNdx := bp.order.Evict()
	
	// if no eviction possible, return nil. 
	// Also return whether the buffer pool is full of dirty pages or not.
	if evictedPageNode == nil {
		return nil, isAllDirty, evictedNdx
	}  

	// Remove node from BufferPool's 
	page := evictedPageNode.page
	bp.frames[evictedPageNode.frameNdx] = nil
	delete(bp.pageIndexMap, evictedPageNode.key)
	delete(bp.pageNodeMap, evictedPageNode.key)
	bp.currentSize--
	
	return page, isAllDirty, evictedNdx
}
// Insert a new page into the buffer pool after retrieving it to disk.
// Add the page's PageNode to the buffer pool's DLL.
func (bp *BufferPool) insertPage(insertionPagePtr *Page, insertionPageKey string, insertionIndex int) {
	insertionPage := *insertionPagePtr
	insertionPageNode := &PageNode{
		key: insertionPageKey,
		frameNdx: insertionIndex,
		isDirty: insertionPage.isDirty(),
		page: insertionPagePtr,
		prev: nil,
		next: nil,
	}
	bp.order.Append(insertionPageNode)
	
	bp.frames[insertionIndex] = &Frame{page: insertionPagePtr}
	bp.pageIndexMap[insertionPageKey] = insertionIndex 
	bp.pageNodeMap[insertionPageKey] = insertionPageNode

	bp.currentSize++

}
func (bp *BufferPool) SetPageDirty(fileKey string, isDirty bool) error {
	if pageNode, exists := bp.pageNodeMap[fileKey]; exists {
		pageNode.SetDirty(isDirty)
		return nil 
	}
	return fmt.Errorf("did not set page with fileKey %v to isDirty = %v", fileKey, isDirty)
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	// TODO: some code goes here

}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtired will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	// TODO: some code goes here
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	// TODO: some code goes here
}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	return nil
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. For lab 1, you do not need to
// implement locking or deadlock detection. [For future labs, before returning the page,
// attempt to lock it with the specified permission. If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock]. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	// TODO: some code goes here

	fmt.Printf("*****bp.GetPage() pageNo = %v, tid = %v, perm = %v\n", pageNo, tid, perm)


	// get unique page key to check if it's already in buffer pool
	pageKey := file.pageKey(pageNo)
	// fmt.Printf("bp.GetPage() pageKey = %v\n", pageKey)
	var uniquePageKey string
	if pageKeyObj, isHeapHash := pageKey.(*heapHash); isHeapHash{
		uniquePageKey = fmt.Sprintf("%s:%d", pageKeyObj.FileName, pageKeyObj.PageNo)
		fmt.Printf("bp.GetPage() uniquePageKey = %v\n", uniquePageKey)

	} else{
		fmt.Printf("bp.GetPage() pageKeyObj is not a heapHash\n")
		return nil, fmt.Errorf("page key is not a string")
	}




	// check if page in buffer pool; if so, move posn in the DLL
	if pageFrameIndex, isPageInBuffer := bp.pageIndexMap[uniquePageKey]; isPageInBuffer{
		fmt.Printf("bp.GetPage() page in buffer\n pageFrameIndex = %v\n", pageFrameIndex)
		pageFrameInBuffer := bp.frames[pageFrameIndex]
		fmt.Printf("bp.GetPage() pageFrameInBuffer = %v\n", pageFrameInBuffer)
		pageNode := bp.pageNodeMap[uniquePageKey]
		fmt.Printf("bp.GetPage() pageNode = %v\n", pageNode)
		// if page was already in buffer, we move its position to front for LRU ordering purposes
		bp.order.MoveToFront(pageNode)
		return pageFrameInBuffer.page, nil
	}

	// if page not in buffer pool, read it from disk
	pageFromDisk, diskReadError := file.readPage(pageNo)
	if diskReadError != nil{
		fmt.Printf("bp.GetPage() diskReadError\n ERROR = %v", diskReadError)
		return nil, diskReadError
	}

	// add page to buffer pool
	
	// if BP is full, try evicting a non-dirty page; if all dirty, throw err
	if bp.currentSize == bp.capacity{
		fmt.Printf("bp.GetPage() bp.currentSize = %v == bp.capacity = %v\n", bp.currentSize, bp.capacity)

		evictedPageNode, isAllDirty, evictedNdx := bp.evictPage()
		fmt.Printf("bp.GetPage() isAllDirty = %v; evictedNdx = %v\n", isAllDirty, evictedNdx)
		fmt.Printf("bp.GetPage() evictedPageNode = %v\n", evictedPageNode)


		// if evicted is nil, then everything is dirty and we need to throw an error
		// or 
		if evictedPageNode == nil && isAllDirty{  
			fmt.Printf("bp.GetPage() evictedPageNode is nil and everything is dirty\n")
			return nil, fmt.Errorf("all pages are dirty, can't evict anything")
		} else {
			fmt.Printf("bp.GetPage() inserting page from disk into BP\n")
			bp.insertPage(pageFromDisk, uniquePageKey, evictedNdx)
			return pageFromDisk, nil
		}
		
	} else{
		bp.currentSize++ 
		fmt.Printf("bp.GetPage() bp.currentSize = %v\n", bp.currentSize)

		bp.insertPage(pageFromDisk, uniquePageKey, -1) // insert at first available index
		return pageFromDisk, nil
	}

	// // add the new page to the end of the buffer pool & make corresponding PageNode
	// frameForPage := &Frame{page: pageFromDisk}
	// // bp.frames = append(bp.frames, frameForPage)
	// frameNdx := len(bp.frames) - 1 // ndx is at end of the slice

	// pageNode := &PageNode{
	// 	key: uniquePageKey,
	// 	frameNdx: frameNdx,
	// 	isDirty: false,
	// 	page: pageFromDisk,
	// }
	// bp.order.Append(pageNode)
	// bp.pageNodeMap[uniquePageKey] = pageNode
	// bp.pageIndex[uniquePageKey] = frameNdx

	// return pageFromDisk, nil



	// // unique key for hashing to get the file
	// pageKey := file.pageKey(pageNo)
	// fileKeyVal, isHeapHash := pageKey.(*heapHash)
	// if !isHeapHash{
	// 	return nil, fmt.Errorf("file.pageKey(pageNo) was not of type *heapHash")
	// }
	// fileKey := fmt.Sprintf("%s|%d", fileKeyVal.FileName, fileKeyVal.PageNo)
	// fmt.Printf("buffer_pool.GetPage fileKey = %v\n", fileKey)

	// // if page already in buffer pool, move to front of list
	// if frameNdx, isPageInBuffer:= bp.pageIndex[fileKey]; isPageInBuffer{
	// 	frame := bp.frames[frameNdx]
	// 	bp.order.MoveToFront(bp.pageNodeMap[fileKey]) // move page to front since it's most recently accessed
	// 	return frame.page, nil
	// }

	// // if page not in buffer pool, read it from file
	// readPage, readErr := file.readPage(pageNo)
	// if readErr != nil {return nil, readErr} 

	// // if buffer pool full, try to evict a page
	// if bp.currentSize == bp.capacity{
	// 	evictedNode := bp.order.Evict()
	// 	// if evictedNode is nil, everything is dirty and we can't evict
	// 	if evictedNode == nil{
	// 		bp.FlushAllPages()
	// 		// try evicting again; if still nil, then buffer is all dirty
	// 		evictedNode = bp.order.Evict()
	// 		if evictedNode == nil{
	// 			return nil, fmt.Errorf("Buffer pool has only dirty pages; no eviction allowed")
	// 		}
	// 	}
	// 	// clean up structures for evicted page
	// 	delete(bp.pageIndex, evictedNode.key)
	// 	delete(bp.pageNodeMap, evictedNode.key)
	// 	bp.frames[evictedNode.frameNdx] = nil
	// } else {
	// 	bp.currentSize++
	// }

	// // add the new page to the end of the buffer pool & make corresponding PageNode
	// frameForPage := &Frame{page: readPage}
	// bp.frames = append(bp.frames, frameForPage)
	// frameNdx := len(bp.frames) - 1 // ndx is at end of the slice

	// pageNode := &PageNode{
	// 	key: fileKey,
	// 	frameNdx: frameNdx,
	// 	isDirty: false,
	// 	page: readPage,
	// }
	// bp.order.Append(pageNode)
	// bp.pageNodeMap[fileKey] = pageNode
	// bp.pageIndex[fileKey] = frameNdx

	// return readPage, nil

}
