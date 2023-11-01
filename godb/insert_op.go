package godb

import (
	"fmt"
)

var p = fmt.Printf

// TODO: some code goes here
type InsertOp struct {
	// TODO: some code goes here
	insertFile DBFile      // specific DBFile being worked on
	pagesModified int64  // num of pages in DBFile that this Insert changed
	child Operator     // child operator for inserting into the page? 
}

// Construtor.  The insert operator insert the records in the child
// Operator into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	// TODO: some code goes here
	return &InsertOp{
		insertFile: insertFile,
		pagesModified: 0,
		child: child,
	}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	var insertionFields []FieldType
	insertionFields = append(insertionFields, FieldType{
		Fname: "count",
		TableQualifier: "",
		Ftype: IntType,
	})
	insertionTupleDesc := &TupleDesc{
		Fields: insertionFields,
	}
	return insertionTupleDesc
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	// how use RecordId? PageNo? SlotNo? offset?
	// p("iop.pagesModified = %v\n", iop.pagesModified)
	fmt.Printf("----- begin of insert iterator OUTER func -----\n")



	// Define iterator 
	childIterator, createIteratorErr := iop.child.Iterator(tid)
	if createIteratorErr != nil{
		fmt.Printf("before iteration, child operator iterator err. Err = %v\n", createIteratorErr)
		return nil, createIteratorErr
	}
	var insertionCompleted = false

	insertFile := iop.insertFile

	
	// var pagesModified int64
	pagesModifiedIntField := IntField{Value: iop.pagesModified} //make IntField 
	var iterationTupleFields []DBValue 
	iterationTupleFields = append(iterationTupleFields, pagesModifiedIntField)
	// iterationTupleFields[0] = pagesModified
	// p("Insert | iop.pagesModified = %v\n", iop.pagesModified)
	var iterationTuple Tuple
	iterationTuple = Tuple{ //tuple to start with
		Desc: *iop.Descriptor(),
		Fields: iterationTupleFields,
	}

	// IDEA:
	// call childIterator() repeatedly; if tuple is nil, return count tuple. If iteration err
	// then either A) return count tuple, B) return err, C) ??
	return func() (*Tuple, error) {
		fmt.Printf("----- begin of insert iterator inner func -----\n")
		if insertionCompleted {
			// insertionCompleted = false
			// iop.pagesModified = 0
			return nil, nil
		}
		// check if at end of iterator, stop if so
		// start, with iteration, check conditions, then end each loop with iteration 
		index := 1
		for !insertionCompleted {
			tuple, childIterateErr := childIterator()
			// if end of tuple, return everything
			if tuple == nil{
				// if errors.Is(childIterateErr, io.EOF) {
				// 	// insertionCompleted = true
				// 	childIterateErr = nil
				// }
				insertionCompleted = true
				iterationTupleFields[0] = IntField{iop.pagesModified}
				iterationTuple = Tuple{
					Desc: *iop.Descriptor(),
					Fields: iterationTupleFields,
				}	
				fmt.Printf("on iteration %v tuple is nil.\nReturning iterationTuple = %v\nor\n%v\n", index, iterationTuple, iterationTuple.PrettyPrintString(true))
				return &iterationTuple, nil
			}
			// if iteration error, return it 
			if childIterateErr != nil {
				fmt.Printf("on iteration %v, child iterate error. err = %v\n", index, childIterateErr)
				// p("Insert: childIterateErr arose\n")
				return nil, childIterateErr
			}
			
			
			tupInsertErr := insertFile.insertTuple(tuple, tid)
			if tupInsertErr != nil{
				fmt.Printf("on iteration %v, tuple insert error. err = %v\n", index, tupInsertErr)
				return nil, tupInsertErr
			}
			index += 1
			iop.pagesModified += 1	
			iterationTupleFields[0] = IntField{Value: iop.pagesModified}
			iterationTuple = Tuple{
				Desc: *iop.child.Descriptor(),
				Fields: iterationTupleFields,
			}	
		}
		iterationTuple = Tuple{
			Desc: *iop.Descriptor(),
			Fields: iterationTupleFields,
		}	
		insertionCompleted = false
		fmt.Printf("on iteration %v tuple is nil.\nReturning iterationTuple = %v\nor\n%v\n", index, iterationTuple, iterationTuple.PrettyPrintString(true))
		return &iterationTuple, nil
		// do another insertion
		return nil, nil
	}, nil
}
