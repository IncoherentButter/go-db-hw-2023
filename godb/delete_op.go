package godb

type DeleteOp struct {
	// TODO: some code goes here
	dbFile DBFile      // specific DBFile being worked on
	pagesModified int64  // num of pages in DBFile that this Insert changed
	child Operator     // child operator for inserting into the page? 

}

// Construtor.  The delete operator deletes the records in the child
// Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	// TODO: some code goes here
	return &DeleteOp{
		dbFile: deleteFile,
		pagesModified: 0,
		child: child,
	}

}

// The delete TupleDesc is a one column descriptor with an integer field named "count"
func (i *DeleteOp) Descriptor() *TupleDesc {
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

// Return an iterator function that deletes all of the tuples from the child
// iterator from the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were deleted.  Tuples should be deleted using the [DBFile.deleteTuple]
// method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	p("dop.pagesModified = %v\n", dop.pagesModified)
	childIterator, createIteratorErr := dop.child.Iterator(tid)
	if createIteratorErr != nil{
		return nil, createIteratorErr
	}

	var insertionCompleted = false


	// var pagesModified int64
	pagesModifiedIntField := IntField{Value: dop.pagesModified}
	var iterationTupleFields []DBValue 
	iterationTupleFields = append(iterationTupleFields, pagesModifiedIntField)
	// iterationTupleFields[0] = pagesModified
	p("Insert | dop.pagesModified = %v\n", dop.pagesModified)
	var iterationTuple Tuple
	iterationTuple = Tuple{
		Desc: *dop.Descriptor(),
		Fields: iterationTupleFields,
	}
	

	return func() (*Tuple, error) {
		if insertionCompleted {
			// insertionCompleted = false
			// dop.pagesModified = 0
			return nil, nil
		}
		// check if at end of iterator, stop if so
		// start, with iteration, check conditions, then end each loop with iteration 
		for {
			tuple, childIterateErr := childIterator()
			// if iteration error, return it 
			// if end of tuple, return everything
			if tuple == nil{
				insertionCompleted = true
				iterationTupleFields[0] = IntField{dop.pagesModified}
				iterationTuple = Tuple{
					Desc: *dop.Descriptor(),
					Fields: iterationTupleFields,
				}	
				return &iterationTuple, nil
			}
			if childIterateErr != nil {
				p("Insert: childIterateErr arose\n")
				return nil, childIterateErr
			}
			
 
			tupInsertErr := dop.dbFile.insertTuple(tuple, tid)
			if tupInsertErr != nil{
				return nil, tupInsertErr
			}
			dop.pagesModified += 1	
			iterationTupleFields[0] = IntField{Value: dop.pagesModified}
			iterationTuple = Tuple{
				Desc: *dop.child.Descriptor(),
				Fields: iterationTupleFields,
			}	
		}
		// do another insertion
		return nil, nil
	}, nil

}
