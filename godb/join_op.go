package godb

import "fmt"

// import "io"

type EqualityJoin[T comparable] struct {
	// Expressions that when applied to tuples from the left or right operators,
	// respectively, return the value of the left or right side of the join
	leftField, rightField Expr

	left, right *Operator //operators for the two inputs of the join

	// Function that when applied to a DBValue returns the join value; will be
	// one of intFilterGetter or stringFilterGetter
	getter func(DBValue) T

	// The maximum number of records of intermediate state that the join should use
	// (only required for optional exercise)
	maxBufferSize int
}

// Constructor for a  join of integer expressions
// Returns an error if either the left or right expression is not an integer
func NewIntJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin[int64], error) {
	if leftField.GetExprType().Ftype != rightField.GetExprType().Ftype {
		return nil, GoDBError{TypeMismatchError, "can't join fields of different types"}
	}
	switch leftField.GetExprType().Ftype {
	case StringType:
		return nil, GoDBError{TypeMismatchError, "join field is not an int"}
	case IntType:
		return &EqualityJoin[int64]{leftField, rightField, &left, &right, intFilterGetter, maxBufferSize}, nil
	}
	return nil, GoDBError{TypeMismatchError, "unknown type"}
}

// Constructor for a  join of string expressions
// Returns an error if either the left or right expression is not a string
func NewStringJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin[string], error) {

	if leftField.GetExprType().Ftype != rightField.GetExprType().Ftype {
		return nil, GoDBError{TypeMismatchError, "can't join fields of different types"}
	}
	switch leftField.GetExprType().Ftype {
	case StringType:
		return &EqualityJoin[string]{leftField, rightField, &left, &right, stringFilterGetter, maxBufferSize}, nil
	case IntType:
		return nil, GoDBError{TypeMismatchError, "join field is not a string"}
	}
	return nil, GoDBError{TypeMismatchError, "unknown type"}
}

// Return a TupleDescriptor for this join. The returned descriptor should contain
// the union of the fields in the descriptors of the left and right operators.
// HINT: use the merge function you implemented for TupleDesc in lab1
func (hj *EqualityJoin[T]) Descriptor() *TupleDesc {
	// TODO: some code goes here
	leftTupleDesc := (*hj.left).Descriptor()
	rightTupleDesc := (*hj.right).Descriptor()
	joinedDesc := leftTupleDesc.merge(rightTupleDesc)
	return joinedDesc
}

// Join operator implementation.  This function should iterate over the results
// of the join. The join should be the result of joining joinOp.left and
// joinOp.right, applying the joinOp.leftField and joinOp.rightField expressions
// to the tuples of the left and right iterators respectively, and joining them
// using an equality predicate.
// HINT: When implementing the simple nested loop join, you should keep in mind that
// you only iterate through the left iterator once (outer loop) but iterate through the right iterator
// once for every tuple in the the left iterator (inner loop).
// HINT: You can use joinTuples function you implemented in lab1 to join two tuples.
//
// OPTIONAL EXERCISE:  the operator implementation should not use more than
// maxBufferSize records, and should pass the testBigJoin test without timing
// out.  To pass this test, you will need to use something other than a nested
// loops join.
func (joinOp *EqualityJoin[T]) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	joinOp.printTuples(tid)
	fmt.Printf("=========\n\n")
	// // TODO: some code goes here
	leftOperator := (*joinOp.left)
	leftIterator, leftIteratorErr := leftOperator.Iterator(tid)
	if leftIteratorErr != nil {
		return nil, leftIteratorErr
	}

	rightOperator := (*joinOp.right)
	rightIterator, rightIteratorErr := rightOperator.Iterator(tid)
	if rightIteratorErr != nil {
		return nil, rightIteratorErr
	}

	var leftCurrentTuple *Tuple
	var rightCurrentTuple *Tuple

	var leftCounter = -1
	var rightCounter = -1

	var leftIterationError error
	// var rightIterationError error

	leftCurrentTuple, leftIterationError = leftIterator()
	if leftIterationError != nil || leftCurrentTuple == nil {
		return nil, leftIterationError
	}
	// joinOp.printTupleValue(true, leftCurrentTuple, tid)

	leftCounter += 1

	// rightCurrentTuple, rightIterationError = rightIterator()
	// if rightIterationError != nil || rightCurrentTuple == nil {return nil, rightIterationError}

	// rightCounter += 1
	// fmt.Printf("Number of fields in left operator:%v\n", len(leftOperator.Descriptor().Fields))
	// fmt.Printf("Number of fields in right operator:%v\n", len(rightOperator.Descriptor().Fields))

	// fmt.Printf("leftCounter = %v / rightCounter = %v\n", leftCounter, rightCounter)
	counter := -1

	// joinTuples()
	// var needResetRightIterator = false
	return func() (*Tuple, error) {

		for{
			counter += 1
			// right iterate
			rightCurrentTuple, _ = rightIterator()
			// joinOp.printTupleValue(false, rightCurrentTuple, tid)
			rightCounter += 1
			// fmt.Printf("--------------\njoin_op.Iterator | start of loop - counter = %v\n(l, r) = (%v, %v)\n", counter, leftCounter, rightCounter)
			if (rightCounter == 1 && counter >= 4){
				// fmt.Printf("right tuple should give 999")
				joinOp.printTupleValue(false, rightCurrentTuple, tid)
			}
			// if reach end of Right Table, reset right iterator and iterate the left iterator
			if rightCurrentTuple == nil {
				if rightCounter != -1 { // iterate left table unless this is beginning
					leftCurrentTuple, _ = leftIterator()
					// joinOp.printTupleValue(true, leftCurrentTuple, tid)

					leftCounter += 1
					if leftCurrentTuple == nil { // reached end of left table
						return nil, nil
					}
				}
				rightIterator, _ = rightOperator.Iterator(tid)
				rightCurrentTuple, _ = rightIterator()
				// joinOp.printTupleValue(false, rightCurrentTuple, tid)
				rightCounter = 0
			}
			// fmt.Printf("--------------\njoin_op.Iterator | after iter check - counter = %v\n(l, r) = (%v, %v)\n", counter, leftCounter, rightCounter)
			isEqual, _ := joinOp.checkValues(leftCurrentTuple, rightCurrentTuple)
			if isEqual {
				joinedTuple := joinTuples(leftCurrentTuple, rightCurrentTuple)
				joinOp.printTupleValue(true, joinedTuple, tid)
				joinOp.printTupleValue(false, joinedTuple, tid)
				return joinedTuple, nil
			}
		}

		// // if there's no match found after looping over everything, then return nil
		// for counter < 50 {
		// 	fmt.Printf("--------------\njoin_op.Iterator | counter = %v, (l, r) = (%v, %v)\n", counter, leftCounter, rightCounter)
		// 	if leftCurrentTuple == nil {return nil, nil} // end of iteration

		// 	// if the right iterator needs to go back to start of right table, reset it
		// 	if (needResetRightIterator){
		// 		needResetRightIterator = false
		// 		fmt.Printf("join_op.Iterator | RESET RIGHT ITERATOR\n")
		// 		rightIterator, rightIteratorErr := rightOperator.Iterator(tid)
		// 		if rightIteratorErr != nil {return nil, rightIteratorErr}
		// 		rightCounter = -1

		// 		// note: check that we dont miss first tuple
				// rightCurrentTuple, rightIterationError = rightIterator()
				// if rightIterationError != nil || rightCurrentTuple == nil {return nil, rightIterationError}
		// 		rightCounter += 1
		// 		fmt.Printf("rightCounter inc'd\n")
		// 		fmt.Printf("join_op.Iterator | Checking vals with leftCounter = %v and rightCounter = %v\n", leftCounter, rightCounter)
		// 		// joinOp.checkValues(leftCurrentTuple, rightCurrentTuple)
		// 	} else {
		// 		fmt.Printf("join_op.Iterator | iterating right tuple\n")
		// 		rightCurrentTuple, _ = rightIterator()
		// 		rightCounter += 1
		// 		fmt.Printf("rightCounter inc'd\n")
		// 		fmt.Printf("join_op.Iterator | rightCounter inc'd: Checking vals with leftCounter = %v and rightCounter = %v\n", leftCounter, rightCounter)
		// 		// joinOp.checkValues(leftCurrentTuple, rightCurrentTuple)
		// 		// rightCurrentTuple, rightIterationError = rightIterator()
		// 		// if rightIterationError != nil || rightCurrentTuple == nil {return nil, rightIterationError}
		// 	}

		// 	// if the right iterator/tuple is at end of file, move left tuple
		// 	// and then mark down that we need to reset the right iterator
		// 	if rightIterationError == io.EOF || rightCurrentTuple == nil{
		// 		fmt.Printf("join_op.Iterator | Right tuple is nil or EOF\n")
		// 		// iterate left table's tuple if we aren't currently iterating thru right tuples
		// 		leftCurrentTuple, leftIterationError = leftIterator()
		// 		if leftIterationError != nil || leftCurrentTuple == nil {return nil, leftIterationError}
		// 		leftCounter += 1
		// 		fmt.Printf("leftCounter inc'd\n")
		// 		fmt.Printf("join_op.Iterator | leftCounter inc'd: Checking vals with leftCounter = %v and rightCounter = %v\n", leftCounter, rightCounter)
		// 		// joinOp.checkValues(leftCurrentTuple, rightCurrentTuple)
		// 		fmt.Printf("join_op.Iterator | leftCounter is %v\n", leftCounter)

		// 		// fmt.Printf("join_op.Iterator | leftCurrentTuple is %T\n", leftCurrentTuple)

		// 		//after moving left iterator, reset right one
		// 		needResetRightIterator = true
		// 		// rightIterator, rightIteratorErr := rightOperator.Iterator(tid)
		// 		// if rightIteratorErr != nil {return nil, rightIteratorErr}
		// 		counter += 1
		// 		continue
		// 	} else{
		// 		fmt.Printf("join_op.Iterator | right tuple was not nil or at EOF\n")
		// 	}

		// 	// get next right tuple
		// 	if rightIterationError != nil || rightCurrentTuple == nil {return nil, rightIterationError}
		// 	// fmt.Printf("join_op.Iterator | rightCurrentTuple is %T\n", rightCurrentTuple)

		// 	// matching logic

		// 	// leftEvaluation, leftEvalErr := joinOp.leftField.EvalExpr(leftCurrentTuple)
		// 	// if leftEvalErr != nil {return nil, leftEvalErr}
		// 	// // fmt.Printf("join_op.Iterator | leftEvaluation is %T\n", leftEvaluation)

		// 	// rightEvaluation, rightEvalErr := joinOp.rightField.EvalExpr(rightCurrentTuple) // DBValue, err
		// 	// if rightEvalErr != nil {return nil, rightEvalErr}
		// 	// // fmt.Printf("join_op.Iterator | rightEvaluation is %T\n", rightEvaluation)

		// 	// leftVal := joinOp.getter(leftEvaluation)
		// 	// rightVal := joinOp.getter(rightEvaluation)

		// 	// fmt.Printf("join_op.Iterator | leftVal is %v\n", leftVal)
		// 	// fmt.Printf("join_op.Iterator | rightVal is %v\n", rightVal)

		// 	// fmt.Printf("join_op.Iterator | type(leftVal) is %T\n", leftVal)
		// 	// fmt.Printf("join_op.Iterator | type(rightVal) is %T\n", rightVal)
		// 	fmt.Printf("join_op.Iterator | Checking vals with leftCounter = %v and rightCounter = %v\n", leftCounter, rightCounter)
		// 	isEqual, _ := joinOp.checkValues(leftCurrentTuple, rightCurrentTuple)
		// 	if isEqual {
		// 		joinedTuple := joinTuples(leftCurrentTuple, rightCurrentTuple)
		// 		fmt.Printf("join_op.Iterator | isEQual is true; returning %v\n", joinedTuple)
		// 		counter += 1
		// 		return joinedTuple, nil
		// 		// isEqual = false
		// 	} else{
		// 		// rightCurrentTuple, rightIterationError = rightIterator()
		// 		// if rightIterationError != nil || rightCurrentTuple == nil {return nil, rightIterationError}

		// 		// rightEvaluation, rightEvalErr := joinOp.rightField.EvalExpr(rightCurrentTuple) // DBValue, err
		// 		// if rightEvalErr != nil {return nil, rightEvalErr}
		// 		// rightVal := joinOp.getter(rightEvaluation)
		// 		// fmt.Printf("join_op.Iterator | isEQual is FALSE; iterated right tuple val is %v\n", rightVal)
		// 		fmt.Printf("join_op.Iterator | isEQual is FALSE\n")
		// 	}
		// 	counter += 1
		// }
		return nil, nil
	}, nil
}

func (joinOp *EqualityJoin[T]) printTuples(tid TransactionID) {
	leftOperator := (*joinOp.left)
	leftIterator, _ := leftOperator.Iterator(tid)
	var leftTuple *Tuple
	var rightTuple *Tuple

	rightOperator := (*joinOp.right)
	rightIterator, _ := rightOperator.Iterator(tid)
	fmt.Printf("LLLLLLLL\nleft vals\n")
	leftCounter := 0
	for {
		leftTuple, _ = leftIterator()

		if leftTuple == nil {
			break
		}

		leftEvaluation, _ := joinOp.leftField.EvalExpr(leftTuple)
		leftVal := joinOp.getter(leftEvaluation)

		fmt.Printf("%v: %v\n", leftCounter, leftVal)
		leftCounter += 1
	}

	fmt.Printf("RRRRRRRR\nleft vals\n")
	rightCounter := 0
	for {
		rightTuple, _ = rightIterator()

		if rightTuple == nil {
			break
		}

		rightEvaluation, _ := joinOp.rightField.EvalExpr(rightTuple)
		rightVal := joinOp.getter(rightEvaluation)

		fmt.Printf("%v: %v\n", rightCounter, rightVal)
		rightCounter += 1
	}
}

func (joinOp *EqualityJoin[T]) printTupleValue(isLeft bool, tuple *Tuple, tid TransactionID){
	
	if isLeft{
		leftEvaluation, _ := joinOp.leftField.EvalExpr(tuple)
		leftVal := joinOp.getter(leftEvaluation)
	
		// fmt.Printf("\n LEFT TUPLE VAL = %v\n", leftVal)
	} else{
		rightEvaluation, _ := joinOp.leftField.EvalExpr(tuple)
		rightVal := joinOp.getter(rightEvaluation)
	
		// fmt.Printf("\n RIGHT TUPLE VAL = %v\n", rightVal)
	}

}

func (joinOp *EqualityJoin[T]) checkValues(leftTuple *Tuple, rightTuple *Tuple) (bool, error) {
	leftEvaluation, leftEvalErr := joinOp.leftField.EvalExpr(leftTuple)
	if leftEvalErr != nil {
		return false, leftEvalErr
	}
	// fmt.Printf("join_op.Iterator | leftEvaluation is %T\n", leftEvaluation)

	rightEvaluation, rightEvalErr := joinOp.rightField.EvalExpr(rightTuple) // DBValue, err
	if rightEvalErr != nil {
		return false, rightEvalErr
	}
	// fmt.Printf("join_op.Iterator | rightEvaluation is %T\n", rightEvaluation)

	leftVal := joinOp.getter(leftEvaluation)
	rightVal := joinOp.getter(rightEvaluation)

	// fmt.Printf("join_op.Iterator | type(leftVal) is %T\n", leftVal)
	// fmt.Printf("join_op.Iterator | type(rightVal) is %T\n", rightVal)
	isEqual := leftVal == rightVal

	fmt.Printf("join_op.Iterator | leftVal is %v\n", leftVal)
	fmt.Printf("join_op.Iterator | rightVal is %v\n isEqual = %v\n", rightVal, isEqual)
	return isEqual, nil

}
