package godb

import "fmt"
import "io"

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

	// // TODO: some code goes here
	leftOperator := (*joinOp.left)
	leftIterator, leftIteratorErr := leftOperator.Iterator(tid)
	if leftIteratorErr != nil {
		return nil, leftIteratorErr
	}

	rightOperator := (*joinOp.right)
	rightIterator, rightIteratorErr := rightOperator.Iterator(tid)
	if rightIteratorErr != nil {return nil, rightIteratorErr}

	var leftCurrentTuple *Tuple
	var rightCurrentTuple *Tuple

	var leftCounter = -1
	var rightCounter = -1

	var leftIterationError error
	var rightIterationError error

	leftCurrentTuple, leftIterationError = leftIterator()
	if leftIterationError != nil || leftCurrentTuple == nil {return nil, leftIterationError}
	leftCounter += 1
	
	rightCurrentTuple, rightIterationError = rightIterator()
	if rightIterationError != nil || rightCurrentTuple == nil {return nil, rightIterationError}
	rightCounter += 1

	// joinTuples()
	var needResetRightIterator = false
	return func() (*Tuple, error) {
		// isMatch := false

		// if there's no match found after looping over everything, then return nil
		for {
			if leftCurrentTuple == nil {return nil, nil} // end of iteration

			if (needResetRightIterator){
				fmt.Printf("join_op.Iterator | RESET RIGHT ITERATOR\n")
				rightIterator, rightIteratorErr := rightOperator.Iterator(tid)
				if rightIteratorErr != nil {return nil, rightIteratorErr}

				// note: check that we dont miss first tuple
				rightCurrentTuple, rightIterationError = rightIterator()
				if rightIterationError != nil || rightCurrentTuple == nil {return nil, rightIterationError}
				needResetRightIterator = false
			}

			
			
			if rightIterationError == io.EOF || rightCurrentTuple == nil{
				fmt.Printf("join_op.Iterator | Right tuple is nil or EOF\n")
				// iterate left table's tuple if we aren't currently iterating thru right tuples
				leftCurrentTuple, leftIterationError = leftIterator()
				if leftIterationError != nil || leftCurrentTuple == nil {return nil, leftIterationError}
				leftCounter += 1
				fmt.Printf("----\n")
				fmt.Printf("join_op.Iterator | leftCounter is %v\n", leftCounter)
				// fmt.Printf("join_op.Iterator | leftCurrentTuple is %T\n", leftCurrentTuple)

				//after moving left iterator, reset right one
				needResetRightIterator = true
				// rightIterator, rightIteratorErr := rightOperator.Iterator(tid)
				// if rightIteratorErr != nil {return nil, rightIteratorErr}
				continue
			}
			
			// get next right tuple
			// if (needResetRightIterator){
			// 	rightIterator, rightIteratorErr := rightOperator.Iterator(tid)
			// 	if rightIteratorErr != nil {return nil, rightIteratorErr}
			// 	rightCurrentTuple, rightIterationError = rightIterator()
			// } else{
			// 	rightCurrentTuple, rightIterationError = rightIterator()
			// }
			// rightCounter += 1
			// fmt.Printf("~~~\n")
			// fmt.Printf("join_op.Iterator | rightCounter is %v\n", rightCounter)

			if rightIterationError != nil || rightCurrentTuple == nil {return nil, rightIterationError}
			// fmt.Printf("join_op.Iterator | rightCurrentTuple is %T\n", rightCurrentTuple)

			// if rightCurrentTuple == nil {
			// 	leftCurrentTuple = nil
			// 	continue
			// }
			

			// matching logic

			leftEvaluation, leftEvalErr := joinOp.leftField.EvalExpr(leftCurrentTuple)
			if leftEvalErr != nil {return nil, leftEvalErr}
			// fmt.Printf("join_op.Iterator | leftEvaluation is %T\n", leftEvaluation)

			rightEvaluation, rightEvalErr := joinOp.rightField.EvalExpr(rightCurrentTuple) // DBValue, err
			if rightEvalErr != nil {return nil, rightEvalErr}
			// fmt.Printf("join_op.Iterator | rightEvaluation is %T\n", rightEvaluation)


			leftVal := joinOp.getter(leftEvaluation)
			rightVal := joinOp.getter(rightEvaluation)

			fmt.Printf("join_op.Iterator | leftVal is %v\n", leftVal)
			fmt.Printf("join_op.Iterator | rightVal is %v\n", rightVal)

			// fmt.Printf("join_op.Iterator | type(leftVal) is %T\n", leftVal)
			// fmt.Printf("join_op.Iterator | type(rightVal) is %T\n", rightVal)
			isEqual := leftVal == rightVal
			if isEqual {
				joinedTuple := joinTuples(leftCurrentTuple, rightCurrentTuple)
				fmt.Printf("join_op.Iterator | isEQual is true; returning %v", joinedTuple)
				return joinedTuple, nil
				// isEqual = false
			}
			// // operate on left tuple
			// leftEvaluation, leftEvalErr := joinOp.leftField.EvalExpr(leftCurrentTuple)
			// if leftEvalErr != nil {return nil, leftEvalErr}
			// // fmt.Printf("join_op.Iterator | leftEvaluation is %T\n", leftEvaluation)
			// // iterate over right table and make comparisons
			// for {
			// 	rightCurrentTuple, rightIterationError = rightIterator()
			// 	rightCounter += 1
			// 	fmt.Printf("~~~\n")
			// 	fmt.Printf("join_op.Iterator | rightCounter is %v\n", rightCounter)

			// 	if rightIterationError != nil || rightCurrentTuple == nil {return nil, rightIterationError}
			// 	// fmt.Printf("join_op.Iterator | rightCurrentTuple is %T\n", rightCurrentTuple)


			// 	rightEvaluation, rightEvalErr := joinOp.rightField.EvalExpr(rightCurrentTuple) // DBValue, err
			// 	if rightEvalErr != nil {return nil, rightEvalErr}
			// 	// fmt.Printf("join_op.Iterator | rightEvaluation is %T\n", rightEvaluation)


			// 	leftVal := joinOp.getter(leftEvaluation)
			// 	rightVal := joinOp.getter(rightEvaluation)

			// 	fmt.Printf("join_op.Iterator | leftVal is %v\n", leftVal)
			// 	fmt.Printf("join_op.Iterator | rightVal is %v\n", rightVal)

			// 	// fmt.Printf("join_op.Iterator | type(leftVal) is %T\n", leftVal)
			// 	// fmt.Printf("join_op.Iterator | type(rightVal) is %T\n", rightVal)
			// 	isEqual := leftVal == rightVal
			// 	if isEqual {
			// 		joinedTuple := joinTuples(leftCurrentTuple, rightCurrentTuple)
			// 		return joinedTuple, nil
			// 	}
			// }
			// // iterate left tuple 
		}
		return nil, nil
	}, nil
}
