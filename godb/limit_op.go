package godb

import "fmt"

type LimitOp struct {
	child     Operator //required fields for parser
	limitTups Expr
	count     int64
	//add additional fields here, if needed
}

// Limit constructor -- should save how many tuples to return and the child op.
// lim is how many tuples to return and child is the child op.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	// TODO: some code goes here
	return &LimitOp{
		child:     child,
		limitTups: lim,
		count:     0,
	}
}

// Return a TupleDescriptor for this limit
func (l *LimitOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return l.child.Descriptor()

}

// Limit operator implementation. This function should iterate over the
// results of the child iterator, and limit the result set to the first
// [lim] tuples it sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	childIterator, createIteratorErr := l.child.Iterator(tid)
	if createIteratorErr != nil {
		return nil, createIteratorErr
	}

	// get the first tuple from the iterator
	firstTuple, firstTupleIterateErr := childIterator()
	if firstTupleIterateErr != nil {
		return nil, firstTupleIterateErr
	}

	// value from evaluating the first tuple w/ expression
	limitVal, limExprEvalErr := l.limitTups.EvalExpr(firstTuple)
	if limExprEvalErr != nil {
		return nil, limExprEvalErr
	}

	// get IntField version of the limit / verify that it is an int
	limit, isIntField := limitVal.(IntField)
	if !isIntField {
		return nil, fmt.Errorf("limit expression isn't an IntField")
	}

	return func() (*Tuple, error) {
		// if already at or past the limit, we're done
		if l.count >= limit.Value {
			return nil, nil
		}
		var tuple *Tuple
		// handle case where we don't yet have a starting tuple
		if l.count == 0{
			tuple = firstTuple
		} else {
			var err error 
			tuple, err = childIterator()
			if err != nil {return nil, err}
			if tuple == nil {return nil, nil}
		}
		l.count += 1
		return tuple, nil
	}, nil
}
