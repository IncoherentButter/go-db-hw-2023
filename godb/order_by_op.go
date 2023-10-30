package godb

import "sort"

// TODO: some code goes here
type OrderBy struct {
	orderBy []Expr // OrderBy should include these two fields (used by parser)
	child   Operator
	ascending []bool // ascending or descending for each of the fields
	tuples []*Tuple
	comparisonFunctions []isLessFunc
	//add additional fields here
}

type isLessFunc func(t1 *Tuple, t2 *Tuple) bool


type MultiSorter struct {
	tuples	[]*Tuple
	less	[]isLessFunc
}


func (ms *MultiSorter) Len() int{
	return len(ms.tuples)
}

func (ms *MultiSorter) Swap(i int, j int){
	ms.tuples[i], ms.tuples[j] = ms.tuples[j], ms.tuples[i]
}

// loops along the less functions until finding comparison that discriminates
// between two items
func (ms *MultiSorter) Less(i int, j int) bool {
	p, q := ms.tuples[i], ms.tuples[j]
	// Try all but the last comparison.
	var k int
	for k = 0; k < len(ms.less)-1; k++ {
		less := ms.less[k]
		switch {
		case less(p, q):
			// p < q, so we have a decision.
			return true
		case less(q, p):
			// p > q, so we have a decision.
			return false
		}
		// p == q; try the next comparison.
	}
	// All comparisons to here said "equal", so just return whatever
	// the final comparison reports.
	return ms.less[k](p, q)
}

func (ms *MultiSorter) Sort(tuples []*Tuple){
	ms.tuples = tuples 
	sort.Sort(ms)
}

func OrderedBy(less... isLessFunc) *MultiSorter {
	return &MultiSorter{
		less: less,
	}
}


// Order by constructor -- should save the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extacted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields
// list should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	// TODO: some code goes here
	comparisonFunctions := make([]isLessFunc, len(orderByFields))
	for ndx, expr := range orderByFields{
		comparisonFunctions[ndx] = func(t1 *Tuple, t2 *Tuple) bool {
			fieldVal1, _ := expr.EvalExpr(t1)
			fieldVal2, _ := expr.EvalExpr(t2)

			switch fv1 := fieldVal1.(type){
			case StringField:
					fv2 := fieldVal2.(StringField)
					if ascending[ndx] {
						return fv1.Value < fv2.Value
					} else {
						return fv1.Value > fv2.Value
					}
			case IntField:
					fv2 := fieldVal2.(IntField)
					if ascending[ndx] {
						return fv1.Value < fv2.Value
					} else {
						return fv1.Value > fv2.Value
					}
			}
			// at this point, no more comparisons, just default to 1st > 2nd
			return false
		}
	}

	return &OrderBy{
		orderBy: orderByFields,
		child: child,
		ascending: ascending,
		comparisonFunctions: comparisonFunctions,
	}, nil

}

func (o *OrderBy) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return o.child.Descriptor()
}



// Return a function that iterators through the results of the child iterator in
// ascending/descending order, as specified in the construtor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort pacakge and the [sort.Sort] method for this purpose.  To
// use this you will need to implement three methods:  Len, Swap, and Less that
// the sort algorithm will invoke to preduce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at: https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	childIterator, createIteratorErr := o.child.Iterator(tid)
	if createIteratorErr != nil{return nil, createIteratorErr}
	
	
	sorter := OrderedBy(o.comparisonFunctions...)


	chunkSize := 100 
	basicBuffer := make([]*Tuple, 0, chunkSize)

	return func() (*Tuple, error){
		for {
            if len(basicBuffer) == 0 {
                // Fetch next chunk of tuples
                for i := 0; i < chunkSize; i++ {
                    tuple, childIteratorErr := childIterator() 
					if childIteratorErr != nil{return nil, childIteratorErr}
                    if tuple == nil {break}
                    basicBuffer = append(basicBuffer, tuple)
                }

                // Sort the buffer using the MultiSorter
                sorter.Sort(basicBuffer)

                // If buffer is still empty after fetching, we're done
                if len(basicBuffer) == 0 {
                    return nil, nil
                }
            }

            // Pop the next tuple from the buffer and return it
            nextTuple := basicBuffer[0]
            basicBuffer = basicBuffer[1:]
            return nextTuple, nil
        }
    }, nil
}
