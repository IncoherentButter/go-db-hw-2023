package godb

import "fmt"

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	distinct	 bool 
	seenTuples 	 map[any]bool
	//add additional fields here
	// TODO: some code goes here
}

// Project constructor -- should save the list of selected field, child, and the child op.
// Here, selectFields is a list of expressions that represents the fields to be selected,
// outputNames are names by which the selected fields are named (should be same length as
// selectFields; throws error if not), distinct is for noting whether the projection reports
// only distinct results, and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	// TODO: some code goes here
	//len(outputNames) == len(selectFields)
	if len(selectFields) != len(outputNames) {
		return nil, fmt.Errorf("outputNames and selectFields are different length")
	}
	return &Project{
		selectFields: selectFields,
		outputNames: outputNames,
		child: child,
		distinct: distinct,
		seenTuples: make(map[any]bool),
	}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should contain
// fields for each field in the constructor selectFields list with outputNames
// as specified in the constructor.
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	// TODO: some code goes here
	selectFieldTypes := make([]FieldType, len(p.selectFields))
	for i, selectField := range p.selectFields{
		// exprFieldType := selectField.GetExprType()
		// want: DBType from : FieldType
		selectFieldTypes[i] = selectField.GetExprType()
		// selectFieldTypes[i] = FieldType{
		// 	Fname: p.outputNames[i],
		// 	TableQualifier: p.outputNames[i],
		// 	Ftype: selectField.GetExprType(), // FieldType
		// } 
	}
	projectTupleDesc := TupleDesc{
		Fields: selectFieldTypes,
	}
	return &projectTupleDesc

}

// Project operator implementation.  This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed.
// To implement this you will need to record in some data structure with the
// distinct tuples seen so far.  Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here

	childIterator, createIteratorErr := p.child.Iterator(tid)
	if createIteratorErr != nil{return nil, createIteratorErr}

	return func() (*Tuple, error) {
		for{
			// iterate to next tuple
			tuple, childIteratorErr := childIterator() 
			if childIteratorErr != nil{return nil, childIteratorErr}
			
			// evaluate tuple with selected field expression
			projectedFields := make([]DBValue, len(p.selectFields))
			for ndx, selectField := range p.selectFields{
				projDbVal, evalExprErr := selectField.EvalExpr(tuple)
				if evalExprErr != nil {return nil, evalExprErr}
				projectedFields[ndx] = projDbVal
			}

			// construct projected tuple
			projectedTuple := &Tuple{
				Desc: *p.Descriptor(),
				Fields: projectedFields,
			}
			

			// ensure that we haven't seen the tuple already
			if p.distinct &&  !p.seenTuples[projectedTuple.tupleKey()]{
				// if the projection query requires distinctness,
				// only add 
				// ====*==== CHECK do want add case for it already in the seenTuples?
				return projectedTuple, nil
			} else {
				return projectedTuple, nil
			}
		}

	}, nil
}
