package godb

type Aggregator struct {
	// Expressions that when applied to tuples from the child operators,
	// respectively, return the value of the group by key tuple
	groupByFields []Expr

	// Aggregation states that serves as a template as to which types of
	// aggregations in which order are to be computed for every group.
	newAggState []AggState

	child Operator // the child operator for the inputs to aggregate
}

type AggType int

const (
	IntAggregator    AggType = iota
	StringAggregator AggType = iota
)

const DefaultGroup int = 0 // for handling the case of no group-by

// Constructor for an aggregator with a group-by
func NewGroupedAggregator(emptyAggState []AggState, groupByFields []Expr, child Operator) *Aggregator {
	return &Aggregator{groupByFields, emptyAggState, child}
}

// Constructor for an aggregator with no group-by
func NewAggregator(emptyAggState []AggState, child Operator) *Aggregator {
	return &Aggregator{nil, emptyAggState, child}
}

// Return a TupleDescriptor for this aggregation. If the aggregator has no group-by, the
// returned descriptor should contain the union of the fields in the descriptors of the
// aggregation states. If the aggregator has a group-by, the returned descriptor will
// additionally start with the group-by fields, and then the aggregation states descriptors
// like that without group-by.
//
// HINT: for groupByFields, you can use [Expr.GetExprType] to get the FieldType
// HINT: use the merge function you implemented for TupleDesc in lab1 to merge the two TupleDescs
func (a *Aggregator) Descriptor() *TupleDesc {
	// TODO: some code goes here

	// []Expr, []AggState, Operator

	// 1) if groupByFields empty -> find union of descriptors in newAggState of type []AggState
	// 2) otherwise, apply the groupByFields expressions with the AggState objects appropriately
	// 	use GetExprType() method from Expr interface to get FieldType for groupByFields
	// use TupleDesc.merge method to merge TupleDescs
	var aggregatedTupleDesc *TupleDesc
	// no group-by -> union of agg state field descriptors
	if len(a.groupByFields) == 0 {
		for _, aggState := range a.newAggState {
			aggStateTupleDesc := aggState.GetTupleDesc().copy()
			if aggregatedTupleDesc == nil {
				// get TupleDesc for current AggState
				aggregatedTupleDesc = aggStateTupleDesc
			} else {
				aggregatedTupleDesc = aggregatedTupleDesc.merge(aggStateTupleDesc)
			}
		}
	} else {
		groupByFieldsTupleDesc := &TupleDesc{Fields: make([]FieldType, len(a.groupByFields))}
		for i, expr := range a.groupByFields {
			groupByFieldsTupleDesc.Fields[i] = expr.GetExprType()
		}

		for _, aggState := range a.newAggState {
			if aggregatedTupleDesc == nil {
				aggregatedTupleDesc = groupByFieldsTupleDesc.merge(aggState.GetTupleDesc())
			} else {
				aggregatedTupleDesc = aggregatedTupleDesc.merge(aggState.GetTupleDesc())
			}
		}
	}

	return aggregatedTupleDesc
}

// Aggregate operator implementation: This function should iterate over the results of
// the aggregate. The aggregate should be the result of aggregating each group's tuples
// and the iterator should iterate through each group's result. In the case where there
// is no group-by, the iterator simply iterates through only one tuple, representing the
// aggregation of all child tuples.
func (a *Aggregator) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// the child iterator

	childIter, childIterationErr := a.child.Iterator(tid)
	if childIterationErr != nil {
		// fmt.Printf("agg_op.Iterator | a.child.Iterator(tid) error\n")
		return nil, childIterationErr
	}
	if childIter == nil {
		// fmt.Printf("agg_op.Iterator | childIter == nil\n")
		return nil, GoDBError{MalformedDataError, "child iter unexpectedly nil\n"}
	// REPO CODE =================================
	// childIter, err := a.child.Iterator(tid)
	// if err != nil {
	// 	return nil, err
	// }
	// if childIter == nil {
	// 	return nil, GoDBError{MalformedDataError, "child iter unexpectedly nil"}
	// =============================================
	}
	// the map that stores the aggregation state of each group
	aggState := make(map[any]*[]AggState)
	if a.groupByFields == nil {
		var newAggState []AggState
		for _, as := range a.newAggState {
			copy := as.Copy()
			if copy == nil {
				// fmt.Printf("agg_op.Iterator | copy is nil for as = %v\n", as)
				return nil, GoDBError{MalformedDataError, "aggState Copy unexpectedly returned nil\n"}
			}
			newAggState = append(newAggState, copy)
		}

		aggState[DefaultGroup] = &newAggState
	}
	// the list of group key tuples
	var groupByList []*Tuple
	// the iterator for iterating thru the finalized aggregation results for each group
	var finalizedIter func() (*Tuple, error)
	return func() (*Tuple, error) {
		// iterates thru all child tuples
		for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
			if err != nil {
				// fmt.Printf("agg_op.Iterator | childIter tuple is not nil or err not nil\n")
				break
				// return nil, err
			}
			if t == nil {
				// fmt.Printf("agg_op.Iterator | tuple from childIter is nil\n")
				return nil, nil
			}
			// fmt.Printf("t = %v\n", t)

			if a.groupByFields == nil { // adds tuple to the aggregation in the case of no group-by
				for i := 0; i < len(a.newAggState); i++ {
					// fmt.Printf("i = %v ||| adding tuple = %v\n", i, t)
					(*aggState[DefaultGroup])[i].AddTuple(t)
				}
			} else { // adds tuple to the aggregation with grouping
				keygenTup, extractGroupingKeyErr := extractGroupByKeyTuple(a, t)
				if extractGroupingKeyErr != nil {
					// fmt.Printf("agg_op.Iterator | extractGroupByKeyTuple(a, t) is not nil\n")
					return nil, extractGroupingKeyErr
			// REPO CODE ===============================================================================
			// 	return nil, err
			// }
			// if t == nil {
			// 	return nil, nil
			// }

			// if a.groupByFields == nil { // adds tuple to the aggregation in the case of no group-by
			// 	for i := 0; i < len(a.newAggState); i++ {
			// 		(*aggState[DefaultGroup])[i].AddTuple(t)
			// 	}
			// } else { // adds tuple to the aggregation with grouping
			// 	keygenTup, err := extractGroupByKeyTuple(a, t)
			// 	if err != nil {
			// 		return nil, err
			// ===========================================================================================
				}

				key := keygenTup.tupleKey()
				if aggState[key] == nil {
					asNew := make([]AggState, len(a.newAggState))
					aggState[key] = &asNew
					groupByList = append(groupByList, keygenTup)
				}
				addTupleToGrpAggState(a, t, aggState[key])
			}
		}
		if finalizedIter == nil { // builds the iterator for iterating thru the finalized aggregation results for each group
			// fmt.Printf("agg_op.Iterator | finalizedIter is nil\n")
			if a.groupByFields == nil {
				// fmt.Printf("agg_op.Iterator | a.groupByFields is nil\n")
				var tup *Tuple
				for i := 0; i < len(a.newAggState); i++ {
					newTup := (*aggState[DefaultGroup])[i].Finalize()
					// fmt.Printf("agg_op.Iterator | tup = %v\nnewTup = %v\n", tup, newTup)
					tup = joinTuples(tup, newTup) // error here
					// fmt.Printf("tup = %v\n", tup)
				}
				finalizedIter = func() (*Tuple, error) { return nil, nil }
				// fmt.Printf("Agg.Iterator() | return tup = %v", tup)
				return tup, nil
			} else {
				// fmt.Printf("group by fields in the non-first iteration\n")
		// REPO CODE ===============================================================
		// 		addTupleToGrpAggState(a, t, aggState[key])
		// 	}
		// }

		// if finalizedIter == nil { // builds the iterator for iterating thru the finalized aggregation results for each group
		// 	if a.groupByFields == nil {
		// 		var tup *Tuple
		// 		for i := 0; i < len(a.newAggState); i++ {
		// 			newTup := (*aggState[DefaultGroup])[i].Finalize()
		// 			tup = joinTuples(tup, newTup)
		// 		}
		// 		finalizedIter = func() (*Tuple, error) { return nil, nil }
		// 		return tup, nil
		// 	} else {
		// ========================================================================
				finalizedIter = getFinalizedTuplesIterator(a, groupByList, aggState)
			}
		}
		return finalizedIter()
	}, nil
}

// Given a tuple t from a child iteror, return a tuple that identifies t's group.
// The returned tuple should contain the fields from the groupByFields list
// passed into the aggregator constructor.  The ith field can be extracted
// from the supplied tuple using the EvalExpr method on the ith expression of
// groupByFields.
// If there is any error during expression evaluation, return the error.
func extractGroupByKeyTuple(a *Aggregator, t *Tuple) (*Tuple, error) {
	// TODO: some code goes here
	if len(a.groupByFields) == 0 {
		return nil, fmt.Errorf("aggregator has no fields to group by\n")
	}

	// evaluate expressions with tuple value
	groupKeyFields := make([]DBValue, len(a.groupByFields))
	groupKeyFieldTypes := make([]FieldType, len(a.groupByFields))
	for ndx, expr := range a.groupByFields {
		// calculating field of type DBVal
		dbVal, evalTupleErr := expr.EvalExpr(t)
		if evalTupleErr != nil {
			return nil, evalTupleErr
		}
		groupKeyFields[ndx] = dbVal

		// calculating FieldType
		groupKeyFieldTypes[ndx] = expr.GetExprType()
	}
	groupByKeyTupleDesc := TupleDesc{Fields: groupKeyFieldTypes}
	// fauxRecordID := &RecordId{PageNo: -1, SlotNo: -1} // temp record id to change later
	groupKeyTuple := &Tuple{
		Desc:   groupByKeyTupleDesc,
		Fields: groupKeyFields,
		Rid:    t.Rid,
	}
	return groupKeyTuple, nil
}

// Given a tuple t from child and (a pointer to) the array of partially computed aggregates
// grpAggState, add t into all partial aggregations using the [AggState AddTuple] method.
// If any of the array elements is of grpAggState is null (i.e., because this is the first
// invocation of this method, create a new aggState using aggState.Copy() on appropriate
// element of the a.newAggState field and add the new aggState to grpAggState.
func addTupleToGrpAggState(a *Aggregator, t *Tuple, grpAggState *[]AggState) {
	// TODO: some code goes here

	// iterate over parameter AggStates from Aggregator
	for ndx, aggState := range a.newAggState {
		// if corresponding partial aggregations are nil, initialize them from newAggState
		if (*grpAggState)[ndx] == nil {
			(*grpAggState)[ndx] = aggState.Copy()
		}
		// add tuple to aggstate
		(*grpAggState)[ndx].AddTuple(t)
	}
}

// Given that all child tuples have been added, return an iterator that iterates
// through the finalized aggregate result one group at a time. The returned tuples should
// be structured according to the TupleDesc returned from the Descriptor() method.
// HINT: you can call [aggState.Finalize()] to get the field for each AggState.
// Then, you should get the groupByTuple and merge it with each of the AggState tuples using the
// joinTuples function in tuple.go you wrote in lab 1.
func getFinalizedTuplesIterator(a *Aggregator, groupByList []*Tuple, aggState map[any]*[]AggState) func() (*Tuple, error) {
	// fmt.Printf("-~-~-~-~-~-~-~\ngetFinalizedTuplesIterator\n-~-~-~-~-~-~-~\n")
	var currentNdx int
	var aggStateNdx int
	currentNdx = 0
	aggStateNdx = 0
	var maxGroupByListLen int
	maxGroupByListLen = len(groupByList)

	// tupleDesc := a.Descriptor()

	var groupTuple *Tuple
	var groupKey any
	fmt.Printf("groupkey = %v\n", groupKey)

	var groupAggStates *[]AggState
	// var groupAggState AggState
	var isAggState bool

	groupTuple = groupByList[currentNdx]
	groupKey = groupTuple.tupleKey()

	groupAggStates, isAggState = aggState[groupKey]
	if !isAggState {
		return func() (*Tuple, error) {
			return nil, fmt.Errorf("no agg state with associated groupKey = %v", groupKey)
		}
	}
	var maxAggStatesLen int
	maxAggStatesLen = len(*groupAggStates)

	return func() (*Tuple, error) {
		// TODO: some code goes here
		// fmt.Printf("current index = %v\n", currentNdx)
		// no more tuples to group
		if currentNdx >= maxGroupByListLen {
			fmt.Printf("currentNdx= %v > groupByList length, %v\n", currentNdx, len(groupByList))
			return nil, nil
		}
		// if aggStateNdx >= maxAggStatesLen {
		// 	fmt.Printf("aggStateNdx = %v > groupAggStates length, %v\n", aggStateNdx, len(*groupAggStates))
		// 	return nil, nil
		// }
		fmt.Printf("aggStateNdx = %v > groupAggStates length, %v\n", aggStateNdx, maxAggStatesLen)


		groupTuple = groupByList[currentNdx]
		groupKey = groupTuple.tupleKey()
		groupAggStates, isAggState := aggState[groupKey]
		if !isAggState {
			return nil, fmt.Errorf("no agg state with associated groupKey = %v", groupKey)
		}
		fmt.Printf("groupTuple = %v\n", groupTuple)
		for _, groupAggState := range *groupAggStates {
			tupleField := groupAggState.Finalize()
			mergedTupleDesc := groupTuple.Desc.merge(tupleField.Desc.copy())
			groupTuple = joinTuples(groupTuple, tupleField)
			groupTuple.Desc = *mergedTupleDesc
		}
		fmt.Printf("groupTuple = %v\n", groupTuple)

		

		tupleFields := make([]DBValue, len(groupTuple.Fields))
		for ndx, groupTupleField := range groupTuple.Fields {
			tupleFields[ndx] = groupTupleField
		}


		currentNdx += 1
		aggStateNdx += 1

		return groupTuple, nil


		// groupAggState = (*groupAggStates)[aggStateNdx]
		// tupleField := groupAggState.Finalize()
		
		// finalTuple := &Tuple{
		// 	Desc:   *a.Descriptor(),
		// 	Fields: tupleFields,
		// 	Rid:    groupTuple.Rid,
		// }
		// return finalTuple, nil // TODO change me
	}
}

// func printSlice(slice []any) {
// 	for _, element := range slice{
// 		fmt.Printf("element = %v", element)
// 	}
// }
