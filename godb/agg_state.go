package godb

import (
	// "fmt"
	// "reflect"

	"golang.org/x/exp/constraints"
)

type Number interface {
	constraints.Integer | constraints.Float
}

// interface for an aggregation state
type AggState interface {

	// Initializes an aggregation state. Is supplied with an alias,
	// an expr to evaluate an input tuple into a DBValue, and a getter
	// to extract from the DBValue its int or string field's value.
	Init(alias string, expr Expr, getter func(DBValue) any) error

	// Makes an copy of the aggregation state.
	Copy() AggState

	// Adds an tuple to the aggregation state.
	AddTuple(*Tuple)

	// Returns the final result of the aggregation as a tuple.
	Finalize() *Tuple

	// Gets the tuple description of the tuple that Finalize() returns.
	GetTupleDesc() *TupleDesc
}

// Implements the aggregation state for COUNT
type CountAggState struct {
	alias string
	expr  Expr
	count int
}

func (a *CountAggState) Copy() AggState {
	return &CountAggState{
		alias: a.alias,
		expr: a.expr,
		count: a.count,
	}
}

func (a *CountAggState) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *CountAggState) AddTuple(t *Tuple) {
	a.count++
	// fmt.Printf("~~~~~~~~~~~~~~~~~~\nCOUNT Agg | count = %v for string = %v\n~~~~~~~~~~~~~~~~~~\n", a.count, a.alias)
}

func (a *CountAggState) Finalize() *Tuple {
	tupleDesc := a.GetTupleDesc()
	field := IntField{int64(a.count)}
	fields := []DBValue{field}
	t := Tuple{*tupleDesc, fields, nil}
	return &t
}

func (a *CountAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// Implements the aggregation state for SUM
type SumAggState[T Number] struct {
	// TODO: some code goes here
	// TODO add fields that can help implement the aggregation state
	alias  string
	expr   Expr
	sum    int64
	getter func(DBValue) any
}

func (a *SumAggState[T]) Copy() AggState {
	// TODO: some code goes here
	copySumAggState := &SumAggState[T]{
		alias:  a.alias,
		expr:   a.expr,
		sum:    a.sum,
		getter: a.getter,
	}
	return copySumAggState
}

func intAggGetter(v DBValue) any {
	// TODO: some code goes here
	intV, isIntField := v.(IntField)
	if !isIntField {
		// fmt.Printf("intAggGetter v is not an IntField")
		return nil
	}
	return intV.Value
}

func stringAggGetter(v DBValue) any {
	// TODO: some code goes here
	stringV, isStringField := v.(StringField)
	if !isStringField {
		// fmt.Printf("intAggGetter v is not an StringField")
		return nil
	}
	return stringV.Value
}

func (a *SumAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any) error {
	// TODO: some code goes here
	a.sum = 0
	a.expr = expr
	a.alias = alias
	a.getter = getter
	return nil
}

func (a *SumAggState[T]) AddTuple(t *Tuple) {
	// TODO: some code goes here
	tupleDBVal, _ := a.expr.EvalExpr(t)
	tupleVal := a.getter(tupleDBVal)
	a.sum += tupleVal.(int64)
	// tupleDBVal, sumAggStateEvalExprErr := a.expr.EvalExpr(t)
	// if sumAggStateEvalExprErr != nil {return}
	// if (tupleDBVal == nil){
	// 	fmt.Printf("Sum.AddTuple tupleDBVal is nil\n")
	// } else if (a.getter != nil){
	// 	tupleVal := a.getter(tupleDBVal)
	// 	tupleValInt, isInt64 := tupleVal.(int64)
	// 	if !isInt64{return}
	// 	a.sum += tupleValInt
	// } else{
	// 	fmt.Printf("Sum.AddTuple a.getter is nil\n")
	// 	return
	// }
}

func (a *SumAggState[T]) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	tableQualifier := " "
	fieldType := FieldType{
		Fname:          a.alias,
		TableQualifier: tableQualifier,
		Ftype:          IntType,
	}
	fieldTypes := []FieldType{
		fieldType,
	}
	tupleDesc := TupleDesc{
		Fields: fieldTypes,
	}
	// tupleDesc.Fields = fieldTypes
	// fmt.Printf("Sum.GetTupleDesc() tupleDesc = %v\n", tupleDesc)
	return &tupleDesc
}

func (a *SumAggState[T]) Finalize() *Tuple {
	// TODO: some code goes here
	tupleDesc := a.GetTupleDesc()
	// fmt.Printf("Sum.Finalize() tupleDesc = %v\n", tupleDesc)
	field := IntField{Value: int64(a.sum)}
	// fmt.Printf("Sum.Finalize() field = %v\n", field)
	fields := []DBValue{field}
	// fmt.Printf("Sum.Finalize() fields = %v\n", fields)
	tuple := Tuple{
		Desc:   *tupleDesc,
		Fields: fields,
	}
	// fmt.Printf("Sum.Finalize() tuple = %v\n", tuple)
	return &tuple // TODO change me
}

// Implements the aggregation state for AVG
// Note that we always AddTuple() at least once before Finalize()
// so no worries for divide-by-zero
type AvgAggState[T Number] struct {
	// TODO: some code goes here
	// TODO add fields that can help implement the aggregation state
	alias  string
	expr   Expr
	sum    int64
	count  int64
	getter func(DBValue) any
}

func (a *AvgAggState[T]) Copy() AggState {
	// TODO: some code goes here
	AvgAggState := &AvgAggState[T]{
		alias:  a.alias,
		expr:   a.expr,
		sum:    a.sum,
		count:  a.count,
		getter: a.getter,
	}
	return AvgAggState
}

func (a *AvgAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any) error {
	// TODO: some code goes here
	a.sum = 0
	a.count = 0
	a.expr = expr
	a.alias = alias
	a.getter = a.getter
	return nil
}

func (a *AvgAggState[T]) AddTuple(t *Tuple) {
	// TODO: some code goes here
	tupleDBVal, tupleEvalExprErr := a.expr.EvalExpr(t)
	if tupleEvalExprErr != nil {
		return
	}
	tupleVal := a.getter(tupleDBVal)
	a.count += 1
	a.sum += tupleVal.(int64)
}

func (a *AvgAggState[T]) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	tableQualifier := ""
	fieldType := FieldType{a.alias, tableQualifier, IntType}
	fieldTypes := []FieldType{fieldType}
	tupleDesc := TupleDesc{}
	tupleDesc.Fields = fieldTypes
	return &tupleDesc
}

func (a *AvgAggState[T]) Finalize() *Tuple {
	// TODO: some code goes here
	tupleDesc := a.GetTupleDesc()
	avg := a.sum / a.count
	field := IntField{avg}
	fields := []DBValue{field}
	tuple := Tuple{
		Desc:   *tupleDesc,
		Fields: fields,
	}
	return &tuple // TODO change me
}

// Implements the aggregation state for MAX
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN max
type MaxAggState[T constraints.Ordered] struct {
	alias  string
	expr   Expr
	max    T
	null   bool // whether the agg state have not seen any tuple inputted yet
	getter func(DBValue) any
}

func (a *MaxAggState[T]) Copy() AggState {
	return &MaxAggState[T]{a.alias, a.expr, a.max, true, a.getter}
}

func (a *MaxAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any) error {
	a.expr = expr
	a.getter = getter
	a.alias = alias
	return nil
}

func (a *MaxAggState[T]) AddTuple(t *Tuple) {
	v, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}
	val := a.getter(v).(T)
	if a.null {
		a.max = val
		a.null = false
	} else if val > a.max {
		a.max = val
	}
}

func (a *MaxAggState[T]) GetTupleDesc() *TupleDesc {
	var ft FieldType
	switch any(a.max).(type) {
	case string:
		ft = FieldType{a.alias, "", StringType}
	default:
		ft = FieldType{a.alias, "", IntType}
	}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *MaxAggState[T]) Finalize() *Tuple {
	td := a.GetTupleDesc()
	var f any
	switch any(a.max).(type) {
	case string:
		f = StringField{any(a.max).(string)}
	default:
		f = IntField{any(a.max).(int64)}
	}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for MIN
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN min
type MinAggState[T constraints.Ordered] struct {
	// TODO: some code goes here
	// TODO add fields that can help implement the aggregation state
	alias  string
	expr   Expr
	min    T
	null   bool // whether the agg state have not seen any tuple inputted yet
	getter func(DBValue) any
}

func (a *MinAggState[T]) Copy() AggState {
	// TODO: some code goes here
	return &MinAggState[T]{
		alias:  a.alias,
		expr:   a.expr,
		min:    a.min,
		null:   true,
		getter: a.getter,
	}
}

func (a *MinAggState[T]) Init(alias string, expr Expr, getter func(DBValue) any) error {
	// TODO: some code goes here
	a.expr = expr
	a.getter = getter
	a.alias = alias
	return nil
	// a.alias = alias
	// a.expr = expr
	// a.getter = getter
	// a.null = false
	// // a.min = math.MaxInt64
	// return nil
}

func (a *MinAggState[T]) AddTuple(t *Tuple) {
	// TODO: some code goes here

	v, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}
	val := a.getter(v).(T)
	if a.null {
		a.min = val
		a.null = false
	} else if val < a.min {
		a.min = val
	}


	// tupleDBVal, _ := a.expr.EvalExpr(t)
	// tupleVal := a.getter(tupleDBVal).(T)
	// if !a.null {
	// 	a.min = tupleVal // ???? how to handle any applied to T
	// 	a.null = true
	// } else {
	// 	if a.min > tupleVal {
	// 		a.min = tupleVal
	// 	}
	// }
}

func (a *MinAggState[T]) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here

	var ft FieldType
	switch any(a.min).(type) {
	case string:
		ft = FieldType{a.alias, "", StringType}
	default:
		ft = FieldType{a.alias, "", IntType}
	}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td



	
	// tableQualifier := " "
	// fieldType := FieldType{
	// 	Fname:          a.alias,
	// 	TableQualifier: tableQualifier,
	// 	Ftype:          IntType,
	// }
	// fieldTypes := []FieldType{
	// 	fieldType,
	// }
	// tupleDesc := TupleDesc{
	// 	Fields: fieldTypes,
	// }
	// // tupleDesc.Fields = fieldTypes
	// fmt.Printf("Sum.GetTupleDesc() tupleDesc = %v\n", tupleDesc)
	// return &tupleDesc
}

// func getDbVal(val T) DBValue {
// 	if intValue, isIntField := val.(IntField); isIntField{
// 		return
// 	} else if stringValue, isIntField := val.(StringField); isStringField{
// 		return
// 	} else {
// 		return
// 	}
// }

func (a *MinAggState[T]) Finalize() *Tuple {
	// TODO: some code goes here

	td := a.GetTupleDesc()
	var f any
	switch any(a.min).(type) {
	case string:
		f = StringField{any(a.min).(string)}
	default:
		f = IntField{any(a.min).(int64)}
	}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t

	// val := a.min

	// if intValue, isIntField := val.(IntField); isIntField{
	// 	field := IntField{Value: intValue}
	// } else if stringValue, isStringField := val.(StringField); isStringField{
	// 	field := StringField{Value: stringValue}
	// } else{
	// 	fmt.Printf("MIN.Finalize | a.min not int or string")
	// }

	// extract min value with appropriate typing
	// dbVal := a.min
	// switch dbVal.(type) {
	// case int64:
	// 	field = IntField{Value: dbVal}
	// case string:
	// 	field = StringField{Value: dbVal}
	// default:
	// 	fmt.Printf("minAggState min val isn't int64 or string: %v", dbVal.Kind())
	// }

	// fields := []DBValue{field}
	// tupleDesc := a.GetTupleDesc()
	// // dbVal := a.min
	// switch val := (a.min).(type) {
	// case int64:
	// 	return IntField{Value: val}
	// case string:
	// 	return StringField{Value: val}
	// default:
	// 	panic(fmt.Sprintf("min agg state not int64 and string", val))
	// }
	// field := IntField{Value: int64(a.min)}
	// // fields := []DBValue{val}
	// fmt.Printf("Min.Finalize() fields = %v\n", fields)
	// tuple := Tuple{
	// 	Desc:   *tupleDesc,
	// 	Fields: fields,
	// 	Rid:    RecordId{PageNo: nil, SlotNo: nil},
	// }
	// fmt.Printf("Min.Finalize() tuple = %v\n", tuple)
	// return &tuple // TODO change me
}
