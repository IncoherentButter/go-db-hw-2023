package godb

//This file defines methods for working with tuples, including defining
// the types DBType, FieldType, TupleDesc, DBValue, and Tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"

	"github.com/mitchellh/hashstructure/v2"
)

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
type DBType int

const (
	IntType     DBType = iota
	StringType  DBType = iota
	UnknownType DBType = iota //used internally, during parsing, because sometimes the type is unknown
)

var typeNames map[DBType]string = map[DBType]string{IntType: "int", StringType: "string"}

// FieldType is the type of a field in a tuple, e.g., its name, table, and [godb.DBType].
// TableQualifier may or may not be an emtpy string, depending on whether the table
// was specified in the query
type FieldType struct {
	Fname          string
	TableQualifier string
	Ftype          DBType
}

// TupleDesc is "type" of the tuple, e.g., the field names and types
type TupleDesc struct {
	Fields []FieldType
}

// Compare two tuple descs, and return true iff
// all of their field objects are equal and they
// are the same length
func (d1 *TupleDesc) equals(d2 *TupleDesc) bool {
	// TODO: some code goes here
	if (len(d1.Fields) != len(d2.Fields)){
		return false
	}
	for i:=0; i < len(d1.Fields); i++ {
		d1Field, d2Field := d1.Fields[i], d2.Fields[i]
		if (d1Field != d2Field){
			return false
		}
	}
	return true

}

// Given a FieldType f and a TupleDesc desc, find the best
// matching field in desc for f.  A match is defined as
// having the same Ftype and the same name, preferring a match
// with the same TableQualifier if f has a TableQualifier
// We have provided this implementation because it's details are
// idiosyncratic to the behavior of the parser, which we are not
// asking you to write
func findFieldInTd(field FieldType, desc *TupleDesc) (int, error) {
	best := -1
	for i, f := range desc.Fields {
		if f.Fname == field.Fname && (f.Ftype == field.Ftype || field.Ftype == UnknownType) {
			if field.TableQualifier == "" && best != -1 {
				return 0, GoDBError{AmbiguousNameError, fmt.Sprintf("select name %s is ambiguous", f.Fname)}
			}
			if f.TableQualifier == field.TableQualifier || best == -1 {
				best = i
			}
		}
	}
	if best != -1 {
		return best, nil
	}
	return -1, GoDBError{IncompatibleTypesError, fmt.Sprintf("field %s.%s not found", field.TableQualifier, field.Fname)}

}

// Make a copy of a tuple desc.  Note that in go, assignment of a slice to
// another slice object does not make a copy of the contents of the slice.
// Look at the built-in function "copy".
func (td *TupleDesc) copy() *TupleDesc {
	// TODO: some code goes here
	var tdCopy TupleDesc
	tdCopy.Fields = make([]FieldType, len(td.Fields))
	for ndx, field := range td.Fields{
		tdCopy.Fields[ndx] = field
	}
	return &tdCopy
}

// Assign the TableQualifier of every field in the TupleDesc to be the
// supplied alias.  We have provided this function as it is only used
// by the parser.
func (td *TupleDesc) setTableAlias(alias string) {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	for i := range fields {
		fields[i].TableQualifier = alias
	}
	td.Fields = fields
}

// Merge two TupleDescs together.  The resulting TupleDesc
// should consist of the fields of desc2
// appended onto the fields of desc.
func (desc *TupleDesc) merge(desc2 *TupleDesc) *TupleDesc {
	// TODO: some code goes here
	var descMerged TupleDesc
	descMerged = *desc.copy()
	for _, field := range desc2.Fields{
		descMerged.Fields = append(descMerged.Fields, field)
	}
	return &descMerged 
}

// Check whether a given field type is already contained within a given
// TupleDesc object.
func (tdFields *TupleDesc) tupleDescContainsField(field2 FieldType) (bool, int) {
	for ndx, field := range tdFields.Fields{
		if (field == field2){
			return true, ndx
		}
	}
	return false, -1
}

func (tupleDesc *TupleDesc) size() int{
	tupleDescSize := 0
	for _, field := range tupleDesc.Fields {
		switch field.Ftype {
			case IntType:
				tupleDescSize += 8 //IntType is 8 bytes
			case StringType:
				tupleDescSize += StringLength // StringLength bytes is already provided
			default:
				continue
		}
	}
	return tupleDescSize
}

func isDBValInSlice(dbVal DBValue, dbVals []DBValue) (bool){
	for _, val := range dbVals{
		if val == dbVal{
			return true
		}
	}
	return false
}

func isTupleFieldsContainsValue(tFields []DBValue, dbVal DBValue) (bool) {
	for _, tField := range tFields{
		if tField == dbVal{
			return true
		}
	}
	return false
}


// ================== Tuple Methods ======================

// Interface used for tuple field values
// Since it implements no methods, any object can be used
// but having an interface for this improves code readability
// where tuple values are used
type DBValue interface {
}

// Integer field value
type IntField struct {
	Value int64
}

// String field value
type StringField struct {
	Value string
}

// Tuple represents the contents of a tuple read from a database
// It includes the tuple 	, and the value of the fields
type Tuple struct {
	Desc   TupleDesc
	Fields []DBValue
	Rid    recordID //used to track the page and position this page was read from
}

type recordID interface {
	getPageNumber() int
	getSlotNumber() (int, bool)
}

// Serialize the contents of the tuple into a byte array. Since all tuples are of
// fixed size, this method should simply write the fields in sequential order
// into the supplied buffer.
//
// See the function [binary.Write].  Objects should be serialized in little
// endian oder.
//
// Strings can be converted to byte arrays by casting to []byte. Note that all
// strings need to be padded to StringLength bytes (set in types.go). For
// example if StringLength is set to 5, the string 'mit' should be written as
// 'm', 'i', 't', 0, 0
//
// May return an error if the buffer has insufficient capacity to store the
// tuple.
func (t *Tuple) writeTo(b *bytes.Buffer) error {
	// TODO: some code goes here
	
	for _, field := range t.Fields{
		switch reflect.TypeOf(field).Name() {
			case "StringField":
				// fmt.Printf("string field = %v\n", field.(StringField).Value)
				stringByteArray := []byte(field.(StringField).Value)
				paddedByteArray := make([]byte, StringLength)
				for i:=0; i< len(stringByteArray) && i < StringLength; i++{
					if i < len(stringByteArray){
						paddedByteArray[i] = stringByteArray[i]
					} else {
						paddedByteArray[i] = 0
					}
				}
				// fmt.Printf("paddedByteArray = %v\n", paddedByteArray)

				// paddedString := fmt.Sprintf("%-*s", StringLength - len(field.(StringField).Value) + 1, field)
				// fmt.Printf("paddedString = %v\n", paddedString)
				// stringToWrite := []byte(paddedString)
				// fmt.Printf("writeTo stringToWrite = %v\n", stringToWrite)
				if err := binary.Write(b, binary.LittleEndian, &paddedByteArray); err != nil{
					return err
				}
			case "IntField":
				intToWrite := int64(field.(IntField).Value)
				// fmt.Printf("writeTo intToWrite = %v\n", intToWrite)
				if err := binary.Write(b, binary.LittleEndian, &intToWrite); err != nil{
					return err
				}
		}
		// fieldType := reflect.TypeOf(field)
		// if fieldType.Name() == "StringField"{
		// 	paddedString := fmt.Sprintf("%-*s", StringLength, field)
		// 	if err := binary.Write(b, binary.LittleEndian, []byte(paddedString)); err != nil{
		// 		return err
		// 	}
		// } else {
		// 	if err := binary.Write(b, binary.LittleEndian, field); err != nil{
		// 		return err
		// 	}
		// }
	}
	return nil 
}

// Read the contents of a tuple with the specified [TupleDesc] from the
// specified buffer, returning a Tuple.
//
// See [binary.Read]. Objects should be deserialized in little endian oder.
//
// All strings are stored as StringLength byte objects.
//
// Strings with length < StringLength will be padded with zeros, and these
// trailing zeros should be removed from the strings.  A []byte can be cast
// directly to string.
//
// May return an error if the buffer has insufficent data to deserialize the
// tuple.
func readTupleFrom(b *bytes.Buffer, desc *TupleDesc) (*Tuple, error) {
	// TODO: some code goes here
	var tupleContents []DBValue
	// fmt.Printf("readTupleFrom b = %v \ndesc = %v\n", b, desc)
	// binary.Read(b, binary.LittleEndian, desc)
	// fmt.Printf("b = %v \ndesc = %v\n", b, desc)
	for _, fieldType := range desc.Fields{
		// fmt.Printf("---\nfieldType = %v \n", fieldType)
		switch fieldType.Ftype {
			case 1:
				stringObj := make([]byte, StringLength)
				// fmt.Printf("stringObj = %v \n", stringObj)
				if _, err := b.Read(stringObj); err != nil {
					// fmt.Printf("str reading error = %v\n", err)
					return nil, err
				}
				// fmt.Printf("readTupleFrom stringObj = %v \n", stringObj)
				deserializedString := StringField{Value: string(removeTrailingZeros(stringObj))}
				// fmt.Printf("readTupleFrom deserializedString = %v\n", deserializedString)
				// stringObjByteArray := []byte()
				tupleContents = append(tupleContents, deserializedString)
			case 0:
				var intObj int64
				// fmt.Printf("readTupleFrom before int read, b = %v\n", b.Bytes())
				// fmt.Printf("readTupleFrom intObj  = %v\n", intObj)
				if err:= binary.Read(b, binary.LittleEndian, &intObj); err != nil{
					// fmt.Printf("readTupleFrom int reading error = %v\n", err)
					return nil, err
				}
				// fmt.Printf("readTupleFrom intObj  = %v\n", intObj)
				deserializedInt := IntField{Value: intObj}
				// fmt.Printf("readTupleFrom deserializedInt  = %v\n", deserializedInt)
				tupleContents = append(tupleContents, deserializedInt)
			default:
				return nil, fmt.Errorf("Unsupported field type")
		}
		// fmt.Printf("tupleContents = %v \n", tupleContents)
	}
	tuple := &Tuple{
		Fields: tupleContents,
		Desc: *desc,
		Rid: nil,
	}
	// fmt.Printf("tuple = %v\n", tuple)
	return tuple, nil //replace me
}

func removeTrailingZeros(strByteArray []byte) []byte {
	// fmt.Printf("strByteArray = %v\n", strByteArray)
	i := len(strByteArray) - 1
	isTrailingZeros := true
	for isTrailingZeros {
		if (strByteArray[i] == 0 || strByteArray[i] == 32){
			i -= 1
		} else {
			isTrailingZeros = false
		}
	}
	trimmedString := strByteArray[:i+1]
	// fmt.Printf("trimmedString = %v\n", trimmedString)
	return trimmedString
}

// Compare two tuples for equality.  Equality means that the TupleDescs are equal
// and all of the fields are equal.  TupleDescs should be compared with
// the [TupleDesc.equals] method, but fields can be compared directly with equality
// operators.
func (t1 *Tuple) equals(t2 *Tuple) bool {
	// TODO: some code goes here
	// check that TupleDescs are equal
	if (!t1.Desc.equals(&t2.Desc)){
		return false
	}
	for ndx, field := range t1.Fields{
		if field != t2.Fields[ndx]{
			return false
		}
	}

	return true
}

// Merge two tuples together, producing a new tuple with the fields of t2 appended to t1.
func joinTuples(t1 *Tuple, t2 *Tuple) *Tuple {
	// TODO: some code goes here
	// fmt.Printf("t1 = %v \nt2 = %v\n", t1, t2)
	joinedDesc := t1.Desc.merge(&t2.Desc)
	// fmt.Printf("joinedDesc = %v\n", joinedDesc)
	// joinedFields := append(t1.Fields, t2.Fields)
	joinedFields := joinTupleFields(t1, t2)
	// fmt.Printf("joinedFields = %v", joinedFields)
	joinedTuple := Tuple{
		Desc: *joinedDesc,
		Fields: joinedFields,
		Rid: nil,
	}
	return &joinedTuple
}

func joinTupleFields(t1 *Tuple, t2 *Tuple) []DBValue {
	var joinedFields []DBValue
	for _, t1Field := range t1.Fields {
		if !isDBValInSlice(t1Field, joinedFields){
			joinedFields = append(joinedFields, t1Field)
		}
	}
	for _, t2Field := range t2.Fields {
		if !isDBValInSlice(t2Field, joinedFields){
			joinedFields = append(joinedFields, t2Field)
		}
	}
	return joinedFields
}

type orderByState int

const (
	OrderedLessThan    orderByState = iota
	OrderedEqual       orderByState = iota
	OrderedGreaterThan orderByState = iota
)

// Apply the supplied expression to both t and t2, and compare the results,
// returning an orderByState value.
//
// Takes an arbitrary expressions rather than a field, because, e.g., for an
// ORDER BY SQL may ORDER BY arbitrary expressions, e.g., substr(name, 1, 2)
//
// Note that in most cases Expr will be a [godb.FieldExpr], which simply
// extracts a named field from a supplied tuple.
//
// Calling the [Expr.EvalExpr] method on a tuple will return the value of the
// expression on the supplied tuple.
func (t *Tuple) compareField(t2 *Tuple, field Expr) (orderByState, error) {
	// TODO: some code goes here
	// fmt.Printf("field = %v \n", field)
	// fmt.Printf("t = %v \n", *&t.Fields)
	// fmt.Printf("t2 = %v \n", *&t2.Fields)
	if (t == nil || t2 == nil){
		return OrderedEqual, fmt.Errorf("t or t2 are nil *Tuple objects")
	} else if (field == nil){
		return OrderedEqual, fmt.Errorf("no field expression provided")
	}
	// fmt.Printf("field type = %v \n", reflect.TypeOf(field))
	tDbValOfExpr, err := field.EvalExpr(t)
	if err != nil{
		return OrderedEqual, err
	} else if tDbValOfExpr == nil{
		return OrderedEqual, fmt.Errorf("tuple doesn't match field")
	}
	// fmt.Printf("tDbValOfExpr %v \n", tDbValOfExpr)
	t2DbValOfExpr, err2 := field.EvalExpr(t2)
	if err2 != nil{
		return OrderedEqual, err2
	} else if t2DbValOfExpr == nil{
		return OrderedEqual, fmt.Errorf("tuple2 doesn't match field")
	}
	// fmt.Printf("t2DbValOfExpr %v \n", t2DbValOfExpr)
	switch tDbValType := tDbValOfExpr.(type){
		case IntField:
			t2DbValInt, isIntField := t2DbValOfExpr.(IntField)
			if !isIntField{
				return OrderedEqual, fmt.Errorf("t field type of Int doesn't match t2 field type of %v", t2DbValInt)
			}
			if tDbValType.Value < t2DbValInt.Value{
				return OrderedLessThan, nil
			} else if tDbValType.Value > t2DbValInt.Value{
				return OrderedGreaterThan, nil
			} 
		case StringField:
			t2DbValString, isStringField := t2DbValOfExpr.(StringField)
			if !isStringField {
				return OrderedEqual, fmt.Errorf("t field type of String doesn't match t2 field type of %v", t2DbValString)
			}
			strComparison := strings.Compare(tDbValType.Value, t2DbValString.Value)
			if strComparison < 0 {
				return OrderedLessThan, nil
			} else if strComparison > 0{
				return OrderedGreaterThan, nil
			}
	}	
	return OrderedEqual, nil 
}

// Project out the supplied fields from the tuple. Should return a new Tuple
// with just the fields named in fields.
//
// Should not require a match on TableQualifier, but should prefer fields that
// do match on TableQualifier (e.g., a field  t1.name in fields should match an
// entry t2.name in t, but only if there is not an entry t1.name in t)
func (t *Tuple) project(fields []FieldType) (*Tuple, error) {
	// TODO: some code goes here
	// fmt.Printf("t.Fields  = %v \n", t.Fields)
	// fmt.Printf("fields  = %v \n", fields)
	projectedFields := make([]DBValue, 0)
	for _, field := range fields{
		isTupleContainsField, matchingIndex := t.Desc.tupleDescContainsField(field)
		isTableQualifierMatch, matchingTupleField := t.checkTableQualifier(field)
		if (isTableQualifierMatch){
			// fmt.Printf("table qualifier match with field = %v, matchingTupleField = %v \n", field, matchingTupleField)
			projectedFields = append(projectedFields, matchingTupleField)
			break
		} else if (isTupleContainsField){
			// fmt.Printf("non TQ match with field = %v \n", field)
			projectedFields = append(projectedFields, t.Fields[matchingIndex])
			break 
		} else {
			return nil, fmt.Errorf("field %v was not in the supplied tuple", t)
		}
	}
	// fmt.Printf("project projectedFields = %v \n", projectedFields)
	projectedTuple := Tuple{
		Fields: projectedFields,
		Desc: t.Desc,
		Rid: t.Rid,
	}
	return &projectedTuple, nil 
}

func (t* Tuple) checkTableQualifier(field FieldType) (bool, DBValue) {
	for _, tupleField := range t.Fields{
		if tupleField == field.Ftype{
			return true, tupleField
		}
	}
	return false, nil
}

// Compute a key for the tuple to be used in a map structure
func (t *Tuple) tupleKey() any {

	//todo efficiency here is poor - hashstructure is probably slow
	hash, _ := hashstructure.Hash(t, hashstructure.FormatV2, nil)

	return hash
}

var winWidth int = 120

func fmtCol(v string, ncols int) string {
	colWid := winWidth / ncols
	nextLen := len(v) + 3
	remLen := colWid - nextLen
	if remLen > 0 {
		spacesRight := remLen / 2
		spacesLeft := remLen - spacesRight
		return strings.Repeat(" ", spacesLeft) + v + strings.Repeat(" ", spacesRight) + " |"
	} else {
		return " " + v[0:colWid-4] + " |"
	}
}

// Return a string representing the header of a table for a tuple with the
// supplied TupleDesc.
//
// Aligned indicates if the tuple should be foramtted in a tabular format
func (d *TupleDesc) HeaderString(aligned bool) string {
	outstr := ""
	for i, f := range d.Fields {
		tableName := ""
		if f.TableQualifier != "" {
			tableName = f.TableQualifier + "."
		}

		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(tableName+f.Fname, len(d.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, tableName+f.Fname)
		}
	}
	return outstr
}

// Return a string representing the tuple
// Aligned indicates if the tuple should be formatted in a tabular format
func (t *Tuple) PrettyPrintString(aligned bool) string {
	outstr := ""
	for i, f := range t.Fields {
		str := ""
		switch f := f.(type) {
		case IntField:
			str = fmt.Sprintf("%d", f.Value)
		case StringField:
			str = f.Value
		}
		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(str, len(t.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, str)
		}
	}
	return outstr

}
