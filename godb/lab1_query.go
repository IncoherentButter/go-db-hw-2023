package godb

import (
	"errors"
	"os"
)

// This function should load the csv file in fileName into a heap file (see
// [HeapFile.LoadFromCSV]) and then compute the sum of the integer field in
// string and return its value as an int The supplied csv file is comma
// delimited and has a header If the file doesn't exist or can't be opened, or
// the field doesn't exist, or the field is not and integer, should return an
// err. Note that when you create a HeapFile, you will need to supply a file
// name;  you can supply a non-existant file, in which case it will be created.
// However, subsequent invocations of this method will result in tuples being
// reinserted into this file unless you delete (e.g., with [os.Remove] it before
// calling NewHeapFile.
func computeFieldSum(fileName string, td TupleDesc, sumField string) (int, error) {
	os.Remove("nonexistentfile.dat")

	bp := NewBufferPool(16)
	hf, _ := NewHeapFile("nonexistentfile.dat", &td, bp)
	tid := NewTID()
	sum := 0

	file, err := os.Open(fileName)
	if err != nil {
		return -1, err
	}

	hf.LoadFromCSV(file, true, ",", false)
	iter, _ := hf.Iterator(tid)

	for {
		t, _ := iter()

		if t == nil {
			break
		}

		field := []FieldType{FieldType{Fname: sumField, TableQualifier: "", Ftype: IntType}}

		tnum, _ := t.project(field)

		if len(tnum.Fields) == 0 {
			return -1, errors.New("field doesn't exist, or field isn't an integer")
		}

		sum += int(tnum.Fields[0].(IntField).Value)
	}

	return sum, nil // replace me
}
