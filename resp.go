package radix

import (
	"fmt"
	"io"
)

var (
	delim    = []byte{'\r', '\n'}
	delimEnd = delim[len(delim)-1]
	delimLen = len(delim)
)

var (
	simpleStrPrefix = []byte{'+'}
	errPrefix       = []byte{'-'}
	intPrefix       = []byte{':'}
	bulkStrPrefix   = []byte{'$'}
	arrayPrefix     = []byte{'*'}
	nilBulkStr      = []byte("$-1\r\n")
	nilArray        = []byte("*-1\r\n")
)

const (
	rSimpleStr = iota
	rBulkStr
	rAppErr // An error returned by redis, e.g. WRONGTYPE
	rInt
	rArray
)

// Resp can be used to encode or decode exact values of the resp protocol (the
// network protocol that redis uses). When encoding, the first non-nil field (or
// one of the nil booleans) will be used as the resp value. When decoding the
// value being read will be filled into the corresponding field based on its
// type, the others being left nil.
//
// When all fields are their zero value (i.e. Resp{}) the Int field is the one
// used, and the Resp will encode/decode as an int resp of the value 0.
type Resp struct {
	SimpleStr  []byte
	BulkStr    []byte
	Err        error
	Arr        []Resp
	BulkStrNil bool
	ArrNil     bool

	Int int64
}

// Unmarshaler will be used by the Decoder when reading into a type implementing
// it. The function given can be used to read the data into a separate temporary
// value first.
//
// Errors returned from Unmarshal will automatically be wrapped in the
// UnmarshalErr type by the Decoder.
//
// TODO example
type Unmarshaler interface {
	Unmarshal(func(interface{}) error) error
}

// ArrayHeader represents the header sent preceding array elements in the RESP
// protocol. It does not actually encompass any elements itself, it only
// declares how many elements will come after it.
type ArrayHeader int

// Marshal implements the method for the Marshaler interface. It will only write
// the array header, for your RESP stream to be valid you must write the same
// number of elements as the ArrayHeader's value after it.
func (ah ArrayHeader) Marshal(w io.Writer) error {
	_, err := fmt.Fprintf(w, "*%d\r\n", ah)
	return err
}

// SimpleString is a special RESP type which differs in the protocol from a
// BulkStr. The built-in Encoder will always encode []byte and strings into bulk
// strings, and the Decoder treats simple strings and bulk strings as
// equivalent. This type can be used if you explicitly wish to encode into a
// SimpleString.
type SimpleString string

// Marshal implements the method for the Marshaler interface. It will write the
// string in the format of a RESP simple string.
func (ss SimpleString) Marshal(w io.Writer) error {
	_, err := fmt.Fprintf(w, "+%s\r\n", ss)
	return err
}
