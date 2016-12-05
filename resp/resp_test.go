package resp

import (
	"errors"
	"io"
	"reflect"
	. "testing"

	"github.com/stretchr/testify/assert"
)

func TestReadSlice(t *T) {
	assertReadSlice := func(in, left, right string, eof bool) {
		var m Pool
		m.buf.WriteString(in)
		b, err := m.readSlice('\n')
		if eof {
			assert.Equal(t, io.EOF, err)
		} else {
			assert.Nil(t, err)
		}
		assert.Equal(t, []byte(left), b)
		assert.Equal(t, []byte(right), m.buf.Bytes())
	}

	assertReadSlice("foo\n", "foo\n", "", false)
	assertReadSlice("foo\nbar", "foo\n", "bar", false)
	assertReadSlice("\nbar", "\n", "bar", false)
	assertReadSlice("foobar", "foobar", "", true)
}

func TestBufferedIntDelim(t *T) {
	assertBID := func(in string, left int64, right string, eof bool) {
		var m Pool
		m.buf.WriteString(in)
		i, err := m.bufferedIntDelim()
		if eof {
			assert.Equal(t, io.EOF, err)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, left, i)
		}
		assert.Equal(t, []byte(right), m.buf.Bytes())
	}

	assertBID("5\r\n", 5, "", false)
	assertBID("5\r\nfoo", 5, "foo", false)
	assertBID("5foo", 0, "", true)
}

func TestRESPTypes(t *T) {
	assertType := func(m Marshaler, expb string) {
		b, err := m.MarshalRESP()
		assert.Nil(t, err)
		assert.Equal(t, expb, string(b))

		umr := reflect.New(reflect.TypeOf(m))
		err = umr.Interface().(Unmarshaler).UnmarshalRESP(b)
		assert.Nil(t, err)
		assert.Equal(t, m, umr.Elem().Interface())
	}

	assertType(SimpleString{S: []byte("")}, "+\r\n")
	assertType(SimpleString{S: []byte("foo")}, "+foo\r\n")

	assertType(Error{E: errors.New("")}, "-\r\n")
	assertType(Error{E: errors.New("foo")}, "-foo\r\n")

	assertType(Int{I: 5}, ":5\r\n")
	assertType(Int{I: 0}, ":0\r\n")
	assertType(Int{I: -5}, ":-5\r\n")

	assertType(BulkString{B: nil}, "$-1\r\n")
	assertType(BulkString{B: []byte{}}, "$0\r\n\r\n")
	assertType(BulkString{B: []byte("foo")}, "$3\r\nfoo\r\n")
	assertType(BulkString{B: []byte("foo\r\nbar")}, "$8\r\nfoo\r\nbar\r\n")

	assertType(ArrayHeader{N: 5}, "*5\r\n")
	assertType(ArrayHeader{N: -1}, "*-1\r\n")
}

type textCPMarshaler []byte

func (cm textCPMarshaler) MarshalText() ([]byte, error) {
	cm = append(cm, '_')
	return cm, nil
}

type binCPMarshaler []byte

func (cm binCPMarshaler) MarshalBinary() ([]byte, error) {
	cm = append(cm, '_')
	return cm, nil
}

func TestAnyMarshal(t *T) {
	var encodeTests = []struct {
		in  interface{}
		out string
	}{
		//// Bulk strings
		//{in: []byte("ohey"), out: "$4\r\nohey\r\n"},
		//{in: "ohey", out: "$4\r\nohey\r\n"},
		//{in: true, out: "$1\r\n1\r\n"},
		//{in: false, out: "$1\r\n0\r\n"},
		//{in: nil, out: "$-1\r\n"},
		//{in: float32(5.5), out: "$3\r\n5.5\r\n"},
		//{in: float64(5.5), out: "$3\r\n5.5\r\n"},
		//{in: textCPMarshaler("ohey"), out: "$5\r\nohey_\r\n"},
		//{in: binCPMarshaler("ohey"), out: "$5\r\nohey_\r\n"},

		//// Int
		//{in: 5, out: ":5\r\n"},
		//{in: int64(5), out: ":5\r\n"},
		//{in: uint64(5), out: ":5\r\n"},

		// Error
		{in: errors.New(":("), out: "-:(\r\n"},

		// Simple arrays
		{in: []string{}, out: "*0\r\n"},
		//{in: []string{"a", "b"}, out: "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
		//{in: []int{1, 2}, out: "*2\r\n:1\r\n:2\r\n"},

		//// Complex arrays
		//{in: []interface{}{}, out: "*0\r\n"},
		//{in: []interface{}{"a", 1}, out: "*2\r\n$1\r\na\r\n:1\r\n"},

		//// Embedded arrays
		//{
		//	in:  []interface{}{[]string{"a", "b"}, []int{1, 2}},
		//	out: "*2\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n*2\r\n:1\r\n:2\r\n",
		//},

		//// Maps
		//{in: map[string]int{"one": 1}, out: "*2\r\n$3\r\none\r\n:1\r\n"},
		//{
		//	in:  map[string]interface{}{"one": []byte("1")},
		//	out: "*2\r\n$3\r\none\r\n$1\r\n1\r\n",
		//},
		//{
		//	in:  map[string]interface{}{"one": []string{"1", "2"}},
		//	out: "*2\r\n$3\r\none\r\n*2\r\n$1\r\n1\r\n$1\r\n2\r\n",
		//},
	}

	// first we do the tests with the same pool each time
	p := new(Pool)
	for _, et := range encodeTests {
		b, err := Any{Pool: p, I: et.in}.MarshalRESP()
		assert.Nil(t, err)
		assert.Equal(t, et.out, string(b))
	}

	//// then again with a new Pool each time
	//for _, et := range encodeTests {
	//	b, err := Any{I: et.in}.MarshalRESP()
	//	assert.Nil(t, err)
	//	assert.Equal(t, et.out, string(b))
	//}
}
