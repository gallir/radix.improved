// Package resp implements the redis RESP protocol, a plaintext protocol which
// is also binary safe. Redis uses the RESP protocol to communicate with its
// clients, but there's nothing about the protocol which ties it to redis, it
// could be used for almost anything.
//
// See https://redis.io/topics/protocol for more details on the protocol.
package resp

import (
	"bytes"
	"encoding"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"strconv"
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
	nilBulkString   = []byte("$-1\r\n")
	nilArray        = []byte("*-1\r\n")
)

var bools = [][]byte{
	{'0'},
	{'1'},
}

func anyIntToInt64(m interface{}) int64 {
	switch mt := m.(type) {
	case int:
		return int64(mt)
	case int8:
		return int64(mt)
	case int16:
		return int64(mt)
	case int32:
		return int64(mt)
	case int64:
		return mt
	case uint:
		return int64(mt)
	case uint8:
		return int64(mt)
	case uint16:
		return int64(mt)
	case uint32:
		return int64(mt)
	case uint64:
		return int64(mt)
	}
	panic(fmt.Sprintf("anyIntToInt64 got bad arg: %#v", m))
}

// Marshaler is the interface implemented by types that can marshal themselves
// into valid RESP.
//
// As an implementation detail, the []byte returned by MarshalRESP will be
// written to its destination _before_ the next MarshalRESP is called. In other
// words, re-using byte slices across multiple MarshalRESP calls is explicitly
// supported
type Marshaler interface {
	MarshalRESP() ([]byte, error)
}

// Unmarshaler is the interface implemented by types that can unmarshal a RESP
// description of themselves. The input can be assumed to be a valid encoding of
// a RESP value. UnmarshalRESP must copy the data if it wishes to retain the
// data after returning.
type Unmarshaler interface {
	UnmarshalRESP([]byte) error
}

// It's not a factory if we don't call it a factory

// Pool is used to create RESP types which will be written to or read from a
// stream. It's main function is to allow for resource-sharing to limit memory
// allocations. Its zero value can be used as-is.

// Pool is used between RESP types so that they may share resources during
// reading and writing, primarily to avoid memory allocations. `new(resp.Pool)`
// is the proper way to initialize a Pool. It is optional in all places which
// it appears.
type Pool struct {
	buf     bytes.Buffer
	scratch []byte
}

// used to retrieve or initialize a Pool, depending on whether it's been
// initialized already. It will Reset the buffer and scratch if it has already
// been initialized. This is the only method on Pool which allows for a nil
// Pool.
func (p *Pool) get() *Pool {
	if p == nil {
		return new(Pool)
	}
	p.buf.Reset()
	p.scratch = p.scratch[:0]
	return p
}

// because for some reason bytes.Buffer doesn't expose this, even though it has
// it private and bufio.Reader has it public. Luckily it's not hard to implement
func (p *Pool) readSlice(delim byte) ([]byte, error) {
	b := p.buf.Bytes()
	i := bytes.IndexByte(b, delim)
	var err error
	if i < 0 {
		i = len(b)
		err = io.EOF
	}
	return p.buf.Next(i + 1), err
}

// effectively an assert that the buffered data starts with the given slice,
// discarding the slice at the same time
func (p *Pool) bufferedPrefix(prefix []byte) error {
	b := p.buf.Next(len(prefix))
	if !bytes.Equal(b, prefix) {
		return fmt.Errorf("expected prefix %q, got %q", prefix, b)
	}
	return nil
}

// reads bytes up to a delim and returns them, or an error
func (p *Pool) bufferedBytesDelim() ([]byte, error) {
	b, err := p.readSlice('\r')
	if err != nil {
		return nil, err
	}

	// there's a trailing \n we have to read
	_, err = p.buf.ReadByte()
	return b[:len(b)-1], err
}

// reads an integer out of the buffer, followed by a delim. It parses the
// integer, or returns an error
func (p *Pool) bufferedIntDelim() (int64, error) {
	b, err := p.bufferedBytesDelim()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(b), 10, 64)
}

////////////////////////////////////////////////////////////////////////////////

// SimpleString represents the simple string type in the RESP protocol. An S
// value of nil is equivalent to empty string.
type SimpleString struct {
	*Pool
	S []byte
}

// MarshalRESP implements the Marshaler method
func (ss SimpleString) MarshalRESP() ([]byte, error) {
	p := ss.get()
	p.buf.Write(simpleStrPrefix)
	p.buf.Write(ss.S)
	p.buf.Write(delim)
	return p.buf.Bytes(), nil
}

// UnmarshalRESP implements the Unmarshaler method
func (ss *SimpleString) UnmarshalRESP(b []byte) error {
	p := ss.get()
	p.buf.Write(b)
	if err := p.bufferedPrefix(simpleStrPrefix); err != nil {
		return err
	}
	b, err := p.bufferedBytesDelim()
	ss.S = b
	return err
}

////////////////////////////////////////////////////////////////////////////////

// Error represents an error type in the RESP protocol. Note that this only
// represents an actual error message being read/written on the stream, it is
// separate from network or parsing errors. An E value of nil is equivalent to
// an empty error string
type Error struct {
	*Pool
	E error
}

// MarshalRESP implements the Marshaler method
func (e Error) MarshalRESP() ([]byte, error) {
	p := e.get()
	p.buf.Write(errPrefix)
	if e.E != nil {
		p.buf.WriteString(e.E.Error())
	}
	p.buf.Write(delim)
	return p.buf.Bytes(), nil
}

// UnmarshalRESP implements the Unmarshaler method
func (e *Error) UnmarshalRESP(b []byte) error {
	p := e.get()
	p.buf.Write(b)
	if err := p.bufferedPrefix(errPrefix); err != nil {
		return err
	}
	b, err := p.bufferedBytesDelim()
	e.E = errors.New(string(b))
	return err
}

////////////////////////////////////////////////////////////////////////////////

// Int represents an int type in the RESP protocol
type Int struct {
	*Pool
	I int64
}

// MarshalRESP implements the Marshaler method
func (i Int) MarshalRESP() ([]byte, error) {
	p := i.get()
	p.buf.Write(intPrefix)
	p.buf.Write(strconv.AppendInt(p.scratch, int64(i.I), 10))
	p.buf.Write(delim)
	return p.buf.Bytes(), nil
}

// UnmarshalRESP implements the Unmarshaler method
func (i *Int) UnmarshalRESP(b []byte) error {
	p := i.get()
	p.buf.Write(b)
	if err := p.bufferedPrefix(intPrefix); err != nil {
		return err
	}
	n, err := p.bufferedIntDelim()
	i.I = n
	return err
}

////////////////////////////////////////////////////////////////////////////////

// BulkString represents the bulk string type in the RESP protocol. A B value of
// nil indicates the nil bulk string message, versus a B value of []byte{} which
// indicates a bulk string of length 0.
type BulkString struct {
	*Pool
	B []byte
}

// MarshalRESP implements the Marshaler method
func (b BulkString) MarshalRESP() ([]byte, error) {
	if b.B == nil {
		return nilBulkString, nil
	}
	p := b.get()
	p.buf.Write(bulkStrPrefix)
	p.buf.Write(strconv.AppendInt(p.scratch, int64(len(b.B)), 10))
	p.buf.Write(delim)
	p.buf.Write(b.B)
	p.buf.Write(delim)
	return p.buf.Bytes(), nil
}

// UnmarshalRESP implements the Unmarshaler method
func (b *BulkString) UnmarshalRESP(bb []byte) error {
	p := b.get()
	p.buf.Write(bb)
	if err := p.bufferedPrefix(bulkStrPrefix); err != nil {
		return err
	}
	n, err := p.bufferedIntDelim()
	nn := int(n)
	if err != nil {
		return err
	} else if n == -1 {
		b.B = nil
		return nil
	} else if b.B == nil {
		// if b has any length we don't want it to be nil, but we also don't
		// want to overwrite any byte slice which has already been allocated
		// there.
		b.B = make([]byte, 0, nn)
	}

	if b.B = append(b.B[0:], p.buf.Next(nn)...); len(b.B) != nn {
		return fmt.Errorf("bulk string expected %d bytes but got %d", nn, len(b.B))
	} else if d := p.buf.Next(2); !bytes.Equal(d, delim) {
		return fmt.Errorf("bulk string expected delim %q but got %q", delim, d)
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// ArrayHeader represents the header sent preceding array elements in the RESP
// protocol. It does not actually encompass any elements itself, it only
// declares how many elements will come after it.
//
// An N of -1 may also be used to indicate a nil response, as per the RESP spec
type ArrayHeader struct {
	*Pool
	N int
}

// MarshalRESP implements the Marshaler method
func (ah ArrayHeader) MarshalRESP() ([]byte, error) {
	p := ah.get()
	p.buf.Write(arrayPrefix)
	p.buf.Write(strconv.AppendInt(p.scratch, int64(ah.N), 10))
	p.buf.Write(delim)
	return p.buf.Bytes(), nil
}

// UnmarshalRESP implements the Unmarshaler method
func (ah *ArrayHeader) UnmarshalRESP(b []byte) error {
	p := ah.get()
	p.buf.Write(b)
	if err := p.bufferedPrefix(arrayPrefix); err != nil {
		return err
	}
	n, err := p.bufferedIntDelim()
	ah.N = int(n)
	return err
}

////////////////////////////////////////////////////////////////////////////////

// Any represents any primitive go type, such as integers, floats, strings,
// bools, etc... It also includes encoding.Text(Un)Marshalers and
// encoding.(Un)BinaryMarshalers, and will properly handle the case where I is a
// resp.(Un)Marshaler.
//
// Most things will be treated as bulk strings, except for those that have their
// own corresponding type in the RESP protocol (e.g. ints). strings and []bytes
// will always be encoded as bulk strings, never simple strings.
//
// Arrays and slices will be treated as RESP arrays, and their values we be
// treated as if also wrapped in an Any struct. Maps will be similarly treated,
// but they will be flattened into arrays of their alternating keys/values
// first.
type Any struct {
	*Pool
	I interface{}
}

// MarshalRESP implements the Marshaler method
func (a Any) MarshalRESP() ([]byte, error) {
	p := a.get()

	// helper that we use for cases where we append data to scratch but we want
	// that data to be the bulk string value. We can't just pass it directly
	// because BulkString also uses scratch. So instead we do something hacky
	bulkStrFromScratch := func() ([]byte, error) {
		ogScratch := p.scratch
		p.scratch = p.scratch[len(p.scratch):]
		b, err := BulkString{Pool: p, B: ogScratch}.MarshalRESP()
		// We append the new scratch to the old, because that's the actual
		// length of data we wish we could handle. Technically this step isn't
		// necessary.
		p.scratch = append(ogScratch, p.scratch...)
		return b, err
	}

	switch at := a.I.(type) {
	case []byte:
		return BulkString{Pool: p, B: at}.MarshalRESP()
	case string:
		p.scratch = append(p.scratch, at...)
		return bulkStrFromScratch()
	case bool:
		b := bools[0]
		if at {
			b = bools[1]
		}
		return BulkString{Pool: p, B: b}.MarshalRESP()
	case float32:
		p.scratch = strconv.AppendFloat(p.scratch, float64(at), 'f', -1, 32)
		return bulkStrFromScratch()
	case float64:
		p.scratch = strconv.AppendFloat(p.scratch, at, 'f', -1, 64)
		return bulkStrFromScratch()
	case nil:
		return BulkString{Pool: p}.MarshalRESP()
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return Int{Pool: p, I: anyIntToInt64(at)}.MarshalRESP()
	case error:
		return Error{Pool: p, E: at}.MarshalRESP()
	case Marshaler:
		return at.MarshalRESP()
	case encoding.TextMarshaler:
		b, err := at.MarshalText()
		if err != nil {
			return nil, err
		}
		return BulkString{Pool: p, B: b}.MarshalRESP()
	case encoding.BinaryMarshaler:
		b, err := at.MarshalBinary()
		if err != nil {
			return nil, err
		}
		return BulkString{Pool: p, B: b}.MarshalRESP()
	}

	// now we use.... reflection! duhduhduuuuh....
	vv := reflect.ValueOf(a.I)

	// if it's a pointer we de-reference and try the pointed to value directly
	if vv.Kind() == reflect.Ptr {
		return Any{Pool: p, I: reflect.Indirect(vv)}.MarshalRESP()
	}

	// for array types we're going to be creating new Any's for each
	// sub-element, each with their own Pool. The Pool will be based off this
	// one, but with the buffer pointer skipped ahead so as to be appending. So
	// we keep the original buffer out here for convenience
	ogBuf := p.buf
	log.Printf("ogBuf: %q", ogBuf.Bytes())

	// some helper functions
	var err error
	arrHeader := func(l int) {
		if err != nil {
			return
		}
		var ahb []byte
		if ahb, err = (ArrayHeader{Pool: p, N: l}.MarshalRESP()); err == nil {
			ogBuf.Write(ahb)
		}
		log.Printf("wrote header %d, ogBuf:%q", l, ogBuf.Bytes())
	}
	arrVal := func(v interface{}) {
		// this is what ensures we can re-use the tail of our buffer if it's big
		// enough, but we never overwrite what's there
		p.buf = *bytes.NewBuffer(ogBuf.Bytes()[ogBuf.Len():])
		var ib []byte
		if ib, err = (Any{Pool: p, I: v}).MarshalRESP(); err == nil {
			ogBuf.Write(ib)
		}
	}

	switch vv.Kind() {
	case reflect.Slice, reflect.Array:
		if vv.IsNil() {
			ogBuf.Write(nilArray)
			break
		}

		l := vv.Len()
		arrHeader(l)
		for i := 0; i < l; i++ {
			arrVal(vv.Index(i).Interface())
		}
		log.Printf("returning %q", ogBuf.Bytes())

	case reflect.Map:
		if vv.IsNil() {
			ogBuf.Write(nilArray)
			break
		}
		kkv := vv.MapKeys()
		arrHeader(len(kkv) * 2)
		for _, kv := range kkv {
			arrVal(kv.Interface())
			arrVal(vv.MapIndex(kv).Interface())
		}

	default:
		return nil, fmt.Errorf("could not marshal value of type %T", a.I)
	}

	// ogBuf may have grown while we were using it outside the pool, add it back
	// in
	p.buf = ogBuf
	log.Printf("returning %q", ogBuf.Bytes())
	return ogBuf.Bytes(), err
}
