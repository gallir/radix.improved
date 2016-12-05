package radix

import (
	"bufio"
	"bytes"
	"encoding"
	"fmt"
	"io"
	"reflect"
	"strconv"
)

type encoder2 struct {
	w       *bufio.Writer
	bodyBuf *bytes.Buffer
	scratch []byte
}

// NewEncoder2 initializes an encoder instance which will write to the given
// io.Writer. The io.Writer should not be used outside of the encoder after this
func NewEncoder2(w io.Writer) Encoder {
	return &encoder2{
		w:       bufio.NewWriter(w),
		bodyBuf: bytes.NewBuffer(make([]byte, 0, 1024)),
		scratch: make([]byte, 0, 1024),
	}
}

func (e *encoder2) Encode(v interface{}) error {
	err := e.write(v)
	if ferr := e.w.Flush(); ferr != nil && err == nil {
		err = ferr
	}
	return err
}

// write writes whatever arbitrary data it's given as a resp. It does not handle
// any of the types which would be turned into arrays, those must be handled
// through walk
func (e *encoder2) write(v interface{}) error {
	switch vt := v.(type) {
	case LenReader:
		return e.writeLenReader(vt)
	case []byte:
		e.bodyBuf.Write(vt)
		return e.writeLenReader(e.bodyBuf)
	case string:
		e.bodyBuf.WriteString(vt)
		return e.writeLenReader(e.bodyBuf)
	case bool:
		b := bools[0]
		if vt {
			b = bools[1]
		}
		e.bodyBuf.Write(b)
		return e.writeLenReader(e.bodyBuf)
	case float32:
		b := strconv.AppendFloat(e.scratch[:0], float64(vt), 'f', -1, 32)
		e.bodyBuf.Write(b)
		return e.writeLenReader(e.bodyBuf)
	case float64:
		b := strconv.AppendFloat(e.scratch[:0], vt, 'f', -1, 64)
		e.bodyBuf.Write(b)
		return e.writeLenReader(e.bodyBuf)
	case nil:
		return e.writeBulkNil()
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return e.writeInt(anyIntToInt64(vt))
	case error:
		// if we're writing an error we just assume that they want it as an
		// error type on the wire
		return e.writeAppErr(AppErr{Err: vt})
	case encoding.TextMarshaler:
		b, err := vt.MarshalText()
		if err != nil {
			return err
		}
		e.bodyBuf.Write(b)
		return e.writeLenReader(e.bodyBuf)
	case encoding.BinaryMarshaler:
		b, err := vt.MarshalBinary()
		if err != nil {
			return err
		}
		e.bodyBuf.Write(b)
		return e.writeLenReader(e.bodyBuf)
	//case RawCmd:
	//	return e.writeCmd(vt)
	//case Resp:
	//	return e.writeResp(vt)
	case Marshaler:
		return vt.Marshal(e.w)
	}

	if vv := reflect.ValueOf(v); vv.Kind() == reflect.Ptr {
		return e.write(vv.Elem().Interface())
	}

	return fmt.Errorf("cannot encode %T as a redis type", v)
}

func (e *encoder2) writeLenReader(lr LenReader) error {
	var err error
	err = e.writeBytes(err, bulkStrPrefix)
	err = e.writeBytes(err, strconv.AppendInt(e.scratch[:0], int64(lr.Len()), 10))
	err = e.writeBytes(err, delim)
	if err != nil {
		return err
	}

	_, err = io.Copy(e.w, lr)
	err = e.writeBytes(err, delim)
	return err
}

func (e *encoder2) writeInt(i int64) error {
	var err error
	err = e.writeBytes(err, intPrefix)
	err = e.writeBytes(err, strconv.AppendInt(e.scratch[:0], i, 10))
	err = e.writeBytes(err, delim)
	return err
}

func (e *encoder2) writeAppErr(ae AppErr) error {
	var err error
	err = e.writeBytes(err, errPrefix)
	err = e.writeBytes(err, append(e.scratch[:0], ae.Error()...))
	err = e.writeBytes(err, delim)
	return err
}

func (e *encoder2) writeArrayHeader(l int) error {
	var err error
	err = e.writeBytes(err, arrayPrefix)
	err = e.writeBytes(err, strconv.AppendInt(e.scratch[:0], int64(l), 10))
	err = e.writeBytes(err, delim)
	return err
}

func (e *encoder2) writeBulkNil() error {
	return e.writeBytes(nil, nilBulkStr)
}

func (e *encoder2) writeArrayNil() error {
	return e.writeBytes(nil, nilArray)
}

func (e *encoder2) writeBytes(prevErr error, b []byte) error {
	if prevErr != nil {
		return prevErr
	}
	_, err := e.w.Write(b)
	return err
}
