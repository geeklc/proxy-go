package build

import (
	"bytes"
	sql2 "database/sql"
	"encoding/binary"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/utils"
	"github.com/pingcap/errors"
	"log"
	"math"
	"net"
	"strconv"
	"strings"
	"time"
)

func WriteColumnNames(cols []string, conn net.Conn, colTypes []*sql2.ColumnType, seq *uint8) {
	// Column definitions
	for i, c := range cols {
		log.Println(c)
		field := &mysql.Field{
			Name: []byte(c),
			Type: mysqlTypeFromDatabaseType(colTypes[i]),
		}
		_ = WritePacket(conn, seq, field.Dump())
	}
	WriteEOF(conn, seq)
}

func mysqlTypeFromDatabaseType(ct *sql2.ColumnType) byte {
	t := strings.ToUpper(ct.DatabaseTypeName())

	switch t {

	// -------- Integer --------
	case "TINYINT", "BOOL", "BOOLEAN":
		return mysql.MYSQL_TYPE_TINY
	case "SMALLINT":
		return mysql.MYSQL_TYPE_SHORT
	case "MEDIUMINT":
		return mysql.MYSQL_TYPE_INT24
	case "INT", "INTEGER":
		return mysql.MYSQL_TYPE_LONG
	case "BIGINT":
		return mysql.MYSQL_TYPE_LONGLONG

	// -------- Float / Decimal --------
	case "FLOAT":
		return mysql.MYSQL_TYPE_FLOAT
	case "DOUBLE":
		return mysql.MYSQL_TYPE_DOUBLE
	case "DECIMAL", "NUMERIC":
		return mysql.MYSQL_TYPE_NEWDECIMAL

	// -------- String --------
	case "CHAR":
		return mysql.MYSQL_TYPE_STRING
	case "VARCHAR":
		return mysql.MYSQL_TYPE_VAR_STRING
	case "ENUM":
		return mysql.MYSQL_TYPE_ENUM
	case "SET":
		return mysql.MYSQL_TYPE_SET

	// -------- Text / Blob --------
	case "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
		return mysql.MYSQL_TYPE_BLOB
	case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB":
		return mysql.MYSQL_TYPE_BLOB

	// -------- Binary --------
	case "BINARY":
		return mysql.MYSQL_TYPE_STRING
	case "VARBINARY":
		return mysql.MYSQL_TYPE_VAR_STRING

	// -------- Time --------
	case "DATE":
		return mysql.MYSQL_TYPE_DATE
	case "TIME":
		return mysql.MYSQL_TYPE_TIME
	case "DATETIME":
		return mysql.MYSQL_TYPE_DATETIME
	case "TIMESTAMP":
		return mysql.MYSQL_TYPE_TIMESTAMP
	case "YEAR":
		return mysql.MYSQL_TYPE_YEAR

	// -------- JSON --------
	case "JSON":
		return mysql.MYSQL_TYPE_JSON

	// -------- Bit / Geometry --------
	case "BIT":
		return mysql.MYSQL_TYPE_BIT
	case "GEOMETRY":
		return mysql.MYSQL_TYPE_GEOMETRY

	default:
		return mysql.MYSQL_TYPE_VAR_STRING
	}
}

func Uint64ToBytes(n uint64) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
		byte(n >> 32),
		byte(n >> 40),
		byte(n >> 48),
		byte(n >> 56),
	}
}

func appendUintN(row []byte, n uint64, size int) []byte {
	for i := 0; i < size; i++ {
		row = append(row, byte(n>>(8*i)))
	}
	return row
}

func toUint64(value interface{}) (uint64, error) {
	switch v := value.(type) {
	case int8:
		return uint64(v), nil
	case int16:
		return uint64(v), nil
	case int32:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case int:
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return v, nil
	case uint:
		return uint64(v), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case []byte:
		return strconv.ParseUint(string(v), 10, 64)
	case string:
		return strconv.ParseUint(v, 10, 64)
	default:
		return 0, errors.Errorf("invalid integer type %T", value)
	}
}

func toFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case []byte:
		return strconv.ParseFloat(string(v), 64)
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		n, err := toUint64(value)
		if err != nil {
			return 0, errors.Errorf("invalid float type %T", value)
		}
		return float64(n), nil
	}
}

func toBytes(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return []byte(fmt.Sprintf("%v", value)), nil
	}
}

func toBinaryDateTime(t time.Time) ([]byte, error) {
	var buf bytes.Buffer

	if t.IsZero() {
		// ⚠️ 关键：Binary DATETIME zero 必须有 length byte = 0
		buf.WriteByte(0x00)
		return buf.Bytes(), nil
	}

	year, month, day := t.Year(), t.Month(), t.Day()
	hour, min, sec := t.Hour(), t.Minute(), t.Second()
	nanosec := t.Nanosecond()

	if nanosec > 0 {
		buf.WriteByte(byte(11))
		_ = binary.Write(&buf, binary.LittleEndian, uint16(year))
		buf.WriteByte(byte(month))
		buf.WriteByte(byte(day))
		buf.WriteByte(byte(hour))
		buf.WriteByte(byte(min))
		buf.WriteByte(byte(sec))
		_ = binary.Write(&buf, binary.LittleEndian, uint32(nanosec/1000))
	} else if hour > 0 || min > 0 || sec > 0 {
		buf.WriteByte(byte(7))
		_ = binary.Write(&buf, binary.LittleEndian, uint16(year))
		buf.WriteByte(byte(month))
		buf.WriteByte(byte(day))
		buf.WriteByte(byte(hour))
		buf.WriteByte(byte(min))
		buf.WriteByte(byte(sec))
	} else {
		buf.WriteByte(byte(4))
		_ = binary.Write(&buf, binary.LittleEndian, uint16(year))
		buf.WriteByte(byte(month))
		buf.WriteByte(byte(day))
	}

	return buf.Bytes(), nil
}

func formatBinaryValue(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case int8:
		return Uint64ToBytes(uint64(v)), nil
	case int16:
		return Uint64ToBytes(uint64(v)), nil
	case int32:
		return Uint64ToBytes(uint64(v)), nil
	case int64:
		return Uint64ToBytes(uint64(v)), nil
	case int:
		return Uint64ToBytes(uint64(v)), nil
	case uint8:
		return Uint64ToBytes(uint64(v)), nil
	case uint16:
		return Uint64ToBytes(uint64(v)), nil
	case uint32:
		return Uint64ToBytes(uint64(v)), nil
	case uint64:
		return Uint64ToBytes(v), nil
	case uint:
		return Uint64ToBytes(uint64(v)), nil
	case float32:
		return Uint64ToBytes(math.Float64bits(float64(v))), nil
	case float64:
		bits := math.Float64bits(v)

		return Uint64ToBytes(bits), nil
	case []byte:
		return v, nil
	case string:
		return utils.StringToByteSlice(v), nil
	case time.Time:
		//return utils.StringToByteSlice(v.Format("2006-01-02 15:04:05")), nil
		return toBinaryDateTime(v)
	default:
		return nil, errors.Errorf("invalid type %T", value)
	}
}

// 参考/github.com/go-mysql-org/go-mysql@v1.13.0/mysql/resultset_helper.go:233
func MakeBinaryRow1(values []interface{}, colTypes []*sql2.ColumnType) ([]byte, error) {
	if len(values) != len(colTypes) {
		return nil, errors.Errorf("values count %d != column count %d", len(values), len(colTypes))
	}

	bitmapLen := (len(values) + 7 + 2) >> 3
	nullBitmap := make([]byte, bitmapLen)

	var row []byte
	row = append(row, 0)
	row = append(row, nullBitmap...)

	for j, value := range values {
		if value == nil {
			nullBitmap[(j+2)/8] |= 1 << (uint(j+2) % 8)
			continue
		}

		typ := mysqlTypeFromDatabaseType(colTypes[j])
		var err error
		row, err = appendBinaryValueByType(row, value, typ)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	copy(row[1:], nullBitmap)
	return row, nil
}

func appendBinaryValueByType(row []byte, value interface{}, typ byte) ([]byte, error) {
	switch typ {
	case mysql.MYSQL_TYPE_TINY:
		n, err := toUint64(value)
		if err != nil {
			return nil, err
		}
		return append(row, byte(n)), nil
	case mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_YEAR:
		n, err := toUint64(value)
		if err != nil {
			return nil, err
		}
		return appendUintN(row, n, 2), nil
	case mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_INT24:
		n, err := toUint64(value)
		if err != nil {
			return nil, err
		}
		return appendUintN(row, n, 4), nil
	case mysql.MYSQL_TYPE_LONGLONG:
		n, err := toUint64(value)
		if err != nil {
			return nil, err
		}
		return appendUintN(row, n, 8), nil
	case mysql.MYSQL_TYPE_FLOAT:
		f, err := toFloat64(value)
		if err != nil {
			return nil, err
		}
		return appendUintN(row, uint64(math.Float32bits(float32(f))), 4), nil
	case mysql.MYSQL_TYPE_DOUBLE:
		f, err := toFloat64(value)
		if err != nil {
			return nil, err
		}
		return appendUintN(row, math.Float64bits(f), 8), nil
	case mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIMESTAMP:
		t, ok := value.(time.Time)
		if !ok {
			return nil, errors.Errorf("invalid temporal type %T", value)
		}
		b, err := toBinaryDateTime(t)
		if err != nil {
			return nil, err
		}
		return append(row, b...), nil
	case mysql.MYSQL_TYPE_TIME,
		mysql.MYSQL_TYPE_DECIMAL,
		mysql.MYSQL_TYPE_NEWDECIMAL,
		mysql.MYSQL_TYPE_BIT,
		mysql.MYSQL_TYPE_BLOB,
		mysql.MYSQL_TYPE_TINY_BLOB,
		mysql.MYSQL_TYPE_MEDIUM_BLOB,
		mysql.MYSQL_TYPE_LONG_BLOB,
		mysql.MYSQL_TYPE_STRING,
		mysql.MYSQL_TYPE_VARCHAR,
		mysql.MYSQL_TYPE_VAR_STRING,
		mysql.MYSQL_TYPE_JSON,
		mysql.MYSQL_TYPE_GEOMETRY,
		mysql.MYSQL_TYPE_ENUM,
		mysql.MYSQL_TYPE_SET:
		b, err := toBytes(value)
		if err != nil {
			return nil, err
		}
		return append(row, mysql.PutLengthEncodedString(b)...), nil
	default:
		return nil, errors.Errorf("unsupported column type %d for value %T", typ, value)
	}
}

func formatField(field *mysql.Field, value interface{}) error {
	switch value.(type) {
	case int8, int16, int32, int64, int:
		field.Charset = 63
		field.Flag = mysql.BINARY_FLAG | mysql.NOT_NULL_FLAG
	case uint8, uint16, uint32, uint64, uint:
		field.Charset = 63
		field.Flag = mysql.BINARY_FLAG | mysql.NOT_NULL_FLAG | mysql.UNSIGNED_FLAG
	case float32, float64:
		field.Charset = 63
		field.Flag = mysql.BINARY_FLAG | mysql.NOT_NULL_FLAG
	case time.Time:
		field.Charset = 33
	case string, []byte:
		field.Charset = 33
	case nil:
		field.Charset = 33
	default:
		return errors.Errorf("unsupport type %T for resultset", value)
	}
	return nil
}

func fieldType(value interface{}) (typ uint8, err error) {
	switch value.(type) {
	case int8, int16, int32, int64, int:
		typ = mysql.MYSQL_TYPE_LONGLONG
	case uint8, uint16, uint32, uint64, uint:
		typ = mysql.MYSQL_TYPE_LONGLONG
	case float32, float64:
		typ = mysql.MYSQL_TYPE_DOUBLE
	case string, []byte:
		typ = mysql.MYSQL_TYPE_VAR_STRING
	case time.Time:
		typ = mysql.MYSQL_TYPE_DATETIME
	case nil:
		typ = mysql.MYSQL_TYPE_NULL
	default:
		err = errors.Errorf("unsupport type %T for resultset", value)
	}
	return
}
