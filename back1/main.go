package main

import (
	sql2 "database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"io"
	"log"
	"math"
	"net"
	"proxy-go/sql"
	"strconv"
	"strings"
)

/* ======================================================
 * Protocol (Shadow RPC v1)
 * ====================================================== */

/* ---------- WHERE expr (only for OPEN) ---------- */

type WhereExpr struct {
	Type  string      `json:"type"`            // and / or / cmp / col / const
	Op    string      `json:"op,omitempty"`    // = != < > <= >= LIKE
	Name  string      `json:"name,omitempty"`  // column name
	Value string      `json:"value,omitempty"` // literal
	Args  []WhereExpr `json:"args,omitempty"`
	Left  *WhereExpr  `json:"left,omitempty"`
	Right *WhereExpr  `json:"right,omitempty"`
}
type Aggregate struct {
	Type   string `json:"type"`   // COUNT / MIN / MAX
	Column string `json:"column"` // "*" or column name
}

type OrderBy struct {
	Col  string `json:"col"`
	Desc bool   `json:"desc"`
}

/* ---------- Unified Request ---------- */

type Request struct {
	Op      string `json:"op"`
	Table   string `json:"table"`
	DBName  string `json:"dbname,omitempty"`
	TraceID string `json:"trace_id,omitempty"`

	ResultFormat string `json:"result_format,omitempty"`
	/* OPEN */
	Projection []string   `json:"projection,omitempty"`
	Where      *WhereExpr `json:"where,omitempty"`
	OrderBy    []OrderBy  `json:"order_by,omitempty"`
	Limit      uint64     `json:"limit,omitempty"`
	Offset     uint64     `json:"offset,omitempty"`
	Aggregate  *Aggregate `json:"aggregate,omitempty"`
	/* INSERT / DELETE */
	Row map[string]interface{} `json:"row,omitempty"`

	/* UPDATE */
	Set      map[string]interface{} `json:"set,omitempty"`
	WhereRow map[string]interface{} `json:"where_row,omitempty"`

	/* POINT_GET */
	RowID string `json:"row_id,omitempty"`
}

/* ======================================================
 * Entry
 * ====================================================== */

func main() {
	err := sql.InitFromDB()
	if err != nil {
		log.Println(err)
	}

	ln, err := net.Listen("tcp", "0.0.0.0:5555")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("[ShadowGoServer] listening on 127.0.0.1:5555")

	for {
		conn, _ := ln.Accept()
		go handleConn(conn)
	}
}

/* ======================================================
 * Connection
 * ====================================================== */

func handleConn(conn net.Conn) {

	defer conn.Close()

	decoder := json.NewDecoder(conn)

	for {
		var req Request
		if err := decoder.Decode(&req); err != nil {
			if err == io.EOF {
				// ✅ 客户端正常结束（短连接 or 长连接关闭）
				log.Println("[INFO] client closed connection")
				return
			}
			// 真正的协议 / JSON 错误
			log.Println("[ERR] decode failed:", err)
			return
		}

		log.Printf(
			"[REQ] op=%s table=%s db=%s trace=%s",
			req.Op,
			req.Table,
			req.DBName,
			req.TraceID,
		)
		switch req.Op {
		case "OPEN":
			handleOpen(conn, req)
		case "INSERT":
			handleInsert(conn, req)
		case "UPDATE":
			handleUpdate(conn, req)
		case "DELETE":
			handleDelete(conn, req)
		case "POINT_GET":
			handlePointGet(conn, req)
		default:
			sendError(conn, "unknown op")
		}
	}
}

/* ======================================================
 * MySQL Packet Helpers
 * ====================================================== */

func writePacket(conn net.Conn, seq *uint8, payload []byte) error {
	l := len(payload)
	header := []byte{byte(l), byte(l >> 8), byte(l >> 16), *seq}
	*seq++
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

func writeEOF(conn net.Conn, seq *uint8) {
	_ = writePacket(conn, seq, []byte{0xFE, 0x00, 0x00, 0x00, 0x00})
}

func sendOK(conn net.Conn) {
	var seq uint8 = 0
	ok := []byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x00}
	_ = writePacket(conn, &seq, ok)
}

func sendError(conn net.Conn, msg string) {
	var seq uint8 = 0
	payload := append([]byte{0xFF, 0x00, 0x00}, []byte(msg)...)
	_ = writePacket(conn, &seq, payload)
}

func makeTextRow(values []interface{}) []byte {
	var row []byte
	for _, v := range values {
		if v == nil {
			// MySQL 协议中，NULL 值用 0xFB 表示
			row = append(row, 0xfb)
			continue
		}

		// 关键点：所有类型在 Text Protocol 中都转为字符串处理
		var s string
		switch val := v.(type) {
		case []byte:
			s = string(val)
		default:
			s = fmt.Sprintf("%v", val)
		}

		// 使用 Length Encoded String 包装
		row = append(row, mysql.PutLengthEncodedString([]byte(s))...)
	}
	return row
}

func makeBinaryRow(values []interface{}) []byte {
	colNum := len(values)
	nullBitmapLen := (colNum + 7 + 2) / 8

	row := make([]byte, 1+nullBitmapLen)
	row[0] = 0x00

	for _, v := range values {
		switch x := v.(type) {
		case int32:
			row = append(row, byte(x), byte(x>>8), byte(x>>16), byte(x>>24))
		case int64:
			for i := 0; i < 8; i++ {
				row = append(row, byte(x>>(8*i)))
			}
		case float64:
			b := math.Float64bits(x)
			for i := 0; i < 8; i++ {
				row = append(row, byte(b>>(8*i)))
			}
		case []uint8: // 即 []byte
			// 关键：使用 MySQL 的长度编码规则放入字节数组
			row = append(row, mysql.PutLengthEncodedString(x)...)
		case string:
			// 字符串同理，先转成字节数组
			row = append(row, mysql.PutLengthEncodedString([]byte(x))...)
		}
	}
	return row
}

/* ======================================================
 * WHERE evaluator (ONLY for OPEN)
 * ====================================================== */

func evalWhere(e *WhereExpr, row map[string]interface{}) bool {
	if e == nil {
		return true
	}

	switch e.Type {
	case "and":
		for _, a := range e.Args {
			if !evalWhere(&a, row) {
				return false
			}
		}
		return true

	case "or":
		for _, a := range e.Args {
			if evalWhere(&a, row) {
				return true
			}
		}
		return false

	case "cmp":
		var lv, rv interface{}
		if e.Left != nil && e.Left.Type == "col" {
			lv = row[e.Left.Name]
		}
		if e.Right != nil && e.Right.Type == "const" {
			rv = e.Right.Value
		}
		return evalCmp(e.Op, lv, rv)
	}
	return false
}

func evalCmp(op string, l, r interface{}) bool {
	switch lv := l.(type) {
	case int32, int64, int:
		return compareFloat(op, toFloat(lv), toFloat(r))
	case float64:
		return compareFloat(op, lv, toFloat(r))
	case string:
		rs, _ := r.(string)
		switch op {
		case "=":
			return lv == rs
		case "!=":
			return lv != rs
		case "LIKE":
			return strings.Contains(lv, strings.Trim(rs, "%"))
		}
	}
	return false
}

func compareFloat(op string, l, r float64) bool {
	switch op {
	case "=":
		return l == r
	case "!=":
		return l != r
	case "<":
		return l < r
	case "<=":
		return l <= r
	case ">":
		return l > r
	case ">=":
		return l >= r
	}
	return false
}

func toFloat(v interface{}) float64 {
	switch x := v.(type) {
	case int:
		return float64(x)
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	case float64:
		return x
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	}
	return 0
}

/* ======================================================
 * Projection helpers (WRITE OPS)
 * ====================================================== */

// 把 projection 转成 set；nil 表示不限制
func projectionSet(p []string) map[string]struct{} {
	if len(p) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(p))
	for _, c := range p {
		m[c] = struct{}{}
	}
	return m
}

// 只保留 projection 中允许的列
func filterByProjection(
	input map[string]interface{},
	proj map[string]struct{},
) map[string]interface{} {

	if proj == nil {
		return input
	}

	out := make(map[string]interface{})
	for k, v := range input {
		if _, ok := proj[k]; ok {
			out[k] = v
		}
	}
	return out
}

/* ======================================================
 * Handlers
 * ====================================================== */

func handleOpen(conn net.Conn, req Request) {
	var sb strings.Builder
	args := []any{}

	// SELECT
	if req.Aggregate != nil {
		sb.WriteString("SELECT ")
		sb.WriteString(req.Aggregate.Type)
		sb.WriteString("(")
		sb.WriteString(req.Aggregate.Column)
		sb.WriteString(")")
	} else if len(req.Projection) > 0 {
		sb.WriteString("SELECT ")
		for i, c := range req.Projection {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(sql.QuoteIdent(c))
		}
	} else {
		sb.WriteString("SELECT *")
	}

	sb.WriteString(" FROM ")
	sb.WriteString(sql.QuoteIdent(req.DBName))
	sb.WriteString(".")
	sb.WriteString(sql.QuoteIdent(req.Table))

	// WHERE（直接信任 where_json）
	if req.Where != nil {
		sb.WriteString(" WHERE ")
		whereSQL := whereExprToSQL(req.Where)
		if whereSQL != "" {
			sb.WriteString(whereSQL)
		}
	}

	// ORDER BY
	if len(req.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, ob := range req.OrderBy {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(sql.QuoteIdent(ob.Col))
			if ob.Desc {
				sb.WriteString(" DESC")
			}
		}
	}

	if req.Limit > 0 {
		sb.WriteString(" LIMIT ")
		sb.WriteString(strconv.FormatUint(req.Limit, 10))
	}
	if req.Offset > 0 {
		sb.WriteString(" OFFSET ")
		sb.WriteString(strconv.FormatUint(req.Offset, 10))
	}

	query, err := sql.MyDB.Query(sb.String(), args...)
	if err != nil {
		sendError(conn, err.Error())
		return
	}
	cols, _ := query.Columns()
	colTypes, _ := query.ColumnTypes()

	var seq uint8 = 0

	// Column count
	_ = writePacket(conn, &seq,
		mysql.PutLengthEncodedInt(uint64(len(cols))))

	// Column definitions
	for i, c := range cols {
		log.Println(colTypes[i])
		field := &mysql.Field{
			Schema:  []byte(req.DBName),
			Table:   []byte(req.Table),
			Name:    []byte(c),
			Type:    mysqlTypeFromDatabaseType(colTypes[i]),
			Flag:    mysqlNullable(colTypes[i]),
			Charset: uint16(33),
		}
		_ = writePacket(conn, &seq, field.Dump())
	}
	writeEOF(conn, &seq)
	//writeColumns(conn, &seq)

	// Rows
	values := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}

	for query.Next() {
		query.Scan(ptrs...)
		row := makeBinaryRow(values)
		_ = writePacket(conn, &seq, row)
	}

	writeEOF(conn, &seq)
}

func handleOpen1(conn net.Conn, req Request) {
	var sb strings.Builder
	args := []any{}

	// SELECT
	if req.Aggregate != nil {
		sb.WriteString("SELECT ")
		sb.WriteString(req.Aggregate.Type)
		sb.WriteString("(")
		sb.WriteString(req.Aggregate.Column)
		sb.WriteString(")")
	} else if len(req.Projection) > 0 {
		sb.WriteString("SELECT ")
		for i, c := range req.Projection {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(sql.QuoteIdent(c))
		}
	} else {
		sb.WriteString("SELECT *")
	}

	sb.WriteString(" FROM ")
	sb.WriteString(sql.QuoteIdent(req.DBName))
	sb.WriteString(".")
	sb.WriteString(sql.QuoteIdent(req.Table))

	// WHERE（直接信任 where_json）
	if req.Where != nil {
		sb.WriteString(" WHERE ")
		whereSQL := whereExprToSQL(req.Where)
		if whereSQL != "" {
			sb.WriteString(whereSQL)
		}
	}

	// ORDER BY
	if len(req.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, ob := range req.OrderBy {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(sql.QuoteIdent(ob.Col))
			if ob.Desc {
				sb.WriteString(" DESC")
			}
		}
	}

	if req.Limit > 0 {
		sb.WriteString(" LIMIT ")
		sb.WriteString(strconv.FormatUint(req.Limit, 10))
	}
	if req.Offset > 0 {
		sb.WriteString(" OFFSET ")
		sb.WriteString(strconv.FormatUint(req.Offset, 10))
	}

	query, err := sql.MyDB.Query(sb.String(), args...)
	if err != nil {
		sendError(conn, err.Error())
		return
	}
	cols, _ := query.Columns()
	colTypes, _ := query.ColumnTypes()

	var seq uint8 = 0

	// Column count
	_ = writePacket(conn, &seq,
		mysql.PutLengthEncodedInt(uint64(len(cols))))

	// Column definitions
	for i, c := range cols {
		log.Println(colTypes[i])
		field := &mysql.Field{
			Schema:  []byte(req.DBName),
			Table:   []byte(req.Table),
			Name:    []byte(c),
			Type:    mysqlTypeFromDatabaseType(colTypes[i]),
			Flag:    mysqlNullable(colTypes[i]),
			Charset: uint16(33),
		}
		_ = writePacket(conn, &seq, field.Dump())
	}
	writeEOF(conn, &seq)
	//writeColumns(conn, &seq)

	// Rows
	values := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}

	for query.Next() {
		query.Scan(ptrs...)
		row := makeBinaryRow(values)
		_ = writePacket(conn, &seq, row)
	}

	writeEOF(conn, &seq)
}

func mysqlNullable(ct *sql2.ColumnType) uint16 {
	nullable, _ := ct.Nullable()
	if nullable {
		return mysql.NUM_FLAG
	} else {
		return mysql.NOT_NULL_FLAG
	}

}

func mysqlTypeFromDatabaseType(ct *sql2.ColumnType) byte {
	t := strings.ToUpper(ct.DatabaseTypeName())
	log.Println(t)
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

func writeColumns(conn net.Conn, seq *uint8) {
	cols := []*mysql.Field{
		{Name: []byte("id"), Type: mysql.MYSQL_TYPE_LONG},
		{Name: []byte("big"), Type: mysql.MYSQL_TYPE_LONGLONG},
		{Name: []byte("score"), Type: mysql.MYSQL_TYPE_DOUBLE},
		{Name: []byte("name"), Type: mysql.MYSQL_TYPE_VAR_STRING},
	}
	for _, c := range cols {
		_ = writePacket(conn, seq, c.Dump())
	}
	writeEOF(conn, seq)
}

func handlePointGet(conn net.Conn, req Request) {
	log.Printf("[POINT_GET] table=%s row_id=%s", req.Table, req.RowID)

	sqlStr := fmt.Sprintf(
		"SELECT * FROM %s WHERE id = ? LIMIT 1",
		sql.QuoteIdent(req.Table),
	)
	queryRow, err := sql.MyDB.Query(sqlStr, req.RowID)
	if err != nil {
		sendError(conn, err.Error())
		return
	}
	cols, _ := queryRow.Columns()
	colTypes, _ := queryRow.ColumnTypes()
	var seq uint8 = 0
	// 写返回列
	_ = writePacket(conn, &seq,
		mysql.PutLengthEncodedInt(uint64(len(cols))))
	// Column definitions
	for i, c := range cols {
		field := &mysql.Field{
			Name: []byte(c),
			Type: mysqlTypeFromDatabaseType(colTypes[i]),
		}
		_ = writePacket(conn, &seq, field.Dump())
	}
	writeEOF(conn, &seq)

	// Rows
	values := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}

	for queryRow.Next() {
		queryRow.Scan(ptrs...)
		row := makeBinaryRow(values)
		_ = writePacket(conn, &seq, row)
	}
	writeEOF(conn, &seq)
}

func handleInsert(conn net.Conn, req Request) {
	log.Println("[INSERT]")
	log.Printf("  table = %s", req.Table)
	log.Printf("  row   = %v", req.Row)
	log.Printf("  projection = %v", req.Projection)

	proj := projectionSet(req.Projection)
	row := filterByProjection(req.Row, proj)

	var cols []string
	var vals []string

	for k, v := range row {
		cols = append(cols, k)
		vals = append(vals, sqlLiteral(v))
	}

	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		sql.QuoteIdent(req.Table),
		strings.Join(cols, ", "),
		strings.Join(vals, ", "),
	)

	log.Printf("  SQL = %s", sql)
	sendOK(conn)
}

func handleUpdate(conn net.Conn, req Request) {
	log.Println("[UPDATE]")
	log.Printf("  table     = %s", req.Table)
	log.Printf("  set       = %v", req.Set)
	log.Printf("  where_row = %v", req.WhereRow)
	log.Printf("  projection = %v", req.Projection)

	proj := projectionSet(req.Projection)

	set := filterByProjection(req.Set, proj)
	where := filterByProjection(req.WhereRow, proj)

	var sets []string
	for k, v := range set {
		sets = append(sets, fmt.Sprintf("%s = %s", k, sqlLiteral(v)))
	}

	var wheres []string
	for k, v := range where {
		wheres = append(wheres, fmt.Sprintf("%s = %s", k, sqlLiteral(v)))
	}

	sql := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		req.Table,
		strings.Join(sets, ", "),
		strings.Join(wheres, " AND "),
	)

	log.Printf("  SQL = %s", sql)
	sendOK(conn)
}

func handleDelete(conn net.Conn, req Request) {
	log.Println("[DELETE]")
	log.Printf("  table      = %s", req.Table)
	log.Printf("  row        = %v", req.Row)
	log.Printf("  projection = %v", req.Projection)

	// Projection 为空 ⇒ 无条件删除
	if len(req.Projection) == 0 {
		sql := fmt.Sprintf("DELETE FROM %s", req.Table)
		log.Printf("  SQL = %s", sql)
		sendOK(conn)
		return
	}

	// Projection 非空 ⇒ 只使用 projection ∩ row
	proj := projectionSet(req.Projection)
	where := filterByProjection(req.Row, proj)

	var wheres []string
	for k, v := range where {
		wheres = append(wheres, fmt.Sprintf("%s = %s", k, sqlLiteral(v)))
	}

	sql := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		req.Table,
		strings.Join(wheres, " AND "),
	)

	log.Printf("  SQL = %s", sql)
	sendOK(conn)
}

func whereExprToSQL(e *WhereExpr) string {
	if e == nil {
		return ""
	}

	switch e.Type {
	case "and":
		var parts []string
		for _, a := range e.Args {
			parts = append(parts, whereExprToSQL(&a))
		}
		return "(" + strings.Join(parts, " AND ") + ")"

	case "or":
		var parts []string
		for _, a := range e.Args {
			parts = append(parts, whereExprToSQL(&a))
		}
		return "(" + strings.Join(parts, " OR ") + ")"

	case "cmp":
		if e.Left == nil || e.Right == nil {
			return ""
		}
		col := e.Left.Name
		val := sqlLiteral(e.Right.Value)
		return fmt.Sprintf("%s %s %s", col, e.Op, val)
	}

	return ""
}

func sqlLiteral(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		// 简化版，demo 用；生产要处理转义
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case int, int32, int64:
		return fmt.Sprintf("%d", x)
	case float32, float64:
		return fmt.Sprintf("%v", x)
	default:
		return "'" + fmt.Sprintf("%v", x) + "'"
	}
}

func handleOpenAggregate(conn net.Conn, req Request) {
	log.Printf("[OPEN-AGG] %s(%s)",
		req.Aggregate.Type,
		req.Aggregate.Column)

	var (
		count int64
		min   *float64
		max   *float64
	)

	// ===== 模拟后端扫描 + WHERE =====
	for i := 0; i < 100; i++ {
		row := map[string]interface{}{
			"id":    int64(i),
			"big":   int64(i * 1000),
			"score": float64(i) * 1.5,
			"name":  "row_name",
		}

		if !evalWhere(req.Where, row) {
			continue
		}

		switch req.Aggregate.Type {
		case "COUNT":
			count++

		case "MIN":
			v := toFloat(row[req.Aggregate.Column])
			if min == nil || v < *min {
				tmp := v
				min = &tmp
			}

		case "MAX":
			v := toFloat(row[req.Aggregate.Column])
			if max == nil || v > *max {
				tmp := v
				max = &tmp
			}
		}
	}

	/* ===== MySQL 协议返回（Text Resultset：1 列 1 行）===== */

	var seq uint8 = 0

	// ---- 列数 ----
	_ = writePacket(conn, &seq, mysql.PutLengthEncodedInt(1))

	// ---- 列定义（列名必须是表达式）----
	colName := req.Aggregate.Type
	if req.Aggregate.Type == "COUNT" {
		colName = "COUNT(*)"
	} else {
		colName = fmt.Sprintf("%s(%s)", req.Aggregate.Type, req.Aggregate.Column)
	}

	col := &mysql.Field{
		Name: []byte(colName),
		Type: mysql.MYSQL_TYPE_LONGLONG,
	}
	_ = writePacket(conn, &seq, col.Dump())
	writeEOF(conn, &seq)

	// ---- 行（Text Protocol）----
	switch req.Aggregate.Type {
	case "COUNT":
		row := mysql.PutLengthEncodedString(
			[]byte(strconv.FormatInt(count, 10)),
		)
		_ = writePacket(conn, &seq, row)
	case "MIN":
		if min == nil {
			// Text protocol NULL
			_ = writePacket(conn, &seq, []byte{0xfb})
		} else {
			_ = writePacket(
				conn,
				&seq,
				mysql.PutLengthEncodedString(
					[]byte(strconv.FormatFloat(*min, 'f', -1, 64)),
				),
			)
		}

	case "MAX":
		if max == nil {
			_ = writePacket(conn, &seq, []byte{0xfb})
		} else {
			_ = writePacket(
				conn,
				&seq,
				mysql.PutLengthEncodedString(
					[]byte(strconv.FormatFloat(*max, 'f', -1, 64)),
				),
			)
		}
	}

	writeEOF(conn, &seq)
}
