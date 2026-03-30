package main

import (
	stdsql "database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"proxy-go/build"
	"proxy-go/sql"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	driverMysql "github.com/go-sql-driver/mysql"
)

/* ======================================================
 * Protocol (Shadow RPC v1)
 * ====================================================== */

/* ---------- WHERE expr (only for OPEN) ---------- */

type WhereExpr struct {
	Type  string      `json:"type"`            // and / or / cmp / col / const / func / raw
	Op    string      `json:"op,omitempty"`    // = != < > <= >= LIKE ...
	Name  string      `json:"name,omitempty"`  // column name / function name
	Value string      `json:"value,omitempty"` // literal / raw sql
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

	KeyData map[string]interface{} `json:"key_data,omitempty"`

	/* POINT_GET */
	RowID string `json:"row_id,omitempty"`

	/* RAW */
	Sql string `json:"sql,omitempty"`
}

type queryExecutor interface {
	Exec(query string, args ...any) (stdsql.Result, error)
	Query(query string, args ...any) (*stdsql.Rows, error)
}

type Session struct {
	tx *stdsql.Tx
}

func (s *Session) executor() queryExecutor {
	if s != nil && s.tx != nil {
		return s.tx
	}
	return sql.MyDB
}

func (s *Session) statusFlags() uint16 {
	if s != nil && s.tx != nil {
		return 0x0001
	}
	return 0x0002
}

func (s *Session) begin() error {
	if s.tx != nil {
		return errors.New("transaction already active")
	}
	tx, err := sql.MyDB.Begin()
	if err != nil {
		return err
	}
	s.tx = tx
	return nil
}

func (s *Session) commit() error {
	if s.tx == nil {
		return nil
	}
	err := s.tx.Commit()
	s.tx = nil
	return err
}

func (s *Session) rollback() error {
	if s.tx == nil {
		return nil
	}
	err := s.tx.Rollback()
	s.tx = nil
	return err
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
	session := &Session{}
	defer func() {
		if err := session.rollback(); err != nil {
			log.Printf("[WARN] rollback on close failed: %v", err)
		}
	}()

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
			handleOpen(conn, session, req)
		case "INSERT":
			handleInsert(conn, session, req)
		case "UPDATE":
			handleUpdate(conn, session, req)
		case "DELETE":
			handleDelete(conn, session, req)
		case "POINT_GET":
			handlePointGet(conn, session, req)
		case "RAW":
			handleRaw(conn, session, req)
		case "TX_BEGIN":
			handleTxBegin(conn, session)
		case "TX_COMMIT":
			handleTxCommit(conn, session)
		case "TX_ROLLBACK":
			handleTxRollback(conn, session)
		default:
			sendError(conn, "unknown op")
		}
	}
}

func sendOK(conn net.Conn) {
	var seq uint8 = 0
	ok := []byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x00}
	_ = build.WritePacket(conn, &seq, ok)
}

func writeLenEncInt(buf *[]byte, n uint64) {
	switch {
	case n < 251:
		*buf = append(*buf, byte(n))
	case n < 1<<16:
		*buf = append(*buf, 0xfc)
		*buf = append(*buf, byte(n), byte(n>>8))
	case n < 1<<24:
		*buf = append(*buf, 0xfd)
		*buf = append(*buf,
			byte(n),
			byte(n>>8),
			byte(n>>16),
		)
	default:
		*buf = append(*buf, 0xfe)
		for i := 0; i < 8; i++ {
			*buf = append(*buf, byte(n>>(8*i)))
		}
	}
}

func buildOKPayload(
	affectedRows uint64,
	insertId uint64,
	status uint16,
	warnings uint16,
	info string,
) []byte {

	var payload []byte

	// header
	payload = append(payload, 0x00)

	// affected_rows
	writeLenEncInt(&payload, affectedRows)

	// last_insert_id
	writeLenEncInt(&payload, insertId)

	// status_flags (2 bytes little endian)
	payload = append(payload,
		byte(status),
		byte(status>>8),
	)

	// warnings (2 bytes little endian)
	payload = append(payload,
		byte(warnings),
		byte(warnings>>8),
	)

	// info (optional)
	if info != "" {
		payload = append(payload, []byte(info)...)
	}

	return payload
}

func sendErrorPacket(conn net.Conn, errno uint16, sqlState string, msg string) {
	var seq uint8 = 0
	payload := []byte{0xFF, byte(errno), byte(errno >> 8)}
	if sqlState == "" {
		sqlState = "HY000"
	}
	if len(sqlState) < 5 {
		sqlState += strings.Repeat("0", 5-len(sqlState))
	}
	payload = append(payload, '#')
	payload = append(payload, []byte(sqlState[:5])...)
	payload = append(payload, []byte(msg)...)
	_ = build.WritePacket(conn, &seq, payload)
}

func fallbackSQLState(errno uint16) string {
	switch errno {
	case 1062:
		return "23000"
	default:
		return "HY000"
	}
}

func sendError(conn net.Conn, msg string) {
	sendErrorPacket(conn, 1105, "HY000", msg)
}

func sendDBError(conn net.Conn, err error) {
	if err == nil {
		sendError(conn, "unknown database error")
		return
	}

	var mysqlErr *driverMysql.MySQLError
	if errors.As(err, &mysqlErr) {
		sqlState := string(mysqlErr.SQLState[:])
		if strings.Trim(sqlState, "\x00") == "" {
			sqlState = fallbackSQLState(mysqlErr.Number)
		}
		sendErrorPacket(conn, mysqlErr.Number, sqlState, mysqlErr.Message)
		return
	}

	sendError(conn, err.Error())
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

func normalizeShadowValue(v any) (any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return v, nil
	}

	rawType, ok := m["__shadow_type"]
	if !ok {
		return v, nil
	}

	shadowType, ok := rawType.(string)
	if !ok || shadowType != "blob" {
		return v, nil
	}

	encoding, ok := m["encoding"].(string)
	if !ok {
		return nil, errors.New("blob value missing encoding")
	}
	if encoding != "base64" {
		return nil, fmt.Errorf("unsupported blob encoding: %s", encoding)
	}

	data, ok := m["data"].(string)
	if !ok {
		return nil, errors.New("blob value missing data")
	}

	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, fmt.Errorf("decode blob base64 failed: %w", err)
	}

	return decoded, nil
}

func normalizeShadowMapValues(input map[string]interface{}) (map[string]interface{}, error) {
	if input == nil {
		return nil, nil
	}

	out := make(map[string]interface{}, len(input))
	for k, v := range input {
		normalized, err := normalizeShadowValue(v)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", k, err)
		}
		out[k] = normalized
	}
	return out, nil
}

/* ======================================================
 * Handlers
 * ====================================================== */
func handleTxBegin(conn net.Conn, session *Session) {
	if err := session.begin(); err != nil {
		sendDBError(conn, err)
		return
	}
	var seq uint8 = 0
	payload := buildOKPayload(0, 0, session.statusFlags(), 0, "")
	build.WritePacket(conn, &seq, payload)
}

func handleTxCommit(conn net.Conn, session *Session) {
	if err := session.commit(); err != nil {
		sendDBError(conn, err)
		return
	}
	var seq uint8 = 0
	payload := buildOKPayload(0, 0, session.statusFlags(), 0, "")
	build.WritePacket(conn, &seq, payload)
}

func handleTxRollback(conn net.Conn, session *Session) {
	if err := session.rollback(); err != nil {
		sendDBError(conn, err)
		return
	}
	var seq uint8 = 0
	payload := buildOKPayload(0, 0, session.statusFlags(), 0, "")
	build.WritePacket(conn, &seq, payload)
}

func handleRaw(conn net.Conn, session *Session, req Request) {
	log.Println("[OPEN]")
	log.Printf("  table      = %s", req.Table)
	log.Printf("  DBName 	= %s", req.DBName)
	log.Printf("  sql      	= %s", req.Sql)

	log.Printf("RAW  SQL = %s", req.Sql)
	query, err := session.executor().Query(req.Sql)
	if err != nil {
		sendDBError(conn, err)
		return
	}
	defer query.Close()
	cols, _ := query.Columns()
	colTypes, _ := query.ColumnTypes()

	var seq uint8 = 0

	// Column count
	_ = build.WritePacket(conn, &seq,
		mysql.PutLengthEncodedInt(uint64(len(cols))))
	build.WriteColumnNames(cols, conn, colTypes, &seq)

	// Rows
	values := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))

	for i := range values {
		ptrs[i] = &values[i]
	}
	i := 0
	for query.Next() {
		query.Scan(ptrs...)
		row, err := build.MakeBinaryRow1(values, colTypes)
		if err != nil {
			log.Printf("[ERR] build RAW row failed: %v", err)
			return
		}
		_ = build.WritePacket(conn, &seq, row)
		i = i + 1
	}
	build.WriteEOF(conn, &seq)
	//如果数据为空则返回字段名
}

//	func handleOpen(conn net.Conn, session *Session, req Request) {
//		log.Println("[OPEN]")
//		log.Printf("  table      = %s", req.Table)
//		log.Printf("  projection = %v", req.Projection)
//		log.Printf("  limit      = %d", req.Limit)
//		log.Printf("  offset     = %d", req.Offset)
//		var sb strings.Builder
//		args := []any{}
//
//		// SELECT
//		if req.Aggregate != nil {
//			sb.WriteString("SELECT ")
//			sb.WriteString(req.Aggregate.Type)
//			sb.WriteString("(")
//			sb.WriteString(req.Aggregate.Column)
//			sb.WriteString(")")
//		} else if len(req.Projection) > 0 {
//			sb.WriteString("SELECT ")
//			for i, c := range req.Projection {
//				if i > 0 {
//					sb.WriteString(",")
//				}
//				sb.WriteString(sql.QuoteIdent(c))
//			}
//		} else {
//			sb.WriteString("SELECT *")
//		}
//
//		sb.WriteString(" FROM ")
//		sb.WriteString(sql.QuoteIdent(req.DBName))
//		sb.WriteString(".")
//		sb.WriteString(sql.QuoteIdent(req.Table))
//
//		// WHERE（直接信任 where_json）
//		if req.Where != nil {
//			sb.WriteString(" WHERE ")
//			whereSQL := whereExprToSQL(req.Where)
//			if whereSQL != "" {
//				sb.WriteString(whereSQL)
//			}
//		}
//
//		// ORDER BY
//		if len(req.OrderBy) > 0 {
//			sb.WriteString(" ORDER BY ")
//			for i, ob := range req.OrderBy {
//				if i > 0 {
//					sb.WriteString(",")
//				}
//				sb.WriteString(sql.QuoteIdent(ob.Col))
//				if ob.Desc {
//					sb.WriteString(" DESC")
//				}
//			}
//		}
//
//		if req.Limit > 0 {
//			sb.WriteString(" LIMIT ")
//			sb.WriteString(strconv.FormatUint(req.Limit, 10))
//		}
//		if req.Offset > 0 {
//			sb.WriteString(" OFFSET ")
//			sb.WriteString(strconv.FormatUint(req.Offset, 10))
//		}
//		log.Println(sb.String())
//		query, err := session.executor().Query(sb.String(), args...)
//		if err != nil {
//			sendDBError(conn, err)
//			return
//		}
//		defer query.Close()
//		cols, _ := query.Columns()
//		colTypes, _ := query.ColumnTypes()
//
//		var seq uint8 = 0
//
//		// Column count
//		_ = build.WritePacket(conn, &seq,
//			mysql.PutLengthEncodedInt(uint64(len(cols))))
//		build.WriteColumnNames(cols, conn, colTypes, &seq)
//
//		// Rows
//		values := make([]interface{}, len(cols))
//		ptrs := make([]interface{}, len(cols))
//
//		for i := range values {
//			ptrs[i] = &values[i]
//		}
//		i := 0
//		for query.Next() {
//			query.Scan(ptrs...)
//			row, err := build.MakeBinaryRow1(values, colTypes)
//			if err != nil {
//				log.Printf("[ERR] build OPEN row failed: %v", err)
//				return
//			}
//			_ = build.WritePacket(conn, &seq, row)
//			i = i + 1
//		}
//		build.WriteEOF(conn, &seq)
//		//如果数据为空则返回字段名
//	}
func handleOpen(conn net.Conn, session *Session, req Request) {
	log.Println("[OPEN]")
	log.Printf("  table      = %s", req.Table)
	log.Printf("  projection = %v", req.Projection)
	log.Printf("  limit      = %d", req.Limit)
	log.Printf("  offset     = %d", req.Offset)

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

	// WHERE
	if req.Where != nil {
		whereSQL := whereExprToSQL(req.Where)
		if whereSQL != "" {
			sb.WriteString(" WHERE ")
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

	log.Println(sb.String())

	query, err := session.executor().Query(sb.String(), args...)
	if err != nil {
		sendDBError(conn, err)
		return
	}
	defer query.Close()

	cols, _ := query.Columns()
	colTypes, _ := query.ColumnTypes()

	var seq uint8 = 0

	_ = build.WritePacket(conn, &seq,
		mysql.PutLengthEncodedInt(uint64(len(cols))))
	build.WriteColumnNames(cols, conn, colTypes, &seq)

	values := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}

	for query.Next() {
		query.Scan(ptrs...)
		row, err := build.MakeBinaryRow1(values, colTypes)
		if err != nil {
			log.Printf("[ERR] build OPEN row failed: %v", err)
			return
		}
		_ = build.WritePacket(conn, &seq, row)
	}

	build.WriteEOF(conn, &seq)
}

func quoteQualifiedIdent(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}

	parts := strings.Split(name, ".")
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return ""
		}
		parts[i] = sql.QuoteIdent(part)
	}
	return strings.Join(parts, ".")
}

func normalizeCmpOp(op string) string {
	switch strings.ToUpper(strings.TrimSpace(op)) {
	case "=":
		return "="
	case "!=":
		return "!="
	case "<>":
		return "<>"
	case "<":
		return "<"
	case "<=":
		return "<="
	case ">":
		return ">"
	case ">=":
		return ">="
	case "LIKE":
		return "LIKE"
	case "NOT LIKE":
		return "NOT LIKE"
	case "REGEXP":
		return "REGEXP"
	case "NOT REGEXP":
		return "NOT REGEXP"
	case "RLIKE":
		return "RLIKE"
	default:
		return ""
	}
}

func scalarExprToSQL(e *WhereExpr) string {
	if e == nil {
		return ""
	}

	switch strings.ToLower(strings.TrimSpace(e.Type)) {
	case "col":
		return quoteQualifiedIdent(e.Name)

	case "const":
		return sqlLiteral(e.Value)

	case "func":
		funcName := strings.TrimSpace(e.Name)
		if funcName == "" {
			return ""
		}

		args := make([]string, 0, len(e.Args))
		for i := range e.Args {
			argSQL := scalarExprToSQL(&e.Args[i])
			if argSQL == "" {
				return ""
			}
			args = append(args, argSQL)
		}

		return fmt.Sprintf("%s(%s)", funcName, strings.Join(args, ", "))

	case "raw":
		return strings.TrimSpace(e.Value)

	case "cmp":
		sqlText := whereExprToSQL(e)
		if sqlText == "" {
			return ""
		}
		return "(" + sqlText + ")"

	default:
		return ""
	}
}

func whereExprToSQL(e *WhereExpr) string {
	if e == nil {
		return ""
	}

	switch strings.ToLower(strings.TrimSpace(e.Type)) {
	case "and":
		parts := make([]string, 0, len(e.Args))
		for i := range e.Args {
			part := whereExprToSQL(&e.Args[i])
			if part == "" {
				return ""
			}
			parts = append(parts, part)
		}
		if len(parts) == 0 {
			return ""
		}
		if len(parts) == 1 {
			return parts[0]
		}
		return "(" + strings.Join(parts, " AND ") + ")"

	case "or":
		parts := make([]string, 0, len(e.Args))
		for i := range e.Args {
			part := whereExprToSQL(&e.Args[i])
			if part == "" {
				return ""
			}
			parts = append(parts, part)
		}
		if len(parts) == 0 {
			return ""
		}
		if len(parts) == 1 {
			return parts[0]
		}
		return "(" + strings.Join(parts, " OR ") + ")"

	case "cmp":
		if e.Left == nil || e.Right == nil {
			return ""
		}

		op := normalizeCmpOp(e.Op)
		if op == "" {
			return ""
		}

		leftSQL := scalarExprToSQL(e.Left)
		rightSQL := scalarExprToSQL(e.Right)
		if leftSQL == "" || rightSQL == "" {
			return ""
		}

		return fmt.Sprintf("%s %s %s", leftSQL, op, rightSQL)

	case "raw":
		return strings.TrimSpace(e.Value)

	default:
		// 允许布尔函数表达式直接出现在 WHERE 中
		return scalarExprToSQL(e)
	}
}

func sqlLiteral(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return "NULL"

	case string:
		if strings.EqualFold(x, "NULL") {
			return "NULL"
		}
		escaped := strings.NewReplacer(`\`, `\\`, `'`, `''`).Replace(x)
		return "'" + escaped + "'"

	case int:
		return fmt.Sprintf("%d", x)
	case int8:
		return fmt.Sprintf("%d", x)
	case int16:
		return fmt.Sprintf("%d", x)
	case int32:
		return fmt.Sprintf("%d", x)
	case int64:
		return fmt.Sprintf("%d", x)

	case uint:
		return fmt.Sprintf("%d", x)
	case uint8:
		return fmt.Sprintf("%d", x)
	case uint16:
		return fmt.Sprintf("%d", x)
	case uint32:
		return fmt.Sprintf("%d", x)
	case uint64:
		return fmt.Sprintf("%d", x)

	case float32, float64:
		return fmt.Sprintf("%v", x)

	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"

	default:
		escaped := strings.NewReplacer(`\`, `\\`, `'`, `''`).Replace(fmt.Sprintf("%v", x))
		return "'" + escaped + "'"
	}
}

func writeFields(fields []*mysql.Field, seq *uint8, conn net.Conn) {
	for _, field := range fields {
		_ = build.WritePacket(conn, seq, field.Dump())
	}
	build.WriteEOF(conn, seq)
}

func handlePointGet(conn net.Conn, session *Session, req Request) {
	log.Printf("[POINT_GET] table=%s row_id=%s", req.Table, req.RowID)
	sqlStr := ""
	args := []any{}
	// 增加索引查询的兼容度
	if req.KeyData != nil {
		normalizedKeyData, err := normalizeShadowMapValues(req.KeyData)
		if err != nil {
			sendError(conn, err.Error())
			return
		}
		var sb strings.Builder
		sb.WriteString("SELECT *")
		sb.WriteString(" FROM ")
		sb.WriteString(sql.QuoteIdent(req.DBName))
		sb.WriteString(".")
		sb.WriteString(sql.QuoteIdent(req.Table))
		sb.WriteString(" WHERE ")
		i := 0
		for k, v := range normalizedKeyData {
			if i > 0 {
				sb.WriteString(" AND ")
			}
			sb.WriteString(sql.QuoteIdent(k))
			sb.WriteString(" = ? ")
			args = append(args, v)
			i = i + 1
		}
		sqlStr = sb.String()
	} else {
		sqlStr = fmt.Sprintf(
			"SELECT * FROM %s WHERE id = ? LIMIT 1",
			sql.QuoteIdent(req.Table),
		)
		args = append(args, req.RowID)
	}
	log.Printf("[POINT_GET] sql=%s args=%v", sqlStr, args)
	queryRow, err := session.executor().Query(sqlStr, args...)
	if err != nil {
		sendDBError(conn, err)
		return
	}
	defer queryRow.Close()
	cols, _ := queryRow.Columns()
	colTypes, _ := queryRow.ColumnTypes()
	var seq uint8 = 0
	// 写返回列
	_ = build.WritePacket(conn, &seq,
		mysql.PutLengthEncodedInt(uint64(len(cols))))
	build.WriteColumnNames(cols, conn, colTypes, &seq)

	// Rows
	values := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}
	i := 0
	for queryRow.Next() {
		queryRow.Scan(ptrs...)
		row, err := build.MakeBinaryRow1(values, colTypes)
		if err != nil {
			log.Printf("[ERR] build POINT_GET row failed: %v", err)
			return
		}
		_ = build.WritePacket(conn, &seq, row)
		i = i + 1
	}
	//如果数据为空则返回字段名
	build.WriteEOF(conn, &seq)
}

func handleInsert(conn net.Conn, session *Session, req Request) {
	log.Println("[INSERT]")
	log.Printf("  table = %s", req.Table)
	log.Printf("  row   = %v", req.Row)
	log.Printf("  projection = %v", req.Projection)

	normalizedRow, err := normalizeShadowMapValues(req.Row)
	if err != nil {
		sendError(conn, err.Error())
		return
	}

	proj := projectionSet(req.Projection)
	row := filterByProjection(normalizedRow, proj)

	var cols []string
	var vals []interface{}

	for k, v := range row {
		cols = append(cols, sql.QuoteIdent(k))
		vals = append(vals, v)
	}

	sqlStr := fmt.Sprintf(
		"INSERT INTO %s.%s (%s) VALUES (%s)",
		sql.QuoteIdent(req.DBName),
		sql.QuoteIdent(req.Table),
		strings.Join(cols, ","),
		sql.Placeholders(len(cols)),
	)

	log.Printf("INSERT  SQL = %s", sqlStr)
	exec, err := session.executor().Exec(sqlStr, vals...)
	if err != nil {
		sendDBError(conn, err)
		return
	}
	id, err := exec.LastInsertId()
	if err != nil {
		sendDBError(conn, err)
		return
	}

	affected, err := exec.RowsAffected()
	if err != nil {
		sendDBError(conn, err)
		return
	}
	var seq uint8 = 0
	payload := buildOKPayload(uint64(affected), uint64(id), session.statusFlags(), 0, "")
	build.WritePacket(conn, &seq, payload)
}

func handleUpdate(conn net.Conn, session *Session, req Request) {
	log.Println("[UPDATE]")
	log.Printf("  table     = %s", req.Table)
	log.Printf("  set       = %v", req.Set)
	log.Printf("  where_row = %v", req.WhereRow)
	log.Printf("  projection = %v", req.Projection)

	normalizedSet, err := normalizeShadowMapValues(req.Set)
	if err != nil {
		sendError(conn, err.Error())
		return
	}
	normalizedWhere, err := normalizeShadowMapValues(req.WhereRow)
	if err != nil {
		sendError(conn, err.Error())
		return
	}

	proj := projectionSet(req.Projection)

	set := filterByProjection(normalizedSet, proj)
	where := filterByProjection(normalizedWhere, proj)

	var sets []string
	var args []any

	for k, v := range set {
		sets = append(sets, sql.QuoteIdent(k)+"=?")
		args = append(args, v)
	}

	var cond []string
	for k, v := range where {
		if v == nil {
			cond = append(cond, sql.QuoteIdent(k)+"IS NULL")
		} else {
			cond = append(cond, sql.QuoteIdent(k)+"=?")
			args = append(args, v)
		}
	}

	sqlStr := fmt.Sprintf(
		"UPDATE %s.%s SET %s WHERE %s",
		sql.QuoteIdent(req.DBName),
		sql.QuoteIdent(req.Table),
		strings.Join(sets, ","),
		strings.Join(cond, " AND "),
	)

	log.Printf("UPDATE  SQL = %s", sqlStr)

	exec, err := session.executor().Exec(sqlStr, args...)
	if err != nil {
		sendDBError(conn, err)
		return
	}
	affected, err := exec.RowsAffected()
	if err != nil {
		sendDBError(conn, err)
		return
	}
	var seq uint8 = 0
	payload := buildOKPayload(uint64(affected), uint64(0), session.statusFlags(), 0, "")
	build.WritePacket(conn, &seq, payload)
}

func handleDelete(conn net.Conn, session *Session, req Request) {
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
	normalizedRow, err := normalizeShadowMapValues(req.Row)
	if err != nil {
		sendError(conn, err.Error())
		return
	}
	proj := projectionSet(req.Projection)
	where := filterByProjection(normalizedRow, proj)

	var cond []string
	var args []any

	for k, v := range where {
		if v == nil {
			cond = append(cond, sql.QuoteIdent(k)+"IS NULL")
		} else {
			cond = append(cond, sql.QuoteIdent(k)+"=?")
			args = append(args, v)
		}
	}
	sqlStr := ""
	if len(where) == 0 {
		sqlStr = fmt.Sprintf(
			"DELETE FROM %s.%s",
			sql.QuoteIdent(req.DBName),
			sql.QuoteIdent(req.Table),
		)
	} else {
		sqlStr = fmt.Sprintf(
			"DELETE FROM %s.%s WHERE %s",
			sql.QuoteIdent(req.DBName),
			sql.QuoteIdent(req.Table),
			strings.Join(cond, " AND "),
		)
	}

	log.Printf("DELETE  SQL = %s", sqlStr)

	exec, err := session.executor().Exec(sqlStr, args...)
	if err != nil {
		sendDBError(conn, err)
		return
	}
	affected, err := exec.RowsAffected()
	if err != nil {
		sendDBError(conn, err)
		return
	}
	var seq uint8 = 0
	payload := buildOKPayload(uint64(affected), uint64(0), session.statusFlags(), 0, "")
	build.WritePacket(conn, &seq, payload)
}

//func whereExprToSQL(e *WhereExpr) string {
//	if e == nil {
//		return ""
//	}
//
//	switch e.Type {
//	case "and":
//		var parts []string
//		for _, a := range e.Args {
//			parts = append(parts, whereExprToSQL(&a))
//		}
//		return "(" + strings.Join(parts, " AND ") + ")"
//
//	case "or":
//		var parts []string
//		for _, a := range e.Args {
//			parts = append(parts, whereExprToSQL(&a))
//		}
//		return "(" + strings.Join(parts, " OR ") + ")"
//
//	case "cmp":
//		if e.Left == nil || e.Right == nil {
//			return ""
//		}
//		col := e.Left.Name
//		val := sqlLiteral(e.Right.Value)
//		return fmt.Sprintf("%s %s %s", col, e.Op, val)
//	}
//
//	return ""
//}

//func sqlLiteral(v interface{}) string {
//	switch x := v.(type) {
//	case nil:
//		return "NULL"
//	case string:
//		// 简化版，demo 用；生产要处理转义
//		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
//	case int, int32, int64:
//		return fmt.Sprintf("%d", x)
//	case float32, float64:
//		return fmt.Sprintf("%v", x)
//	default:
//		return "'" + fmt.Sprintf("%v", x) + "'"
//	}
//}
