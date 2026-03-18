package sql

import "strings"

func QuoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "") + "`"
}

func Placeholders(n int) string {
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}
