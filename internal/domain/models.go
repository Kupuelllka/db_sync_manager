package domain

import "strings"

// Record представляет одну запись для синхронизации
type Record map[string]interface{}

// TableSchema описывает структуру таблицы
type TableSchema struct {
	Columns    []ColumnInfo
	PrimaryKey string
	Indexes    []string
}

type ColumnInfo struct {
	Name          string
	DataType      string
	IsNullable    bool
	AutoIncrement bool
}

func (c *ColumnInfo) GetColumnName(isMapping bool) string {
	if isMapping {
		return strings.ToLower(strings.ReplaceAll(c.Name, "_", ""))
	} else {
		return c.Name
	}
}
