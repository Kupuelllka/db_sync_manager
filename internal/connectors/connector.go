package connectors

import (
	"db_swapper/internal/domain"
)

type DatabaseConnector interface {
	// Функции по умолчанию
	Connect() error
	Ping() error
	Disconnect() error

	// Функции с пачками
	GetCount(schema *domain.TableSchema) (int, error)
	GetBatch(tableName string, offset int, batchSize int, schema *domain.TableSchema) ([]domain.Record, error)
	CreateTempTable(originalTable, tempTable string, schema *domain.TableSchema) error
	InsertBatch(tableName string, records []domain.Record, columns []string) error

	// Функции с участием временных таблиц
	SwapTables(originalTable, tempTable string) error
	DropTable(tableName string) error

	// Для процедур
	ExecuteProcedure(procName string, args ...interface{}) (int, error)
	// Если хотим использовать SELECT query and return []records
	ExecuteSelect(query string, args ...interface{}) ([]domain.Record, error)
	// Если хотим после выборки вернуть схему таблицы SELECT query and return table schema (create temp table for this schema)
	ExecuteSelectWithSchema(query string, args ...interface{}) (*domain.TableSchema, error)
}
