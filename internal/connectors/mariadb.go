package connectors

import (
	"context"
	"database/sql"
	"db_swapper/internal/config"
	"db_swapper/internal/domain"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MariaDBConnector struct {
	config config.DatabaseConfig
	db     *sql.DB
}

func NewMariaDBConnector(cfg config.DatabaseConfig) *MariaDBConnector {
	return &MariaDBConnector{config: cfg}
}

func (m *MariaDBConnector) Connect() error {
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		m.config.User, m.config.Password, m.config.Host, m.config.Port, m.config.DBName)
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}

	m.db = db
	return nil
}

func (m *MariaDBConnector) Disconnect() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

func (m *MariaDBConnector) Ping() error {
	if m.db == nil {
		return fmt.Errorf("not connected to database")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return m.db.PingContext(ctx)
}

func (m *MariaDBConnector) GetCount(schema *domain.TableSchema) (int, error) {
	if schema == nil {
		return 0, fmt.Errorf("schema cannot be nil")
	}

	// Используем первый столбец, если он доступен, в противном случае используем *
	column := "*"
	if len(schema.Columns) > 0 {
		column = schema.Columns[0].Name
	}

	query := fmt.Sprintf("SELECT COUNT(%s) FROM %s", column, schema.PrimaryKey)
	var count int
	err := m.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count query failed: %w", err)
	}
	return count, nil
}

func (m *MariaDBConnector) GetBatch(tableName string, offset, batchSize int, schema *domain.TableSchema) ([]domain.Record, error) {
	if batchSize <= 0 {
		return nil, fmt.Errorf("batchSize must be positive")
	}
	if offset < 0 {
		return nil, fmt.Errorf("offset cannot be negative")
	}

	// Собираем список колонок
	var columns []string
	if len(schema.Columns) > 0 {
		columns = make([]string, len(schema.Columns))
		for i, col := range schema.Columns {
			columns[i] = col.Name
		}
	}

	// Собираем запрос
	var query string
	if len(columns) > 0 {
		query = fmt.Sprintf("SELECT %s FROM %s LIMIT ? OFFSET ?",
			strings.Join(columns, ","), tableName)
	} else {
		query = fmt.Sprintf("SELECT * FROM %s LIMIT ? OFFSET ?", tableName)
	}

	rows, err := m.db.Query(query, batchSize, offset)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns failed: %w", err)
	}

	// Предварительно выделяем срез с емкостью для записей batchSize
	records := make([]domain.Record, 0, batchSize)
	values := make([]interface{}, len(colNames))
	valuePtrs := make([]interface{}, len(colNames))

	// Инициализируем указатели значений один раз
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("row scan failed: %w", err)
		}

		record := make(domain.Record, len(colNames))
		for i, col := range colNames {
			switch v := values[i].(type) {
			case nil:
				record[col] = nil
			case []byte:
				record[col] = string(v)
			case time.Time:
				record[col] = v
			case string:
				record[col] = v
			case int64, float64, bool:
				record[col] = v
			default:
				record[col] = fmt.Sprintf("%v", v)
			}
		}
		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return records, nil
}
func (m *MariaDBConnector) CreateTempTable(originalTable, tempTable string, schema *domain.TableSchema) error {
	tx, err := m.db.Begin()
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("transaction begin failed: %w", err)
	}

	// Удаляем временную таблицу если она есть
	if _, err := tx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTable)); err != nil {
		tx.Rollback()
		return fmt.Errorf("drop temp table failed: %w", err)
	}

	// По схеме создаем временную таблицу
	if len(schema.Columns) > 0 {
		var createColumns []string
		for _, col := range schema.Columns {
			colDef := fmt.Sprintf("%s %s", col.Name, col.DataType)
			if !col.IsNullable {
				colDef += " NOT NULL"
			}
			if col.AutoIncrement {
				colDef += " AUTO_INCREMENT"
			}
			createColumns = append(createColumns, colDef)
		}

		// Добавляем основной ключ если есть
		if schema.PrimaryKey != "" {
			createColumns = append(createColumns, fmt.Sprintf("PRIMARY KEY (%s)", schema.PrimaryKey))
		}
		// Добавляем индексы если есть
		if len(schema.Indexes) > 0 {
			for i := 0; i < len(schema.Indexes); i++ {
				createColumns = append(createColumns, fmt.Sprintf("INDEX idx_%s (%s)", schema.Indexes[i], schema.Indexes[i]))
			}
		}
		createStmt := fmt.Sprintf("CREATE TABLE %s (%s)", tempTable, strings.Join(createColumns, ","))
		if _, err := tx.Exec(createStmt); err != nil {
			tx.Rollback()
			return fmt.Errorf("create temp table failed: %w", err)
		}
	} else {
		// Создать точную копию, если столбцы не указаны
		if _, err := tx.Exec(fmt.Sprintf("CREATE TABLE %s LIKE %s", tempTable, originalTable)); err != nil {
			tx.Rollback()
			return fmt.Errorf("create temp table failed: %w", err)
		}
	}

	return tx.Commit()
}

func (m *MariaDBConnector) InsertBatch(tableName string, records []domain.Record, columns []string) error {
	if len(records) == 0 {
		return nil
	}
	tx, err := m.db.Begin()
	if err != nil {
		return fmt.Errorf("transaction begin failed: %w", err)
	}

	// Определяем колонки для вставки
	if len(columns) == 0 {
		// Получаем все если не определены
		for col := range records[0] {
			columns = append(columns, col)
		}
	}

	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", tableName, strings.Join(columns, ","))
	var valueStrings []string
	var valueArgs []interface{}

	for _, record := range records {
		var placeholders []string
		for _, col := range columns {
			placeholders = append(placeholders, "?")

			if val, exists := record[col]; exists {
				valueArgs = append(valueArgs, val)
			} else {
				valueArgs = append(valueArgs, nil)
			}
		}
		valueStrings = append(valueStrings, "("+strings.Join(placeholders, ",")+")")
	}
	stmt += strings.Join(valueStrings, ",")
	if _, err := tx.Exec(stmt, valueArgs...); err != nil {
		tx.Rollback()
		return fmt.Errorf("insert failed: %w", err)
	}

	return tx.Commit()
}

func (m *MariaDBConnector) SwapTables(originalTable, tempTable string) error {
	backupTable := originalTable + "_backup"
	// удаляем старый бекап
	m.DropTable(backupTable)

	tx, err := m.db.Begin()
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("transaction begin failed: %w", err)
	}
	if _, err := tx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", backupTable)); err != nil {
		tx.Rollback()
		return fmt.Errorf("drop backup table failed: %w", err)
	}

	swapQuery := fmt.Sprintf("RENAME TABLE %s TO %s, %s TO %s",
		originalTable, backupTable,
		tempTable, originalTable)

	if _, err := tx.Exec(swapQuery); err != nil {
		tx.Rollback()
		return fmt.Errorf("swap tables failed: %w", err)
	}

	return tx.Commit()
}

func (m *MariaDBConnector) DropTable(tableName string) error {
	if _, err := m.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
		return fmt.Errorf("drop table failed: %w", err)
	}
	return nil
}

func (m *MariaDBConnector) ExecuteProcedure(procName string, args ...interface{}) (int, error) {
	query := fmt.Sprintf("CALL %s(", procName)
	for i := range args {
		if i > 0 {
			query += ", "
		}
		query += "?"
	}
	query += ")"

	result, err := m.db.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute procedure %s: %v", procName, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected for procedure %s: %v", procName, err)
	}

	return int(rowsAffected), nil
}
func (m *MariaDBConnector) ExecuteSelectWithSchema(query string, args ...interface{}) (*domain.TableSchema, error) {
	// Генерируем уникальное имя для временной таблицы
	tempTableName := fmt.Sprintf("temp_%d", time.Now().UnixNano())

	// 1. Создаем временную таблицу и заполняем данными
	createQuery := fmt.Sprintf("CREATE TEMPORARY TABLE %s AS %s", tempTableName, query)
	_, err := m.db.Exec(createQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp table with data: %w", err)
	}
	defer m.DropTable(tempTableName) // Удаляем временную таблицу при завершении

	// 2. Получаем полную информацию о схеме из information_schema
	schemaQuery := `
        SELECT 
            column_name,
            data_type,
            column_type,
            is_nullable = 'YES',
            extra LIKE '%auto_increment%',
            column_key = 'PRI'
        FROM information_schema.columns 
        WHERE table_name = ? AND table_schema = DATABASE()`

	rows, err := m.db.Query(schemaQuery, tempTableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query schema info: %w", err)
	}
	defer rows.Close()

	schema := &domain.TableSchema{
		Columns:    make([]domain.ColumnInfo, 0),
		PrimaryKey: "",
		Indexes:    make([]string, 0),
	}

	var primaryKeys []string

	for rows.Next() {
		var (
			name          string
			dataType      string
			columnType    string
			isNullable    bool
			autoIncrement bool
			isPrimary     bool
		)

		if err := rows.Scan(&name, &dataType, &columnType, &isNullable, &autoIncrement, &isPrimary); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}

		column := domain.ColumnInfo{
			Name:          name,
			DataType:      dataType,
			IsNullable:    isNullable,
			AutoIncrement: autoIncrement,
		}
		schema.Columns = append(schema.Columns, column)

		if isPrimary {
			primaryKeys = append(primaryKeys, name)
		}
	}

	// Устанавливаем первичный ключ
	if len(primaryKeys) > 0 {
		schema.PrimaryKey = strings.Join(primaryKeys, ",")
	}

	// 3. Дополнительно получаем информацию об индексах
	indexQuery := `
        SELECT index_name 
        FROM information_schema.statistics 
        WHERE table_name = ? AND table_schema = DATABASE() 
        GROUP BY index_name 
        HAVING index_name != 'PRIMARY'`

	indexRows, err := m.db.Query(indexQuery, tempTableName)
	if err == nil {
		defer indexRows.Close()
		for indexRows.Next() {
			var indexName string
			if err := indexRows.Scan(&indexName); err == nil {
				schema.Indexes = append(schema.Indexes, indexName)
			}
		}
	}
	return schema, nil
}
func (m *MariaDBConnector) ExecuteSelect(query string, args ...interface{}) ([]domain.Record, error) {
	// Выполняем запрос
	rows, err := m.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	// Получаем имена колонок
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Подготавливаем структуру для результатов
	var records []domain.Record

	// Читаем данные
	for rows.Next() {
		// Создаем срез для значений
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		// Сканируем строку
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("row scan failed: %w", err)
		}

		// Создаем запись
		record := make(domain.Record)
		for i, colName := range columns {
			// Обрабатываем NULL значения и специальные типы
			if values[i] == nil {
				record[colName] = nil
				continue
			}

			// Конвертируем []byte в string (для TEXT, BLOB и т.д.)
			switch v := values[i].(type) {
			case []byte:
				record[colName] = string(v)
			case time.Time:
				record[colName] = v
			case int64:
				record[colName] = v
			case float64:
				record[colName] = v
			case bool:
				record[colName] = v
			default:
				record[colName] = fmt.Sprintf("%v", v)
			}
		}

		records = append(records, record)
	}

	// Проверяем ошибки после итерации
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return records, nil
}
