package connectors

import (
	"context"
	"database/sql"
	"db_swapper/internal/config"
	"db_swapper/internal/domain"
	"fmt"
	"strings"
	"time"

	_ "github.com/sijms/go-ora/v2"
)

type OracleConnector struct {
	config config.DatabaseConfig
	db     *sql.DB
}

func NewOracleConnector(cfg config.DatabaseConfig) *OracleConnector {
	return &OracleConnector{config: cfg}
}

func (o *OracleConnector) Connect() error {
	connectionString := fmt.Sprintf(
		"oracle://%s:%s@%s:%d/%s",
		o.config.User,
		o.config.Password,
		o.config.Host,
		o.config.Port,
		o.config.DBName,
	)

	db, err := sql.Open("oracle", connectionString)
	if err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}

	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(20)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}

	o.db = db
	return nil
}

func (o *OracleConnector) Ping() error {
	if o.db == nil {
		return fmt.Errorf("not connected to database")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return o.db.PingContext(ctx)
}

func (o *OracleConnector) Disconnect() error {
	if o.db != nil {
		return o.db.Close()
	}
	return nil
}

func (o *OracleConnector) GetCount(schema *domain.TableSchema) (int, error) {
	if schema == nil {
		return 0, fmt.Errorf("schema cannot be nil")
	}

	// Определяем столбец для подсчета (используем первый столбец, если он доступен, в противном случае используем *)
	column := "*"
	if len(schema.Columns) > 0 {
		column = schema.Columns[0].Name
	}

	// Предположим, что имя таблицы хранится в поле PrimaryKey схемы.
	query := fmt.Sprintf("SELECT COUNT(%s) FROM %s", column, schema.PrimaryKey)
	var count int
	err := o.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count query failed: %w", err)
	}
	return count, nil
}

func (o *OracleConnector) GetBatch(tableName string, offset, batchSize int, schema *domain.TableSchema) ([]domain.Record, error) {
	if batchSize <= 0 {
		return nil, fmt.Errorf("batchSize must be positive")
	}
	if offset < 0 {
		return nil, fmt.Errorf("offset cannot be negative")
	}

	// Собираем select
	var selectClause string
	if len(schema.Columns) > 0 {
		columns := make([]string, len(schema.Columns))
		for i, col := range schema.Columns {
			columns[i] = col.Name
		}
		selectClause = strings.Join(columns, ", ")
	} else {
		selectClause = "*"
	}

	// Делаем пагинацию для оракла
	query := fmt.Sprintf(
		`SELECT %s FROM (
            SELECT %s, ROW_NUMBER() OVER (ORDER BY 1) AS rn 
            FROM %s
        ) WHERE %s IS NOT NULL AND (rn > %d AND rn <= %d)`,
		selectClause, selectClause, tableName, schema.PrimaryKey, offset, offset+batchSize)

	rows, err := o.db.Query(query)
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

func (o *OracleConnector) CreateTempTable(originalTable, tempTable string, schema *domain.TableSchema) error {
	tx, err := o.db.Begin()
	if err != nil {
		return fmt.Errorf("transaction begin failed: %w", err)
	}
	defer tx.Rollback()

	// Удаляем временную таблицу если она есть
	if _, err := tx.Exec(fmt.Sprintf(
		"BEGIN EXECUTE IMMEDIATE 'DROP TABLE %s'; EXCEPTION WHEN OTHERS THEN NULL; END;",
		tempTable)); err != nil {
		return fmt.Errorf("drop temp table failed: %w", err)
	}

	// Создаем временную табличку
	if len(schema.Columns) > 0 {
		// Собираем запрос на  создание временной таблицы
		var createColumns []string
		for _, col := range schema.Columns {
			colDef := fmt.Sprintf("%s %s", col.Name, col.DataType)
			if !col.IsNullable {
				colDef += " NOT NULL"
			}
			createColumns = append(createColumns, colDef)
		}

		// Добавляем основной ключ если он есть
		if schema.PrimaryKey != "" {
			createColumns = append(createColumns, fmt.Sprintf("PRIMARY KEY (%s)", schema.PrimaryKey))
		}

		createStmt := fmt.Sprintf(
			"CREATE GLOBAL TEMPORARY TABLE %s (%s) ON COMMIT PRESERVE ROWS",
			tempTable,
			strings.Join(createColumns, ","))

		if _, err := tx.Exec(createStmt); err != nil {
			return fmt.Errorf("create temp table failed: %w", err)
		}
	} else {
		//Создаем пустую копию, если столбцы не указаны
		createStmt := fmt.Sprintf(
			"CREATE GLOBAL TEMPORARY TABLE %s ON COMMIT PRESERVE ROWS AS SELECT * FROM %s WHERE 1=0",
			tempTable,
			originalTable)
		if _, err := tx.Exec(createStmt); err != nil {
			return fmt.Errorf("create temp table failed: %w", err)
		}
	}

	return tx.Commit()
}

func (o *OracleConnector) InsertBatch(tableName string, records []domain.Record, columns []string) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := o.db.Begin()
	if err != nil {
		return fmt.Errorf("transaction begin failed: %w", err)
	}

	// Определяем столбцы для вставки
	if len(columns) == 0 {
		// Получаем столбцы из первой записи, если не указано
		for col := range records[0] {
			columns = append(columns, col)
		}
	}

	// Подготовка пакетной вставки с использованием привязки массива Oracle
	stmt, err := tx.Prepare(fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ","),
		strings.Join(o.generatePlaceholders(len(columns)), ","),
	))
	if err != nil {
		return fmt.Errorf("prepare statement failed: %w", err)
	}
	defer stmt.Close()

	for _, record := range records {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = record[col]
		}

		if _, err := stmt.Exec(values...); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert failed: %w", err)
		}
	}

	return tx.Commit()
}

func (o *OracleConnector) SwapTables(originalTable, tempTable string) error {
	backupTable := originalTable + "_backup_" + time.Now().Format("20060102150405")

	tx, err := o.db.Begin()
	if err != nil {
		return fmt.Errorf("transaction begin failed: %w", err)
	}
	defer tx.Rollback()

	// Oracle не поддерживает прямую замену таблиц, поэтому мы используем операции переименования.
	renameStmts := []string{
		fmt.Sprintf("ALTER TABLE %s RENAME TO %s", originalTable, backupTable),
		fmt.Sprintf("ALTER TABLE %s RENAME TO %s", tempTable, originalTable),
		fmt.Sprintf("ALTER TABLE %s RENAME TO %s", backupTable, tempTable),
	}

	for _, stmt := range renameStmts {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("rename operation failed: %w", err)
		}
	}

	return tx.Commit()
}

func (o *OracleConnector) DropTable(tableName string) error {
	// Oracle не поддерживает синтаксис IF EXISTS, поэтому мы используем блок PL/SQL
	_, err := o.db.Exec(fmt.Sprintf(
		`BEGIN
		   EXECUTE IMMEDIATE 'DROP TABLE %s';
		 EXCEPTION
		   WHEN OTHERS THEN
		     IF SQLCODE != -942 THEN
		       RAISE;
		     END IF;
		 END;`,
		tableName))
	if err != nil {
		return fmt.Errorf("drop table failed: %w", err)
	}
	return nil
}

func (o *OracleConnector) generatePlaceholders(count int) []string {
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}
	return placeholders
}
func (o *OracleConnector) ExecuteProcedure(procName string, args ...interface{}) (int, error) {
	return 0, nil
}
func (o *OracleConnector) ExecuteSelectWithSchema(query string, args ...interface{}) (*domain.TableSchema, error) {
	// Генерируем уникальное имя для временной таблицы
	tempTableName := fmt.Sprintf("temp_%d", time.Now().UnixNano())

	// 1. Создаем временную таблицу и заполняем данными
	createQuery := fmt.Sprintf(
		"CREATE GLOBAL TEMPORARY TABLE %s ON COMMIT PRESERVE ROWS AS %s",
		tempTableName,
		query,
	)

	_, err := o.db.Exec(createQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp table with data: %w", err)
	}
	defer o.DropTable(tempTableName) // Удаляем временную таблицу при завершении

	// 2. Получаем информацию о колонках
	columnQuery := `
        SELECT 
            column_name, 
            data_type,
            nullable,
            (SELECT 1 FROM all_sequences 
             WHERE sequence_name = (SELECT trigger_body 
                                   FROM all_triggers 
                                   WHERE table_name = :1 AND 
                                   triggering_event = 'INSERT') 
             AND ROWNUM = 1) as is_auto_increment
        FROM all_tab_columns 
        WHERE table_name = :1`

	rows, err := o.db.Query(columnQuery, tempTableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query column info: %w", err)
	}
	defer rows.Close()

	schema := &domain.TableSchema{
		Columns:    make([]domain.ColumnInfo, 0),
		PrimaryKey: "",
		Indexes:    make([]string, 0),
	}

	var columnNames []string

	for rows.Next() {
		var (
			name          string
			dataType      string
			nullable      string
			autoIncrement int
		)

		if err := rows.Scan(&name, &dataType, &nullable, &autoIncrement); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}

		column := domain.ColumnInfo{
			Name:          name,
			DataType:      dataType,
			IsNullable:    nullable == "Y",
			AutoIncrement: autoIncrement == 1,
		}
		schema.Columns = append(schema.Columns, column)
		columnNames = append(columnNames, name)
	}

	// 3. Получаем информацию о первичных ключах
	if len(columnNames) > 0 {
		pkQuery := `
            SELECT cols.column_name 
            FROM all_constraints cons, all_cons_columns cols
            WHERE cons.constraint_type = 'P'
            AND cons.constraint_name = cols.constraint_name
            AND cons.owner = cols.owner
            AND cols.table_name = :1`

		pkRows, err := o.db.Query(pkQuery, tempTableName)
		if err == nil {
			defer pkRows.Close()

			var primaryKeys []string
			for pkRows.Next() {
				var pkColumn string
				if err := pkRows.Scan(&pkColumn); err == nil {
					primaryKeys = append(primaryKeys, pkColumn)
				}
			}

			if len(primaryKeys) > 0 {
				schema.PrimaryKey = strings.Join(primaryKeys, ",")
			}
		}
	}

	// 4. Получаем информацию об индексах (кроме первичных ключей)
	indexQuery := `
        SELECT index_name 
        FROM all_indexes 
        WHERE table_name = :1 
        AND index_name NOT LIKE 'SYS_%'
        AND uniqueness = 'NONUNIQUE'`

	indexRows, err := o.db.Query(indexQuery, tempTableName)
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
func (o *OracleConnector) ExecuteSelect(query string, args ...interface{}) ([]domain.Record, error) {
	// Выполняем запрос
	rows, err := o.db.Query(query, args...)
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
		for i := range values {
			var v interface{}
			values[i] = &v
		}

		// Сканируем строку
		if err := rows.Scan(values...); err != nil {
			return nil, fmt.Errorf("row scan failed: %w", err)
		}

		// Создаем запись
		record := make(domain.Record)
		for i, colName := range columns {
			val := values[i].(*interface{})

			// Конвертируем Oracle-specific типы в стандартные
			switch v := (*val).(type) {
			case []byte:
				record[colName] = string(v)
			case time.Time:
				record[colName] = v
			default:
				record[colName] = v
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
