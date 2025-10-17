package main

import (
	"db_swapper/internal/config"
	"db_swapper/internal/connectors"
	"db_swapper/internal/domain"
	"log"
	"logger"
)

func main() {
	// Загрузка конфигурации
	cfg, err := config.GetConfig("config.yml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Инициализация логгера
	l, err := logger.NewLogger(cfg.Logger.Target, cfg.Logger.Level, cfg.Logger.Filename)
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	l.Info("Logger initialized successfully")

	// Поиск нужной конфигурации MariaDB (предполагаем, что нам нужна первая в списке)
	if len(cfg.MariaDB) == 0 {
		l.Fatal("No MariaDB configurations found")
	}
	mariadbCfg := cfg.MariaDB[0]

	// Создание и подключение к MariaDB
	mariadbConn := connectors.NewMariaDBConnector(mariadbCfg)
	l.Infof("Connecting to MariaDB '%s' at %s:%d", mariadbCfg.Name, mariadbCfg.Host, mariadbCfg.Port)

	err = mariadbConn.Connect()
	if err != nil {
		l.Fatalf("Failed to connect to MariaDB: %v", err)
	}

	err = mariadbConn.Ping()
	if err != nil {
		l.Fatalf("Failed to ping MariaDB: %v", err)
	}
	l.Info("MariaDB connection established successfully")

	// Создание схемы целевой таблицы на основе конфига
	// Предполагаем, что у нас есть соответствующая sync конфигурация
	if len(cfg.Sync) == 0 {
		l.Fatal("No sync configurations found")
	}
	syncCfg := cfg.Sync[0] // берем первую конфигурацию синхронизации

	// Преобразуем ColumnConfig из конфига в domain.ColumnInfo
	var columns []domain.ColumnInfo
	for _, col := range syncCfg.Target.Columns {
		columns = append(columns, domain.ColumnInfo{
			Name:          col.Name,
			DataType:      col.DataType,
			IsNullable:    col.IsNullable,
			AutoIncrement: col.AutoIncrement,
		})
	}

	targetSchema := &domain.TableSchema{
		Columns:    columns,
		PrimaryKey: syncCfg.Target.PrimaryKey,
	}

	// Создание временной таблицы
	tempTableName := syncCfg.Target.Table + syncCfg.TempTableSuffix
	l.Infof("Creating temp table %s based on %s", tempTableName, syncCfg.Target.Table)

	err = mariadbConn.CreateTempTable(syncCfg.Target.Table, tempTableName, targetSchema)
	if err != nil {
		l.Fatalf("Failed to create temp table: %v", err)
	}
	l.Info("Temp table created successfully")
}
