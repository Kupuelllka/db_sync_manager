package main

import (
	"db_swapper/internal/config"
	"db_swapper/internal/connectors"
	"db_swapper/internal/domain"
	"db_swapper/internal/services/sims_sync"
	"logger"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func initTransformFunction() map[string]func(r domain.Record) domain.Record {
	listTransformFunctions := make(map[string]func(r domain.Record) domain.Record)
	listTransformFunctions["transformDataModelPhones"] = TransformForModelPhones
	listTransformFunctions["transformDataAllImsi"] = TransformForAllImsi
	return listTransformFunctions
}
func main() {
	cfg, err := config.GetConfig("prod.yaml")
	if err != nil {
		panic(err)
	}
	l, err := logger.NewLogger(cfg.Logger.Target, cfg.Logger.Level, cfg.Logger.Filename)
	if err != nil {
		panic(err)
	}

	l.Info("init logger")
	// Создаем маппинг соединений по именам из конфига
	connections := make(map[string]connectors.DatabaseConnector)
	// Инициализируем Oracle соединения
	for _, oracleCfg := range cfg.Oracle {
		conn := connectors.NewOracleConnector(oracleCfg)
		err := conn.Connect()
		if err != nil {
			l.Fatalf("Failed to connect to Oracle %s: %v", oracleCfg.Name, err)
		}
		err = conn.Ping()
		if err != nil {
			l.Fatalf("Failed to ping Oracle %s: %v", oracleCfg.Name, err)
		}
		connections[oracleCfg.Name] = conn
		l.Infof("Oracle connection %s successful", oracleCfg.Name)
	}

	// Инициализируем MariaDB соединения
	for _, mariadbCfg := range cfg.MariaDB {
		conn := connectors.NewMariaDBConnector(mariadbCfg)
		err := conn.Connect()
		if err != nil {
			l.Fatalf("Failed to connect to MariaDB %s: %v", mariadbCfg.Name, err)
		}
		err = conn.Ping()
		if err != nil {
			l.Fatalf("Failed to ping MariaDB %s: %v", mariadbCfg.Name, err)
		}
		connections[mariadbCfg.Name] = conn
		l.Infof("MariaDB connection %s successful", mariadbCfg.Name)
	}
	listTransformFunctions := initTransformFunction()

	// Канал для graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// WaitGroup для ожидания завершения всех горутин
	var wg sync.WaitGroup

	for _, syncCfg := range cfg.Sync {
		sourceConn, ok := connections[syncCfg.SourceDB]
		if !ok {
			l.Fatalf("Source DB connection %s not found", syncCfg.SourceDB)
		}

		targetConn, ok := connections[syncCfg.TargetDB]
		if !ok {
			l.Fatalf("Target DB connection %s not found", syncCfg.TargetDB)
		}

		// Если есть таблицы, обрабатываем их
		if len(syncCfg.Tables) > 0 {
			for _, tableCfg := range syncCfg.Tables {
				wg.Add(1)
				go func(cfg config.SyncConfig, table config.TableSyncConfig, listTransformFunctions map[string]func(r domain.Record) domain.Record) {
					defer wg.Done()

					// Создаем копию конфига синхронизации для таблицы
					tableSyncCfg := cfg
					tableSyncCfg.Source = table.Source
					tableSyncCfg.Target = table.Target

					// Переопределяем параметры, если они заданы для конкретной таблицы
					if table.BatchSize != nil {
						tableSyncCfg.BatchSize = *table.BatchSize
					}
					if table.TempTableSuffix != nil {
						tableSyncCfg.TempTableSuffix = *table.TempTableSuffix
					}
					if table.BufferSize != nil {
						tableSyncCfg.BufferSize = *table.BufferSize
					}
					if table.SyncInterval != nil {
						tableSyncCfg.SyncInterval = *table.SyncInterval
					}
					if len(table.PostProcedure) > 0 {
						tableSyncCfg.PostProcedure = table.PostProcedure
					}
					var syncService *sims_sync.SyncService
					transformFunction, ok := listTransformFunctions[tableSyncCfg.TransformFunction]
					if ok {
						syncService, err = sims_sync.NewSyncService(
							sourceConn,
							targetConn,
							tableSyncCfg,
							l,
							sims_sync.WithTransform(transformFunction))
					} else {
						syncService, err = sims_sync.NewSyncService(
							sourceConn,
							targetConn,
							tableSyncCfg,
							l)
					}
					if err != nil {
						l.Errorf("Failed to create sync service for table %s: %v", table.Source.Table, err)
						return
					}

					l.Infof("Starting sync for table %s -> %s", table.Source.Table, table.Target.Table)
					if err := syncService.Run(); err != nil {
						l.Errorf("Sync failed for table %s: %v", table.Source.Table, err)
					}
				}(syncCfg, tableCfg, listTransformFunctions)
			}
		}
	}

	l.Info("Application started successfully")

	// Ожидаем сигнала завершения
	<-quit
	l.Info("Shutting down...")

	// Ожидаем завершения всех горутин или таймаут
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		l.Info("All sync tasks completed")
	case <-time.After(30 * time.Second):
		l.Error("Timeout while waiting for sync tasks to complete")
	}
	l.Info("Application shutdown complete")
}
func TransformForModelPhones(r domain.Record) domain.Record {
	// Преобразование имен колонок из source в target
	if vendorName, exists := r["VENDOR_NAME"]; exists {
		r["vendorName"] = vendorName
		delete(r, "VENDOR_NAME")
	}

	if modelName, exists := r["MODEL_NAME"]; exists {
		r["modelName"] = modelName
		delete(r, "MODEL_NAME")
	}

	if tac, exists := r["TAC"]; exists {
		r["tac"] = tac
		delete(r, "TAC")
	}
	return r
}
func TransformForAllImsi(r domain.Record) domain.Record {
	// Преобразование имен колонок из source в target
	if clientName, exists := r["CLIENT"]; exists {
		r["client"] = clientName
		delete(r, "CLIENT")
	}

	if contract, exists := r["CONTRACT"]; exists {
		r["contract"] = contract
		delete(r, "CONTRACT")
	}

	if iccid, exists := r["ICCID"]; exists {
		r["iccid"] = iccid
		delete(r, "ICCID")
	}

	if imsi, exists := r["IMSI"]; exists {
		r["imsi"] = imsi
		delete(r, "IMSI")
	}
	if msisdn, exists := r["MSISDN"]; exists {
		r["msisdn"] = msisdn
		delete(r, "MSISDN")
	}

	if status, exists := r["STATUS"]; exists {
		r["status"] = status
		delete(r, "STATUS")
	}

	if status, exists := r["TYPESIM"]; exists {
		r["typeSim"] = status
		delete(r, "TYPESIM")
	}

	if department, exists := r["DEPARTMENT"]; exists {
		r["department"] = department
		delete(r, "DEPARTMENT")
	}
	return r
}
