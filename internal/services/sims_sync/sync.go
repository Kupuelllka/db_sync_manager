package sims_sync

import (
	"db_swapper/internal/config"
	"db_swapper/internal/connectors"
	"db_swapper/internal/domain"
	"fmt"
	"logger"
	"time"
)

type SyncService struct {
	source    connectors.DatabaseConnector
	target    connectors.DatabaseConnector
	processor *DataProcessor
	config    config.SyncConfig
	logger    *logger.Log

	sourceSchema *domain.TableSchema
	targetSchema *domain.TableSchema
}

func NewSyncService(
	source connectors.DatabaseConnector,
	target connectors.DatabaseConnector,
	cfg config.SyncConfig,
	logger *logger.Log,
	opts ...ProcessorOption,
) (*SyncService, error) {
	// Инициализируем сервис
	service := &SyncService{
		source: source,
		target: target,
		config: cfg,
		logger: logger,
	}

	// Обработка source (таблица или запрос)
	if cfg.Source.Table != "" {
		// Получаем схему из таблицы
		schema, err := source.ExecuteSelectWithSchema(
			fmt.Sprintf("SELECT * FROM %s WHERE 1=0", cfg.Source.Table))
		if err != nil {
			return nil, fmt.Errorf("failed to get source schema: %w", err)
		}
		service.sourceSchema = schema
	} else if cfg.Source.Query != "" {
		// Используем запрос для получения данных
		opts = append(opts, WithSQL(true, true, cfg.Source.Query))
	}

	// Обработка target (таблица или запрос)
	if cfg.Target.Table != "" {
		// Создаем схему из конфига
		service.targetSchema = &domain.TableSchema{
			Columns:    make([]domain.ColumnInfo, len(cfg.Target.Columns)),
			PrimaryKey: cfg.Target.PrimaryKey,
			Indexes:    cfg.Target.Indexes,
		}
		for i, col := range cfg.Target.Columns {
			service.targetSchema.Columns[i] = domain.ColumnInfo{
				Name:          col.Name,
				DataType:      col.DataType,
				IsNullable:    col.IsNullable,
				AutoIncrement: col.AutoIncrement,
			}
		}
	} else if cfg.Target.Query != "" {
		// Получаем схему из запроса
		schema, err := target.ExecuteSelectWithSchema(cfg.Target.Query)
		if err != nil {
			return nil, fmt.Errorf("failed to get target schema: %w", err)
		}
		service.targetSchema = schema
	}

	// Создаем временный процессор для извлечения опций
	tmpProcessor := NewDataProcessor(0, opts...)

	// Обрабатываем SQL опции
	for _, sqlOpt := range tmpProcessor.sqlOpts {
		if sqlOpt.isSource {
			if sqlOpt.returnData {
				// Получаем данные для источника
				data, err := source.ExecuteSelect(sqlOpt.query, sqlOpt.args...)
				if err != nil {
					logger.Errorf("Failed to get source data from SQL: %v", err)
					continue
				}
				// Сохраняем данные в процессор
				tmpProcessor.SetSourceData(data)
			} else {
				// Получаем схему источника
				schema, err := source.ExecuteSelectWithSchema(sqlOpt.query, sqlOpt.args...)
				if err != nil {
					logger.Errorf("Failed to get source schema from SQL: %v", err)
					continue
				}
				service.sourceSchema = schema
			}
		} else {
			// Получаем схему целевой таблицы
			schema, err := target.ExecuteSelectWithSchema(sqlOpt.query, sqlOpt.args...)
			if err != nil {
				logger.Errorf("Failed to get target schema from SQL: %v", err)
				continue
			}
			service.targetSchema = schema
		}
	}

	// Создаем финальный процессор с актуальными схемами
	processorOpts := []ProcessorOption{
		WithSchemas(service.sourceSchema, service.targetSchema),
	}

	// Добавляем остальные опции (кроме WithSQL)
	for _, opt := range opts {
		if _, isSQL := getSQLOption(opt); !isSQL {
			processorOpts = append(processorOpts, opt)
		}
	}

	service.processor = NewDataProcessor(cfg.BufferSize, processorOpts...)

	// Если были предзагружены данные, передаем их в процессор
	if tmpProcessor.HasPreloadedData() {
		service.processor.SetSourceData(tmpProcessor.sourceData)
	}

	return service, nil
}

func (s *SyncService) processData(tempTableName string) error {
	var totalCount int
	var err error
	// Используем предзагруженные данные если они есть
	if s.processor.HasPreloadedData() {
		totalCount = len(s.processor.sourceData)
		s.logger.Debug(fmt.Sprintf("Using preloaded data, count: %d", totalCount))

		batchSize := s.config.BatchSize
		for offset := 0; offset < totalCount; offset += batchSize {

			processedBatch := s.processor.GetPreloadedBatch(offset, batchSize)
			if len(processedBatch) == 0 {
				break
			}
			if err := s.target.InsertBatch(
				tempTableName,
				processedBatch,
				s.processor.GetTargetColumns(),
			); err != nil {
				return fmt.Errorf("insert batch failed: %w", err)
			}

			s.logger.Info(fmt.Sprintf("Progress: %d/%d records processed", offset+len(processedBatch), totalCount))
		}
	} else {

		totalCount, err = s.source.GetCount(s.sourceSchema)
		if err != nil {
			return err
		}

		s.logger.Debug(fmt.Sprintf("Total rows count: %d", totalCount))
		batchSize := s.config.BatchSize

		// Получаем колонки исходной таблицы
		var sourceColumns []string
		if s.sourceSchema != nil {
			sourceColumns = make([]string, 0, len(s.sourceSchema.Columns))
			for _, col := range s.sourceSchema.Columns {
				sourceColumns = append(sourceColumns, col.Name)
			}
		}

		offset := 0
		for offset < totalCount {
			s.logger.Debug(fmt.Sprintf("Processing offset: %d", offset))
			// 1. Получаем пачку из исходной таблицы
			batch, err := s.source.GetBatch(
				s.config.Source.Table,
				offset,
				batchSize,
				s.sourceSchema,
			)
			if err != nil {
				return err
			}

			// 2. Обрабтываем данные(маппинг между таблицами если схемы разные)
			s.processor.Process(batch)

			// 3. Получаем batch если размер разный
			processedBatch := s.processor.GetBatch(batchSize)
			if len(processedBatch) == 0 {
				break
			}
			s.logger.Debug(fmt.Sprintf("Processed batch size: %d", len(processedBatch)))

			// Для последней пачки
			if batchSize > len(processedBatch) {
				batchSize = len(processedBatch)
			}

			// 4. Вставляем в нужную временнную табличку
			if err := s.target.InsertBatch(
				tempTableName,
				processedBatch,
				s.processor.GetTargetColumns(),
			); err != nil {
				return fmt.Errorf("insert batch failed: %w", err)
			}

			offset += batchSize
			s.logger.Info(fmt.Sprintf("Progress: %d/%d records processed", offset, totalCount))
		}
	}
	return nil
}

func (s *SyncService) syncTables() error {
	tempTableName := s.config.Target.Table + s.config.TempTableSuffix
	// 1. Создаем временную таблицы
	err := s.target.CreateTempTable(
		s.config.Target.Table,
		tempTableName,
		s.processor.targetSchema)
	if err != nil {
		return fmt.Errorf("create temp table failed: %w", err)
	}

	// 2. Обрабатываем данные и записываем их в созданную табличку
	if err := s.processData(tempTableName); err != nil {
		if dropErr := s.target.DropTable(tempTableName); dropErr != nil {
			s.logger.Error(fmt.Sprintf("failed to drop temp table after error: %v", dropErr))
		}
		return fmt.Errorf("data processing failed: %w", err)
	}

	// 3. Меняем таблицы местами (исходную и ту то что мы создали). Создаем бекап таблицы
	if err := s.target.SwapTables(s.config.Target.Table, tempTableName); err != nil {
		return fmt.Errorf("table swap failed: %w", err)
	}

	// 4. Удаляем временную табличку
	if err := s.target.DropTable(tempTableName); err != nil {
		s.logger.Error(fmt.Sprintf("failed to drop temp table: %v", err))
	}

	// 5. Выполняем процедуры если они добавлены
	if len(s.config.PostProcedure) > 0 {
		for i := 0; i < len(s.config.PostProcedure); i++ {
			proc := s.config.PostProcedure[i]
			count, err := s.target.ExecuteProcedure(proc.ProcedureName, proc.Params...)
			if err != nil {
				s.logger.Error(fmt.Sprintf("failed to exec procedure: %v", err))
			}
			s.logger.Info(fmt.Sprintf("Procedure proccessed: %d", count))
		}
	}
	return nil
}

func (s *SyncService) Run() error {
	if err := s.syncTables(); err != nil {
		s.logger.Error(fmt.Sprintf("Sync failed: %v", err))
	}
	ticker := time.NewTicker(s.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.logger.Info("sync start")
			if err := s.syncTables(); err != nil {
				s.logger.Error(fmt.Sprintf("Sync failed: %v", err))
			}
			s.logger.Info("sync end")
		}
	}
}
