package sims_sync

import (
	"db_swapper/internal/domain"
	"sync"
)

// DataProcessor обрабатывает и сохраняет данные в памяти перед записью
type DataProcessor struct {
	buffer        []domain.Record
	sourceData    []domain.Record // Добавлено поле для хранения предзагруженных данных
	mu            sync.Mutex
	transform     func(domain.Record) domain.Record
	sourceSchema  *domain.TableSchema
	targetSchema  *domain.TableSchema
	columnMapping map[string]string
	sqlOpts       []sqlOption
	dataLoaded    bool // Флаг, указывающий что данные были предзагружены
}

type ProcessorOption func(*DataProcessor)

func NewDataProcessor(bufferSize int, opts ...ProcessorOption) *DataProcessor {
	p := &DataProcessor{
		buffer:        make([]domain.Record, 0, bufferSize),
		sourceData:    nil,
		transform:     func(r domain.Record) domain.Record { return r },
		sourceSchema:  nil,
		targetSchema:  nil,
		columnMapping: make(map[string]string),
		dataLoaded:    false,
	}

	for _, opt := range opts {
		opt(p)
	}

	if p.sourceSchema != nil && p.targetSchema != nil {
		p.createColumnMapping()
	}

	return p
}

// SetSourceData устанавливает предзагруженные данные
func (p *DataProcessor) SetSourceData(data []domain.Record) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sourceData = data
	p.dataLoaded = len(data) > 0
}

// HasPreloadedData проверяет наличие предзагруженных данных
func (p *DataProcessor) HasPreloadedData() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dataLoaded
}

// Process обрабатывает данные, учитывая предзагруженные данные и схемы
func (p *DataProcessor) Process(batch []domain.Record) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, record := range batch {
		processed := p.processRecord(record)
		if processed != nil {
			p.buffer = append(p.buffer, processed)
		}
	}
}

// processRecord обрабатывает одну запись с учетом схем и маппинга
func (p *DataProcessor) processRecord(record domain.Record) domain.Record {
	// Если нет схемы источника, просто применяем трансформацию
	if p.sourceSchema == nil {
		if p.transform != nil {
			return p.transform(record)
		}
		return record
	}

	processed := make(domain.Record)

	// Обрабатываем каждую колонку согласно схеме
	for _, col := range p.sourceSchema.Columns {
		sourceName := col.GetColumnName(false)

		// Ищем значение в записи (с учетом возможных разных форматов имен)
		var value interface{}
		if val, exists := record[sourceName]; exists {
			value = val
		} else if val, exists := record[col.Name]; exists {
			value = val
		} else {
			// Колонка не найдена в записи
			continue
		}

		// Применяем маппинг колонок, если есть целевая схема
		targetName := col.Name
		if p.targetSchema != nil {
			if mapped, ok := p.columnMapping[col.Name]; ok {
				targetName = mapped
			}
		}

		processed[targetName] = value
	}

	// Применяем трансформацию если задана
	if p.transform != nil {
		return p.transform(processed)
	}

	return processed
}

// GetPreloadedBatch возвращает пакет предзагруженных данных с обработкой
func (p *DataProcessor) GetPreloadedBatch(offset, batchSize int) []domain.Record {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.dataLoaded || offset >= len(p.sourceData) {
		return nil
	}

	end := offset + batchSize
	if end > len(p.sourceData) {
		end = len(p.sourceData)
	}

	// Обрабатываем каждую запись в пакете
	var processedBatch []domain.Record
	for _, record := range p.sourceData[offset:end] {
		processed := p.processRecord(record)
		if processed != nil {
			processedBatch = append(processedBatch, processed)
		}
	}

	return processedBatch
}

func WithTransform(fn func(domain.Record) domain.Record) ProcessorOption {
	return func(p *DataProcessor) {
		p.transform = fn
	}
}

func WithSchemas(sourceSchema, targetSchema *domain.TableSchema) ProcessorOption {
	return func(p *DataProcessor) {
		p.sourceSchema = sourceSchema
		p.targetSchema = targetSchema
	}
}

func (p *DataProcessor) createColumnMapping() {
	if p.sourceSchema == nil || p.targetSchema == nil {
		return
	}

	for _, srcCol := range p.sourceSchema.Columns {
		for _, tgtCol := range p.targetSchema.Columns {
			if srcCol.GetColumnName(true) == tgtCol.GetColumnName(true) {
				p.columnMapping[srcCol.Name] = tgtCol.Name
				break
			}
		}
	}
}

func (p *DataProcessor) GetBatch(size int) []domain.Record {
	p.mu.Lock()
	defer p.mu.Unlock()

	if size > len(p.buffer) {
		size = len(p.buffer)
	}

	batch := p.buffer[:size]
	p.buffer = p.buffer[size:]

	return batch
}

func (p *DataProcessor) BufferSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.buffer)
}

func (p *DataProcessor) GetTargetColumns() []string {
	if p.targetSchema != nil {
		cols := make([]string, 0, len(p.targetSchema.Columns))
		for _, col := range p.targetSchema.Columns {
			// если не автоинкремент
			if !col.AutoIncrement {
				cols = append(cols, col.Name)
			}
		}
		return cols
	}
	return nil
}
