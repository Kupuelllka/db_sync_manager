package sims_sync

// sqlOption содержит параметры для SQL запроса
type sqlOption struct {
	isSource   bool
	query      string
	args       []interface{}
	returnData bool // Флаг для указания, нужно ли возвращать данные
}

// WithSQL добавляет опцию для выполнения SQL запроса
func WithSQL(isSource bool, returnData bool, query string, args ...interface{}) ProcessorOption {
	return func(p *DataProcessor) {
		if p.sqlOpts == nil {
			p.sqlOpts = make([]sqlOption, 0)
		}
		p.sqlOpts = append(p.sqlOpts, sqlOption{
			isSource:   isSource,
			query:      query,
			args:       args,
			returnData: returnData,
		})
	}
}

// getSQLOption проверяет, является ли опция SQL опцией
func getSQLOption(opt ProcessorOption) (sqlOption, bool) {
	tmp := &DataProcessor{}
	opt(tmp)
	if len(tmp.sqlOpts) > 0 {
		return tmp.sqlOpts[0], true
	}
	return sqlOption{}, false
}
