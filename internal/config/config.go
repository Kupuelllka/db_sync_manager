package config

import (
	"db_swapper/internal/domain"
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Logger struct {
		Level    string `yaml:"level"`
		Target   string `yaml:"target"`
		Filename string `yaml:"filename"`
	} `yaml:"logger"`
	Oracle  []DatabaseConfig `yaml:"oracle"`
	MariaDB []DatabaseConfig `yaml:"mariadb"`
	Sync    []SyncConfig     `yaml:"sync"`
}

type DatabaseConfig struct {
	Name     string `yaml:"name"` // Уникальное имя для идентификации БД в конфиге
	Host     string `yaml:"host" env:"host"`
	Port     int    `yaml:"port" env:"port"`
	User     string `yaml:"user" env:"user"`
	Password string `yaml:"password" env:"pass"`
	DBName   string `yaml:"dbname" env:"db"`
	SSLMode  string `yaml:"sslmode" env:"db_ssl" default:"disable"`
	Timeout  int    `yaml:"timeout" env:"db_timeout" default:"5"` // in seconds
}

type SyncConfig struct {
	// Имена источников и целей из конфига БД
	SourceDB          string `yaml:"source_db"` // Имя БД-источника
	TargetDB          string `yaml:"target_db"` // Имя БД-цели
	TransformFunction string `yaml:"transform_function"`
	SourceType        string `yaml:"source_type"` // "oracle" или "mariadb"
	TargetType        string `yaml:"target_type"` // "oracle" или "mariadb"
	Description       string `yaml:"description"` // Описание задачи синхронизации

	// Добавлен массив таблиц для синхронизации
	Tables []TableSyncConfig `yaml:"tables,omitempty" json:"tables,omitempty"`

	// Для обратной совместимости оставлены старые поля
	// Они будут использоваться, если Tables пуст
	Source struct {
		Table      string         `yaml:"table,omitempty" json:"table,omitempty"`
		Query      string         `yaml:"query,omitempty" json:"query,omitempty"`
		Columns    []ColumnConfig `yaml:"columns,omitempty" json:"columns,omitempty"`
		Indexes    []string       `yaml:"indexes,omitempty" json:"indexes,omitempty"`
		PrimaryKey string         `yaml:"primaryKey,omitempty" json:"primaryKey,omitempty"`
	} `yaml:"source,omitempty" json:"source,omitempty"`

	Target struct {
		Table      string         `yaml:"table,omitempty" json:"table,omitempty"`
		Query      string         `yaml:"query,omitempty" json:"query,omitempty"`
		Columns    []ColumnConfig `yaml:"columns,omitempty" json:"columns,omitempty"`
		Indexes    []string       `yaml:"indexes,omitempty" json:"indexes,omitempty"`
		PrimaryKey string         `yaml:"primaryKey,omitempty" json:"primaryKey,omitempty"`
	} `yaml:"target,omitempty" json:"target,omitempty"`

	// Общие параметры для всех таблиц
	BatchSize       int           `yaml:"batch_size" default:"1000"`
	TempTableSuffix string        `yaml:"temp_table_suffix" default:"_temp"`
	BufferSize      int           `yaml:"buffer_size" default:"5000"`
	SyncInterval    time.Duration `yaml:"sync_interval" default:"5m"`
	PostProcedure   []Procedure   `yaml:"post_procedure_list"`
}

// Новая структура для конфигурации синхронизации отдельной таблицы
type TableSyncConfig struct {
	Source struct {
		Table      string         `yaml:"table,omitempty" json:"table,omitempty"`
		Query      string         `yaml:"query,omitempty" json:"query,omitempty"`
		Columns    []ColumnConfig `yaml:"columns,omitempty" json:"columns,omitempty"`
		Indexes    []string       `yaml:"indexes,omitempty" json:"indexes,omitempty"`
		PrimaryKey string         `yaml:"primaryKey,omitempty" json:"primaryKey,omitempty"`
	} `yaml:"source" json:"source"`

	Target struct {
		Table      string         `yaml:"table,omitempty" json:"table,omitempty"`
		Query      string         `yaml:"query,omitempty" json:"query,omitempty"`
		Columns    []ColumnConfig `yaml:"columns,omitempty" json:"columns,omitempty"`
		Indexes    []string       `yaml:"indexes,omitempty" json:"indexes,omitempty"`
		PrimaryKey string         `yaml:"primaryKey,omitempty" json:"primaryKey,omitempty"`
	} `yaml:"target" json:"target"`

	// Индивидуальные параметры для конкретной таблицы
	BatchSize       *int           `yaml:"batch_size,omitempty"` // Если не указано, используется общее значение
	TempTableSuffix *string        `yaml:"temp_table_suffix,omitempty"`
	BufferSize      *int           `yaml:"buffer_size,omitempty"`
	SyncInterval    *time.Duration `yaml:"sync_interval,omitempty"`
	PostProcedure   []Procedure    `yaml:"post_procedure_list,omitempty"`
}

type ColumnConfig struct {
	Name          string `yaml:"name" json:"name"`
	DataType      string `yaml:"dataType" json:"dataType"`
	IsNullable    bool   `yaml:"isNullable" json:"isNullable"`
	AutoIncrement bool   `yaml:"autoIncrement" json:"autoIncrement"`
}

type Procedure struct {
	ProcedureName string        `yaml:"procedure_name"`
	Params        []interface{} `yaml:"procedure_params"`
}

type RecordTransformFunc func(domain.Record) domain.Record

func (c *SyncConfig) Validate() error {
	// Проверяем типы БД
	if c.SourceType != "oracle" && c.SourceType != "mariadb" {
		return errors.New("source_type must be either 'oracle' or 'mariadb'")
	}
	if c.TargetType != "oracle" && c.TargetType != "mariadb" {
		return errors.New("target_type must be either 'oracle' or 'mariadb'")
	}

	// Проверяем имена БД
	if c.SourceDB == "" {
		return errors.New("source_db cannot be empty")
	}
	if c.TargetDB == "" {
		return errors.New("target_db cannot be empty")
	}

	// Если есть таблицы, валидируем их
	if len(c.Tables) > 0 {
		for _, table := range c.Tables {
			if err := table.Validate(); err != nil {
				return fmt.Errorf("invalid table config: %w", err)
			}
		}
	} else {
		// Иначе валидируем старую конфигурацию (для обратной совместимости)
		if c.Source.Table == "" && c.Source.Query == "" {
			return errors.New("source must have either table or query")
		}
		if c.Source.Table != "" && c.Source.Query != "" {
			return errors.New("source cannot have both table and query")
		}
		if c.Target.Table == "" && c.Target.Query == "" {
			return errors.New("target must have either table or query")
		}
		if c.Target.Table != "" && c.Target.Query != "" {
			return errors.New("target cannot have both table and query")
		}
	}

	return nil
}

func (t *TableSyncConfig) Validate() error {
	// Проверяем source
	if t.Source.Table == "" && t.Source.Query == "" {
		return errors.New("source must have either table or query")
	}
	if t.Source.Table != "" && t.Source.Query != "" {
		return errors.New("source cannot have both table and query")
	}

	// Проверяем target
	if t.Target.Table == "" && t.Target.Query == "" {
		return errors.New("target must have either table or query")
	}
	if t.Target.Table != "" && t.Target.Query != "" {
		return errors.New("target cannot have both table and query")
	}

	return nil
}

func GetConfig(filename string) (*Config, error) {
	f, err := os.Open("./" + filename)
	if err != nil {
		return nil, fmt.Errorf("error opening config file: %w", err)
	}
	defer f.Close()

	var cfg Config
	decoder := yaml.NewDecoder(f)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("error decoding YAML: %w", err)
	}

	// Валидируем все конфиги синхронизации
	for _, syncCfg := range cfg.Sync {
		if err := syncCfg.Validate(); err != nil {
			return nil, fmt.Errorf("invalid sync config: %w", err)
		}
	}

	return &cfg, nil
}

// FindDatabaseConfig вспомогательная функция для поиска конфига БД по имени и типу
func (c *Config) FindDatabaseConfig(dbType, dbName string) (*DatabaseConfig, error) {
	var dbConfigs []DatabaseConfig

	switch dbType {
	case "oracle":
		dbConfigs = c.Oracle
	case "mariadb":
		dbConfigs = c.MariaDB
	default:
		return nil, fmt.Errorf("unknown database type: %s", dbType)
	}

	for _, db := range dbConfigs {
		if db.Name == dbName {
			return &db, nil
		}
	}

	return nil, fmt.Errorf("database '%s' of type '%s' not found in config", dbName, dbType)
}
