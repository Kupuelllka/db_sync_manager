# 🚀 DB Sync Manager | Database Connection Manager

![Go Version](https://img.shields.io/badge/Go-1.20%2B-blue)

Утилита для удобного управления подключениями к различным базам данных с возможностью синхронизации между ними. Написана на Go.

## 📦 Установка 

```bash
git clone git@gitlab.miatel.ru:Kupuelllka/db_swapper.git
cd db_swapper
make install
```
# 🛠️ Технические характеристики
Поддерживаемые СУБД:

    ✅ MariaDB/MySQL

    ✅ Oracle
# Требования
1. Go 1.20+
2. Доступ к целевым БД
# ⚙️ Конфигурация
Создайте config.yml в корне проекта:

# Пример конфигурации
```yml
logger:
  logger_level: "info"     # Уровень логирования (debug, info, warn, error)
  logger_target: "file"    # Куда писать логи (file, console, both)
  logger_file: "sync.log"  # Имя файла для логов (если target включает file)

oracle:
  host: "oracle-db.example.com"  # Хост Oracle
  port: 1521                    # Порт Oracle
  user: "sync_user"             # Пользователь Oracle
  password: "oracle_password"   # Пароль Oracle
  dbname: "ORCL"               # Имя базы данных Oracle
  sslmode: "disable"           # Режим SSL (disable, require, verify-ca, verify-full)
  timeout: 5                   # Таймаут подключения в секундах

mariadb:
  host: "mariadb.example.com"  # Хост MariaDB
  port: 3306                  # Порт MariaDB
  user: "sync_user"           # Пользователь MariaDB
  password: "mariadb_password" # Пароль MariaDB
  dbname: "sync_db"           # Имя базы данных MariaDB
  sslmode: "disable"          # Режим SSL
  timeout: 5                  # Таймаут подключения в секундах

sync:
  source:
    table: "SOURCE_TABLE"     # Исходная таблица в Oracle (альтернатива - query)
    # query: "SELECT * FROM..." # Альтернатива table - произвольный SQL запрос
    columns:                 # Описание колонок (опционально)
      - name: "ID"
        dataType: "NUMBER"
        isNullable: false
        autoIncrement: false
      - name: "NAME"
        dataType: "VARCHAR2"
        isNullable: true
        autoIncrement: false
    indexes:                 # Индексы (опционально)
      - "IDX_NAME"
    primaryKey: "ID"         # Первичный ключ (опционально)

  target:
    table: "target_table"    # Целевая таблица в MariaDB (альтернатива - query)
    # query: "SELECT * FROM..." # Альтернатива table - произвольный SQL запрос
    columns:                # Описание колонок (опционально)
      - name: "id"
        dataType: "BIGINT"
        isNullable: false
        autoIncrement: false
      - name: "name"
        dataType: "VARCHAR"
        isNullable: true
        autoIncrement: false

  batch_size: 1000          # Размер пакета для вставки данных
  temp_table_suffix: "_temp" # Суффикс для временных таблиц
  buffer_size: 5000         # Размер буфера в памяти
  sync_interval: "5m"       # Интервал синхронизации (5 минут)

  post_procedure_list:      # Список процедур для выполнения после синхронизации
    - procedure_name: "update_stats"
      params: []
    - procedure_name: "send_notification"
      params: ["sync_completed", "source_to_target"]

```

## Параметры подключения к БД (DatabaseConfig)

Для каждой БД (Oracle и MariaDB) доступны следующие параметры:

- `host` - адрес сервера БД
- `port` - порт подключения
- `user` - имя пользователя
- `password` - пароль пользователя
- `dbname` - имя базы данных
- `sslmode` - режим SSL (по умолчанию "disable")
- `timeout` - таймаут подключения в секундах (по умолчанию 5)

## Параметры синхронизации (SyncConfig)

### Источник и приемник (Source/Target)

- `table` - имя таблицы (взаимоисключающе с `query`)
- `query` - SQL запрос для получения данных (взаимоисключающе с `table`)
- `columns` - массив конфигураций колонок:
  - `name` - имя колонки
  - `dataType` - тип данных в БД
  - `isNullable` - может ли быть NULL
  - `autoIncrement` - автоинкрементное поле
- `indexes` - список индексов для создания
- `primaryKey` - первичный ключ таблицы

### Общие параметры синхронизации

- `batch_size` - размер пакета для вставки (по умолчанию 1000)
- `temp_table_suffix` - суффикс временной таблицы (по умолчанию "_temp")
- `buffer_size` - размер буфера в памяти (по умолчанию 5000)
- `sync_interval` - интервал синхронизации (формат "5m", "1h", по умолчанию "5m")
- `post_procedure_list` - список хранимых процедур для выполнения после синхронизации:
  - `procedure_name` - имя процедуры
  - `procedure_params` - массив параметров процедуры

## Формат временных интервалов

Параметр `sync_interval` поддерживает следующие форматы:
- "300s" - 300 секунд
- "5m" - 5 минут
- "1h" - 1 час
- "24h" - 24 часа

## Переменные окружения

Все параметры могут быть переопределены через переменные окружения (см. тег `env` в структурах). Например:
- `ORACLE_HOST` вместо `oracle.host`
- `SYNC_BATCH_SIZE` вместо `sync.batch_size`


# 🚀 Использование
Базовые команды
```bash
# Запуск с конфигом
./bin/db_swapper -config=config.yml

# Переключение БД
./bin/db_swapper -use=maria_dev

# Синхронизация таблиц
./bin/db_swapper -sync=source:target -tables=users,products
```
# Программное использование
```go
package main
import (
	"db_swapper/internal/config"
	"db_swapper/internal/connectors"
	"db_swapper/internal/domain"
	"db_swapper/internal/services/sims_sync"
	"log"
	"logger"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg, err := config.GetConfig()
	if err != nil {
		panic(err)
	}
	l := &logger.Log{}
	l.InitLogger("all", "debug", "./all.log")

	oracleConn := connectors.NewOracleConnector(cfg.Oracle)

	mariadbConn := connectors.NewMariaDBConnector(cfg.MariaDB)

	l.Info("logger init succesful")
	err = oracleConn.Connect()
	if err != nil {
		panic(err)
	}
	err = oracleConn.Ping()
	if err != nil {
		panic(err)
	}
	err = mariadbConn.Connect()
	if err != nil {
		panic(err)
	}
	err = mariadbConn.Ping()
	if err != nil {
		panic(err)
	}
	syncService, err := sims_sync.NewSyncService(
		oracleConn,
		mariadbConn,
		cfg.Sync,
		l,
		sims_sync.WithTransform(func(r domain.Record) domain.Record {
			// Преобразование имен колонок из source в target
			if idPet, exists := r["ID_PET"]; exists {
				r["id_pet"] = idPet
				delete(r, "ID_PET")
			}

			if petName, exists := r["PET_NAME"]; exists {
				r["petName"] = petName
				delete(r, "PET_NAME")
			}

			return r
		}),
	)
	if err != nil {
		panic(err)
	}
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := syncService.Run(); err != nil {
			log.Fatalf("Sync failed: %v", err)
		}
	}()

	<-quit
	log.Println("Shutting down...")
  	...
}
```

# 🏗️ Структура проекта
``` schema
├── bin/                 # Собранные бинарники
├── cmd/
│   └── db_swapper/      # Главный исполняемый модуль
├── config.yml           # Пример конфигурации
├── internal/
│   ├── config/          # Загрузка конфигурации
│   ├── connectors/      # Драйверы БД
│   │   ├── connector.go # Базовый интерфейс
│   │   ├── maria.db.go  # MariaDB реализация
│   │   └── oracle.go    # Oracle реализация
│   └── domain/          # Модели данных
├── go.mod              # Зависимости
├── Makefile            # Управление сборкой
└── README.md           # Этот файл
```