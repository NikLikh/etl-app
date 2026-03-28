# ETL Pipeline: E-Commerce Clickstream

Пайплайн для обработки данных поведения пользователей онлайн-магазина.
Датасет - https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

Источник - CSV-файл, назначение - Parquet с партициями по event_date.

## Как это работает

Скрипт `load_kaggle_data.py` скачивает CSV с Kaggle и заливает в PostgreSQL через Spark, затем ETL читает данные из PostgreSQL, прогоняет трансформации и пишет в Parquet (партиции по `event_date`)

Поддерживает два режима:
- **Full snapshot** - перезаписывает всё
- **Incremental** - дописывает только новые записи (отслеживание через HWM по `event_time`)

Управление через FastAPI (4 эндпоинта) или напрямую из CLI.

## Стек

Python 3.13, Apache Spark 4.0 (PySpark), onETL, PostgreSQL 16, FastAPI, Docker Compose

## Структура

```
api/app.py                   - REST API (full, incremental, status, history)

config/__init__.py           - загрузка конфига, создание SparkSession
config/settings.yaml         - все параметры (source, target, spark, hwm)

etl/extract.py               - чтение из PostgreSQL через onETL
etl/transform.py             - трансформации Spsrk
etl/load.py                  - запись в Parquet
etl/pipeline.py              - оркестрация full snapshot / incremental

scripts/load_kaggle_data.py  - загрузка датасета Kaggle в PostgreSQL
scripts/run_etl.py           - запуск incremental из консоли

Dockerfile                   - Python 3.13-slim + Java 21
docker-compose.yaml          - PostgreSQL + Spark (master + 2 workers) + App
```

## Запуск

### Через докер

```bash
cp .env.example .env
docker-compose up --build -d
```

Поднимутся 5 контейнеров: PostgreSQL, Spark Master, 2 воркера, приложение

- API: http://localhost:8000/docs
- Spark Master UI: http://localhost:8080
- Spark Driver UI: http://localhost:4040

### Загрузка данных

```bash
docker exec -it etl-app python scripts/load_kaggle_data.py
```

С флагом `--append` дописывает данные

### Запуск ETL

```bash
# API
curl -X POST http://localhost:8000/etl/full
curl -X POST http://localhost:8000/etl/incremental
curl http://localhost:8000/etl/status/{task_id}
curl http://localhost:8000/etl/history

# CLI
docker exec -it etl-app python scripts/run_etl.py
```

ETL запускается в фоне. В ответе приходит `task_id`, по нему можно смотреть статус и метрики (записей обработано, время, которое ушло на обработку)

### Через терминал

```bash
python -m venv .venv && .venv\Scripts\activate
pip install -r requirements.txt
docker-compose up postgres -d
cp .env.example .env
python scripts/load_kaggle_data.py
uvicorn api.app:app --reload
```

## Трансформации

1. **Фильтрация невалидных** - убираем события без `product_id` и с отрицательной ценой
2. **Фильтрация ботов** - сессии с > 1000 событий считаются ботами
3. **Парсинг категорий** - `category_code` разбивается на `top_category` / `sub_category` / `detail_category`
4. **Временные признаки** - из `event_time` достаём `event_date`, `event_hour`, `day_of_week`
5. **Флаги воронки** - `is_view`, `is_cart`, `is_purchase`

## Incremental Load

HWM-колонка - `event_time`. Состояние хранится в `hwm_store/hwm.yaml`
Первый запуск грузит всё, последующие - только `WHERE event_time > last_hwm`
HWM обновляется после успешной записи (onETL `IncrementalStrategy`)

## Конфигурация

Всё в `config/settings.yaml` - подключение к БД, настройки Spark, пути к Parquet, HWM
Пароли в `.env` (шаблон - `.env.example`), подтягиваются через `python-dotenv`

## Docker Compose

PostgreSQL, Spark Master (UI на :8080), 2 воркера (по 2 ядра, 2 GB), приложение (API на :8000)
