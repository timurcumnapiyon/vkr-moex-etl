# ВКР: Разработка информационной системы автоматизации процессов сбора и обработки данных московской биржи

Автоматизированный конвейер сбора, загрузки, проверки качества и визуализации рыночных данных.

## Что делает проект

- Получает минутные бары из **ALOR** в `PG_RAW`.
- При деградации ALOR включает **MOEX ISS fallback** (чтобы не терять данные).
- Переносит данные в `PG_DWH` (TimescaleDB) с приоритетом ALOR над fallback.
- Строит агрегаты (5m/15m/30m/1h/1d) и view-слой для Grafana.
- Выполняет data quality checks и пишет результаты в `dq_results`.
- Делает резервное копирование в MinIO (S3-compatible).
- Оркестрируется через Prefect (flows + schedules + retries).

## Архитектура

```text
ALOR / MOEX ISS
      ↓
   PG_RAW  (сырой слой, retention 90 дней)
      ↓
   PG_DWH  (аналитический слой, долгосрочное хранение)
      ↓
  Grafana  (дашборды, индикаторы, DQ)

Prefect управляет запуском всех ETL-процессов
MinIO хранит бэкапы
```

## Основные компоненты

- `fetch_alor_to_pgraw_1min.py` — live ingest минутных баров ALOR в `PG_RAW`.
- `fetch_moex_iss_fallback_to_pgraw_1min.py` — fallback ingest 1m из MOEX ISS.
- `load_alor_pgraw_to_pgdwh_1min.py` — загрузка 1m в DWH с дедупликацией и приоритетом ALOR.
- `aggregate_alor_pgdwh_1min_to_1d.py` — агрегация 1m -> 1d.
- `load_moex_pgraw_to_pgdwh_1d.py` — загрузка дневных MOEX данных в DWH.
- `run_data_quality_checks.py` — проверки качества данных.
- `cleanup_pgraw_retention.py` — очистка RAW-данных старше 90 дней.
- `flows/vkr_flows.py` — Prefect flow-обертки.
- `prefect.yaml` — deployments/schedules/job variables.

## Быстрый старт

### 1) Подготовка переменных

```bash
cp .env.example .env
```

Заполните `.env` реальными значениями (особенно токены/пароли).

### 2) Запуск инфраструктуры

```bash
docker compose up -d
```

### 3) Деплой Prefect flows

```bash
python -m prefect deploy --all --prefect-file prefect.yaml
```

### 4) Проверка сервисов

- Prefect UI: `http://localhost:4200`
- Grafana: `http://localhost:3000`
- pgAdmin: `http://localhost:5050`
- MinIO Console: `http://localhost:9001`

## Data Quality и мониторинг

- Результаты DQ проверок: таблица `public.dq_results`.
- В Grafana доступны:
  - статус ALOR stream,
  - свежесть данных,
  - multi-timeframe графики,
  - индикаторы (RSI, ATR, MACD, BB Width, EMA cross),
  - статус DQ.

## Безопасность

- Секреты не хранятся в репозитории.
- Используются переменные окружения (`.env`), файл игнорируется через `.gitignore`.
- В коде и конфигурации исключены hardcoded токены.

## Важные замечания

- В нерабочие часы/выходные рынок может не давать новые бары — это штатно.
- Fallback включается при stale-состоянии ALOR (настраивается через `FAILOVER_STALE_MINUTES`).
- RAW retention: 90 дней (ежедневный cleanup deployment).

## Структура проекта

```text
.
├── flows/
├── grafana/
├── pg_raw/
├── pg_dwh/
├── scripts/
├── fetch_*.py
├── load_*.py
├── run_data_quality_checks.py
├── cleanup_pgraw_retention.py
├── docker-compose.yml
├── prefect.yaml
└── .env.example
```

