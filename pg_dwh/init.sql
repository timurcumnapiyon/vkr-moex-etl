CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS public.dwh_bars_1m (
    dataname text NOT NULL,
    datetime timestamptz NOT NULL,
    open double precision NOT NULL,
    high double precision NOT NULL,
    low double precision NOT NULL,
    close double precision NOT NULL,
    volume double precision NOT NULL,
    source text NOT NULL,
    CONSTRAINT dwh_bars_1m_pkey PRIMARY KEY (dataname, datetime, source)
);

SELECT create_hypertable('public.dwh_bars_1m', 'datetime', if_not_exists => TRUE, migrate_data => TRUE);

CREATE INDEX IF NOT EXISTS idx_dwh_bars_1m_source_datetime
    ON public.dwh_bars_1m (source, datetime DESC);

CREATE TABLE IF NOT EXISTS public.dwh_bars_1d (
    dataname text NOT NULL,
    datetime date NOT NULL,
    open double precision NOT NULL,
    high double precision NOT NULL,
    low double precision NOT NULL,
    close double precision NOT NULL,
    volume double precision NOT NULL,
    source text NOT NULL,
    CONSTRAINT dwh_bars_1d_pkey PRIMARY KEY (dataname, datetime, source)
);

CREATE INDEX IF NOT EXISTS idx_dwh_bars_1d_source_datetime
    ON public.dwh_bars_1d (source, datetime DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS public.dwh_bars_1d_cagg
WITH (timescaledb.continuous) AS
SELECT
    dataname,
    time_bucket('1 day', datetime) AS day,
    first(open, datetime) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, datetime) AS close,
    sum(volume) AS volume,
    source
FROM public.dwh_bars_1m
GROUP BY dataname, day, source
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_dwh_bars_1d_cagg_day
    ON public.dwh_bars_1d_cagg (day DESC, source);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM timescaledb_information.continuous_aggregates
        WHERE view_name = 'dwh_bars_1d_cagg'
    ) THEN
        PERFORM add_continuous_aggregate_policy(
            'public.dwh_bars_1d_cagg',
            start_offset => INTERVAL '7 days',
            end_offset   => INTERVAL '1 hour',
            schedule_interval => INTERVAL '1 hour'
        );
    END IF;
END $$;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.dwh_bars_5m_cagg
WITH (timescaledb.continuous) AS
SELECT
    dataname,
    time_bucket('5 minutes', datetime) AS bucket,
    first(open, datetime) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, datetime) AS close,
    sum(volume) AS volume,
    source
FROM public.dwh_bars_1m
GROUP BY dataname, bucket, source
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_dwh_bars_5m_cagg_bucket
    ON public.dwh_bars_5m_cagg (bucket DESC, source, dataname);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM timescaledb_information.jobs j
        JOIN timescaledb_information.continuous_aggregates c
          ON j.hypertable_schema = c.materialization_hypertable_schema
         AND j.hypertable_name = c.materialization_hypertable_name
        WHERE c.view_name = 'dwh_bars_5m_cagg'
    ) THEN
        PERFORM add_continuous_aggregate_policy(
            'public.dwh_bars_5m_cagg',
            start_offset => INTERVAL '30 days',
            end_offset   => INTERVAL '5 minutes',
            schedule_interval => INTERVAL '5 minutes'
        );
    END IF;
END $$;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.dwh_bars_15m_cagg
WITH (timescaledb.continuous) AS
SELECT
    dataname,
    time_bucket('15 minutes', datetime) AS bucket,
    first(open, datetime) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, datetime) AS close,
    sum(volume) AS volume,
    source
FROM public.dwh_bars_1m
GROUP BY dataname, bucket, source
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_dwh_bars_15m_cagg_bucket
    ON public.dwh_bars_15m_cagg (bucket DESC, source, dataname);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM timescaledb_information.jobs j
        JOIN timescaledb_information.continuous_aggregates c
          ON j.hypertable_schema = c.materialization_hypertable_schema
         AND j.hypertable_name = c.materialization_hypertable_name
        WHERE c.view_name = 'dwh_bars_15m_cagg'
    ) THEN
        PERFORM add_continuous_aggregate_policy(
            'public.dwh_bars_15m_cagg',
            start_offset => INTERVAL '60 days',
            end_offset   => INTERVAL '15 minutes',
            schedule_interval => INTERVAL '15 minutes'
        );
    END IF;
END $$;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.dwh_bars_30m_cagg
WITH (timescaledb.continuous) AS
SELECT
    dataname,
    time_bucket('30 minutes', datetime) AS bucket,
    first(open, datetime) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, datetime) AS close,
    sum(volume) AS volume,
    source
FROM public.dwh_bars_1m
GROUP BY dataname, bucket, source
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_dwh_bars_30m_cagg_bucket
    ON public.dwh_bars_30m_cagg (bucket DESC, source, dataname);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM timescaledb_information.jobs j
        JOIN timescaledb_information.continuous_aggregates c
          ON j.hypertable_schema = c.materialization_hypertable_schema
         AND j.hypertable_name = c.materialization_hypertable_name
        WHERE c.view_name = 'dwh_bars_30m_cagg'
    ) THEN
        PERFORM add_continuous_aggregate_policy(
            'public.dwh_bars_30m_cagg',
            start_offset => INTERVAL '90 days',
            end_offset   => INTERVAL '30 minutes',
            schedule_interval => INTERVAL '30 minutes'
        );
    END IF;
END $$;

CREATE MATERIALIZED VIEW IF NOT EXISTS public.dwh_bars_1h_cagg
WITH (timescaledb.continuous) AS
SELECT
    dataname,
    time_bucket('1 hour', datetime) AS bucket,
    first(open, datetime) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, datetime) AS close,
    sum(volume) AS volume,
    source
FROM public.dwh_bars_1m
GROUP BY dataname, bucket, source
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_dwh_bars_1h_cagg_bucket
    ON public.dwh_bars_1h_cagg (bucket DESC, source, dataname);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM timescaledb_information.jobs j
        JOIN timescaledb_information.continuous_aggregates c
          ON j.hypertable_schema = c.materialization_hypertable_schema
         AND j.hypertable_name = c.materialization_hypertable_name
        WHERE c.view_name = 'dwh_bars_1h_cagg'
    ) THEN
        PERFORM add_continuous_aggregate_policy(
            'public.dwh_bars_1h_cagg',
            start_offset => INTERVAL '180 days',
            end_offset   => INTERVAL '1 hour',
            schedule_interval => INTERVAL '1 hour'
        );
    END IF;
END $$;

CREATE OR REPLACE VIEW public.v_bars_1min AS
SELECT
    '1min'::text AS timeframe,
    dataname,
    datetime AS ts,
    open,
    high,
    low,
    close,
    volume,
    source
FROM public.dwh_bars_1m;

CREATE OR REPLACE VIEW public.v_bars_5min AS
SELECT
    '5min'::text AS timeframe,
    dataname,
    bucket AS ts,
    open,
    high,
    low,
    close,
    volume,
    source
FROM public.dwh_bars_5m_cagg;

CREATE OR REPLACE VIEW public.v_bars_15min AS
SELECT
    '15min'::text AS timeframe,
    dataname,
    bucket AS ts,
    open,
    high,
    low,
    close,
    volume,
    source
FROM public.dwh_bars_15m_cagg;

CREATE OR REPLACE VIEW public.v_bars_30min AS
SELECT
    '30min'::text AS timeframe,
    dataname,
    bucket AS ts,
    open,
    high,
    low,
    close,
    volume,
    source
FROM public.dwh_bars_30m_cagg;

CREATE OR REPLACE VIEW public.v_bars_1h AS
SELECT
    '1h'::text AS timeframe,
    dataname,
    bucket AS ts,
    open,
    high,
    low,
    close,
    volume,
    source
FROM public.dwh_bars_1h_cagg;

CREATE OR REPLACE VIEW public.v_bars_1d AS
SELECT
    '1d'::text AS timeframe,
    dataname,
    datetime::timestamptz AS ts,
    open,
    high,
    low,
    close,
    volume,
    source
FROM public.dwh_bars_1d;

CREATE TABLE IF NOT EXISTS public.dq_results (
    id bigserial PRIMARY KEY,
    check_time timestamptz NOT NULL DEFAULT now(),
    dataset text NOT NULL,
    check_name text NOT NULL,
    severity text NOT NULL,
    status text NOT NULL,
    bad_rows bigint NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_dq_results_check_time
    ON public.dq_results (check_time DESC);


CREATE TABLE IF NOT EXISTS public.moex_agg (
    secid text NOT NULL,
    ts timestamptz NOT NULL,
    avg_price double precision NOT NULL,
    volume double precision NOT NULL,
    CONSTRAINT moex_agg_pkey PRIMARY KEY (secid, ts)
);

CREATE INDEX IF NOT EXISTS idx_moex_agg_ts
    ON public.moex_agg (ts DESC);
