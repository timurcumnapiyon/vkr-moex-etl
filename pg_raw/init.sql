CREATE TABLE IF NOT EXISTS public.bars_stocks_moex
(
    dataname character varying(24) COLLATE pg_catalog."default" NOT NULL,
    tf character varying(3) COLLATE pg_catalog."default" NOT NULL,
    datetime timestamp with time zone NOT NULL,
    open real NOT NULL,
    high real NOT NULL,
    low real NOT NULL,
    close real NOT NULL,
    volume real NOT NULL,
    CONSTRAINT bars_stocks_moex_pkey PRIMARY KEY (dataname, tf, datetime)
);

-- Создание функции для операции INSERT и UPDATE для таблице bars_stocks_moex
CREATE OR REPLACE FUNCTION upsert_bars_stocks_moex(new_dataname character varying,
new_tf character varying, new_datetime timestamptz, new_open real,
new_high real, new_low real, new_close real, new_volume real)
RETURNS void AS $$
 INSERT INTO bars_stocks_moex VALUES(new_dataname, new_tf, new_datetime, new_open, new_high, new_low, new_close, new_volume)
 ON CONFLICT (dataname, tf, datetime)
 DO UPDATE SET dataname=new_dataname, tf=new_tf, datetime=new_datetime, open=new_open, high=new_high, low=new_low, close=new_close, volume=new_volume
$$ LANGUAGE SQL;


CREATE TABLE IF NOT EXISTS public.last_inserted_stocks
(
    dataname character varying(24) COLLATE pg_catalog."default" NOT NULL,
    tf character varying(3) COLLATE pg_catalog."default" NOT NULL,
    datetime timestamp with time zone NOT NULL,
    CONSTRAINT last_inserted_stocks_pkey PRIMARY KEY (dataname, tf)
);

CREATE TABLE IF NOT EXISTS public.symbol_info
(
    dataname character varying(24) COLLATE pg_catalog."default" NOT NULL,
    board character varying(24) COLLATE pg_catalog."default" NOT NULL,
    symbol character varying(8) COLLATE pg_catalog."default" NOT NULL,
    description text COLLATE pg_catalog."default" NOT NULL,
    decimals integer NOT NULL,
    min_step real NOT NULL,
    lot_size integer NOT NULL,
    last_updated timestamp with time zone NOT NULL
);

-- Создание функции для операции INSERT и UPDATE symbol_info
CREATE OR REPLACE FUNCTION upsert_symbol_info(new_dataname character varying,
new_board character varying, new_symbol character varying, new_description text,
new_decimals integer, new_min_step real, new_lot_size integer, new_last_updated timestamptz)
RETURNS void AS $$
 INSERT INTO symbol_info VALUES(new_dataname, new_board, new_symbol, new_description, new_decimals, new_min_step, new_lot_size, new_last_updated)
 ON CONFLICT (dataname)
 DO UPDATE SET dataname=new_dataname, board=new_board, symbol=new_symbol, description=new_description, decimals=new_decimals, min_step=new_min_step, lot_size=new_lot_size, last_updated=new_last_updated
$$ LANGUAGE SQL;


-- Создание функции для триггера
CREATE OR REPLACE FUNCTION date_update_stocks() RETURNS trigger AS $$
BEGIN
	INSERT INTO last_inserted_stocks (dataname, tf, datetime)
	VALUES (NEW.dataname, NEW.tf, NOW())
	ON CONFLICT (dataname, tf)
	DO UPDATE SET datetime = NOW();
	RETURN NEW;
END
$$ LANGUAGE plpgsql;

-- Создание триггера
CREATE TRIGGER update_last_inserted_stocks
AFTER INSERT OR UPDATE ON bars_stocks_moex
FOR EACH ROW EXECUTE FUNCTION date_update_stocks();
