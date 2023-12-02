
CREATE TABLE asset (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL UNIQUE,
    min_lot_size NUMERIC NOT NULL,
    trading INTEGER NOT NULL
);

CREATE TABLE asset_price (
    asset_id INTEGER NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume NUMERIC NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (asset_id, open_time),
    CONSTRAINT fk_asset FOREIGN KEY (asset_id) REFERENCES asset (id)
);

CREATE INDEX ON asset_price (asset_id, open_time DESC);

-- Table to store results from the cointegration test for ml
CREATE TABLE coint_test_results (
    pair TEXT NOT NULL,
    coint_test_stat NUMERIC NOT NULL,
    p_value NUMERIC NOT NULL,
    symbol_1_id INTEGER NOT NULL,
    symbol_2_id INTEGER NOT NULL,
    symbol_1 TEXT NOT NULL,
    symbol_2 TEXT NOT NULL,
    trainset_start TIMESTAMPTZ NOT NULL,
    trainset_end TIMESTAMPTZ NOT NULL,
    test_date TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (symbol_1_id, symbol_2_id, trainset_start, trainset_end),
    CONSTRAINT fk_symbol_1 FOREIGN KEY (symbol_1_id) REFERENCES asset (id),
    CONSTRAINT fk_symbol_2 FOREIGN KEY (symbol_2_id) REFERENCES asset (id)
);

-- Table to store the pair dataframes in json
CREATE TABLE pair_dataframes (
    id SERIAL,
    symbol_1 TEXT NOT NULL,
    symbol_2 TEXT NOT NULL,
    symbol_1_id INTEGER NOT NULL,
    symbol_2_id INTEGER NOT NULL,
    dataset_start TIMESTAMPTZ NOT NULL,
    dataset_end TIMESTAMPTZ NOT NULL,
    test_date TIMESTAMPTZ NOT NULL,    
    json_data jsonb,
    PRIMARY KEY (symbol_1_id, symbol_2_id, dataset_start, dataset_end)
);

-- Table to store the adf_test results
CREATE TABLE adf_test_results (
    pair TEXT NOT NULL,
    adf_test_stat NUMERIC NOT NULL,
    p_value NUMERIC NOT NULL,
    stationary BOOLEAN NOT NULL,
    symbol_1_id INTEGER NOT NULL,
    symbol_2_id INTEGER NOT NULL,
    symbol_1 TEXT NOT NULL,
    symbol_2 TEXT NOT NULL,
    trainset_start TIMESTAMPTZ NOT NULL,
    trainset_end TIMESTAMPTZ NOT NULL,
    test_date TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (symbol_1_id, symbol_2_id, trainset_start, trainset_end),
    CONSTRAINT fk_symbol_1 FOREIGN KEY (symbol_1_id) REFERENCES asset (id),
    CONSTRAINT fk_symbol_2 FOREIGN KEY (symbol_2_id) REFERENCES asset (id)
);

CREATE TABLE trading_pairs (
    symbol_1_id INTEGER NOT NULL,
    symbol_2_id INTEGER NOT NULL,
    trainset_start TIMESTAMPTZ NOT NULL,
    trainset_end TIMESTAMPTZ NOT NULL,
    test_date TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (symbol_1_id, symbol_2_id, trainset_start, trainset_end),
    CONSTRAINT fk_symbol_1 FOREIGN KEY (symbol_1_id) REFERENCES asset (id),
    CONSTRAINT fk_symbol_2 FOREIGN KEY (symbol_2_id) REFERENCES asset (id)
);

CREATE TABLE positions (
    date TIMESTAMPTZ NOT NULL,
    positions JSONB,
    PRIMARY KEY (date, positions)
);

CREATE TABLE all_pairs_pnl (
    date TIMESTAMPTZ NOT NULL,
    pnl JSONB,
    PRIMARY KEY (date, pnl)
);

CREATE TABLE orders (
    symbol TEXT NOT NULL,
    pair TEXT NOT NULL,
    pair_order INT NOT NULL,
    order_id INT NOT NULL,
    client_order_id TEXT NOT NULL,
    transact_time TIMESTAMPTZ NOT NULL,
    price NUMERIC,
    orig_qty NUMERIC,
    executed_qty NUMERIC,
    cummulative_quote_qty NUMERIC,
    status TEXT, 
    time_in_force TEXT,
    type TEXT,
    side TEXT,
    fills JSONB,
    margin_buy_borrow_asset TEXT,
    margin_buy_borrow_amount NUMERIC,
    is_isolated TEXT,
    self_trade_prevention_mode TEXT,
    function TEXT,
    spread TEXT,
    PRIMARY KEY (order_id),
    CONSTRAINT fk_symbol FOREIGN KEY (symbol) REFERENCES asset (symbol)
);

CREATE TABLE orders (
    order_id TEXT,
    symbol TEXT,
    pair TEXT,
    pair_order INT,
    status TEXT,
    spread TEXT,
    client_order_id TEXT,
    price NUMERIC,
    avg_price NUMERIC,
    orig_qty NUMERIC,
    executed_qty NUMERIC,
    cum_qty NUMERIC,
    cum_quote NUMERIC,
    time_in_force TEXT,
    type TEXT,
    reduce_only BOOLEAN,
    close_position BOOLEAN,
    side TEXT,
    position_side TEXT,
    stop_price NUMERIC,
    working_type TEXT,
    price_protect BOOLEAN,
    orig_type TEXT,
    price_match TEXT,
    self_trade_prevention_mode TEXT,
    good_till_date INT,
    update_time TIMESTAMPTZ,
    PRIMARY KEY (order_id),
    CONSTRAINT fk_symbol FOREIGN KEY (symbol) REFERENCES asset (symbol)
);