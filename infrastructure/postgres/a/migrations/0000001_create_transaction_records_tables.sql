CREATE TABLE transaction_records (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,

    -- Transaction details
    trans_date_trans_time timestamptz NOT NULL,
    cc_num VARCHAR(20) NOT NULL,
    merchant VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    amt DECIMAL(10,2) NOT NULL CHECK (amt >= 0),
    trans_num VARCHAR(100) NOT NULL UNIQUE,
    unix_time BIGINT NOT NULL,

    -- Customer information
    first VARCHAR(100) NOT NULL,
    last VARCHAR(100) NOT NULL,
    gender CHAR(1) NOT NULL CHECK (gender IN ('M', 'F')),
    dob DATE NOT NULL,
    job VARCHAR(150) NOT NULL,

    -- Customer address
    street VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state CHAR(2) NOT NULL,
    zip VARCHAR(10) NOT NULL,
    lat DECIMAL(10,7) NOT NULL,
    long DECIMAL(10,7) NOT NULL,
    city_pop INT NOT NULL CHECK (city_pop >= 0),

    -- Merchant location
    merch_lat DECIMAL(10,7) NOT NULL,
    merch_long DECIMAL(10,7) NOT NULL,

    -- Fraud indicator
    is_fraud SMALLINT NOT NULL CHECK (is_fraud IN (0, 1)),

    -- Timestamps for auditing
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
