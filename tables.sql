CREATE TABLE IF NOT EXISTS moex.iss_data_hourly_volume
(
    secid text PRIMARY KEY,
    total_volume DOUBLE,
    window_start TIMESTAMP
);
CREATE TABLE IF NOT EXISTS moex.iss_data
(
    tradeno   BIGINT PRIMARY KEY,
    boardid   TEXT,
    price DOUBLE,
    quantity  INT,
    secid     TEXT,
    tradetime TIMESTAMP,
    value DOUBLE
);
