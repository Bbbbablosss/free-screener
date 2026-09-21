CREATE TABLE series (
    sid        INTEGER PRIMARY KEY,
    key        TEXT    UNIQUE NOT NULL,
    last_ts_ms INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE candles (
    sid    INTEGER NOT NULL,
    ts     INTEGER NOT NULL,
    open   TEXT    NOT NULL,
    high   TEXT    NOT NULL,
    low    TEXT    NOT NULL,
    close  TEXT    NOT NULL,
    volume TEXT    NOT NULL DEFAULT '0',
    PRIMARY KEY (sid, ts)
) WITHOUT ROWID;
