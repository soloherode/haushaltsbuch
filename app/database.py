import sqlite3
import os

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "..", "haushaltsbuch.db"))


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS transactions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source          TEXT NOT NULL,          -- 'comdirect' | 'hanseaticbank'
            account_name    TEXT NOT NULL,
            date            TEXT NOT NULL,          -- ISO: YYYY-MM-DD (Buchungstag)
            transaction_date TEXT,                  -- ISO: YYYY-MM-DD (Valuta / card date)
            amount          REAL NOT NULL,
            description     TEXT,
            merchant_name   TEXT,
            category        TEXT NOT NULL DEFAULT 'Sonstiges',
            subcategory     TEXT,
            city            TEXT,
            country         TEXT,
            transaction_type TEXT,
            booked          INTEGER NOT NULL DEFAULT 1,
            import_hash     TEXT UNIQUE,            -- prevent duplicate imports
            imported_at     TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_transactions_date     ON transactions(date);
        CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions(category);
        CREATE INDEX IF NOT EXISTS idx_transactions_source   ON transactions(source);

        CREATE TABLE IF NOT EXISTS category_rules (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern     TEXT NOT NULL,
            category    TEXT NOT NULL,
            subcategory TEXT,
            priority    INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS user_categories (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );
    """)
    conn.commit()
    # Migrations
    try:
        conn.execute("ALTER TABLE transactions ADD COLUMN note TEXT")
        conn.commit()
    except Exception:
        pass
    conn.close()
