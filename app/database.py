import sqlite3
import os

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "..", "haushaltsbuch.db"))

DEFAULT_CATEGORIES = [
    "Einkommen",
    "Lebensmittel",
    "Restaurant & Cafe",
    "Mobilität",
    "Einkaufen",
    "Kleidung",
    "Kinder",
    "Hobby",
    "Gesundheit",
    "Unterhaltung",
    "Finanzen & Versicherung",
    "Wohnen & Nebenkosten",
    "Sparen & Investieren",
    "Überweisung",
    "Sonstiges",
]


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
            source          TEXT NOT NULL,
            account_name    TEXT NOT NULL,
            date            TEXT NOT NULL,
            transaction_date TEXT,
            amount          REAL NOT NULL,
            description     TEXT,
            merchant_name   TEXT,
            category        TEXT NOT NULL DEFAULT 'Sonstiges',
            subcategory     TEXT,
            city            TEXT,
            country         TEXT,
            transaction_type TEXT,
            booked          INTEGER NOT NULL DEFAULT 1,
            import_hash     TEXT UNIQUE,
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

        CREATE TABLE IF NOT EXISTS categories (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT UNIQUE NOT NULL,
            is_default INTEGER NOT NULL DEFAULT 0
        );
    """)
    conn.commit()

    # Migrations
    for stmt in [
        "ALTER TABLE transactions ADD COLUMN note TEXT",
    ]:
        try:
            conn.execute(stmt)
            conn.commit()
        except Exception:
            pass

    # Migrate old user_categories into new categories table
    try:
        old = [r[0] for r in conn.execute("SELECT name FROM user_categories").fetchall()]
        for name in old:
            try:
                conn.execute("INSERT OR IGNORE INTO categories (name, is_default) VALUES (?, 0)", (name,))
            except Exception:
                pass
        conn.commit()
    except Exception:
        pass

    # Seed default categories (only if not already present)
    for name in DEFAULT_CATEGORIES:
        try:
            conn.execute("INSERT OR IGNORE INTO categories (name, is_default) VALUES (?, 1)", (name,))
        except Exception:
            pass
    conn.commit()
    conn.close()
