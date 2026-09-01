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

# Arten: income = Geldzufluss, consumption = echter Verbrauch,
# savings = zur Seite gelegt (keine Ausgabe im Sinne von Konsum),
# transfer = reine Umbuchung zwischen eigenen Konten, zählt nirgends mit.
VALID_KINDS = ("income", "consumption", "savings", "transfer")

CATEGORY_KINDS = {
    "Einkommen":            "income",
    "Sparen & Investieren": "savings",
    "Überweisung":          "transfer",
}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # journal_mode ist in der DB-Datei persistiert und wird einmalig in init_db()
    # gesetzt – hier nur die Pragmas, die pro Verbindung gelten.
    #
    # synchronous=NORMAL ist zusammen mit WAL crash-sicher (ein Absturz kann die
    # letzte Transaktion kosten, die DB aber nicht beschädigen) und spart pro
    # Commit einen fsync. Auf der SD-Karte eines Pi ist das der Unterschied
    # zwischen "spürbar träge" und "sofort" – und deutlich weniger Schreiblast.
    conn.execute("PRAGMA synchronous=NORMAL")
    # Statt sofortigem "database is locked", wenn ein Import parallel schreibt.
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db():
    conn = get_db()
    conn.execute("PRAGMA journal_mode=WAL")
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

        CREATE TABLE IF NOT EXISTS category_budgets (
            category       TEXT PRIMARY KEY,
            monthly_budget REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS category_corrections (
            merchant_name TEXT NOT NULL,
            category      TEXT NOT NULL,
            count         INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (merchant_name, category)
        );

        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS sessions (
            token_hash TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);

        -- Manuelle Übersteuerung, ob ein Posten als "fix" oder "variabel"
        -- gilt (sonst automatisch über die Betragsschwankung geschätzt).
        -- Fließt in die Fixkosten-Hochrechnung ein. Schlüssel ist Händler +
        -- Kategorie, nicht nur der Händler: comdirect vergibt bei
        -- Eigenüberweisungen (Miete/Lebensmittel/Sparen an sich selbst)
        -- denselben merchant_name für völlig unterschiedliche Buchungen.
        CREATE TABLE IF NOT EXISTS merchant_overrides (
            merchant_name  TEXT NOT NULL,
            category       TEXT NOT NULL,
            recurring_type TEXT NOT NULL CHECK (recurring_type IN ('fix', 'variabel')),
            PRIMARY KEY (merchant_name, category)
        );
    """)
    conn.commit()

    # Migration: frühere Version dieser Tabelle hatte merchant_name als
    # alleinigen Schlüssel – zu grob, siehe Kommentar oben. Bestehende
    # Übersteuerungen werden auf die Kategorie übernommen, die zum Zeitpunkt
    # der Einstufung angezeigt wurde (jüngste Buchung dieses Händlers),
    # damit sie nicht kommentarlos verloren gehen.
    cols = [r[1] for r in conn.execute("PRAGMA table_info(merchant_overrides)").fetchall()]
    if "category" not in cols:
        old_rows = conn.execute("SELECT merchant_name, recurring_type FROM merchant_overrides").fetchall()
        conn.execute("ALTER TABLE merchant_overrides RENAME TO merchant_overrides_old")
        conn.execute("""
            CREATE TABLE merchant_overrides (
                merchant_name  TEXT NOT NULL,
                category       TEXT NOT NULL,
                recurring_type TEXT NOT NULL CHECK (recurring_type IN ('fix', 'variabel')),
                PRIMARY KEY (merchant_name, category)
            )
        """)
        for row in old_rows:
            # `id DESC` als Tiebreaker bei gleichem Datum (z. B. mehrere
            # Buchungen desselben Eigenüberweisungs-Namens am Gehaltstag) –
            # reproduziert damit exakt, welche Kategorie die alte, jetzt
            # entfernte Logik (ORDER BY date ASC, letzter Listeneintrag) zum
            # Zeitpunkt der Einstufung angezeigt hat.
            cat_row = conn.execute("""
                SELECT category FROM transactions
                WHERE merchant_name = ? AND date != '0000-00-00'
                ORDER BY date DESC, id DESC LIMIT 1
            """, (row["merchant_name"],)).fetchone()
            if cat_row:
                conn.execute(
                    "INSERT OR IGNORE INTO merchant_overrides (merchant_name, category, recurring_type) "
                    "VALUES (?, ?, ?)",
                    (row["merchant_name"], cat_row["category"], row["recurring_type"])
                )
        conn.execute("DROP TABLE merchant_overrides_old")
        conn.commit()

    # Migrations
    for stmt in [
        "ALTER TABLE transactions ADD COLUMN note TEXT",
        # Art der Kategorie: trennt echten Konsum von Sparen und reinen
        # Umbuchungen, damit die Ausgaben-Kennzahl nicht Sparraten mitzählt.
        "ALTER TABLE categories ADD COLUMN kind TEXT NOT NULL DEFAULT 'consumption'",
    ]:
        try:
            conn.execute(stmt)
            conn.commit()
        except Exception:
            pass

    # Startbelegung der Arten – nur für die bekannten Standardkategorien und
    # nur einmalig, danach entscheidet der Nutzer.
    if not conn.execute("SELECT value FROM settings WHERE key = 'kinds_seeded'").fetchone():
        for name, kind in CATEGORY_KINDS.items():
            conn.execute("UPDATE categories SET kind = ? WHERE name = ?", (kind, name))
        conn.execute("INSERT INTO settings (key, value) VALUES ('kinds_seeded', '1')")
        conn.commit()

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
