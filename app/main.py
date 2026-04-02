import csv
import hashlib
import io
import os
import re
import sqlite3
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from app.database import init_db, get_db, DB_PATH
from app.categories import CATEGORIES
from app.parsers.comdirect import parse_comdirect_csv
from app.parsers.hanseaticbank import parse_hanseaticbank_json


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Haushaltsbuch", lifespan=lifespan)

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ─── Root ──────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def root():
    with open(os.path.join(STATIC_DIR, "index.html"), encoding="utf-8") as f:
        return f.read()


# ─── Helpers ───────────────────────────────────────────────────────────────────

def _apply_db_rules(description: str, merchant: str, conn) -> str | None:
    """Check user-defined rules in DB; return category or None."""
    text = (description + " " + merchant).lower()
    rows = conn.execute(
        "SELECT pattern, category FROM category_rules ORDER BY priority DESC, id"
    ).fetchall()
    for row in rows:
        if row["pattern"] in text:
            return row["category"]
    return None


def _insert_transactions(transactions: list[dict]) -> dict:
    conn = get_db()
    # Load learned corrections: merchant → most-used category
    corrections = {}
    for row in conn.execute(
        "SELECT merchant_name, category FROM category_corrections ORDER BY count DESC"
    ).fetchall():
        corrections.setdefault(row["merchant_name"], row["category"])

    inserted = 0
    skipped = 0
    for t in transactions:
        # Priority 1: user-defined rules
        rule_cat = _apply_db_rules(t.get("description", ""), t.get("merchant_name", ""), conn)
        if rule_cat:
            t = {**t, "category": rule_cat}
        # Priority 2: learned corrections
        elif t.get("merchant_name") and t["merchant_name"] in corrections:
            t = {**t, "category": corrections[t["merchant_name"]]}
        try:
            conn.execute("""
                INSERT INTO transactions
                    (source, account_name, date, transaction_date, amount, description,
                     merchant_name, category, subcategory, city, country,
                     transaction_type, booked, import_hash)
                VALUES
                    (:source, :account_name, :date, :transaction_date, :amount, :description,
                     :merchant_name, :category, :subcategory, :city, :country,
                     :transaction_type, :booked, :import_hash)
            """, t)
            inserted += 1
        except sqlite3.IntegrityError:
            skipped += 1
    conn.commit()
    conn.close()
    return {"inserted": inserted, "skipped_duplicates": skipped}


# ─── Import ────────────────────────────────────────────────────────────────────

@app.post("/api/import/comdirect")
async def import_comdirect(file: UploadFile = File(...)):
    content = await file.read()
    try:
        transactions = parse_comdirect_csv(content)
    except Exception as e:
        raise HTTPException(400, f"Fehler beim Parsen: {e}")
    return _insert_transactions(transactions)


@app.post("/api/import/hanseaticbank")
async def import_hanseaticbank(file: UploadFile = File(...)):
    content = await file.read()
    try:
        transactions = parse_hanseaticbank_json(content)
    except Exception as e:
        raise HTTPException(400, f"Fehler beim Parsen: {e}")
    return _insert_transactions(transactions)


# ─── Transactions ──────────────────────────────────────────────────────────────

ALLOWED_SORT_COLS = {"date", "merchant_name", "category", "amount"}

@app.get("/api/transactions")
def list_transactions(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    category: str = Query(None),
    source: str = Query(None),
    month: str = Query(None),
    search: str = Query(None),
    sort: str = Query("date"),
    dir: str = Query("desc"),
):
    sort_col = sort if sort in ALLOWED_SORT_COLS else "date"
    sort_dir = "ASC" if dir == "asc" else "DESC"
    # Secondary sort for stability
    order = f"{sort_col} {sort_dir}, id DESC"

    conn = get_db()
    conditions = []
    params = []

    if category:
        conditions.append("category = ?")
        params.append(category)
    if source:
        conditions.append("source = ?")
        params.append(source)
    if month:
        conditions.append("date LIKE ?")
        params.append(f"{month}%")
    if search:
        conditions.append("(description LIKE ? OR merchant_name LIKE ?)")
        params += [f"%{search}%", f"%{search}%"]

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    offset = (page - 1) * page_size

    total = conn.execute(f"SELECT COUNT(*) FROM transactions {where}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM transactions {where} ORDER BY {order} LIMIT ? OFFSET ?",
        params + [page_size, offset]
    ).fetchall()

    conn.close()
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "transactions": [dict(r) for r in rows],
    }


@app.put("/api/transactions/{tx_id}/category")
def update_category(tx_id: int, body: dict):
    category = body.get("category", "").strip()
    conn = get_db()
    if category not in _all_categories(conn):
        conn.close()
        raise HTTPException(400, f"Unbekannte Kategorie: {category}")
    # Track correction for smart categorization
    tx = conn.execute("SELECT merchant_name FROM transactions WHERE id = ?", (tx_id,)).fetchone()
    if tx and tx["merchant_name"]:
        conn.execute("""
            INSERT INTO category_corrections (merchant_name, category, count) VALUES (?, ?, 1)
            ON CONFLICT(merchant_name, category) DO UPDATE SET count = count + 1
        """, (tx["merchant_name"], category))
    conn.execute("UPDATE transactions SET category = ? WHERE id = ?", (category, tx_id))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.put("/api/transactions/{tx_id}/date")
def update_date(tx_id: int, body: dict):
    date = body.get("date", "").strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        raise HTTPException(400, "Datum muss im Format YYYY-MM-DD sein")
    conn = get_db()
    conn.execute("UPDATE transactions SET date = ? WHERE id = ?", (date, tx_id))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.put("/api/transactions/{tx_id}/note")
def update_note(tx_id: int, body: dict):
    note = str(body.get("note", "")).strip()
    conn = get_db()
    conn.execute("UPDATE transactions SET note = ? WHERE id = ?", (note, tx_id))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.post("/api/transactions/batch-category")
def batch_update_category(body: dict):
    ids = body.get("ids", [])
    category = body.get("category", "").strip()
    conn = get_db()
    if not ids or category not in _all_categories(conn):
        conn.close()
        raise HTTPException(400, "Ungültige Anfrage")
    placeholders = ",".join("?" * len(ids))
    conn.execute(
        f"UPDATE transactions SET category = ? WHERE id IN ({placeholders})",
        [category] + list(ids)
    )
    conn.commit()
    conn.close()
    return {"ok": True, "updated": len(ids)}


@app.post("/api/transactions")
def create_transaction(body: dict):
    date = body.get("date", "").strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        raise HTTPException(400, "Datum muss im Format YYYY-MM-DD sein")
    try:
        amount = float(body.get("amount", 0))
    except (ValueError, TypeError):
        raise HTTPException(400, "Ungültiger Betrag")
    description = str(body.get("description", "")).strip()
    account_name = str(body.get("account_name", "Manuell")).strip() or "Manuell"
    raw = f"manual|{account_name}|{date}|{amount}|{description}"
    import_hash = hashlib.sha256(raw.encode()).hexdigest()
    conn = get_db()
    category = body.get("category", "Sonstiges")
    if category not in _all_categories(conn):
        conn.close()
        raise HTTPException(400, f"Unbekannte Kategorie: {category}")
    try:
        conn.execute("""
            INSERT INTO transactions
                (source, account_name, date, transaction_date, amount, description,
                 merchant_name, category, subcategory, city, country,
                 transaction_type, booked, import_hash)
            VALUES ('manual', ?, ?, NULL, ?, ?, ?, ?, NULL, NULL, NULL, 'manual', 1, ?)
        """, (account_name, date, amount, description, description, category, import_hash))
        conn.commit()
        inserted_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    except sqlite3.IntegrityError:
        raise HTTPException(409, "Identischer Eintrag existiert bereits")
    finally:
        conn.close()
    return {"ok": True, "id": inserted_id}


@app.delete("/api/transactions/{tx_id}")
def delete_transaction(tx_id: int):
    conn = get_db()
    conn.execute("DELETE FROM transactions WHERE id = ?", (tx_id,))
    conn.commit()
    conn.close()
    return {"ok": True}


# ─── Rules ─────────────────────────────────────────────────────────────────────

@app.get("/api/rules")
def list_rules():
    conn = get_db()
    rows = conn.execute("SELECT * FROM category_rules ORDER BY priority DESC, id").fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.post("/api/rules")
def create_rule(body: dict):
    pattern = str(body.get("pattern", "")).strip().lower()
    category = body.get("category", "").strip()
    priority = int(body.get("priority", 0))
    conn = get_db()
    if not pattern or category not in _all_categories(conn):
        conn.close()
        raise HTTPException(400, "Ungültige Regel")
    conn.execute(
        "INSERT INTO category_rules (pattern, category, priority) VALUES (?, ?, ?)",
        (pattern, category, priority)
    )
    conn.commit()
    rule_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return {"ok": True, "id": rule_id}


@app.delete("/api/rules/{rule_id}")
def delete_rule(rule_id: int):
    conn = get_db()
    conn.execute("DELETE FROM category_rules WHERE id = ?", (rule_id,))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.post("/api/rules/apply-all")
def apply_rules_to_all():
    """Re-apply all DB rules to existing transactions."""
    conn = get_db()
    rules = conn.execute(
        "SELECT pattern, category FROM category_rules ORDER BY priority DESC, id"
    ).fetchall()
    if not rules:
        conn.close()
        return {"updated": 0}

    rows = conn.execute("SELECT id, description, merchant_name FROM transactions").fetchall()
    updated = 0
    for row in rows:
        text = ((row["description"] or "") + " " + (row["merchant_name"] or "")).lower()
        for rule in rules:
            if rule["pattern"] in text:
                conn.execute("UPDATE transactions SET category = ? WHERE id = ?", (rule["category"], row["id"]))
                updated += 1
                break
    conn.commit()
    conn.close()
    return {"updated": updated}


# ─── Stats ─────────────────────────────────────────────────────────────────────

@app.get("/api/stats/summary")
def stats_summary(month: str = Query(None)):
    conn = get_db()
    where = "WHERE date LIKE ?" if month else ""
    params = [f"{month}%"] if month else []

    excl = "AND category != 'Überweisung'"
    row = conn.execute(f"""
        SELECT
            SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS income,
            SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) AS expenses,
            COUNT(*) AS count
        FROM transactions {where} {excl}
    """, params).fetchone()

    conn.close()
    return {
        "income":   round(row["income"] or 0, 2),
        "expenses": round(row["expenses"] or 0, 2),
        "balance":  round((row["income"] or 0) + (row["expenses"] or 0), 2),
        "count":    row["count"],
    }


@app.get("/api/stats/categories")
def stats_categories(month: str = Query(None), type: str = Query("expense")):
    conn = get_db()
    amount_filter = "amount > 0" if type == "income" else "amount < 0"
    if month:
        where = f"WHERE date LIKE ? AND {amount_filter} AND category != 'Überweisung'"
        params = [f"{month}%"]
    else:
        where = f"WHERE {amount_filter} AND category != 'Überweisung'"
        params = []

    rows = conn.execute(f"""
        SELECT category, SUM(amount) AS total, COUNT(*) AS count
        FROM transactions {where}
        GROUP BY category
        ORDER BY total {"DESC" if type == "income" else "ASC"}
    """, params).fetchall()

    conn.close()
    return [{"category": r["category"], "total": round(r["total"], 2), "count": r["count"]} for r in rows]


@app.get("/api/stats/monthly")
def stats_monthly():
    conn = get_db()
    rows = conn.execute("""
        SELECT
            substr(date, 1, 7) AS month,
            SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS income,
            SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) AS expenses
        FROM transactions
        WHERE date != '0000-00-00' AND category != 'Überweisung'
        GROUP BY month
        ORDER BY month DESC
        LIMIT 24
    """).fetchall()
    conn.close()
    return [{"month": r["month"], "income": round(r["income"] or 0, 2), "expenses": round(r["expenses"] or 0, 2)} for r in rows]


@app.get("/api/stats/timeline")
def stats_timeline(category: str = Query(None)):
    conn = get_db()
    where = "WHERE date != '0000-00-00' AND amount < 0 AND category != 'Überweisung' AND category = ?" if category else "WHERE date != '0000-00-00' AND amount < 0 AND category != 'Überweisung'"
    params = [category] if category else []

    rows = conn.execute(f"""
        SELECT substr(date, 1, 7) AS month, SUM(amount) AS total
        FROM transactions {where}
        GROUP BY month
        ORDER BY month
    """, params).fetchall()
    conn.close()
    return [{"month": r["month"], "total": round(r["total"], 2)} for r in rows]


@app.get("/api/stats/yearly")
def stats_yearly(year: str = Query(None)):
    conn = get_db()
    where = f"WHERE date LIKE '{year}%' AND category != 'Überweisung'" if year else "WHERE date != '0000-00-00' AND category != 'Überweisung'"
    rows = conn.execute(f"""
        SELECT substr(date, 1, 7) AS month,
               SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS income,
               SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) AS expenses,
               SUM(CASE WHEN category = 'Sparen & Investieren' THEN ABS(amount) ELSE 0 END) AS savings
        FROM transactions {where}
        GROUP BY month ORDER BY month
    """).fetchall()
    conn.close()
    result = []
    for r in rows:
        inc = r["income"] or 0
        exp = abs(r["expenses"] or 0)
        sav = r["savings"] or 0
        result.append({
            "month": r["month"],
            "income": round(inc, 2),
            "expenses": round(exp, 2),
            "savings": round(sav, 2),
            "balance": round(inc - exp, 2),
            "savings_rate": round((sav / inc * 100) if inc > 0 else 0, 1),
        })
    return result


@app.get("/api/stats/comparison")
def stats_comparison(month: str = Query(None)):
    conn = get_db()
    if not month:
        row = conn.execute(
            "SELECT MAX(substr(date,1,7)) FROM transactions WHERE date != '0000-00-00'"
        ).fetchone()
        month = row[0]
    if not month:
        conn.close()
        return {}

    year, mon = month.split("-")
    pm = int(mon) - 1
    py = int(year)
    if pm == 0:
        pm, py = 12, py - 1
    prev_month = f"{py:04d}-{pm:02d}"

    def get_cats(m):
        rows = conn.execute("""
            SELECT category, SUM(ABS(amount)) AS total
            FROM transactions WHERE date LIKE ? AND amount < 0 AND category != 'Überweisung'
            GROUP BY category
        """, [f"{m}%"]).fetchall()
        return {r["category"]: r["total"] for r in rows}

    def get_avg(cat):
        row = conn.execute("""
            SELECT AVG(mt) FROM (
                SELECT SUM(ABS(amount)) AS mt
                FROM transactions WHERE amount < 0 AND category = ? AND date != '0000-00-00' AND category != 'Überweisung'
                GROUP BY substr(date,1,7)
            )
        """, [cat]).fetchone()
        return round(row[0] or 0, 2)

    current = get_cats(month)
    previous = get_cats(prev_month)
    all_cats = set(list(current.keys()) + list(previous.keys()))

    result = []
    for cat in sorted(all_cats):
        cur = current.get(cat, 0)
        prev = previous.get(cat, 0)
        avg = get_avg(cat)
        result.append({
            "category": cat,
            "current": round(cur, 2),
            "previous": round(prev, 2),
            "average": avg,
            "diff_prev": round(cur - prev, 2),
            "diff_avg": round(cur - avg, 2),
        })
    result.sort(key=lambda x: x["current"], reverse=True)
    conn.close()
    return {"month": month, "prev_month": prev_month, "categories": result}


@app.get("/api/stats/recurring")
def stats_recurring():
    conn = get_db()
    rows = conn.execute("""
        SELECT t.merchant_name, t.category,
               ROUND(AVG(t.amount), 2) AS avg_amount,
               ROUND(MIN(t.amount), 2) AS min_amount,
               ROUND(MAX(t.amount), 2) AS max_amount,
               COUNT(DISTINCT substr(t.date,1,7)) AS months,
               MIN(t.date) AS first_date, MAX(t.date) AS last_date,
               (SELECT t2.amount FROM transactions t2
                WHERE t2.merchant_name = t.merchant_name AND t2.amount < 0
                  AND t2.date != '0000-00-00'
                ORDER BY t2.date DESC LIMIT 1) AS last_amount
        FROM transactions t
        WHERE t.merchant_name != '' AND t.merchant_name IS NOT NULL
          AND t.amount < 0 AND t.date != '0000-00-00'
        GROUP BY t.merchant_name
        HAVING months >= 2
        ORDER BY months DESC, ABS(avg_amount) DESC
    """).fetchall()
    conn.close()
    result = []
    for r in rows:
        avg = abs(r["avg_amount"] or 0)
        spread = abs((r["max_amount"] or 0) - (r["min_amount"] or 0))
        spread_pct = spread / avg if avg > 0 else 0
        rec_type = "fix" if spread_pct < 0.05 else "variabel"
        last = abs(r["last_amount"] or 0)
        price_changed = avg > 0 and abs(last - avg) / avg > 0.10
        result.append({**dict(r), "type": rec_type, "price_changed": price_changed})
    return result


@app.get("/api/transactions/suspicious")
def suspicious_transactions():
    conn = get_db()
    rows = conn.execute("""
        SELECT DISTINCT a.id, a.date, a.amount, a.description,
               a.merchant_name, a.category, a.source
        FROM transactions a
        JOIN transactions b
          ON a.id < b.id
         AND a.amount = b.amount
         AND ABS(JULIANDAY(a.date) - JULIANDAY(b.date)) <= 3
         AND a.source != b.source
        ORDER BY a.date DESC
        LIMIT 100
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _all_categories(conn) -> list[str]:
    rows = conn.execute("SELECT name FROM categories ORDER BY id").fetchall()
    return [r["name"] for r in rows]


@app.get("/api/categories")
def list_categories():
    conn = get_db()
    cats = _all_categories(conn)
    conn.close()
    return cats


@app.get("/api/categories/detail")
def list_categories_detail():
    conn = get_db()
    rows = conn.execute("SELECT name, is_default FROM categories ORDER BY id").fetchall()
    conn.close()
    return [{"name": r["name"], "is_default": bool(r["is_default"])} for r in rows]


@app.post("/api/categories")
def create_category(body: dict):
    name = str(body.get("name", "")).strip()
    if not name:
        raise HTTPException(400, "Name erforderlich")
    conn = get_db()
    try:
        conn.execute("INSERT INTO categories (name, is_default) VALUES (?, 0)", (name,))
        conn.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(409, "Kategorie existiert bereits")
    finally:
        conn.close()
    return {"ok": True}


@app.put("/api/categories/{old_name}")
def rename_category(old_name: str, body: dict):
    new_name = str(body.get("name", "")).strip()
    if not new_name:
        raise HTTPException(400, "Name erforderlich")
    if old_name == "Sonstiges":
        raise HTTPException(400, '"Sonstiges" kann nicht umbenannt werden')
    conn = get_db()
    try:
        conn.execute("UPDATE categories SET name = ? WHERE name = ?", (new_name, old_name))
        conn.execute("UPDATE transactions SET category = ? WHERE category = ?", (new_name, old_name))
        conn.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(409, "Name bereits vergeben")
    finally:
        conn.close()
    return {"ok": True}


@app.delete("/api/categories/{name}")
def delete_category(name: str):
    if name == "Sonstiges":
        raise HTTPException(400, '"Sonstiges" kann nicht gelöscht werden')
    conn = get_db()
    conn.execute("DELETE FROM categories WHERE name = ?", (name,))
    conn.execute("UPDATE transactions SET category = 'Sonstiges' WHERE category = ?", (name,))
    conn.commit()
    conn.close()
    return {"ok": True}


# ─── Budgets ───────────────────────────────────────────────────────────────────

@app.get("/api/budgets")
def list_budgets():
    conn = get_db()
    rows = conn.execute("SELECT category, monthly_budget FROM category_budgets ORDER BY category").fetchall()
    conn.close()
    return {r["category"]: r["monthly_budget"] for r in rows}


@app.put("/api/budgets/{category}")
def set_budget(category: str, body: dict):
    try:
        amount = float(body.get("amount", 0))
    except (ValueError, TypeError):
        raise HTTPException(400, "Ungültiger Betrag")
    if amount <= 0:
        raise HTTPException(400, "Betrag muss positiv sein")
    conn = get_db()
    if category not in _all_categories(conn):
        conn.close()
        raise HTTPException(400, "Unbekannte Kategorie")
    conn.execute(
        "INSERT INTO category_budgets (category, monthly_budget) VALUES (?, ?) "
        "ON CONFLICT(category) DO UPDATE SET monthly_budget = excluded.monthly_budget",
        (category, amount)
    )
    conn.commit()
    conn.close()
    return {"ok": True}


@app.delete("/api/budgets/{category}")
def delete_budget(category: str):
    conn = get_db()
    conn.execute("DELETE FROM category_budgets WHERE category = ?", (category,))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.get("/api/budgets/status")
def budget_status(month: str = Query(None)):
    conn = get_db()
    if not month:
        row = conn.execute(
            "SELECT MAX(substr(date,1,7)) FROM transactions WHERE date != '0000-00-00'"
        ).fetchone()
        month = row[0]
    if not month:
        conn.close()
        return {"month": None, "categories": []}

    budgets = {r["category"]: r["monthly_budget"] for r in
               conn.execute("SELECT category, monthly_budget FROM category_budgets").fetchall()}

    spending = {r["category"]: abs(r["total"]) for r in conn.execute("""
        SELECT category, SUM(amount) AS total
        FROM transactions
        WHERE date LIKE ? AND amount < 0 AND category != 'Überweisung'
        GROUP BY category
    """, [f"{month}%"]).fetchall()}

    conn.close()

    result = []
    seen = set()
    for cat, budget in sorted(budgets.items()):
        spent = spending.get(cat, 0)
        result.append({
            "category": cat,
            "budget": budget,
            "spent": round(spent, 2),
            "pct": round(spent / budget * 100, 1) if budget > 0 else 0,
        })
        seen.add(cat)
    for cat, spent in sorted(spending.items()):
        if cat not in seen:
            result.append({"category": cat, "budget": None, "spent": round(spent, 2), "pct": None})

    return {"month": month, "categories": result}


# ─── Settings ─────────────────────────────────────────────────────────────────

@app.get("/api/settings/{key}")
def get_setting(key: str):
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    return {"value": row["value"] if row else None}


@app.put("/api/settings/{key}")
def set_setting(key: str, body: dict):
    value = str(body.get("value", ""))
    conn = get_db()
    conn.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value)
    )
    conn.commit()
    conn.close()
    return {"ok": True}


# ─── Import suggestions ────────────────────────────────────────────────────────

@app.get("/api/import/suggestions")
def import_suggestions():
    """Return merchants frequently corrected to same category, without an existing rule."""
    conn = get_db()
    corrections = conn.execute(
        "SELECT merchant_name, category, count FROM category_corrections WHERE count >= 2 ORDER BY count DESC"
    ).fetchall()
    rules = [r["pattern"] for r in conn.execute("SELECT pattern FROM category_rules").fetchall()]
    conn.close()

    suggestions = []
    seen_merchants = set()
    for row in corrections:
        merchant = row["merchant_name"]
        if merchant in seen_merchants:
            continue
        seen_merchants.add(merchant)
        already_covered = any(rule in merchant.lower() for rule in rules)
        if not already_covered:
            suggestions.append({
                "merchant_name": merchant,
                "suggested_category": row["category"],
                "count": row["count"],
            })
    return suggestions


@app.get("/api/months")
def list_months():
    conn = get_db()
    rows = conn.execute("""
        SELECT DISTINCT substr(date, 1, 7) AS month
        FROM transactions
        WHERE date != '0000-00-00'
        ORDER BY month DESC
    """).fetchall()
    conn.close()
    return [r["month"] for r in rows]


# ─── Export & Backup ───────────────────────────────────────────────────────────

@app.get("/api/export/csv")
def export_csv(month: str = Query(None)):
    conn = get_db()
    where = "WHERE date LIKE ?" if month else ""
    params = [f"{month}%"] if month else []
    rows = conn.execute(
        f"SELECT date, merchant_name, description, category, amount, source, account_name, note FROM transactions {where} ORDER BY date DESC",
        params
    ).fetchall()
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["Datum", "Händler", "Beschreibung", "Kategorie", "Betrag", "Konto", "Quelle", "Notiz"])
    for r in rows:
        writer.writerow([r["date"], r["merchant_name"] or "", r["description"] or "",
                         r["category"], r["amount"], r["account_name"], r["source"], r["note"] or ""])

    filename = f"haushaltsbuch_{month or 'gesamt'}.csv"
    return StreamingResponse(
        iter([output.getvalue().encode("utf-8-sig")]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/backup")
def backup_db():
    with open(DB_PATH, "rb") as f:
        data = f.read()
    return StreamingResponse(
        iter([data]),
        media_type="application/octet-stream",
        headers={"Content-Disposition": 'attachment; filename="haushaltsbuch_backup.db"'},
    )


@app.post("/api/restore")
async def restore_db(file: UploadFile = File(...)):
    content = await file.read()
    if not content.startswith(b"SQLite format 3"):
        raise HTTPException(400, "Ungültige SQLite-Datei")
    with open(DB_PATH, "wb") as f:
        f.write(content)
    return {"ok": True}
