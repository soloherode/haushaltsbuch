import csv
import hashlib
import io
import os
import re
import sqlite3
import math
from datetime import date, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Request, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool

from app import analytics, auth, periods
from app.database import init_db, get_db, DB_PATH, VALID_KINDS
from app.categories import CATEGORIES
from app.parsers.comdirect import parse_comdirect_csv
from app.parsers.hanseaticbank import parse_hanseaticbank_pdf


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Haushaltsbuch", lifespan=lifespan)

# index.html sind ~100 KB unkomprimiert, gzipped ~22 KB. Über WLAN vom Pi
# ist das der größte Einzelgewinn beim Seitenaufbau.
app.add_middleware(GZipMiddleware, minimum_size=1000)

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

_PAGE_CACHE: dict[str, str] = {}


def _page(name: str) -> str:
    # Einmal von der SD-Karte lesen, danach aus dem RAM.
    if name not in _PAGE_CACHE:
        with open(os.path.join(STATIC_DIR, name), encoding="utf-8") as f:
            _PAGE_CACHE[name] = f.read()
    return _PAGE_CACHE[name]


# ─── Authentifizierung ─────────────────────────────────────────────────────────

# Ohne Session erreichbar: die Login-Seite selbst, die Auth-Endpoints und die
# statischen Assets (Fonts/CSS, die die Login-Seite braucht – nichts Sensibles).
PUBLIC_PATHS = {"/login", "/api/auth/status", "/api/auth/login", "/api/auth/setup"}
PUBLIC_PREFIXES = ("/static/",)


@app.middleware("http")
async def require_login(request: Request, call_next):
    path = request.url.path
    if path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES):
        return await call_next(request)

    token = request.cookies.get(auth.COOKIE_NAME)
    # sqlite blockiert – nicht im Event-Loop ausführen.
    if await run_in_threadpool(auth.validate_session, token):
        return await call_next(request)

    if path.startswith("/api/"):
        return JSONResponse({"detail": "Nicht angemeldet"}, status_code=401)
    return RedirectResponse("/login", status_code=302)


def _client_id(request: Request) -> str:
    return request.client.host if request.client else "unbekannt"


def _set_session_cookie(response: Response, token: str, max_age: int) -> None:
    response.set_cookie(
        auth.COOKIE_NAME,
        token,
        max_age=max_age,
        httponly=True,          # kein Zugriff aus JavaScript
        samesite="lax",         # blockt Cookies bei Cross-Site-POSTs → CSRF-Schutz
        secure=False,           # das Heimnetz läuft über HTTP; mit HTTPS auf True setzen
        path="/",
    )


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if auth.validate_session(request.cookies.get(auth.COOKIE_NAME)):
        return RedirectResponse("/", status_code=302)
    return HTMLResponse(_page("login.html"))


@app.get("/api/auth/status")
def auth_status(request: Request):
    return {
        "configured": auth.is_configured(),
        "authenticated": auth.validate_session(request.cookies.get(auth.COOKIE_NAME)),
    }


@app.post("/api/auth/setup")
def auth_setup(body: dict, response: Response):
    """Erstmaliges Setzen des Passworts. Danach dauerhaft gesperrt."""
    if auth.is_configured():
        raise HTTPException(409, "Es ist bereits ein Passwort gesetzt")
    password = str(body.get("password", ""))
    if len(password) < auth.MIN_PASSWORD_LENGTH:
        raise HTTPException(400, f"Passwort muss mindestens {auth.MIN_PASSWORD_LENGTH} Zeichen haben")
    conn = get_db()
    try:
        auth.set_password(conn, password)
        conn.commit()
    finally:
        conn.close()
    token, max_age = auth.create_session(remember=False)
    _set_session_cookie(response, token, max_age)
    return {"ok": True}


@app.post("/api/auth/login")
def auth_login(body: dict, request: Request, response: Response):
    client = _client_id(request)
    blocked_for = auth.login_blocked(client)
    if blocked_for:
        raise HTTPException(429, f"Zu viele Fehlversuche. Bitte {blocked_for // 60 + 1} Minuten warten.")

    conn = get_db()
    try:
        stored = auth.get_password_hash(conn)
    finally:
        conn.close()
    if not stored:
        raise HTTPException(409, "Es ist noch kein Passwort gesetzt")

    if not auth.verify_password(str(body.get("password", "")), stored):
        auth.record_failed_login(client)
        raise HTTPException(401, "Falsches Passwort")

    auth.reset_failed_logins(client)
    token, max_age = auth.create_session(remember=bool(body.get("remember")))
    _set_session_cookie(response, token, max_age)
    return {"ok": True}


@app.post("/api/auth/logout")
def auth_logout(request: Request, response: Response):
    auth.delete_session(request.cookies.get(auth.COOKIE_NAME))
    response.delete_cookie(auth.COOKIE_NAME, path="/")
    return {"ok": True}


@app.post("/api/auth/change-password")
def auth_change_password(body: dict, response: Response):
    new = str(body.get("new_password", ""))
    if len(new) < auth.MIN_PASSWORD_LENGTH:
        raise HTTPException(400, f"Passwort muss mindestens {auth.MIN_PASSWORD_LENGTH} Zeichen haben")
    conn = get_db()
    try:
        stored = auth.get_password_hash(conn)
        if not stored or not auth.verify_password(str(body.get("current_password", "")), stored):
            raise HTTPException(401, "Aktuelles Passwort ist falsch")
        auth.set_password(conn, new)
        conn.commit()
    finally:
        conn.close()
    # Alle Geräte abmelden – auch das hier gerade benutzte.
    auth.delete_all_sessions()
    response.delete_cookie(auth.COOKIE_NAME, path="/")
    return {"ok": True}


# ─── Root ──────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def root():
    return _page("index.html")


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
    inserted = skipped = reconciled = 0
    warnings = []
    try:
        for original in transactions:
            t = dict(original)
            t["category_origin"] = "parser"
            # Rules are explicit. Merchant-only historical corrections remain
            # suggestions: the same recipient may be rent, saving or groceries.
            rule_cat = _apply_db_rules(t.get("description", ""), t.get("merchant_name", ""), conn)
            if rule_cat and t["category"] != "Überweisung":
                t.update(category=rule_cat, category_origin="rule")
            elif t["category"] != "Überweisung":
                learned = conn.execute("""SELECT category FROM contextual_corrections
                    WHERE source = ? AND account_name = ? AND merchant_name = ?
                    AND description = ? AND direction = ?""",
                    (t["source"], t["account_name"], t["merchant_name"] or "", t["description"] or "",
                     1 if t["amount"] > 0 else -1)).fetchone()
                if learned:
                    t.update(category=learned["category"], category_origin="learned_context")
            if conn.execute("SELECT 1 FROM transactions WHERE import_hash = ?", (t["import_hash"],)).fetchone():
                skipped += 1
                continue
            candidates = conn.execute("""
                SELECT * FROM transactions WHERE source = ? AND account_name = ?
                AND amount = ? AND description = ? AND booked != ?
                AND transaction_date IS NOT NULL AND transaction_date = ?
                AND ABS(julianday(date) - julianday(?)) <= 7
            """, (t["source"], t["account_name"], t["amount"], t["description"],
                  t["booked"], t["transaction_date"], t["date"])).fetchall()
            if len(candidates) == 1:
                old = candidates[0]
                if not t["booked"]:
                    skipped += 1  # A stale export must not revive a pending row.
                    continue
                conn.execute("""UPDATE transactions SET date = ?, transaction_date = ?,
                    booked = 1, import_hash = ?, transaction_type = ? WHERE id = ?""",
                    (t["date"], t["transaction_date"], t["import_hash"], t["transaction_type"], old["id"]))
                reconciled += 1
                continue
            if len(candidates) > 1:
                warnings.append("Mehrdeutige Vormerkungen: manueller Abgleich erforderlich")
            conn.execute("""
                INSERT INTO transactions
                    (source, account_name, date, transaction_date, amount, description,
                     merchant_name, category, subcategory, city, country,
                     transaction_type, booked, import_hash, category_origin)
                VALUES (:source, :account_name, :date, :transaction_date, :amount, :description,
                        :merchant_name, :category, :subcategory, :city, :country,
                        :transaction_type, :booked, :import_hash, :category_origin)
            """, t)
            inserted += 1
        report = getattr(transactions, "report", None)
        if transactions and report and report.get("balance_verified") and report.get("period_start"):
            conn.execute("INSERT OR IGNORE INTO import_coverage VALUES (?, ?, ?, ?)",
                         (transactions[0]["source"], transactions[0]["account_name"],
                          report["period_start"], report["period_end"]))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"inserted": inserted, "skipped_duplicates": skipped, "reconciled": reconciled,
            "unclassified": sum(t["category"] == "Sonstiges" for t in transactions),
            "warnings": sorted(set(warnings)), "statement": getattr(transactions, "report", None)}


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
        transactions = parse_hanseaticbank_pdf(content)
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
    period: str = Query(None),
    search: str = Query(None),
    sort: str = Query("date"),
    dir: str = Query("desc"),
    only_outliers: bool = Query(False),
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
    if period is not None or month:
        p = _resolve_period(conn, period, month)
        if p.bounded:
            conditions.append("date >= ? AND date < ?")
            params += [p.start, p.end]
    if search:
        conditions.append("(description LIKE ? OR merchant_name LIKE ?)")
        params += [f"%{search}%", f"%{search}%"]

    thresholds, recurring = _outlier_context(conn)
    if only_outliers:
        cond, cond_params = _outlier_sql(thresholds, recurring)
        conditions.append(cond if cond else "0")
        params += cond_params

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    offset = (page - 1) * page_size

    # `t` als Alias, weil _outlier_sql die Spalten so anspricht.
    total = conn.execute(f"SELECT COUNT(*) FROM transactions t {where}", params).fetchone()[0]
    sum_row = conn.execute(
        f"SELECT SUM(t.amount) AS total_sum FROM transactions t {where}", params
    ).fetchone()
    rows = conn.execute(
        f"SELECT t.* FROM transactions t {where} ORDER BY {order} LIMIT ? OFFSET ?",
        params + [page_size, offset]
    ).fetchall()

    conn.close()

    out = []
    for r in rows:
        d = dict(r)
        d["is_outlier"] = _is_outlier(r, thresholds, recurring)
        d["outlier_threshold"] = thresholds.get(d["category"]) if d["is_outlier"] else None
        out.append(d)

    return {
        "total": total,
        "total_sum": round(sum_row["total_sum"] or 0, 2),
        "page": page,
        "page_size": page_size,
        "transactions": out,
    }


@app.put("/api/transactions/{tx_id}/category")
def update_category(tx_id: int, body: dict):
    category = body.get("category", "").strip()
    conn = get_db()
    if category not in _all_categories(conn):
        conn.close()
        raise HTTPException(400, f"Unbekannte Kategorie: {category}")
    # Track correction for smart categorization
    tx = conn.execute("SELECT * FROM transactions WHERE id = ?", (tx_id,)).fetchone()
    if tx and tx["merchant_name"]:
        conn.execute("""INSERT INTO contextual_corrections VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, account_name, merchant_name, description, direction)
            DO UPDATE SET category = excluded.category""",
            (tx["source"], tx["account_name"], tx["merchant_name"], tx["description"] or "",
             1 if tx["amount"] > 0 else -1, category))
        conn.execute("""
            INSERT INTO category_corrections (merchant_name, category, count) VALUES (?, ?, 1)
            ON CONFLICT(merchant_name, category) DO UPDATE SET count = count + 1
        """, (tx["merchant_name"], category))
    conn.execute("UPDATE transactions SET category = ?, category_locked = 1, category_origin = 'manual' WHERE id = ?", (category, tx_id))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.put("/api/transactions/{tx_id}/date")
def update_date(tx_id: int, body: dict):
    date = body.get("date", "").strip()
    try:
        from datetime import date as calendar_date
        calendar_date.fromisoformat(date)
    except ValueError:
        raise HTTPException(400, "Ungültiges Datum; erwartet YYYY-MM-DD")
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
        f"UPDATE transactions SET category = ?, category_locked = 1, category_origin = 'manual' WHERE id IN ({placeholders})",
        [category] + list(ids)
    )
    conn.commit()
    conn.close()
    return {"ok": True, "updated": len(ids)}


@app.post("/api/transactions")
def create_transaction(body: dict):
    date = body.get("date", "").strip()
    try:
        from datetime import date as calendar_date
        calendar_date.fromisoformat(date)
    except ValueError:
        raise HTTPException(400, "Ungültiges Datum; erwartet YYYY-MM-DD")
    try:
        amount = float(body.get("amount", 0))
    except (ValueError, TypeError):
        raise HTTPException(400, "Ungültiger Betrag")
    if not math.isfinite(amount):
        raise HTTPException(400, "Betrag muss endlich sein")
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
        conn.execute("UPDATE transactions SET category_origin = 'manual', category_locked = 1 WHERE id = last_insert_rowid()")
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

    rows = conn.execute("SELECT id, description, merchant_name FROM transactions WHERE category_locked = 0 AND category_origin != 'legacy'").fetchall()
    updated = 0
    for row in rows:
        text = ((row["description"] or "") + " " + (row["merchant_name"] or "")).lower()
        for rule in rules:
            if rule["pattern"] in text:
                conn.execute("UPDATE transactions SET category = ?, category_origin = 'rule' WHERE id = ?", (rule["category"], row["id"]))
                updated += 1
                break
    conn.commit()
    conn.close()
    return {"updated": updated}


# ─── Stats ─────────────────────────────────────────────────────────────────────

# Alle Auswertungen laufen über diesen Join, damit die Art einer Kategorie
# (Konsum / Sparen / Umbuchung / Einkommen) überall gleich behandelt wird.
TX = "transactions t LEFT JOIN categories c ON c.name = t.category"
KIND = "COALESCE(c.kind, 'consumption')"


def _data_quality(conn):
    accounts = [dict(r) for r in conn.execute("""SELECT source, account_name,
        MAX(CASE WHEN booked = 1 AND date != '0000-00-00' THEN date END) AS last_booking,
        SUM(CASE WHEN booked = 0 THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN category = 'Sonstiges' THEN 1 ELSE 0 END) AS unclassified
        FROM transactions GROUP BY source, account_name""")]
    for account in accounts:
        account["confirmed_through"] = conn.execute(
            "SELECT MAX(end_date) FROM import_coverage WHERE source = ? AND account_name = ?",
            (account["source"], account["account_name"])).fetchone()[0]
    warnings = []
    cutoff = (date.today() - timedelta(days=3)).isoformat()
    stale = [a for a in accounts if a["source"] != "manual" and
             max(a["last_booking"] or "", a["confirmed_through"] or "") < cutoff]
    if stale:
        warnings.append("Nicht alle Konten haben aktuelle Buchungen. Prognosen sind bis zum nächsten Import ausgesetzt.")
    if any(a["pending"] for a in accounts):
        warnings.append("Vormerkungen sind separat erfasst und nicht in den Ist-Summen enthalten.")
    if any(a["unclassified"] for a in accounts):
        warnings.append("Ungeklärte Kategorien: Bitte Sonstiges prüfen.")
    suspicious = conn.execute("SELECT COUNT(*) FROM categories WHERE name IN ('Einkommen', 'Sparen & Investieren', 'Überweisung') AND kind = 'consumption'").fetchone()[0]
    if suspicious:
        warnings.append("Kategoriearten prüfen: Einkommen, Sparen oder Umbuchung ist als Konsum eingestellt.")
    return {"accounts": accounts, "warnings": warnings,
            "forecast_ready": bool(accounts) and not stale and not suspicious,
            "coverage_note": "Letzte Buchung ist kein Nachweis vollständiger Kontoabdeckung."}


@app.get("/api/import/coverage")
def import_coverage():
    conn = get_db()
    try:
        return {"accounts": _data_quality(conn)["accounts"],
                "periods": [dict(r) for r in conn.execute("SELECT * FROM import_coverage ORDER BY source, account_name, start_date")]}
    finally:
        conn.close()


@app.post("/api/import/coverage")
def confirm_import_coverage(body: dict):
    try:
        p = periods.custom_period(str(body.get("start", "")), str(body.get("end", "")))
    except periods.PeriodError as exc:
        raise HTTPException(400, str(exc))
    if body["end"] > date.today().isoformat():
        raise HTTPException(400, "Nur bereits abgeschlossene Tage bestätigen")
    conn = get_db()
    try:
        key = (body.get("source"), body.get("account_name"))
        if not conn.execute("SELECT 1 FROM transactions WHERE source = ? AND account_name = ?", key).fetchone():
            raise HTTPException(400, "Unbekanntes Konto")
        if body.get("remove"):
            conn.execute("DELETE FROM import_coverage WHERE source = ? AND account_name = ? AND start_date = ? AND end_date = ?", (*key,p.start,body["end"]))
        else:
            conn.execute("INSERT OR IGNORE INTO import_coverage VALUES (?, ?, ?, ?)",(*key,p.start,body["end"]))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@app.get("/api/stats/forecast-validation")
def forecast_validation():
    from app.forecast_validation import evaluate
    conn = get_db()
    try:
        return evaluate(conn)
    finally:
        conn.close()


@app.get("/api/stats/data-quality")
def data_quality():
    conn = get_db()
    try:
        return _data_quality(conn)
    finally:
        conn.close()


def _observed_months(conn, p=periods.ALL):
    """Zero category spend is valid only in months with account observations.

    Missing entire months stay unknown; calendar_trend preserves their spacing.
    """
    where, params = _where(p)
    return [r[0] for r in conn.execute(
        f"SELECT DISTINCT substr(t.date, 1, 7) FROM transactions t {where} ORDER BY 1", params)]


def _latest_month(conn) -> str | None:
    row = conn.execute(
        "SELECT MAX(substr(date, 1, 7)) FROM transactions WHERE date != '0000-00-00' AND booked = 1"
    ).fetchone()
    return row[0]


def _resolve_period(conn, period: str | None, month: str | None = None) -> periods.Period:
    """`period=` ist der neue Weg, `month=` bleibt rückwärtskompatibel erhalten."""
    try:
        return periods.parse(period if period is not None else month, _latest_month(conn))
    except periods.PeriodError as exc:
        raise HTTPException(400, str(exc))


def _where(period: periods.Period, *extra: str) -> tuple[str, list]:
    frag, params = period.where("t.date")
    clauses = [frag, "t.booked = 1", *[e for e in extra if e]]
    return "WHERE " + " AND ".join(clauses), params


def _period_info(p: periods.Period) -> dict:
    return {"label": p.label, "kind": p.kind, "start": p.start, "end": p.end,
            "months": len(p.months()) or None}


# ─── Ausreißer ─────────────────────────────────────────────────────────────────

def _outlier_context(conn) -> tuple[dict[str, float], set[str]]:
    """(Schwelle je Kategorie, wiederkehrende Händler) über die gesamte Historie.

    Bewusst über alle Daten und nicht nur über den gewählten Zeitraum – sonst
    verschiebt sich die Schwelle, sobald man den Zeitraum wechselt.
    """
    rows = conn.execute(f"""
        SELECT t.category AS category, t.amount AS amount,
               t.date AS date, t.merchant_name AS merchant_name
        FROM {TX}
        WHERE t.booked = 1 AND t.amount < 0 AND t.date != '0000-00-00' AND {KIND} = 'consumption'
    """).fetchall()
    by_cat: dict[str, list[float]] = {}
    for r in rows:
        by_cat.setdefault(r["category"], []).append(r["amount"])
    return analytics.outlier_thresholds(by_cat), analytics.recurring_merchants(rows)


def _is_outlier(row, thresholds: dict[str, float], recurring: set[str]) -> bool:
    if (row["amount"] or 0) >= 0 or ("booked" in row.keys() and not row["booked"]):
        return False
    # Vormerkposten ohne Datum lassen sich zeitlich nicht einordnen.
    if not row["date"] or row["date"] == "0000-00-00":
        return False
    if row["merchant_name"] in recurring:
        return False
    thr = thresholds.get(row["category"])
    return bool(thr and abs(row["amount"]) >= thr)


def _outlier_sql(thresholds: dict[str, float], recurring: set[str]) -> tuple[str, list]:
    """SQL-Bedingung, die genau die Ausreißer trifft (für Filtern und Ausblenden)."""
    if not thresholds:
        return "", []
    parts, params = [], []
    for cat, thr in thresholds.items():
        parts.append("(t.category = ? AND ABS(t.amount) >= ?)")
        params += [cat, thr]
    cond = "(" + " OR ".join(parts) + ")"
    if recurring:
        placeholders = ",".join("?" * len(recurring))
        cond = f"(COALESCE(t.merchant_name, '') NOT IN ({placeholders}) AND {cond})"
        params = list(recurring) + params
    # gleiche Bedingung wie in _is_outlier, damit Liste und Statistik übereinstimmen
    return f"(t.booked = 1 AND t.amount < 0 AND t.date != '0000-00-00' AND {cond})", params


def _exclude_outliers(thresholds: dict[str, float], recurring: set[str]) -> tuple[str, list]:
    cond, params = _outlier_sql(thresholds, recurring)
    return (f"NOT {cond}", params) if cond else ("", [])


@app.get("/api/stats/outliers")
def stats_outliers(period: str = Query(None), month: str = Query(None)):
    """Ungewöhnlich große Einzelausgaben je Kategorie."""
    conn = get_db()
    p = _resolve_period(conn, period, month)
    thresholds, recurring = _outlier_context(conn)
    where, params = _where(p, f"{KIND} = 'consumption'")
    rows = conn.execute(f"""
        SELECT t.id, t.date, t.amount, t.description, t.merchant_name, t.category
        FROM {TX} {where}
        ORDER BY t.date DESC
    """, params).fetchall()
    conn.close()

    out = []
    for r in rows:
        if not _is_outlier(r, thresholds, recurring):
            continue
        thr = thresholds[r["category"]]
        out.append({**dict(r), "threshold": thr,
                    "factor": round(abs(r["amount"]) / thr, 1)})
    out.sort(key=lambda x: abs(x["amount"]), reverse=True)
    return {
        "period": _period_info(p),
        "thresholds": thresholds,
        "recurring_ignored": len(recurring),
        "total": round(sum(abs(o["amount"]) for o in out), 2),
        "outliers": out,
    }


# ─── Übersicht ─────────────────────────────────────────────────────────────────

@app.get("/api/stats/summary")
def stats_summary(period: str = Query(None), month: str = Query(None)):
    conn = get_db()
    p = _resolve_period(conn, period, month)
    where, params = _where(p)
    rows = conn.execute(f"""
        SELECT {KIND} AS kind,
               SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END) AS inflow,
               SUM(CASE WHEN t.amount < 0 THEN -t.amount ELSE 0 END) AS outflow,
               COUNT(*) AS n
        FROM {TX} {where}
        GROUP BY kind
    """, params).fetchall()
    # Für Monatsschnitte zählen nur Monate, in denen es überhaupt Buchungen gibt –
    # sonst rechnet ein angebrochenes Jahr durch zwölf und sieht zu günstig aus.
    active = conn.execute(f"""
        SELECT COUNT(DISTINCT substr(t.date, 1, 7)) FROM {TX} {where}
    """, params).fetchone()[0]
    quality = _data_quality(conn)
    fraction = periods.elapsed_fraction(p) if quality["forecast_ready"] else None
    fixed_ctx = _fixed_forecast_context(conn, p, fraction)
    conn.close()

    agg = {r["kind"]: r for r in rows}

    def io(kind: str) -> tuple[float, float]:
        r = agg.get(kind)
        return ((r["inflow"] or 0.0), (r["outflow"] or 0.0)) if r else (0.0, 0.0)

    inc_in, inc_out = io("income")
    con_in, con_out = io("consumption")
    sav_in, sav_out = io("savings")
    tr_in, tr_out = io("transfer")

    income = inc_in - inc_out         # Rückbuchungen mindern die Einnahmen
    consumption = con_out - con_in    # Erstattungen mindern den Konsum
    savings = sav_out - sav_in        # Entnahmen mindern die Sparrate
    count = sum((r["n"] or 0) for k, r in agg.items() if k != "transfer")

    return {
        "income":       round(income, 2),
        "consumption":  round(consumption, 2),
        "savings":      round(savings, 2),
        "expenses":     round(consumption + savings, 2),   # gesamter Abfluss
        "balance":      round(income - consumption - savings, 2),
        "savings_rate": round(savings / income * 100, 1) if income > 0 else 0.0,
        "surplus_rate": round((income - consumption) / income * 100, 1) if income > 0 else None,
        "transfers":    round(tr_in + tr_out, 2),
        "count":        count,
        "active_months": active,
        "data_quality": quality,
        "period":       _period_info(p),
        "forecast":     _forecast(fraction, income, consumption, savings, fixed_ctx),
    }


def _forecast(fraction: float | None, income: float, consumption: float, savings: float,
              fixed_ctx: dict | None) -> dict | None:
    """Hochrechnung mit diskreten Terminen für Fixkosten, Einkommen und Sparen.

    Konsum ist die einzige Größe, die sich sinnvoll hochrechnen lässt: viele
    kleine, echt variable Buchungen (Tanken, Einkaufen) verteilen sich übers
    ganze Monat, bekannte Fixkosten fließen mit ihrem tatsächlich erwarteten
    Betrag ein statt hochskaliert zu werden (siehe `_fixed_forecast_context`) –
    sonst würde eine Miete, die gerade eben gebucht wurde, mit dem vollen
    Monatsfaktor multipliziert.

    Bekannte Einnahmen und Sparraten werden pro Fälligkeit ergänzt. Nur der
    variable Konsum wird zeitanteilig hochgerechnet.
    """
    if not fraction or fraction >= 1:
        return None
    actual_fixed = fixed_ctx["actual_by_kind"] if fixed_ctx else {}
    projected_fixed = fixed_ctx["projected_by_kind"] if fixed_ctx else {}

    variable_consumption = consumption - actual_fixed.get("consumption", 0.0)
    f_consumption = projected_fixed.get("consumption", 0.0) + max(variable_consumption, 0) / fraction + min(variable_consumption, 0)
    f_income = income + projected_fixed.get("income", 0) - actual_fixed.get("income", 0)
    f_savings = savings + projected_fixed.get("savings", 0) - actual_fixed.get("savings", 0)
    return {
        "income": round(f_income, 2), "savings": round(f_savings, 2),
        "elapsed_fraction": round(fraction, 3),
        "consumption": round(f_consumption, 2),
        "balance":     round(f_income - f_consumption - f_savings, 2),
    }


def _fixed_forecast_context(conn, p: periods.Period, fraction: float | None, as_of: str | None = None) -> dict | None:
    """Add every unmatched scheduled occurrence, across all months in a period."""
    if not fraction or fraction >= 1:
        return None
    as_of = as_of or date.today().isoformat()
    items = [r for r in _recurring_items(conn, as_of=as_of) if r["type"] == "fix"
             and r["active"] and r["interval_months"]]
    actual_cat, actual_kind, projected_cat, projected_kind = {}, {}, {}, {}
    pending = []
    for info in items:
        cat, kind = info["category"], info["kind"]
        rows = conn.execute("""SELECT date, amount FROM transactions
            WHERE source = ? AND account_name = ? AND merchant_name = ? AND category = ?
            AND booked = 1 AND date >= ? AND date < ? AND date <= ?""",
            (info["source"], info["account_name"], info["merchant_name"], cat, p.start, p.end, as_of)).fetchall()
        sign = 1 if kind == "income" else -1
        actual = sum(sign * r["amount"] for r in rows)
        actual_cat[cat] = actual_cat.get(cat, 0) + actual
        actual_kind[kind] = actual_kind.get(kind, 0) + actual
        expected_total = actual
        anchor_y, anchor_m = int(info["last_date"][:4]), int(info["last_date"][5:7])
        for month in p.months():
            y, m = int(month[:4]), int(month[5:7])
            if ((y - anchor_y) * 12 + m - anchor_m) % info["interval_months"]:
                continue
            expected = periods._day_in_month(y, m, info["typical_day"])
            if not p.start <= expected < p.end or expected < info["first_date"]:
                continue
            if any(r["date"][:7] == month and sign * r["amount"] > 0 for r in rows):
                continue
            expected_total += info["last_amount"]
            pending.append({"merchant_name": info["merchant_name"], "category": cat,
                            "amount": info["last_amount"], "expected": expected})
        projected_cat[cat] = projected_cat.get(cat, 0) + expected_total
        projected_kind[kind] = projected_kind.get(kind, 0) + expected_total
    return {"actual_by_category": actual_cat, "actual_by_kind": actual_kind,
            "projected_by_category": projected_cat, "projected_by_kind": projected_kind,
            "pending": pending}


@app.get("/api/stats/categories")
def stats_categories(
    period: str = Query(None),
    month: str = Query(None),
    type: str = Query("expense"),
    kind: str = Query(None),
    exclude_outliers: bool = Query(False),
):
    """Summen je Kategorie. `kind` filtert auf Konsum/Sparen/Einkommen."""
    conn = get_db()
    p = _resolve_period(conn, period, month)

    excl_sql, excl_params = "", []
    if exclude_outliers:
        excl_sql, excl_params = _exclude_outliers(*_outlier_context(conn))

    where, params = _where(p, excl_sql)
    rows = conn.execute(f"""
        SELECT t.category AS category, {KIND} AS kind,
               SUM(t.amount) AS total, COUNT(*) AS count
        FROM {TX} {where}
        GROUP BY t.category, kind
    """, params + excl_params).fetchall()
    conn.close()

    wanted = kind or ("income" if type == "income" else "consumption")
    out = []
    for r in rows:
        if r["kind"] == "transfer":
            continue
        if wanted != "all" and r["kind"] != wanted:
            continue
        total = r["total"] or 0
        # Einnahmen sind positiv, Ausgaben negativ – Kategorien, deren Saldo in
        # die falsche Richtung zeigt (z. B. reine Erstattungen), fallen raus.
        if type == "income" and total <= 0:
            continue
        if type != "income" and total >= 0:
            continue
        out.append({"category": r["category"], "kind": r["kind"],
                    "total": round(total, 2), "count": r["count"]})

    out.sort(key=lambda x: x["total"], reverse=(type == "income"))
    return out


@app.get("/api/stats/monthly")
def stats_monthly(period: str = Query(None), limit: int = Query(24, ge=1, le=120)):
    """Monatsverlauf mit getrenntem Konsum und Sparen."""
    conn = get_db()
    p = _resolve_period(conn, period)
    where, params = _where(p)
    rows = conn.execute(f"""
        SELECT substr(t.date, 1, 7) AS month, {KIND} AS kind,
               SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END) AS inflow,
               SUM(CASE WHEN t.amount < 0 THEN -t.amount ELSE 0 END) AS outflow
        FROM {TX} {where}
        GROUP BY month, kind
        ORDER BY month
    """, params).fetchall()
    conn.close()

    by_month: dict[str, dict[str, float]] = {}
    for r in rows:
        m = by_month.setdefault(r["month"], {"income": 0.0, "consumption": 0.0, "savings": 0.0})
        if r["kind"] == "income":
            m["income"] += (r["inflow"] or 0) - (r["outflow"] or 0)
        elif r["kind"] == "consumption":
            m["consumption"] += (r["outflow"] or 0) - (r["inflow"] or 0)
        elif r["kind"] == "savings":
            m["savings"] += (r["outflow"] or 0) - (r["inflow"] or 0)

    months = sorted(by_month)[-limit:]
    return [{
        "month": m,
        "income": round(by_month[m]["income"], 2),
        "consumption": round(by_month[m]["consumption"], 2),
        "savings": round(by_month[m]["savings"], 2),
        "expenses": round(by_month[m]["consumption"] + by_month[m]["savings"], 2),
    } for m in months]


@app.get("/api/stats/timeline")
def stats_timeline(
    category: str = Query(None),
    period: str = Query(None),
    exclude_outliers: bool = Query(False),
    window: int = Query(3, ge=2, le=12),
):
    """Monatsverlauf einer Kategorie inklusive gleitendem Durchschnitt."""
    conn = get_db()
    p = _resolve_period(conn, period)

    excl_sql, excl_params = "", []
    if exclude_outliers:
        excl_sql, excl_params = _exclude_outliers(*_outlier_context(conn))

    cat_sql, cat_params = ("t.category = ?", [category]) if category else ("", [])
    where, params = _where(p, f"{KIND} = 'consumption'", cat_sql, excl_sql)
    rows = conn.execute(f"""
        SELECT substr(t.date, 1, 7) AS month, SUM(-t.amount) AS total
        FROM {TX} {where}
        GROUP BY month ORDER BY month
    """, params + cat_params + excl_params).fetchall()
    months = _observed_months(conn, p)
    conn.close()
    by_month = {r["month"]: r["total"] or 0 for r in rows}
    totals = [by_month.get(m, 0) for m in months]
    avg = analytics.moving_average(totals, window)
    return {"category": category, "median": round(analytics.median(totals), 2),
            "trend_per_month": round(analytics.calendar_trend(months, totals), 2),
            "window": window,
            "points": [{"month": m, "total": round(t, 2), "moving_avg": round(a, 2)}
                       for m, t, a in zip(months, totals, avg)]}


@app.get("/api/stats/yearly")
def stats_yearly(year: str = Query(None), period: str = Query(None)):
    """Monatsübersicht innerhalb eines Jahres bzw. Zeitraums."""
    conn = get_db()
    p = _resolve_period(conn, period or (year if year else None))
    where, params = _where(p)
    rows = conn.execute(f"""
        SELECT substr(t.date, 1, 7) AS month, {KIND} AS kind,
               SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END) AS inflow,
               SUM(CASE WHEN t.amount < 0 THEN -t.amount ELSE 0 END) AS outflow
        FROM {TX} {where}
        GROUP BY month, kind ORDER BY month
    """, params).fetchall()
    conn.close()

    by_month: dict[str, dict[str, float]] = {}
    for r in rows:
        m = by_month.setdefault(r["month"], {"income": 0.0, "consumption": 0.0, "savings": 0.0})
        if r["kind"] == "income":
            m["income"] += (r["inflow"] or 0) - (r["outflow"] or 0)
        elif r["kind"] == "consumption":
            m["consumption"] += (r["outflow"] or 0) - (r["inflow"] or 0)
        elif r["kind"] == "savings":
            m["savings"] += (r["outflow"] or 0) - (r["inflow"] or 0)

    result = []
    for m in sorted(by_month):
        v = by_month[m]
        inc, con, sav = v["income"], v["consumption"], v["savings"]
        result.append({
            "month": m,
            "income": round(inc, 2),
            "consumption": round(con, 2),
            "expenses": round(con + sav, 2),
            "savings": round(sav, 2),
            "balance": round(inc - con - sav, 2),
            "savings_rate": round(sav / inc * 100, 1) if inc > 0 else 0.0,
        })
    return result


@app.get("/api/stats/wealth")
def stats_wealth(period: str = Query(None)):
    """Kumulierter Vermögensaufbau: Sparsumme über die Zeit plus Sparquote."""
    conn = get_db()
    p = _resolve_period(conn, period)
    where, params = _where(p)
    rows = conn.execute(f"""
        SELECT substr(t.date, 1, 7) AS month, {KIND} AS kind,
               SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END) AS inflow,
               SUM(CASE WHEN t.amount < 0 THEN -t.amount ELSE 0 END) AS outflow
        FROM {TX} {where}
        GROUP BY month, kind ORDER BY month
    """, params).fetchall()
    goal_row = conn.execute(
        "SELECT value FROM settings WHERE key = 'savings-goal'"
    ).fetchone()
    conn.close()

    by_month: dict[str, dict[str, float]] = {}
    for r in rows:
        m = by_month.setdefault(r["month"], {"income": 0.0, "savings": 0.0})
        if r["kind"] == "income":
            m["income"] += (r["inflow"] or 0) - (r["outflow"] or 0)
        elif r["kind"] == "savings":
            m["savings"] += (r["outflow"] or 0) - (r["inflow"] or 0)

    points, cumulative = [], 0.0
    for m in sorted(by_month):
        v = by_month[m]
        cumulative += v["savings"]
        points.append({
            "month": m,
            "savings": round(v["savings"], 2),
            "cumulative": round(cumulative, 2),
            "income": round(v["income"], 2),
            "savings_rate": round(v["savings"] / v["income"] * 100, 1) if v["income"] > 0 else 0.0,
        })

    rates = [pt["savings_rate"] for pt in points]
    try:
        goal = float(goal_row["value"]) if goal_row and goal_row["value"] else None
    except (TypeError, ValueError):
        goal = None

    return {
        "period": _period_info(p),
        "goal": goal,
        "total": round(cumulative, 2),
        "avg_per_month": round(cumulative / len(points), 2) if points else 0.0,
        "median_rate": round(analytics.median(rates), 1),
        "months_goal_met": sum(1 for r in rates if goal is not None and r >= goal),
        "points": points,
    }


@app.get("/api/stats/comparison")
def stats_comparison(
    period: str = Query(None),
    month: str = Query(None),
    exclude_outliers: bool = Query(False),
):
    """Zeitraum gegen Vorzeitraum, Median und gleitenden Durchschnitt.

    Der Median ist hier aussagekräftiger als der Mittelwert: einzelne große
    Buchungen (Urlaub, Anschaffungen) ziehen den Mittelwert so weit hoch, dass
    ein normaler Monat dauerhaft "unter dem Schnitt" liegt.
    """
    conn = get_db()
    p = _resolve_period(conn, period, month)
    if not p.bounded:
        latest = _latest_month(conn)
        if not latest:
            conn.close()
            return {}
        p = periods.month_period(latest)
    prev = periods.previous(p)

    excl_sql, excl_params = "", []
    if exclude_outliers:
        excl_sql, excl_params = _exclude_outliers(*_outlier_context(conn))

    # Exact day boundaries for both current and previous windows. History
    # ends before the selected window to avoid future-data leakage.
    where, params = _where(periods.ALL, f"{KIND} = 'consumption'", excl_sql)
    rows = conn.execute(f"SELECT t.category, t.date, -t.amount AS total FROM {TX} {where}",
                        params + excl_params).fetchall()
    observed = _observed_months(conn)
    conn.close()
    series = {}
    for r in rows:
        series.setdefault(r["category"], []).append(r)
    n_cur = periods.month_weight(p)
    history_months = [m for m in observed if m < p.start[:7]]
    current_months = [m for m in observed if m in p.months()]
    result = []
    for cat, entries in series.items():
        totals = {}
        for r in entries:
            month_key = r["date"][:7]
            totals[month_key] = totals.get(month_key, 0) + r["total"]
        ordered = [totals.get(m, 0) for m in history_months]
        current = sum(r["total"] for r in entries if p.start <= r["date"] < p.end)
        previous_v = sum(r["total"] for r in entries if prev.start <= r["date"] < prev.end)
        med = analytics.median(ordered)
        avg3 = analytics.moving_average(ordered, 3)[-1] if ordered else 0.0
        per_month = current / n_cur if n_cur else 0
        if current == 0 and previous_v == 0:
            continue
        result.append({"category": cat, "current": round(current, 2),
            "current_per_month": round(per_month, 2), "previous": round(previous_v, 2),
            "median": round(med, 2), "moving_avg": round(avg3, 2),
            "diff_prev": round(current - previous_v, 2),
            "diff_median": round(per_month - med, 2),
            "pct_vs_median": round((per_month - med) / med * 100, 1) if med > 0 else None,
            "trend_per_month": round(analytics.calendar_trend(history_months, ordered), 2),
            "months_observed": len(ordered), "active_months": len(current_months)})

    result.sort(key=lambda x: x["current"], reverse=True)
    return {
        "period": _period_info(p),
        "prev_period": _period_info(prev) if prev else None,
        "months_in_period": n_cur,
        "categories": result,
    }


def _recurring_items(conn, as_of: str | None = None) -> list[dict]:
    """Wiederkehrende Zahlungen je (Händler, Kategorie), inkl. manueller
    Fix/Variabel-Übersteuerung.

    Gruppierung NICHT nur nach Händlername: comdirect vergibt bei
    Eigenüberweisungen (Miete/Lebensmittel/Sparen-Transfer auf das eigene
    Konto) allen denselben merchant_name – ohne die Kategorie mit in den
    Schlüssel zu nehmen, würden so drei verschiedene, aber gleich benannte
    Buchungsströme zu einem einzigen (falschen) Median/Betrag verschmelzen.

    Ausgelagert aus `stats_recurring`, damit die Fixkosten-Hochrechnung
    (`_fixed_forecast_context`) dieselbe Klassifizierung nutzt statt eine
    zweite, potenziell abweichende Logik zu pflegen.
    """
    as_of = as_of or date.today().isoformat()
    overrides = {(r["merchant_name"], r["category"]): r["recurring_type"] for r in
                 conn.execute("SELECT merchant_name, category, recurring_type FROM merchant_overrides").fetchall()}

    rows = conn.execute(f"""
        SELECT t.source, t.account_name, t.merchant_name AS merchant_name, t.category AS category,
               t.date AS date, t.amount AS amount, {KIND} AS kind
        FROM {TX}
        WHERE t.merchant_name IS NOT NULL AND t.merchant_name != ''
          AND t.booked = 1 AND t.date != '0000-00-00' AND t.date <= ?
          AND ((t.amount < 0 AND {KIND} IN ('consumption', 'savings'))
               OR (t.amount > 0 AND {KIND} = 'income'))
        ORDER BY t.source, t.account_name, t.merchant_name, t.category, t.date
    """, (as_of,)).fetchall()

    by_item: dict[tuple[str, str], list] = {}
    for r in rows:
        by_item.setdefault((r["source"], r["account_name"], r["merchant_name"], r["category"]), []).append(r)

    result = []
    for (source, account, merchant, category), entries in by_item.items():
        months = sorted({e["date"][:7] for e in entries})
        if len(months) < 2:
            continue

        amounts = [abs(e["amount"]) for e in entries]
        first_date, last_date = entries[0]["date"], entries[-1]["date"]
        med = analytics.median(amounts)
        spread = max(amounts) - min(amounts)
        gaps = [periods.months_between(a, b) - 1 for a, b in zip(months, months[1:])]
        interval = int(analytics.median(gaps))
        regular = interval in (1, 2, 3, 6, 12) and all(g == interval for g in gaps)
        one_per_month = len(entries) == len(months)
        recent = amounts[-3:]
        stable = med > 0 and (max(recent) - min(recent)) / med < 0.05
        auto_type = "fix" if regular and one_per_month and stable else "variabel"
        override = overrides.get((merchant, category))
        rec_type = override if override in ("fix", "variabel") else auto_type

        # Monatliche Belastung über die tatsächlich abgedeckte Spanne, damit
        # vierteljährliche Zahlungen nicht wie monatliche aussehen.
        span = periods.months_between(months[0], months[-1])
        monthly = amounts[-1] / interval if rec_type == "fix" and regular else (sum(amounts) / span if span else 0.0)

        # Preisänderung nur bei betragsstabilen Abos auswerten – bei
        # schwankenden Beträgen (Tanken, Hotels) ist "der Preis ist gestiegen"
        # keine sinnvolle Aussage, sondern nur die normale Streuung.
        previous_amounts = amounts[:-1]
        last_amount = amounts[-1]
        prev_med = analytics.median(previous_amounts) if previous_amounts else 0.0
        changed = (rec_type == "fix" and prev_med > 0
                   and abs(last_amount - prev_med) / prev_med > 0.10)

        # Typischer Abbuchungstag → nächste erwartete Buchung.
        days = sorted(int(e["date"][8:10]) for e in entries)
        typical_day = int(analytics.median([float(d) for d in days]))
        next_y, next_m = periods._add_months(int(last_date[:4]), int(last_date[5:7]), max(interval, 1))
        due = periods._day_in_month(next_y, next_m, typical_day)
        # Overdue for an entire cadence: retain in the list but stop assuming
        # the contract is active. Missing imports are surfaced separately.
        expiry_y, expiry_m = periods._add_months(next_y, next_m, max(interval, 1))
        active = as_of < periods._day_in_month(expiry_y, expiry_m, typical_day)
        next_expected = due
        while next_expected <= as_of:
            next_y, next_m = periods._add_months(next_y, next_m, max(interval, 1))
            next_expected = periods._day_in_month(next_y, next_m, typical_day)

        result.append({
            "source": source, "account_name": account,
            "interval_months": interval if regular else None, "active": active,
            "merchant_name": merchant,
            "category": category,
            "kind": entries[-1]["kind"],
            "type": rec_type,
            "type_auto": auto_type,
            "type_overridden": override is not None and override != auto_type,
            "months": len(months),
            "count": len(entries),
            "median_amount": round(med, 2),
            "last_amount": round(last_amount, 2),
            "min_amount": round(min(amounts), 2),
            "max_amount": round(max(amounts), 2),
            "monthly_cost": round(monthly, 2),
            "yearly_cost": round(monthly * 12, 2),
            "first_date": first_date,
            "last_date": last_date,
            "typical_day": typical_day,
            "next_expected": next_expected,
            "price_changed": changed,
            "price_change": {
                "from": round(prev_med, 2),
                "to": round(last_amount, 2),
                "diff": round(last_amount - prev_med, 2),
                "pct": round((last_amount - prev_med) / prev_med * 100, 1),
                "since": last_date,
            } if changed else None,
        })
    return result


@app.get("/api/stats/recurring")
def stats_recurring():
    """Wiederkehrende Zahlungen mit Jahreskosten und Preisänderungen."""
    conn = get_db()
    result = _recurring_items(conn)
    conn.close()

    result.sort(key=lambda x: x["yearly_cost"], reverse=True)
    # Sparpläne laufen zwar regelmäßig, sind aber keine Kosten – deshalb in den
    # Summen getrennt ausgewiesen.
    costs = [r for r in result if r["kind"] == "consumption" and r["active"]]
    savings_plans = [r for r in result if r["kind"] == "savings" and r["active"]]
    fixed = [r for r in costs if r["type"] == "fix"]
    return {
        "total_yearly":     round(sum(r["yearly_cost"] for r in costs), 2),
        "total_monthly":    round(sum(r["monthly_cost"] for r in costs), 2),
        "fixed_monthly":    round(sum(r["monthly_cost"] for r in fixed), 2),
        "fixed_yearly":     round(sum(r["yearly_cost"] for r in fixed), 2),
        "fixed_count":      len(fixed),
        "variable_monthly": round(sum(r["monthly_cost"] for r in costs if r["type"] != "fix"), 2),
        "savings_monthly":  round(sum(r["monthly_cost"] for r in savings_plans), 2),
        "price_increases":  sum(1 for r in costs if r["price_changed"] and r["price_change"]["diff"] > 0),
        "items": result,
    }


@app.put("/api/recurring/{merchant_name}/{category}/type")
def set_recurring_type(merchant_name: str, category: str, body: dict):
    """Manuelle Fix/Variabel-Einstufung eines Postens, übersteuert die Automatik.

    Schlüssel ist Händler + Kategorie, nicht nur der Händler: derselbe
    merchant_name kann (v. a. bei comdirect-Eigenüberweisungen) mehrere,
    völlig unterschiedliche wiederkehrende Buchungen bündeln.
    """
    rtype = str(body.get("type", "")).strip()
    if rtype not in ("fix", "variabel"):
        raise HTTPException(400, "Art muss 'fix' oder 'variabel' sein")
    conn = get_db()
    conn.execute(
        "INSERT INTO merchant_overrides (merchant_name, category, recurring_type) VALUES (?, ?, ?) "
        "ON CONFLICT(merchant_name, category) DO UPDATE SET recurring_type = excluded.recurring_type",
        (merchant_name, category, rtype)
    )
    conn.commit()
    conn.close()
    return {"ok": True, "merchant_name": merchant_name, "category": category, "type": rtype}


@app.delete("/api/recurring/{merchant_name}/{category}/type")
def clear_recurring_type(merchant_name: str, category: str):
    """Übersteuerung entfernen – zurück zur automatischen Einstufung."""
    conn = get_db()
    conn.execute("DELETE FROM merchant_overrides WHERE merchant_name = ? AND category = ?",
                 (merchant_name, category))
    conn.commit()
    conn.close()
    return {"ok": True}


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
    rows = conn.execute("SELECT name, is_default, kind FROM categories ORDER BY id").fetchall()
    conn.close()
    return [{"name": r["name"], "is_default": bool(r["is_default"]),
             "kind": r["kind"] or "consumption"} for r in rows]


@app.put("/api/categories/{name}/kind")
def set_category_kind(name: str, body: dict):
    """Art einer Kategorie: income | consumption | savings | transfer."""
    kind = str(body.get("kind", "")).strip()
    if kind not in VALID_KINDS:
        raise HTTPException(400, f"Art muss eine von {', '.join(VALID_KINDS)} sein")
    conn = get_db()
    try:
        cur = conn.execute("UPDATE categories SET kind = ? WHERE name = ?", (kind, name))
        if cur.rowcount == 0:
            raise HTTPException(404, f"Unbekannte Kategorie: {name}")
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "name": name, "kind": kind}


@app.post("/api/categories")
def create_category(body: dict):
    name = str(body.get("name", "")).strip()
    if not name:
        raise HTTPException(400, "Name erforderlich")
    kind = str(body.get("kind", "consumption")).strip() or "consumption"
    if kind not in VALID_KINDS:
        raise HTTPException(400, f"Art muss eine von {', '.join(VALID_KINDS)} sein")
    conn = get_db()
    try:
        conn.execute("INSERT INTO categories (name, is_default, kind) VALUES (?, 0, ?)",
                     (name, kind))
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
        for table in ("category_rules", "category_budgets", "category_corrections", "contextual_corrections", "merchant_overrides"):
            conn.execute(f"UPDATE {table} SET category = ? WHERE category = ?", (new_name, old_name))
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
    for table in ("category_rules", "category_budgets", "category_corrections", "contextual_corrections", "merchant_overrides"):
        conn.execute(f"DELETE FROM {table} WHERE category = ?", (name,))
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
def budget_status(month: str = Query(None), period: str = Query(None)):
    conn = get_db()
    p = _resolve_period(conn, period, month)
    if not p.bounded:
        latest = _latest_month(conn)
        if not latest:
            conn.close()
            return {"month": None, "period": None, "categories": []}
        p = periods.month_period(latest)

    # Budgets sind monatlich gedacht – über längere Zeiträume entsprechend
    # hochgerechnet, damit Soll und Ist vergleichbar bleiben.
    factor = periods.month_weight(p)

    budgets = {r["category"]: r["monthly_budget"] for r in
               conn.execute("SELECT category, monthly_budget FROM category_budgets").fetchall()}

    where, params = _where(p, f"{KIND} = 'consumption'")
    spending = {r["category"]: -r["total"] for r in conn.execute(f"""
        SELECT t.category AS category, SUM(t.amount) AS total
        FROM {TX} {where}
        GROUP BY t.category
    """, params).fetchall()}

    # Nur gesetzt, wenn `p` gerade läuft – Hochrechnung für abgeschlossene
    # Monate wäre gegenstandslos, die Ist-Werte sind dort schon final.
    quality = _data_quality(conn)
    fraction = periods.elapsed_fraction(p) if quality["forecast_ready"] else None
    fixed_ctx = _fixed_forecast_context(conn, p, fraction)
    conn.close()

    def _forecast_spent(cat: str, spent: float) -> float | None:
        # Bekannte Fixkosten dieser Kategorie fließen mit ihrem erwarteten
        # Betrag ein, der Rest weiter linear nach Tages-Anteil – siehe
        # `_forecast` für die Begründung.
        if not fraction or fraction >= 1:
            return None
        actual_fixed = fixed_ctx["actual_by_category"].get(cat, 0.0) if fixed_ctx else 0.0
        projected_fixed = fixed_ctx["projected_by_category"].get(cat, 0.0) if fixed_ctx else 0.0
        variable = spent - actual_fixed
        return round(projected_fixed + max(variable, 0) / fraction + min(variable, 0), 2)

    result = []
    seen = set()
    for cat, budget in sorted(budgets.items()):
        spent = spending.get(cat, 0)
        scaled = budget * factor
        forecast = _forecast_spent(cat, spent)
        result.append({
            "category": cat,
            "budget": round(scaled, 2),
            "monthly_budget": budget,
            "spent": round(spent, 2),
            "pct": round(spent / scaled * 100, 1) if scaled > 0 else 0,
            "remaining": round(scaled - spent, 2),
            "forecast_remaining": round(scaled - forecast, 2) if forecast is not None else None,
            "forecast": forecast,
            "forecast_pct": round(forecast / scaled * 100, 1) if forecast is not None and scaled > 0 else None,
        })
        seen.add(cat)
    for cat, spent in sorted(spending.items()):
        if cat not in seen:
            result.append({"category": cat, "budget": None, "monthly_budget": None,
                           "spent": round(spent, 2), "pct": None,
                           "forecast": _forecast_spent(cat, spent), "forecast_pct": None})

    return {"month": p.start[:7], "period": _period_info(p),
            "data_quality": quality, "months": factor, "elapsed_fraction": round(fraction, 3) if fraction else None,
            "categories": result}


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
        WHERE date != '0000-00-00' AND booked = 1
        ORDER BY month DESC
    """).fetchall()
    conn.close()
    return [r["month"] for r in rows]


# ─── Export & Backup ───────────────────────────────────────────────────────────

@app.get("/api/export/csv")
def export_csv(month: str = Query(None), period: str = Query(None)):
    conn = get_db()
    p = _resolve_period(conn, period, month)
    where, params = _where(p, "1=1") if p.bounded else ("", [])
    rows = conn.execute(f"""
        SELECT t.date, t.merchant_name, t.description, t.category,
               COALESCE(c.kind, 'consumption') AS kind,
               t.amount, t.source, t.account_name, t.note
        FROM {TX} {where} ORDER BY t.date DESC
    """, params).fetchall()
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["Datum", "Händler", "Beschreibung", "Kategorie", "Art",
                     "Betrag", "Konto", "Quelle", "Notiz"])
    for r in rows:
        writer.writerow([r["date"], r["merchant_name"] or "", r["description"] or "",
                         r["category"], r["kind"], r["amount"],
                         r["account_name"], r["source"], r["note"] or ""])

    filename = f"haushaltsbuch_{p.start[:7] if p.bounded else 'gesamt'}.csv"
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
