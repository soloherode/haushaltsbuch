import csv
import hashlib
import io
import re
from datetime import datetime
from typing import Optional

from app.categories import categorize_from_comdirect


def _parse_amount(value: str) -> float:
    """Convert German number format '-1.220,00' to float."""
    v = value.strip().replace(".", "").replace(",", ".")
    return float(v)


def _parse_date(value: str) -> Optional[str]:
    """Convert 'DD.MM.YYYY' to 'YYYY-MM-DD'. Returns None if not a date."""
    value = value.strip()
    if value in ("--", "offen", ""):
        return None
    try:
        return datetime.strptime(value, "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _extract_merchant(buchungstext: str) -> str:
    m = re.search(r"Empfänger:\s*(.+?)(?:Kto/IBAN|BLZ/BIC|Buchungstext|$)", buchungstext)
    if m:
        return m.group(1).strip()
    m = re.search(r"Auftraggeber:\s*(.+?)(?:Buchungstext|$)", buchungstext)
    if m:
        return m.group(1).strip()
    return ""


def _make_hash(account: str, date: str, amount: float, description: str) -> str:
    raw = f"{account}|{date}|{amount}|{description}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _is_kartenabrechnung(vorgang: str, buchungstext: str) -> bool:
    text = (vorgang + " " + buchungstext).lower()
    return "kartenabrechnung" in text or "visa-kartenabrechnung" in text


def parse_comdirect_csv(content: bytes, account_name: str = "comdirect Girokonto") -> list[dict]:
    """Parse a comdirect CSV export (Girokonto or Kreditkarte) and return transactions."""
    try:
        text = content.decode("iso-8859-1")
    except Exception:
        text = content.decode("utf-8", errors="replace")

    reader = csv.reader(io.StringIO(text), delimiter=";")
    rows = list(reader)

    # Detect account type and column layout from header row
    # Girokonto:   Buchungstag | Wertstellung | Vorgang | Buchungstext | Umsatz
    # Kreditkarte: Buchungstag | Umsatztag    | Vorgang | Referenz     | Buchungstext | Umsatz
    is_kreditkarte = False
    header_found = False

    for row in rows:
        if not row:
            continue
        cleaned = [c.strip().strip('"') for c in row]
        if "Buchungstag" in cleaned:
            # detect by presence of "Referenz" column
            is_kreditkarte = "Referenz" in cleaned
            if not account_name or account_name == "comdirect Girokonto":
                if is_kreditkarte:
                    account_name = "comdirect Kreditkarte"
            header_found = True
            break

    if not header_found:
        return []

    # Column indices
    if is_kreditkarte:
        IDX_DATE, IDX_WERT, IDX_VORGANG, IDX_TEXT, IDX_UMSATZ = 0, 1, 2, 4, 5
        MIN_COLS = 6
    else:
        IDX_DATE, IDX_WERT, IDX_VORGANG, IDX_TEXT, IDX_UMSATZ = 0, 1, 2, 3, 4
        MIN_COLS = 5

    transactions = []
    in_data = False

    for row in rows:
        if not row:
            continue
        cells = [c.strip().strip('"') for c in row]

        # Skip until after header
        if not in_data:
            if "Buchungstag" in cells:
                in_data = True
            continue

        if len(cells) < MIN_COLS:
            continue
        if cells[0] in ("", "Alter Kontostand", "Neuer Kontostand"):
            continue

        buchungstag  = cells[IDX_DATE]
        wertstellung = cells[IDX_WERT]
        vorgang      = cells[IDX_VORGANG]
        buchungstext = cells[IDX_TEXT]
        umsatz_str   = cells[IDX_UMSATZ]

        if not umsatz_str:
            continue

        try:
            amount = _parse_amount(umsatz_str)
        except ValueError:
            continue

        date = _parse_date(buchungstag) or _parse_date(wertstellung)
        if date is None:
            date = "0000-00-00"

        transaction_date = _parse_date(wertstellung)
        booked = _parse_date(buchungstag) is not None

        # Kartenabrechnung = internal transfer between accounts → "Überweisung"
        # excludes it from expense/income stats on both sides
        if _is_kartenabrechnung(vorgang, buchungstext):
            category = "Überweisung"
            merchant_name = "Kartenabrechnung"
        else:
            merchant_name = _extract_merchant(buchungstext) if not is_kreditkarte else buchungstext.strip()
            category = categorize_from_comdirect(vorgang, buchungstext)

        import_hash = _make_hash(account_name, buchungstag + wertstellung, amount, buchungstext)

        transactions.append({
            "source":           "comdirect",
            "account_name":     account_name,
            "date":             date,
            "transaction_date": transaction_date,
            "amount":           amount,
            "description":      buchungstext,
            "merchant_name":    merchant_name,
            "category":         category,
            "subcategory":      None,
            "city":             None,
            "country":          None,
            "transaction_type": vorgang,
            "booked":           int(booked),
            "import_hash":      import_hash,
        })

    return transactions
