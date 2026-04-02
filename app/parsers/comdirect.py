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
    """Try to extract a clean merchant / counterpart name from Buchungstext."""
    # Empfänger: Name
    m = re.search(r"Empfänger:\s*(.+?)(?:Kto/IBAN|BLZ/BIC|Buchungstext|$)", buchungstext)
    if m:
        return m.group(1).strip()
    # Auftraggeber: Name
    m = re.search(r"Auftraggeber:\s*(.+?)(?:Buchungstext|$)", buchungstext)
    if m:
        return m.group(1).strip()
    return ""


def _make_hash(account: str, date: str, amount: float, description: str) -> str:
    raw = f"{account}|{date}|{amount}|{description}"
    return hashlib.sha256(raw.encode()).hexdigest()


def parse_comdirect_csv(content: bytes, account_name: str = "comdirect Girokonto") -> list[dict]:
    """Parse a comdirect CSV export and return a list of transaction dicts."""
    # Detect encoding
    try:
        text = content.decode("iso-8859-1")
    except Exception:
        text = content.decode("utf-8", errors="replace")

    reader = csv.reader(io.StringIO(text), delimiter=";")
    rows = list(reader)

    transactions = []
    header_found = False

    for row in rows:
        if not row:
            continue

        # Find the data header row
        if not header_found:
            cleaned = [c.strip().strip('"') for c in row]
            if "Buchungstag" in cleaned:
                header_found = True
            continue

        # Skip empty / footer rows
        if len(row) < 5:
            continue
        cells = [c.strip().strip('"') for c in row]
        if cells[0] in ("", "Alter Kontostand", "Neuer Kontostand"):
            continue

        buchungstag   = cells[0]
        wertstellung  = cells[1]
        vorgang       = cells[2]
        buchungstext  = cells[3]
        umsatz_str    = cells[4]

        if not umsatz_str:
            continue

        try:
            amount = _parse_amount(umsatz_str)
        except ValueError:
            continue

        date          = _parse_date(buchungstag) or _parse_date(wertstellung)
        if date is None:
            date = "0000-00-00"  # pending / open booking

        transaction_date = _parse_date(wertstellung)
        merchant_name    = _extract_merchant(buchungstext)
        category         = categorize_from_comdirect(vorgang, buchungstext)
        booked           = _parse_date(buchungstag) is not None
        description      = buchungstext

        import_hash = _make_hash(account_name, buchungstag + wertstellung, amount, buchungstext)

        transactions.append({
            "source":           "comdirect",
            "account_name":     account_name,
            "date":             date,
            "transaction_date": transaction_date,
            "amount":           amount,
            "description":      description,
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
