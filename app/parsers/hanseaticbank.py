import hashlib
import io
import re
from datetime import datetime
from typing import Optional

import pypdf

from app.categories import categorize_from_comdirect

# Anchors a booking row in the extracted PDF text, e.g.:
#   "27.07.2026 23.07.2026 Kartenumsatz"   (Buchungsdatum, Transaktionsdatum, Beschreibung)
#   "31.07.2026 - Gutschrift"              (Kartenabrechnung: keine Transaktionsdatum-Spalte)
_DATE = r"\d{2}\.\d{2}\.\d{4}"
_ROW_RE = re.compile(rf"^({_DATE})[ \t]+({_DATE}|-)[ \t]+([A-Za-zÄÖÜäöüß]+)[ \t]*$", re.MULTILINE)

# The line that closes a booking: an optional 4-stellige Kartennummer, then the
# signed Betrag – alone on its own line, e.g. "8125 -4,50" or "- 169,22".
_AMOUNT_RE = re.compile(r"^(?:(\d{4})[ \t]+)?(-)?[ \t]?([\d.]+,\d{2})[ \t]*$")


def _parse_date(value: str) -> Optional[str]:
    """Convert 'DD.MM.YYYY' to 'YYYY-MM-DD'."""
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _to_amount(sign: Optional[str], value: str) -> float:
    v = float(value.replace(".", "").replace(",", "."))
    return -v if sign == "-" else v


def _make_hash(account: str, date: str, transaction_date: Optional[str], amount: float, description: str) -> str:
    raw = f"{account}|{date}|{transaction_date}|{amount}|{description}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _extract_text(content: bytes) -> str:
    reader = pypdf.PdfReader(io.BytesIO(content))
    return "\n".join(page.extract_text() or "" for page in reader.pages)


def parse_hanseaticbank_pdf(content: bytes, account_name: str = "HanseaticBank GenialCard") -> list[dict]:
    """Parse a HanseaticBank GenialCard Kontoauszug/Abrechnungs-PDF and return transaction dicts.

    Jede Buchungszeile beginnt mit "Buchungsdatum Transaktionsdatum Kartenumsatz" (oder
    "Buchungsdatum - Gutschrift" für die monatliche Kartenabrechnung), gefolgt von der
    Beschreibung (ggf. über mehrere Zeilen) und endet mit "[Kartennummer] Betrag" auf einer
    eigenen Zeile. Umlaufende Kopf-/Fußzeilen (Seitenwechsel, Saldo-Überträge, AGB-Text)
    liegen zwischen den Buchungen und werden ignoriert, da sie nicht auf das Betrags-Muster
    passen.
    """
    text = _extract_text(content)
    matches = list(_ROW_RE.finditer(text))

    transactions = []
    for i, m in enumerate(matches):
        booking_raw, transaction_raw, label = m.groups()
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        block_lines = [ln.strip() for ln in text[start:end].split("\n") if ln.strip()]

        amount = None
        desc_lines = block_lines
        for j in range(len(block_lines) - 1, -1, -1):
            am = _AMOUNT_RE.match(block_lines[j])
            if am:
                amount = _to_amount(am.group(2), am.group(3))
                desc_lines = block_lines[:j]
                break
        if amount is None:
            # Kein Betrag im Block gefunden -> keine echte Buchungszeile, überspringen.
            continue

        date = _parse_date(booking_raw) or "0000-00-00"
        transaction_date = _parse_date(transaction_raw)
        description = re.sub(r"\s+", " ", " ".join(desc_lines)).strip()

        is_settlement = "gutschrift" in label.lower()
        if is_settlement:
            # Kartenabrechnung = Zahlungseingang (gleicht die Einzelumsätze aus), keine
            # Ausgabe → "Überweisung", analog zu comdirect (parsers/comdirect.py:
            # _is_kartenabrechnung) und dem bisherigen JSON-Import, damit sie nicht doppelt
            # zu den bereits erfassten Einzeltransaktionen zählt. Der PDF-Druck zeigt den
            # Betrag mit Minus, tatsächlich verringert er aber den Saldo (siehe
            # Saldo-Übertrag-Reihen) – daher hier der Absolutbetrag genommen.
            amount = abs(amount)
            category = "Überweisung"
            merchant_name = "Kartenabrechnung"
            city = None
        else:
            if "," in description:
                merchant_name, city = (p.strip() for p in description.split(",", 1))
            else:
                merchant_name, city = description, None
            category = categorize_from_comdirect("", description)

        import_hash = _make_hash(account_name, date, transaction_date, amount, description)

        transactions.append({
            "source":           "hanseaticbank",
            "account_name":     account_name,
            "date":             date,
            "transaction_date": transaction_date,
            "amount":           amount,
            "description":      description,
            "merchant_name":    merchant_name,
            "category":         category,
            "subcategory":      None,
            "city":             city or None,
            "country":          None,
            "transaction_type": label,
            "booked":           1,
            "import_hash":      import_hash,
        })

    return transactions
