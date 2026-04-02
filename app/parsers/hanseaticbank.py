import hashlib
import json
from datetime import datetime
from typing import Optional

from app.categories import categorize_from_hanseatic


def _parse_date(value: str) -> Optional[str]:
    """Convert 'DD.MM.YYYY' to 'YYYY-MM-DD'."""
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _make_hash(account: str, transaction_id: str, date: str, amount: float, description: str) -> str:
    raw = f"{account}|{transaction_id}|{date}|{amount}|{description}"
    return hashlib.sha256(raw.encode()).hexdigest()


def parse_hanseaticbank_json(content: bytes, account_name: str = "HanseaticBank GenialCard") -> list[dict]:
    """Parse HanseaticBank transactions JSON and return a list of transaction dicts."""
    data = json.loads(content.decode("utf-8"))
    raw_transactions = data.get("transactions", data) if isinstance(data, dict) else data

    transactions = []

    for t in raw_transactions:
        amount          = float(t.get("amount", 0))
        date            = _parse_date(t.get("date", "")) or "0000-00-00"
        transaction_date = _parse_date(t.get("transactionDate", ""))
        description     = t.get("description", "")
        transaction_id  = t.get("transactionId", "")
        booked          = int(t.get("booked", True))

        merchant_data   = t.get("merchantData") or {}
        merchant_name   = t.get("merchantName") or merchant_data.get("name", "")
        hb_category     = merchant_data.get("category", "")
        hb_subcategories = merchant_data.get("categories", [])
        city            = t.get("city") or merchant_data.get("city", "")
        country         = t.get("country") or merchant_data.get("country", "")

        category = categorize_from_hanseatic(hb_category, hb_subcategories)

        # Special case: monthly settlement (Kartenabrechnung) is a payment, not spending
        if "kartenabrechnung" in description.lower():
            category = "Finanzen & Versicherung"

        import_hash = _make_hash(account_name, transaction_id, date, amount, description)

        transactions.append({
            "source":           "hanseaticbank",
            "account_name":     account_name,
            "date":             date,
            "transaction_date": transaction_date,
            "amount":           amount,
            "description":      description,
            "merchant_name":    merchant_name,
            "category":         category,
            "subcategory":      hb_category,
            "city":             city or None,
            "country":          country or None,
            "transaction_type": t.get("creditDebitKey", ""),
            "booked":           booked,
            "import_hash":      import_hash,
        })

    return transactions
