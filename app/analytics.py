"""Statistische Hilfsfunktionen.

Bewusst robuste Verfahren: ein Haushaltsbuch hat wenige, stark schwankende
Werte pro Kategorie (Urlaub schwankt hier zwischen 223 € und 3.208 € im Monat).
Ein arithmetisches Mittel ist da schnell irreführend, deshalb Median und
gleitender Durchschnitt.
"""


def median(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2


def moving_average(series: list[float], window: int = 3) -> list[float]:
    """Nachlaufender gleitender Durchschnitt; am Anfang über weniger Werte."""
    out = []
    for i in range(len(series)):
        chunk = series[max(0, i - window + 1): i + 1]
        out.append(sum(chunk) / len(chunk))
    return out


def trend(series: list[float]) -> float:
    """Steigung je Schritt über eine einfache lineare Regression.

    > 0 bedeutet steigende Ausgaben. Ohne mindestens zwei Punkte 0.
    """
    n = len(series)
    if n < 2:
        return 0.0
    mean_x = (n - 1) / 2
    mean_y = sum(series) / n
    num = sum((i - mean_x) * (y - mean_y) for i, y in enumerate(series))
    den = sum((i - mean_x) ** 2 for i in range(n))
    return num / den if den else 0.0


# Ausreißer ---------------------------------------------------------------------
#
# Schwelle je Kategorie über den Median statt über Mittelwert/Standardabweichung:
# eine einzelne Urlaubsbuchung zieht den Mittelwert so weit hoch, dass sie sich
# selbst nicht mehr als Ausreißer erkennt.

OUTLIER_FACTOR = 4.0       # ab dem Vielfachen des Medians
OUTLIER_MIN_COUNT = 6      # darunter ist der Median nicht aussagekräftig
OUTLIER_MIN_AMOUNT = 100.0 # kleinere Beträge nie als Ausreißer melden
RECURRING_MIN_MONTHS = 3   # ab wann ein Händler als wiederkehrend gilt

# Die Werte sind an echten Daten kalibriert: mit Faktor 3 und 50 € Untergrenze
# landeten 12 % aller Buchungen in der Liste, was sie nutzlos macht. Faktor 4,
# 100 € Untergrenze und der Abo-Filter unten ergeben rund 2 %.


def outlier_thresholds(amounts_by_category: dict[str, list[float]]) -> dict[str, float]:
    """→ {Kategorie: Schwellenbetrag}. Kategorien ohne Aussagekraft fehlen."""
    thresholds = {}
    for category, amounts in amounts_by_category.items():
        positive = [abs(a) for a in amounts if a]
        if len(positive) < OUTLIER_MIN_COUNT:
            continue
        med = median(positive)
        if med <= 0:
            continue
        threshold = max(med * OUTLIER_FACTOR, OUTLIER_MIN_AMOUNT)
        thresholds[category] = round(threshold, 2)
    return thresholds


def recurring_merchants(rows) -> set[str]:
    """Händler, die in mindestens `RECURRING_MIN_MONTHS` Monaten auftauchen.

    Eine monatlich gleiche Abbuchung ist per Definition kein Ausreißer, auch
    wenn sie für ihre Kategorie groß ist – sonst meldet die Liste jeden Monat
    dieselbe Miete.
    """
    months: dict[str, set] = {}
    for r in rows:
        name = r["merchant_name"]
        if name:
            months.setdefault(name, set()).add(r["date"][:7])
    return {m for m, ms in months.items() if len(ms) >= RECURRING_MIN_MONTHS}
