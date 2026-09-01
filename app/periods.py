"""Zeitraum-Auflösung für alle Statistik-Endpoints.

Ein Zeitraum wird als kompakter String übergeben (Query-Parameter `period`):

    all                        alles
    2026-04                    Monat
    2026-Q2                    Quartal
    2026                       Jahr
    r12                        rollierend, die letzten 12 Monate mit Daten
    2026-01-01..2026-06-30     freier Zeitraum, beide Enden inklusive

Intern wird daraus immer ein halboffenes Intervall [start, end), damit die
Abfragen den Index auf `date` nutzen können statt über LIKE zu scannen.
"""

import re
from dataclasses import dataclass

MONTH_RE = re.compile(r"^(\d{4})-(\d{2})$")
YEAR_RE = re.compile(r"^(\d{4})$")
QUARTER_RE = re.compile(r"^(\d{4})-Q([1-4])$", re.IGNORECASE)
ROLLING_RE = re.compile(r"^r(\d{1,2})$", re.IGNORECASE)
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
CUSTOM_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})$")

MONTH_NAMES = ["Januar", "Februar", "März", "April", "Mai", "Juni",
               "Juli", "August", "September", "Oktober", "November", "Dezember"]


class PeriodError(ValueError):
    """Ungültige Zeitraumangabe."""


@dataclass(frozen=True)
class Period:
    start: str | None   # inklusive, None = offen
    end: str | None     # exklusive, None = offen
    label: str
    kind: str           # all|month|quarter|year|rolling|custom

    @property
    def bounded(self) -> bool:
        return self.start is not None and self.end is not None

    def where(self, column: str = "date") -> tuple[str, list[str]]:
        """→ (SQL-Fragment ohne WHERE, Parameter). Leerer String, wenn unbegrenzt."""
        if not self.bounded:
            # '0000-00-00' sind Buchungen ohne verwertbares Datum.
            return f"{column} != '0000-00-00'", []
        return f"{column} >= ? AND {column} < ?", [self.start, self.end]

    def months(self) -> list[str]:
        """Alle Monate 'YYYY-MM' im Zeitraum (nur bei begrenzten Zeiträumen)."""
        if not self.bounded:
            return []
        out = []
        y, m = int(self.start[:4]), int(self.start[5:7])
        while f"{y:04d}-{m:02d}-01" < self.end:
            out.append(f"{y:04d}-{m:02d}")
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        return out


def _add_months(year: int, month: int, delta: int) -> tuple[int, int]:
    total = year * 12 + (month - 1) + delta
    return total // 12, total % 12 + 1


def month_period(month: str) -> Period:
    m = MONTH_RE.match(month)
    if not m:
        raise PeriodError("Monat muss im Format YYYY-MM sein")
    year, mon = int(m.group(1)), int(m.group(2))
    if not 1 <= mon <= 12:
        raise PeriodError(f"Ungültiger Monat: {month}")
    ny, nm = _add_months(year, mon, 1)
    return Period(f"{year:04d}-{mon:02d}-01", f"{ny:04d}-{nm:02d}-01",
                  f"{MONTH_NAMES[mon - 1]} {year}", "month")


def year_period(year: str) -> Period:
    m = YEAR_RE.match(year)
    if not m:
        raise PeriodError("Jahr muss im Format YYYY sein")
    y = int(m.group(1))
    return Period(f"{y:04d}-01-01", f"{y + 1:04d}-01-01", str(y), "year")


def quarter_period(value: str) -> Period:
    m = QUARTER_RE.match(value)
    if not m:
        raise PeriodError("Quartal muss im Format YYYY-Qn sein")
    y, q = int(m.group(1)), int(m.group(2))
    first = (q - 1) * 3 + 1
    ny, nm = _add_months(y, first, 3)
    return Period(f"{y:04d}-{first:02d}-01", f"{ny:04d}-{nm:02d}-01",
                  f"Q{q} {y}", "quarter")


def rolling_period(count: int, latest_month: str | None) -> Period:
    """Die letzten `count` Monate, endend mit dem jüngsten Monat, der Daten hat.

    Am jüngsten Datenmonat ausgerichtet statt an heute – sonst zeigt der
    Zeitraum ins Leere, wenn gerade nichts importiert wurde.
    """
    if not 1 <= count <= 60:
        raise PeriodError("Rollierender Zeitraum muss zwischen 1 und 60 Monaten liegen")
    if not latest_month:
        return Period(None, None, f"letzte {count} Monate", "rolling")
    y, m = int(latest_month[:4]), int(latest_month[5:7])
    ey, em = _add_months(y, m, 1)                 # exklusives Ende
    sy, sm = _add_months(y, m, -(count - 1))      # inklusiver Start
    return Period(f"{sy:04d}-{sm:02d}-01", f"{ey:04d}-{em:02d}-01",
                  f"letzte {count} Monate", "rolling")


def custom_period(start: str, end_inclusive: str) -> Period:
    if not DATE_RE.match(start) or not DATE_RE.match(end_inclusive):
        raise PeriodError("Datumsangaben müssen im Format YYYY-MM-DD sein")
    if end_inclusive < start:
        raise PeriodError("Das Ende darf nicht vor dem Anfang liegen")
    # Ende inklusive → exklusiv, indem ein Tag addiert wird. Über die
    # ISO-Sortierung ist ein String-Increment nicht möglich, also über date().
    from datetime import date, timedelta
    e = date.fromisoformat(end_inclusive) + timedelta(days=1)
    return Period(start, e.isoformat(),
                  f"{start} bis {end_inclusive}", "custom")


ALL = Period(None, None, "Gesamter Zeitraum", "all")


def parse(period: str | None, latest_month: str | None = None) -> Period:
    """Zentrale Auflösung. `None`/leer → gesamter Zeitraum."""
    if not period or period == "all":
        return ALL
    period = period.strip()
    if MONTH_RE.match(period):
        return month_period(period)
    if QUARTER_RE.match(period):
        return quarter_period(period)
    if YEAR_RE.match(period):
        return year_period(period)
    m = ROLLING_RE.match(period)
    if m:
        return rolling_period(int(m.group(1)), latest_month)
    m = CUSTOM_RE.match(period)
    if m:
        return custom_period(m.group(1), m.group(2))
    raise PeriodError(
        "Unbekannter Zeitraum. Erlaubt: all, YYYY-MM, YYYY-Qn, YYYY, rN, "
        "YYYY-MM-DD..YYYY-MM-DD"
    )


def months_between(first: str, last: str) -> int:
    """Anzahl Monate von 'YYYY-MM' bis 'YYYY-MM', beide inklusive."""
    fy, fm = int(first[:4]), int(first[5:7])
    ly, lm = int(last[:4]), int(last[5:7])
    return (ly * 12 + lm) - (fy * 12 + fm) + 1


def _day_in_month(year: int, month: int, day: int) -> str:
    """Datum im gegebenen Monat am gewünschten Tag, kurze Monate abgefangen
    (der 31. wird im Februar zum 28./29.)."""
    from datetime import date
    for d in range(min(day, 31), 0, -1):
        try:
            return date(year, month, d).isoformat()
        except ValueError:
            continue
    return f"{year:04d}-{month:02d}-01"


def next_occurrence(last_date: str, day_of_month: int, today: str | None = None) -> str:
    """Nächster erwarteter Termin am typischen Abbuchungstag, STRIKT nach heute.

    Für die Anzeige gedacht ("wann kommt die nächste Abbuchung") – ein Termin,
    der genau heute fällig, aber noch nicht gebucht ist, zählt hier bewusst
    nicht: sonst schlägt die Abo-Übersicht Termine vor, die längst hätten
    passieren müssen, aber vermutlich nur schlicht noch nicht importiert
    wurden. Für "fällt ein Termin in einen bestimmten Zeitraum" (Hochrechnung)
    ist das die falsche Frage – siehe `expected_in_period`.
    """
    from datetime import date
    ref = today or date.today().isoformat()
    y, m = int(last_date[:4]), int(last_date[5:7])
    for step in range(1, 25):
        ny, nm = _add_months(y, m, step)
        candidate = _day_in_month(ny, nm, day_of_month)
        if candidate > ref:
            return candidate
    return candidate


def expected_in_period(period: Period, day_of_month: int) -> str | None:
    """Datum des Fixkosten-Termins im ersten Monat des Zeitraums, falls er
    hineinfällt – anders als `next_occurrence` OHNE Ausschluss des heutigen
    Tages: eine Miete, die heute fällig, aber noch nicht gebucht ist, gilt
    hier als "im Zeitraum erwartet". Grundlage für die Fixkosten-Hochrechnung.
    Deckt nur den ersten Monat ab; bei mehrmonatigen Zeiträumen (Quartal o.ä.)
    werden spätere Fälligkeiten hier bewusst nicht mitgezählt.
    """
    if not period.bounded:
        return None
    y, m = int(period.start[:4]), int(period.start[5:7])
    candidate = _day_in_month(y, m, day_of_month)
    return candidate if period.start <= candidate < period.end else None


def elapsed_fraction(period: Period, today: str | None = None) -> float | None:
    """Anteil eines Zeitraums, der bis heute bereits vergangen ist.

    Grundlage für Hochrechnungen ("bei aktuellem Tempo..."). None, wenn der
    Zeitraum gerade nicht läuft (unbegrenzt, noch nicht begonnen oder schon
    vorbei) – dort ist eine Hochrechnung sinnlos: es liegen entweder noch gar
    keine oder schon die endgültigen Daten vor.
    """
    if not period.bounded:
        return None
    from datetime import date
    ref = today or date.today().isoformat()
    if ref < period.start or ref >= period.end:
        return None
    start = date.fromisoformat(period.start)
    end = date.fromisoformat(period.end)
    now = date.fromisoformat(ref)
    total_days = (end - start).days
    if total_days <= 0:
        return None
    elapsed_days = (now - start).days + 1   # heute zählt als vergangener Tag
    return min(elapsed_days / total_days, 1.0)


def previous(period: Period) -> Period | None:
    """Der unmittelbar davorliegende, gleich lange Zeitraum – für Vergleiche."""
    if not period.bounded:
        return None
    sy, sm = int(period.start[:4]), int(period.start[5:7])
    if period.kind == "month":
        py, pm = _add_months(sy, sm, -1)
        return month_period(f"{py:04d}-{pm:02d}")
    if period.kind == "quarter":
        py, pm = _add_months(sy, sm, -3)
        return quarter_period(f"{py:04d}-Q{(pm - 1) // 3 + 1}")
    if period.kind == "year":
        return year_period(str(sy - 1))
    # rolling/custom: gleich langes Fenster direkt davor
    from datetime import date, timedelta
    start = date.fromisoformat(period.start)
    end = date.fromisoformat(period.end)
    length = (end - start).days
    prev_start = start - timedelta(days=length)
    return Period(prev_start.isoformat(), period.start,
                  f"vorherige {length} Tage", period.kind)
