"""Rolling origin evaluation; only bank periods with confirmed coverage qualify."""
from datetime import date, timedelta
from app import periods


def complete_months(conn, before):
    accounts = list(conn.execute("SELECT DISTINCT source, account_name FROM transactions WHERE source != 'manual'"))
    ranges = {}
    for row in conn.execute('SELECT * FROM import_coverage ORDER BY start_date'):
        ranges.setdefault((row['source'], row['account_name']), []).append((row['start_date'], row['end_date']))
    if not accounts or any(tuple(a) not in ranges for a in accounts):
        return []
    first = min(lo for values in ranges.values() for lo, _ in values)
    months = periods.custom_period(first, before).months()
    result = []
    for month in months:
        p = periods.month_period(month)
        if p.end > before:
            continue
        covered = True
        for account in accounts:
            cursor = p.start
            for lo, hi in ranges[tuple(account)]:
                if lo <= cursor <= hi:
                    cursor = (date.fromisoformat(hi) + timedelta(days=1)).isoformat()
                if cursor >= p.end:
                    break
            if cursor < p.end:
                covered = False
                break
        if covered:
            result.append(month)
    return result


def evaluate(conn):
    # Local import avoids circular module initialization.
    from app.main import TX, KIND, _fixed_forecast_context, _forecast
    months = complete_months(conn, date.today().isoformat())[-12:]
    observations = []
    for month in months:
        p = periods.month_period(month)
        actual = conn.execute(f"SELECT COALESCE(SUM(-t.amount),0) FROM {TX} WHERE t.booked = 1 AND {KIND} = 'consumption' AND t.date >= ? AND t.date < ?",(p.start,p.end)).fetchone()[0]
        for day in (7, 14, 21):
            cutoff = f'{month}-{day:02d}'
            partial = conn.execute(f"SELECT COALESCE(SUM(-t.amount),0) FROM {TX} WHERE t.booked = 1 AND {KIND} = 'consumption' AND t.date >= ? AND t.date <= ?",(p.start,cutoff)).fetchone()[0]
            fraction = periods.elapsed_fraction(p, cutoff)
            context = _fixed_forecast_context(conn,p,fraction,as_of=cutoff)
            prediction = _forecast(fraction,0,partial,0,context)['consumption']
            observations.append({'month':month,'day':day,'actual':round(actual,2),
                                 'forecast':prediction,'error':round(prediction-actual,2),
                                 'pace_error':round(partial/fraction-actual,2)})
    n = len(observations)
    return {'months':len(months),'observations':n,
            'mae':round(sum(abs(o['error']) for o in observations)/n,2) if n else None,
            'pace_mae':round(sum(abs(o['pace_error']) for o in observations)/n,2) if n else None,
            'status':'evaluated' if n else 'insufficient_coverage',
            'note':'Nur bestätigte vollständige Importzeiträume; historische Kategoriekorrekturen sind enthalten. Keine kalibrierte Unsicherheitsbandbreite.',
            'points':observations}
