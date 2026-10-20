"""Consistent, read-only statistics for the four analysis views."""
import math
from calendar import monthrange
from datetime import date, timedelta
from collections import defaultdict
from app import periods, analytics


def comparison_period(p, today):
    """Calendar-aligned partial periods; custom intervals retain equal lengths."""
    if not p.bounded or p.start > today.isoformat():
        return p, None
    previous = periods.previous(p)
    if p.end <= today.isoformat():
        return p, previous
    end = min(p.end, (today + timedelta(days=1)).isoformat())
    current = periods.Period(p.start, end, p.label, p.kind)
    if p.kind in ('month', 'quarter', 'year'):
        delta = {'month': 1, 'quarter': 3, 'year': 12}[p.kind]
        y, m = periods._add_months(today.year, today.month, -delta)
        day = min(today.day, monthrange(y, m)[1])
        prev_end = min(previous.end, (date(y, m, day) + timedelta(days=1)).isoformat())
    else:
        length = date.fromisoformat(end) - date.fromisoformat(p.start)
        prev_end = p.start
        previous = periods.Period((date.fromisoformat(prev_end) - length).isoformat(), prev_end,
                                  'Gleich langer Vorzeitraum', 'custom')
    return current, periods.Period(previous.start, prev_end if p.kind in ('month', 'quarter', 'year') else previous.end,
                                   previous.label, previous.kind)


def coverage(conn, p):
    accounts = [dict(r) for r in conn.execute("""SELECT source, account_name,
        MAX(CASE WHEN booked=1 AND date != '0000-00-00' THEN date END) AS last_booking
        FROM transactions WHERE source != 'manual' GROUP BY source, account_name""")]
    for a in accounts:
        ranges = list(conn.execute('SELECT start_date,end_date FROM import_coverage WHERE source=? AND account_name=? ORDER BY start_date',
                                   (a['source'], a['account_name'])))
        cursor = p.start
        any_overlap = False
        if p.bounded:
            for lo, hi in ranges:
                any_overlap |= lo < p.end and hi >= p.start
                if lo <= cursor <= hi:
                    cursor = (date.fromisoformat(hi) + timedelta(days=1)).isoformat()
            a['status'] = 'complete' if cursor >= p.end else 'partial' if any_overlap else 'unverified'
        else:
            a['status'] = 'unverified'
    complete = bool(accounts) and all(a['status'] == 'complete' for a in accounts)
    return {'complete': complete, 'accounts': accounts,
            'status': 'complete' if complete else 'partial' if any(a['status'] != 'unverified' for a in accounts) else 'unverified'}


def build(conn, p, *, exclude_outliers=False, category=None, today=None, include_bookings=False):
    from app.main import TX, KIND, NECESSITY, _outlier_context, _is_outlier, _period_info, _recurring_items
    today = today or date.today()
    current, previous = comparison_period(p, today)
    rows = [dict(r) for r in conn.execute(f"""SELECT t.id,t.date,t.amount,t.category,t.merchant_name,
        t.description,t.source,t.account_name,{KIND} AS kind,{NECESSITY} AS necessity
        FROM {TX} WHERE t.booked=1 AND t.date != '0000-00-00' ORDER BY t.date DESC,t.id DESC""")]
    thresholds, recurring = _outlier_context(conn)
    for r in rows:
        r['outlier'] = r['kind'] == 'consumption' and _is_outlier(r, thresholds, recurring)
    def in_period(r, period):
        return period is not None and (not period.bounded or period.start <= r['date'] < period.end)
    selected = [r for r in rows if in_period(r, current)]
    prior_all = [r for r in rows if in_period(r, previous)]
    filtered = lambda rr: [r for r in rr if not (exclude_outliers and r['outlier'])]
    active, prior = filtered(selected), filtered(prior_all)
    def totals(rr):
        result = {'income': 0, 'consumption': 0, 'savings': 0}
        for r in rr:
            if r['kind'] in result:
                result[r['kind']] += r['amount'] * (1 if r['kind'] == 'income' else -1)
        result = {k: round(v, 2) for k, v in result.items()}
        result['balance'] = round(result['income'] - result['consumption'] - result['savings'], 2)
        result['savings_rate'] = round(result['savings'] / result['income'] * 100, 1) if result['income'] > 0 else None
        return result
    def grouped(rr, key):
        out = defaultdict(list)
        for r in rr:
            out[key(r)].append(r)
        return out
    old_categories = {k: -sum(r['amount'] for r in rr) for k, rr in grouped(
        [r for r in prior if r['kind'] == 'consumption'], lambda r: r['category']).items()}
    cats = grouped([r for r in active if r['kind'] == 'consumption'], lambda r: r['category'])
    history_months = sorted({r['date'][:7] for r in rows if current.bounded and r['date']<current.start})
    history_by_category = defaultdict(lambda: defaultdict(float))
    for r in filtered(rows):
        if r['kind']=='consumption' and current.bounded and r['date']<current.start:
            history_by_category[r['category']][r['date'][:7]] -= r['amount']
    categories = []
    for name in cats.keys() | old_categories.keys():
        amount = round(-sum(r['amount'] for r in cats.get(name, [])), 2)
        before = round(old_categories.get(name, 0), 2)
        history_totals = [history_by_category[name].get(m, 0) for m in history_months]
        categories.append({'median': round(analytics.median(history_totals),2) if history_totals else None,
                           'moving_average': round(sum(history_totals[-3:])/len(history_totals[-3:]),2) if history_totals else None,
                           'trend': round(analytics.calendar_trend(history_months,history_totals),2) if len(history_totals)>1 else None,
                           'category': name, 'total': amount, 'previous': before,
                           'diff': round(amount-before, 2),
                           'pct': round((amount-before)/before*100, 1) if before > 0 else None})
    categories.sort(key=lambda r: r['total'], reverse=True)
    observed = {r['date'][:7] for r in selected}
    monthly = []
    cumulative = 0
    month_groups = grouped(active, lambda r: r['date'][:7])
    month_keys = current.months() if current.bounded else sorted(observed)
    # Preserve missing calendar months as unknown rather than fabricated zeros.
    for m in month_keys:
        if m > today.isoformat()[:7]:
            continue
        mp = periods.month_period(m)
        visible_month = periods.Period(max(mp.start,current.start) if current.start else mp.start,
            min(mp.end,current.end) if current.end else mp.end, m, 'month')
        known = m in observed or coverage(conn, visible_month)['complete']
        v = totals(month_groups.get(m, [])) if known else {k: None for k in ('income','consumption','savings','balance','savings_rate')}
        if known:
            cumulative += v['savings']
        monthly.append({'label': m, **v, 'cumulative': round(cumulative, 2) if known else None})
    quarterly = []
    for quarter, values in grouped(monthly, lambda r: r['label'][:4]+'-Q'+str((int(r['label'][5:7])-1)//3+1)).items():
        valid = all(r['income'] is not None for r in values)
        quarterly.append({'label': quarter, **{k: round(sum(r[k] for r in values),2) if valid else None
            for k in ('income','consumption','savings')}})
    single_month = current.bounded and current.start[:7] == (date.fromisoformat(current.end)-timedelta(days=1)).isoformat()[:7]
    if single_month:
        day_groups = grouped(active, lambda r: r['date'])
        series = []
        day = date.fromisoformat(current.start)
        while day.isoformat() < current.end:
            series.append({'label': day.isoformat(), **totals(day_groups.get(day.isoformat(), []))})
            day += timedelta(days=1)
    else:
        series = monthly
    necessity = {'essential': 0, 'discretionary': 0}
    for r in active:
        if r['kind'] == 'consumption':
            necessity[r['necessity']] -= r['amount']
    necessity = {k: round(v, 2) for k, v in necessity.items()}
    necessity_monthly = []
    for m in monthly:
        rr = month_groups.get(m['label'], [])
        necessity_monthly.append({'label': m['label'], **{
            k: round(-sum(r['amount'] for r in rr if r['kind']=='consumption' and r['necessity']==k),2)
            if m['consumption'] is not None else None for k in necessity}})
    fixed_keys = {(r['source'], r['account_name'], r['merchant_name'], r['category'])
                  for r in _recurring_items(conn, as_of=today.isoformat())
                  if r['kind']=='consumption' and r['type']=='fix' and not r['excluded']}
    def expense_structure(rr):
        fixed = round(-sum(r['amount'] for r in rr if r['kind']=='consumption' and
                           (r['source'], r['account_name'], r['merchant_name'], r['category']) in fixed_keys), 2)
        total = totals(rr)['consumption']
        return {'fixed': fixed, 'variable': round(total-fixed, 2)}
    structure = expense_structure(active)
    structure_monthly = [{'label': m['label'], **(expense_structure(month_groups.get(m['label'], []))
                         if m['consumption'] is not None else {'fixed': None, 'variable': None})} for m in monthly]
    observed_months = [i for i, m in enumerate(monthly) if m['consumption'] is not None]
    expense_monthly_average = None
    if len(observed_months) >= 3:
        count = len(observed_months)
        expense_monthly_average = {'months': count,
            'partial': bool(current.bounded and
                            (date.fromisoformat(current.start).day != 1 or
                             date.fromisoformat(current.end).day != 1)),
            **{key: round(sum(values[i][key] for i in observed_months) / count, 2)
               for values, keys in ((structure_monthly, ('fixed', 'variable')),
                                    (necessity_monthly, ('essential', 'discretionary')))
               for key in keys}}
    outliers = [{**r, 'threshold': thresholds[r['category']],
                 'factor': round(abs(r['amount']) / thresholds[r['category']], 1)} for r in selected if r['outlier']]
    outliers.sort(key=lambda r: abs(r['amount']), reverse=True)
    detail_rows = [r for r in active if r['kind']=='consumption' and r['category']==category] if category else []
    detail_groups = grouped(detail_rows, lambda r: r['date'])
    detail_series = [{'label': d, 'total': round(-sum(r['amount'] for r in rr), 2)} for d, rr in sorted(detail_groups.items())]
    # Detail rows are allocations: amounts sum exactly to the selected category,
    # including split bookings and refunds. The original transaction ID is kept.
    goal_row = conn.execute("SELECT value FROM settings WHERE key='savings-goal'").fetchone()
    try:
        goal = float(goal_row[0]) if goal_row else None
    except (ValueError, TypeError):
        goal = None
    if goal is not None and not math.isfinite(goal):
        goal = None
    q = coverage(conn, current)
    pq = coverage(conn, previous) if previous else None
    previous_observed = bool(prior_all) or bool(pq and pq['complete'])
    return {'period': _period_info(current), 'selection': _period_info(p),
            'previous_period': _period_info(previous) if previous else None,
            'comparison_available': bool(previous and previous_observed),
            'comparison_verified': bool(previous and q['complete'] and pq['complete']),
            'previous_quality': pq, 'quality': q, 'today': today.isoformat(),
            'summary': totals(active), 'previous': totals(prior) if previous_observed else None,
            'count': len({r['id'] for r in active}), 'series': series,
            'granularity': 'day' if single_month else 'month', 'monthly': monthly, 'quarterly': quarterly,
            'categories': categories, 'necessity': necessity, 'necessity_monthly': necessity_monthly,
            'expense_structure': structure, 'expense_structure_monthly': structure_monthly,
            'expense_monthly_average': expense_monthly_average,
            'income_categories': [{'category': k, 'total': round(sum(r['amount'] for r in rr), 2)}
                for k, rr in grouped([r for r in active if r['kind']=='income'], lambda r:r['category']).items()],
            'outliers': outliers, 'excluded_count': len(outliers) if exclude_outliers else 0,
            'excluded_total': round(sum(-r['amount'] for r in outliers),2) if exclude_outliers else 0,
            'goal': goal, 'bookings': active if include_bookings else [], 'detail': {'category': category, 'total': round(-sum(r['amount'] for r in detail_rows),2),
                'transactions': detail_rows, 'series': detail_series}}
