"""Exercise migrated data without network or authentication side effects.
Run with DB_PATH pointing at a disposable SQLite backup.
"""
import inspect
import json
from app import main, database

before = database.get_db()
count = before.execute('SELECT COUNT(*) FROM transactions').fetchone()[0]
before.close()
database.init_db()
for name in ['stats_summary','stats_categories','stats_monthly','stats_timeline','stats_yearly',
             'stats_wealth','stats_comparison','stats_recurring','stats_outliers','budget_status',
             'list_transactions','data_quality','forecast_validation']:
    fn = getattr(main,name)
    args = {}
    for key,param in inspect.signature(fn).parameters.items():
        default=param.default
        if default is not inspect.Parameter.empty:
            args[key] = getattr(default,'default',default)
    result = fn(**args)
    json.dumps(result,allow_nan=False)
    print(name, 'OK')
after=database.get_db()
assert after.execute('SELECT COUNT(*) FROM transactions').fetchone()[0] == count
assert after.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
print('transactions preserved:',count)
