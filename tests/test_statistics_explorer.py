import json
import tempfile
import unittest
from datetime import date
from unittest.mock import patch
from app import database, main, periods
from app.statistics_explorer import build, comparison_period, coverage


class StatisticsExplorerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(database, 'DB_PATH', self.temp.name+'/stats.db')
        self.db_patch.start()
        database.init_db()
        self.conn = database.get_db()

    def tearDown(self):
        self.conn.close()
        self.db_patch.stop()
        self.temp.cleanup()

    def tx(self, day, amount, category='Einkaufen', booked=1, merchant='Shop'):
        self.conn.execute('INSERT INTO transactions(source,account_name,date,amount,category,merchant_name,booked) VALUES (?,?,?,?,?,?,?)',
                          ('test','Giro',day,amount,category,merchant,booked))
        self.conn.commit()
        return self.conn.execute('SELECT MAX(id) FROM transactions').fetchone()[0]

    def view(self, period='2026-08', **kw):
        return build(self.conn,periods.parse(period,'2026-08'),today=date(2026,10,16),**kw)

    def test_current_month_same_calendar_progress(self):
        current,prev=comparison_period(periods.parse('2026-10'),date(2026,10,16))
        self.assertEqual((current.start,current.end),('2026-10-01','2026-10-17'))
        self.assertEqual((prev.start,prev.end),('2026-09-01','2026-09-17'))
        self.tx('2026-09-16',-30)
        self.tx('2026-09-17',-1000)
        self.tx('2026-10-16',-40)
        v=self.view('2026-10')
        self.assertEqual(v['previous']['consumption'],30)
        self.assertEqual(v['summary']['consumption'],40)
        self.assertFalse(v['comparison_verified'])

    def test_calendar_clamp_quarter_and_year(self):
        _,prev=comparison_period(periods.parse('2026-03'),date(2026,3,31))
        self.assertEqual(prev.end,'2026-03-01')
        current,prev=comparison_period(periods.parse('2026-Q4'),date(2026,10,16))
        self.assertEqual((current.end,prev.start,prev.end),('2026-10-17','2026-07-01','2026-07-17'))
        _,prev=comparison_period(periods.parse('2024'),date(2024,2,29))
        self.assertEqual(prev.end,'2023-03-01')

    def test_all_data_has_no_implicit_comparison(self):
        self.tx('2026-08-01',-10)
        v=self.view('all')
        self.assertIsNone(v['previous_period'])
        self.assertFalse(v['comparison_available'])
        self.assertEqual(v['summary']['consumption'],10)

    def test_custom_equal_length_and_future_no_comparison(self):
        p=periods.parse('2026-08-04..2026-08-08')
        current,prev=comparison_period(p,date(2026,10,16))
        self.assertEqual((prev.start,prev.end),('2026-07-30','2026-08-04'))
        current,prev=comparison_period(periods.parse('2027-01'),date(2026,10,16))
        self.assertIsNone(prev)

    def test_refunds_splits_pending_and_details_reconcile(self):
        tx=self.tx('2026-08-12',-100)
        main.set_transaction_splits(tx,{'splits':[{'category':'Lebensmittel','amount':60},{'category':'Unterhaltung','amount':40}]})
        self.tx('2026-08-13',20,'Lebensmittel')
        self.tx('2026-08-14',-999,'Lebensmittel',booked=0)
        self.tx('2026-08-01',2000,'Einkommen')
        self.tx('2026-08-02',-500,'Sparen & Investieren')
        self.tx('2026-08-03',-1000,'Überweisung')
        v=self.view(category='Lebensmittel')
        self.assertEqual(v['summary']['consumption'],80)
        self.assertEqual(v['detail']['total'],40)
        self.assertEqual(sum(r['amount'] for r in v['detail']['transactions']),-40)
        self.assertEqual(v['necessity']['essential'],40)
        self.assertEqual(v['summary']['balance'],1420)
        self.assertEqual(v['summary']['savings_rate'],25)
        self.assertEqual(sum(r['consumption'] for r in v['series']),80)
        json.dumps(v,allow_nan=False)

    def test_outlier_exclusion_consistent_across_views(self):
        for day in range(1,9):
            self.tx(f'2026-08-{day:02d}',-20,merchant=f'Shop{day}')
        self.tx('2026-08-10',-500,merchant='Big purchase')
        v=self.view(exclude_outliers=True,category='Einkaufen',include_bookings=True)
        self.assertEqual((v['excluded_count'],v['excluded_total']),(1,500))
        self.assertEqual(v['summary']['consumption'],160)
        self.assertEqual(v['categories'][0]['total'],160)
        self.assertEqual(v['detail']['total'],160)
        self.assertEqual(v['necessity']['discretionary'],160)
        self.assertEqual(v['expense_structure'],{'fixed':0,'variable':160})
        self.assertEqual(-sum(r['amount'] for r in v['bookings']),160)
        self.assertEqual(sum(r['consumption'] for r in v['series']),160)
        self.assertEqual(v['monthly'][0]['consumption'],160)

    def test_fixed_and_variable_booked_expenses_follow_recurring_type(self):
        for month in ('06','07','08'):
            self.tx(f'2026-{month}-03',-100,'Wohnen & Nebenkosten',merchant='Miete')
        self.tx('2026-08-12',-40,'Einkaufen',merchant='Laden')
        v=self.view()
        self.assertEqual(v['summary']['consumption'],140)
        self.assertEqual(v['expense_structure'],{'fixed':100,'variable':40})
        self.assertEqual(v['necessity'],{'essential':100,'discretionary':40})
        self.assertEqual(v['expense_structure_monthly'][0]['fixed'],100)
        average=self.view('2026-06-01..2026-08-31')['expense_monthly_average']
        self.assertEqual(average,{'months':3,'partial':False,'fixed':100,'variable':13.33,
                                  'essential':100,'discretionary':13.33})
        self.assertTrue(self.view('2026-06-01..2026-08-15')['expense_monthly_average']['partial'])
        self.conn.execute("INSERT INTO merchant_overrides (merchant_name,category,recurring_type) VALUES ('Miete','Wohnen & Nebenkosten','variabel')")
        self.conn.commit()
        self.assertEqual(self.view()['expense_structure'],{'fixed':0,'variable':140})

    def test_month_gaps_unknown_and_zero_income_rate_missing(self):
        self.tx('2026-06-01',-10)
        self.tx('2026-08-01',-20)
        v=self.view('2026-06-01..2026-08-31')
        self.assertEqual(v['monthly'][1]['label'],'2026-07')
        self.assertIsNone(v['monthly'][1]['consumption'])
        self.assertIsNone(v['monthly'][1]['cumulative'])
        self.assertEqual(v['expense_structure_monthly'][1],{'label':'2026-07','fixed':None,'variable':None})
        self.assertIsNone(v['expense_monthly_average'])
        self.assertIsNone(v['summary']['savings_rate'])
        self.assertIsNone(v['monthly'][0]['savings_rate'])
        self.assertEqual(v['monthly'][2]['cumulative'],0)

    def test_coverage_requires_union_for_every_account(self):
        self.tx('2026-08-03',-10)
        self.conn.executemany('INSERT INTO import_coverage VALUES (?,?,?,?)',[
            ('test','Giro','2026-08-01','2026-08-10'),('test','Giro','2026-08-11','2026-08-31')])
        self.conn.commit()
        self.assertTrue(coverage(self.conn,periods.parse('2026-08'))['complete'])
        self.conn.execute("INSERT INTO transactions(source,account_name,date,amount,category) VALUES ('test','Karte','2026-08-03',-1,'Einkaufen')")
        self.conn.commit()
        v=self.view()
        self.assertFalse(v['quality']['complete'])
        self.assertEqual(v['quality']['status'],'partial')

    def test_coverage_hole_cannot_be_hidden_by_last_date(self):
        self.tx('2026-08-03',-10)
        self.conn.executemany('INSERT INTO import_coverage VALUES (?,?,?,?)',[
            ('test','Giro','2026-08-01','2026-08-10'),('test','Giro','2026-08-12','2026-08-31')])
        self.conn.commit()
        self.assertFalse(coverage(self.conn,periods.parse('2026-08'))['complete'])

    def test_confirmed_comparison_and_new_category(self):
        self.tx('2026-07-01',2000,'Einkommen')
        self.tx('2026-08-01',-10)
        self.conn.execute("INSERT INTO import_coverage VALUES ('test','Giro','2026-07-01','2026-08-31')")
        self.conn.commit()
        v=self.view()
        self.assertTrue(v['comparison_verified'])
        self.assertIsNone(v['categories'][0]['pct'])

    def test_goal_and_bookings_opt_in(self):
        self.tx('2026-08-01',-10)
        self.conn.execute("INSERT INTO settings VALUES ('savings-goal','40')")
        self.conn.commit()
        self.assertEqual(self.view()['goal'],40)
        self.assertEqual(self.view()['bookings'],[])
        self.assertEqual(len(self.view(include_bookings=True)['bookings']),1)

    def test_confirmed_zero_month_is_known_and_comparable(self):
        self.tx('2026-08-01',-10)
        self.conn.execute("INSERT INTO import_coverage VALUES ('test','Giro','2026-07-01','2026-08-31')")
        self.conn.commit()
        v=self.view('2026-08')
        self.assertTrue(v['comparison_available'])
        self.assertEqual(v['previous']['consumption'],0)
        july=self.view('2026-07')
        self.assertEqual(july['monthly'][0]['consumption'],0)

    def test_quarter_aggregation_retains_missing_months(self):
        self.tx('2026-07-01',-10)
        self.tx('2026-09-01',-20)
        v=self.view('2026-Q3')
        self.assertEqual(v['quarterly'][0]['label'],'2026-Q3')
        self.assertIsNone(v['quarterly'][0]['consumption'])
        self.tx('2026-08-01',-5)
        self.assertEqual(self.view('2026-Q3')['quarterly'][0]['consumption'],35)

    def test_no_bookings_serializes_without_nan(self):
        v=self.view()
        self.assertEqual(v['count'],0)
        self.assertFalse(v['comparison_available'])
        json.dumps(v,allow_nan=False)


if __name__=='__main__':
    unittest.main()
