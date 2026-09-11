import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from app import database as db, main as api, periods, analytics
from app.parsers.comdirect import parse_comdirect_csv
from app.parsers import hanseaticbank as hb


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.patch = patch.object(db, 'DB_PATH', self.tmp.name + '/test.db')
        self.patch.start()
        db.init_db()
        self.conn = db.get_db()

    def tearDown(self):
        self.conn.close()
        self.patch.stop()
        self.tmp.cleanup()

    def tx(self, day, amount, category='Einkaufen', merchant='Shop', booked=1, account='test'):
        self.conn.execute('INSERT INTO transactions(source,account_name,date,amount,category,merchant_name,booked) VALUES (?,?,?,?,?,?,?)',
                          ('test', account, day, amount, category, merchant, booked))
        self.conn.commit()

    def test_fresh_kinds_and_restart(self):
        for name, kind in db.CATEGORY_KINDS.items():
            self.assertEqual(self.conn.execute('SELECT kind FROM categories WHERE name=?',(name,)).fetchone()[0],kind)
        self.conn.execute("UPDATE categories SET kind='transfer' WHERE name='Einkaufen'")
        self.conn.commit()
        db.init_db()
        self.assertEqual(self.conn.execute("SELECT kind FROM categories WHERE name='Einkaufen'").fetchone()[0], 'transfer')

    def test_refunds_consistent(self):
        self.tx('2026-08-01', -100)
        self.tx('2026-08-02', 40)
        self.assertEqual(api.stats_summary(period='2026-08', month=None)['consumption'], 60)
        self.assertEqual(api.budget_status(period='2026-08', month=None)['categories'][0]['spent'], 60)
        self.assertEqual(api.stats_timeline(category='Einkaufen',period='2026-08',exclude_outliers=False,window=3)['points'][0]['total'],60)

    def test_pending_excluded(self):
        self.tx('2026-08-01',-100,booked=0)
        self.assertEqual(api.stats_summary(period='2026-08', month=None)['consumption'],0)

    def test_exact_custom_period(self):
        self.tx('2026-09-01',-100)
        self.tx('2026-09-20',-200)
        result=api.stats_comparison(period='2026-09-10..2026-09-25',month=None,exclude_outliers=False)['categories'][0]
        self.assertEqual(result['current'],200)
        self.assertEqual(result['previous'],100)

    def test_category_zero_month_and_calendar_gap(self):
        self.tx('2026-01-01',-100)
        self.tx('2026-02-01',-50,'Lebensmittel')
        self.tx('2026-04-01',-200)
        result=api.stats_timeline(category='Einkaufen',period='2026',exclude_outliers=False,window=3)
        self.assertEqual([p['total'] for p in result['points']],[100,0,200])
        self.assertEqual(analytics.calendar_trend(['2026-01','2026-03'],[100,200]),50)

    def test_quarter_forecast_includes_third_rent(self):
        for day in ['2026-06-01','2026-07-01','2026-08-01']:
            self.tx(day,-1000,'Wohnen & Nebenkosten','Rent')
        with patch('app.main.date') as today:
            today.today.return_value=date(2026,9,11)
            result=api._fixed_forecast_context(self.conn,periods.parse('2026-Q3'),.8)
        self.assertEqual(result['projected_by_kind']['consumption'],3000)

    def test_quarterly_cadence(self):
        self.tx('2026-01-01',-120,'Finanzen & Versicherung','Insurance')
        self.tx('2026-04-01',-120,'Finanzen & Versicherung','Insurance')
        item=api._recurring_items(self.conn,as_of='2026-04-15')[0]
        self.assertEqual(item['yearly_cost'],480)
        self.assertEqual(item['next_expected'],'2026-07-01')
        self.assertFalse(api._recurring_items(self.conn,as_of='2026-11-01')[0]['active'])

    def test_accounts_not_merged(self):
        for account,amount in [('one',-100),('two',-200)]:
            for day in ['2026-07-01','2026-08-01']:
                self.tx(day,amount,account=account)
        self.assertEqual(len(api._recurring_items(self.conn,as_of='2026-08-15')),2)

    def test_future_income_and_savings(self):
        forecast=api._forecast(.5,0,100,0,{'actual_by_kind':{},'projected_by_kind':{'income':3000,'savings':500}})
        self.assertEqual(forecast['balance'],2300)

    def test_pending_reconciliation_keeps_manual_category(self):
        template='Buchungstag;Wertstellung;Vorgang;Buchungstext;Umsatz\n{day};10.09.2026;Lastschrift;Café;-10,00\n'
        pending=parse_comdirect_csv(template.format(day='offen').encode())
        booked=parse_comdirect_csv(template.format(day='11.09.2026').encode())
        self.assertEqual(booked[0]['description'],'Café')
        api._insert_transactions(pending)
        api.update_category(1,{'category':'Lebensmittel'})
        self.assertEqual(api._insert_transactions(booked)['reconciled'],1)
        self.assertEqual(api._insert_transactions(pending)['skipped_duplicates'],1)
        rows=self.conn.execute('SELECT * FROM transactions').fetchall()
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]['category'],'Lebensmittel')
        self.assertEqual(rows[0]['booked'],1)

    def test_manual_and_legacy_categories_are_protected(self):
        self.tx('2026-08-01',-100)
        api.update_category(1,{'category':'Lebensmittel'})
        api.create_rule({'pattern':'shop','category':'Mobilität'})
        api.apply_rules_to_all()
        self.assertEqual(self.conn.execute('SELECT category FROM transactions').fetchone()[0],'Lebensmittel')

    def test_surplus_rate(self):
        self.tx('2026-08-01',3000,'Einkommen')
        self.tx('2026-08-02',-2000)
        result=api.stats_summary(period='2026-08',month=None)
        self.assertEqual(result['surplus_rate'],33.3)
        self.assertEqual(result['savings_rate'],0)

    def test_custom_budget_proration(self):
        self.conn.execute("INSERT INTO category_budgets VALUES ('Einkaufen',300)")
        self.conn.commit()
        result=api.budget_status(period='2026-09-01..2026-09-10',month=None)
        self.assertEqual(result['categories'][0]['budget'],100)

    def test_stale_data_disables_forecast(self):
        self.tx('2020-01-01',-10)
        self.assertFalse(api._data_quality(self.conn)['forecast_ready'])

    def test_backtest_uses_only_confirmed_months(self):
        from app.forecast_validation import complete_months, evaluate
        self.tx('2026-01-01',-100)
        self.tx('2026-02-01',-100)
        self.assertEqual(complete_months(self.conn,'2026-03-01'),[])
        self.conn.execute("INSERT INTO import_coverage VALUES ('test','test','2026-01-01','2026-01-31')")
        self.conn.commit()
        self.assertEqual(complete_months(self.conn,'2026-03-01'),['2026-01'])
        self.assertEqual(evaluate(self.conn)['observations'],3)

    def test_backtest_does_not_see_later_bookings(self):
        for day in ['2026-06-01','2026-07-01','2026-08-01']:
            self.tx(day,-1000,'Wohnen & Nebenkosten','Rent')
        self.tx('2026-09-01',-9000,'Wohnen & Nebenkosten','Rent')
        result=api._fixed_forecast_context(self.conn,periods.parse('2026-Q3'),.5,as_of='2026-08-15')
        self.assertEqual(result['projected_by_kind']['consumption'],3000)

    def test_retail_outlier_not_hidden_by_repeat_purchases(self):
        rows=[{'merchant_name':'Retail','date':f'2026-0{i+1}-01','amount':a} for i,a in enumerate([-10,-20,-1000])]
        self.assertNotIn('Retail',analytics.recurring_merchants(rows))

    def test_learned_context_is_account_specific(self):
        def parsed(account):
            text='Buchungstag;Wertstellung;Vorgang;Buchungstext;Umsatz\n01.08.2026;01.08.2026;Lastschrift;Empfänger: Someone Buchungstext Zweck;-10,00\n'
            return parse_comdirect_csv(text.encode(),account_name=account)
        api._insert_transactions(parsed('one'))
        api.update_category(1,{'category':'Lebensmittel'})
        api._insert_transactions(parsed('two'))
        self.assertEqual(self.conn.execute('SELECT category FROM transactions WHERE id=2').fetchone()[0],'Sonstiges')


class PDFTests(unittest.TestCase):
    def parse_text(self, text):
        with patch.object(hb,'_extract_text',return_value=text):
            return hb.parse_hanseaticbank_pdf(b'')

    def test_settlement_placeholder_and_page_balances(self):
        rows=self.parse_text('''Abrechnungszeitraum: 01.08.2026 - 31.08.2026
Alter Saldo -100,00
01.08.2026 01.08.2026 Kartenumsatz
SHOP, CITY
8125 -10,00
Übertrag Saldo auf Seite 2 -110,00
Übertrag Saldo von Seite 1 -110,00
02.08.2026 - Gutschrift
Kartenabrechnung 07/2026
Hanseatic Bank
- 100,00
Neuer Saldo -10,00
999,00
''')
        self.assertEqual([r['amount'] for r in rows],[-10,100])
        self.assertEqual(rows[1]['category'],'Überweisung')
        self.assertEqual(rows.report['balance_checks'],3)

    def test_mismatched_statement_rejected(self):
        with self.assertRaisesRegex(ValueError,'Saldoabgleich'):
            self.parse_text('Alter Saldo 0,00\n01.08.2026 01.08.2026 Kartenumsatz\nSHOP\n8125 -10,00\nNeuer Saldo -99,00')

    def test_refund_is_not_settlement(self):
        rows=self.parse_text('Alter Saldo -10,00\n01.08.2026 01.08.2026 Gutschrift\nSHOP\n8125 10,00\nNeuer Saldo 0,00')
        self.assertNotEqual(rows[0]['category'],'Überweisung')
        self.assertEqual(rows[0]['amount'],10)

    def test_missing_row_amount_rejected(self):
        with self.assertRaisesRegex(ValueError,'nicht eindeutig'):
            self.parse_text('Alter Saldo 0,00\n01.08.2026 01.08.2026 Kartenumsatz\nSHOP\nNeuer Saldo -10,00')

    @unittest.skipUnless(os.environ.get('HANSEATIC_TEST_PDF'),'Set HANSEATIC_TEST_PDF for private real-statement test')
    def test_real_august_statement(self):
        rows=hb.parse_hanseaticbank_pdf(Path(os.environ['HANSEATIC_TEST_PDF']).read_bytes())
        # Independently transcribed amounts by page; no names/account data in fixtures.
        expected=[-4.50,-2.65,-12,169.22,-48.15,-1,-1.50,-86,-31,-1,-12.50,-8.10,-36.92,-4.40,-8,-62,-75,-18,-87.18,-2.99,-52,-8.40,-4,-12.80,-18,-8.40,-4.20,-8,-88.40,-16.50,-5,-9,-18.10,-5.50,-10.50,-16,-13,-45.50,-8.50,-14.70,-72.88,-66.76,-3,-7.69]
        self.assertEqual([r['amount'] for r in rows],expected)
        self.assertEqual(len({r['import_hash'] for r in rows}),44)
        self.assertEqual(rows.report['closing_balance'],-1019.72)
        self.assertEqual(rows.report['balance_checks'],5)
        self.assertEqual(rows.report['period_end'],'2026-08-25')


class PeriodTests(unittest.TestCase):
    def test_invalid_dates(self):
        with self.assertRaises(periods.PeriodError): periods.parse('2026-02-31..2026-03-03')


if __name__ == '__main__':
    unittest.main()
