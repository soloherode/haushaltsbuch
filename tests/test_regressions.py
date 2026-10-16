import asyncio
import csv
import io
import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch
from fastapi import HTTPException

from app import database as db, main as api, periods, analytics
from app.categories import categorize_from_comdirect
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

    def test_split_preserves_original_and_allocates_reports(self):
        self.tx('2026-08-12', -100, 'Einkaufen', 'Supermarket')
        api.set_transaction_splits(1, {'splits': [
            {'category': 'Lebensmittel', 'amount': 60},
            {'category': 'Unterhaltung', 'amount': 40},
        ]})
        original = self.conn.execute('SELECT amount, category, category_locked FROM transactions WHERE id=1').fetchone()
        self.assertEqual((original['amount'], original['category'], original['category_locked']), (-100, 'Einkaufen', 1))
        self.assertEqual(self.conn.execute('SELECT SUM(amount) FROM transaction_allocations').fetchone()[0], -100)
        summary = api.stats_summary(period='2026-08', month=None)
        self.assertEqual((summary['consumption'], summary['count']), (100, 1))
        cats = api.stats_categories(period='2026-08', month=None, type='expense', kind=None, exclude_outliers=False)
        self.assertEqual({r['category']: r['total'] for r in cats}, {'Lebensmittel': -60, 'Unterhaltung': -40})
        self.assertEqual(api.stats_timeline(category='Lebensmittel', period='2026-08', exclude_outliers=False, window=3)['points'][0]['total'], 60)
        self.assertEqual(api.stats_necessity(period='2026-08')['essential'], 60)
        self.assertEqual(api.stats_necessity(period='2026-08')['discretionary'], 40)
        self.conn.execute("INSERT INTO category_budgets VALUES ('Lebensmittel',80)")
        self.conn.commit()
        budget = api.budget_status(period='2026-08', month=None)
        self.assertEqual(next(r for r in budget['categories'] if r['category']=='Lebensmittel')['spent'], 60)

    def test_split_filter_returns_original_with_allocated_sum(self):
        self.tx('2026-08-12', -100, 'Einkaufen', 'Supermarket')
        api.set_transaction_splits(1, {'splits': [
            {'category': 'Lebensmittel', 'amount': 60},
            {'category': 'Unterhaltung', 'amount': 40},
        ]})
        result = api.list_transactions(page=1, page_size=50, category='Lebensmittel',
            source=None, month=None, period='2026-08', search=None, sort='date', dir='desc', only_outliers=False)
        self.assertEqual(result['total'], 1)
        self.assertEqual(result['total_sum'], -60)
        self.assertEqual(result['transactions'][0]['amount'], -100)
        self.assertEqual(result['transactions'][0]['splits'], [
            {'category': 'Lebensmittel', 'amount': -60},
            {'category': 'Unterhaltung', 'amount': -40},
        ])

    def test_split_validation_is_atomic_and_delete_cascades(self):
        self.tx('2026-08-12', -100)
        valid = {'splits': [{'category': 'Lebensmittel', 'amount': 60},
                            {'category': 'Unterhaltung', 'amount': 40}]}
        api.set_transaction_splits(1, valid)
        for invalid in [
            {'splits': [{'category': 'Lebensmittel', 'amount': 60}, {'category': 'Unterhaltung', 'amount': 39.99}]},
            {'splits': [{'category': 'Lebensmittel', 'amount': 60}, {'category': 'Lebensmittel', 'amount': 40}]},
            {'splits': [{'category': 'Lebensmittel', 'amount': 60.001}, {'category': 'Unterhaltung', 'amount': 39.999}]},
        ]:
            with self.assertRaises(HTTPException):
                api.set_transaction_splits(1, invalid)
        self.assertEqual(self.conn.execute('SELECT SUM(amount_cents) FROM transaction_splits').fetchone()[0], 10000)
        with self.assertRaises(HTTPException):
            api.update_category(1, {'category': 'Mobilität'})
        api.delete_transaction(1)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM transaction_splits').fetchone()[0], 0)

    def test_split_category_rename_and_removal(self):
        self.tx('2026-08-12', -100)
        api.set_transaction_splits(1, {'splits': [
            {'category': 'Lebensmittel', 'amount': 60},
            {'category': 'Unterhaltung', 'amount': 40},
        ]})
        api.rename_category('Unterhaltung', {'name': 'Freizeit'})
        self.assertEqual(self.conn.execute('SELECT category FROM transaction_splits WHERE position=1').fetchone()[0], 'Freizeit')
        api.set_transaction_splits(1, {'splits': []})
        self.assertEqual(self.conn.execute('SELECT category, amount FROM transaction_allocations').fetchone()[:], ('Einkaufen', -100))

    def test_split_refund_keeps_positive_amounts(self):
        self.tx('2026-08-12', 100)
        api.set_transaction_splits(1, {'splits': [
            {'category': 'Lebensmittel', 'amount': 60},
            {'category': 'Unterhaltung', 'amount': 40},
        ]})
        self.assertEqual([r['amount'] for r in self.conn.execute(
            'SELECT amount FROM transaction_allocations ORDER BY split_position')], [60, 40])
        self.assertEqual(api.stats_summary(period='2026-08', month=None)['consumption'], -100)

    def test_split_across_kinds_does_not_double_count(self):
        self.tx('2026-08-12', -100)
        api.set_transaction_splits(1, {'splits': [
            {'category': 'Lebensmittel', 'amount': 60},
            {'category': 'Sparen & Investieren', 'amount': 40},
        ]})
        summary = api.stats_summary(period='2026-08', month=None)
        self.assertEqual((summary['consumption'], summary['savings'], summary['balance'], summary['count']),
                         (60, 40, -100, 1))

    def test_split_category_deletion_merges_duplicate_remainder(self):
        self.tx('2026-08-12', -100)
        api.set_transaction_splits(1, {'splits': [
            {'category': 'Unterhaltung', 'amount': 60},
            {'category': 'Sonstiges', 'amount': 40},
        ]})
        api.delete_category('Unterhaltung')
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM transaction_splits').fetchone()[0], 0)
        row = self.conn.execute('SELECT category, amount FROM transaction_allocations').fetchone()
        self.assertEqual(row[:], ('Sonstiges', -100))

    def test_split_csv_export_retains_original_reference(self):
        self.tx('2026-08-12', -100)
        api.set_transaction_splits(1, {'splits': [
            {'category': 'Lebensmittel', 'amount': 60},
            {'category': 'Unterhaltung', 'amount': 40},
        ]})
        async def content():
            return b''.join([chunk async for chunk in api.export_csv(period='2026-08', month=None).body_iterator])
        rows = list(csv.DictReader(io.StringIO(asyncio.run(content()).decode('utf-8-sig')), delimiter=';'))
        self.assertEqual([(r['Kategorie'], r['Betrag'], r['Originalbetrag'], r['Teilposition']) for r in rows],
                         [('Lebensmittel', '-60.0', '-100.0', '1'), ('Unterhaltung', '-40.0', '-100.0', '2')])

    def test_split_outlier_filter_uses_matching_category_part(self):
        for i in range(5):
            self.tx(f'2026-08-{i+1:02d}', -10, 'Lebensmittel', f'Shop {i}')
        self.tx('2026-08-12', -1000, 'Einkaufen', 'Big shop')
        api.set_transaction_splits(6, {'splits': [
            {'category': 'Lebensmittel', 'amount': 150},
            {'category': 'Unterhaltung', 'amount': 850},
        ]})
        def listed(category):
            return api.list_transactions(page=1, page_size=50, category=category,
                source=None, month=None, period='2026-08', search=None,
                sort='date', dir='desc', only_outliers=True)
        self.assertEqual(listed('Unterhaltung')['total'], 0)
        food = listed('Lebensmittel')
        self.assertEqual(food['total'], 1)
        self.assertEqual(food['total_sum'], -150)
        self.assertEqual(food['transactions'][0]['outlier_category'], 'Lebensmittel')

    def test_split_recurring_forecast_uses_parts_once(self):
        for index, day in enumerate(['2026-06-05', '2026-07-05', '2026-08-05'], start=1):
            self.tx(day, -100, 'Einkaufen', 'Monthly bundle')
            api.set_transaction_splits(index, {'splits': [
                {'category': 'Lebensmittel', 'amount': 60},
                {'category': 'Unterhaltung', 'amount': 40},
            ]})
        with patch('app.main.date') as today:
            today.today.return_value = date(2026, 9, 1)
            context = api._fixed_forecast_context(self.conn, periods.parse('2026-09'), .5)
        self.assertEqual(context['projected_by_category']['Lebensmittel'], 60)
        self.assertEqual(context['projected_by_category']['Unterhaltung'], 40)
        self.assertEqual(context['projected_by_kind']['consumption'], 100)

    def test_split_survives_database_restart(self):
        self.tx('2026-08-12', -100)
        api.set_transaction_splits(1, {'splits': [
            {'category': 'Lebensmittel', 'amount': 60},
            {'category': 'Unterhaltung', 'amount': 40},
        ]})
        db.init_db()
        self.assertEqual(self.conn.execute('SELECT SUM(amount) FROM transaction_allocations').fetchone()[0], -100)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM transaction_splits').fetchone()[0], 2)

    def test_pending_excluded(self):
        self.tx('2026-08-01',-100,booked=0)
        self.assertEqual(api.stats_summary(period='2026-08', month=None)['consumption'],0)

    def test_pending_reserves_budget_until_manually_confirmed(self):
        self.conn.execute("INSERT INTO category_budgets VALUES ('Mobilität',350)")
        self.tx('2026-10-05', -1.86, 'Mobilität')
        self.tx('2026-10-14', -66.90, 'Mobilität', booked=0)
        self.tx('2026-10-15', 20, 'Mobilität', booked=0)
        self.tx('2026-09-30', -30, 'Mobilität', booked=0)
        self.tx('2026-10-14', -100, 'Überweisung', booked=0)
        status = api.budget_status(period='2026-10', month=None)
        mobility = next(r for r in status['categories'] if r['category'] == 'Mobilität')
        self.assertEqual((mobility['spent'], mobility['pending'], mobility['used']), (1.86, 66.9, 68.76))
        self.assertEqual((mobility['remaining'], mobility['pct']), (281.24, 19.6))
        api.confirm_booking(2)
        api.confirm_booking(2)  # Idempotent.
        mobility = next(r for r in api.budget_status(period='2026-10', month=None)['categories']
                        if r['category'] == 'Mobilität')
        self.assertEqual((mobility['spent'], mobility['pending'], mobility['used']), (68.76, 0, 68.76))
        self.assertEqual(api.stats_summary(period='2026-10', month=None)['consumption'], 68.76)

    def test_pending_budget_allocations_without_budget(self):
        self.tx('2026-10-14', -100, booked=0)
        api.set_transaction_splits(1, {'splits': [
            {'category': 'Mobilität', 'amount': 60},
            {'category': 'Sparen & Investieren', 'amount': 40},
        ]})
        rows = api.budget_status(period='2026-10', month=None)['categories']
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]['category'], rows[0]['spent'], rows[0]['pending'], rows[0]['used']),
                         ('Mobilität', 0, 60, 60))

    def test_budget_defaults_to_latest_month_even_with_only_pending(self):
        self.tx('2026-09-01', -10)
        self.tx('2026-10-14', -66.90, 'Mobilität', booked=0)
        result = api.budget_status(period='all', month=None)
        self.assertEqual(result['month'], '2026-10')
        self.assertEqual(result['categories'][0]['used'], 66.90)

    def test_confirm_booking_validates_transaction_and_date(self):
        with self.assertRaises(HTTPException) as error:
            api.confirm_booking(999)
        self.assertEqual(error.exception.status_code, 404)
        self.tx('0000-00-00', -10, booked=0)
        with self.assertRaises(HTTPException) as error:
            api.confirm_booking(1)
        self.assertEqual(error.exception.status_code, 400)
        self.assertEqual(self.conn.execute('SELECT booked FROM transactions').fetchone()[0], 0)

    def test_final_import_reconciles_manually_confirmed_booking(self):
        template = 'Buchungstag;Wertstellung;Vorgang;Buchungstext;Umsatz\n{day};14.10.2026;Lastschrift;BAYWA;-66,90\n'
        pending = parse_comdirect_csv(template.format(day='offen').encode())
        booked = parse_comdirect_csv(template.format(day='16.10.2026').encode())
        api._insert_transactions(pending)
        api.confirm_booking(1)
        api.update_category(1, {'category': 'Mobilität'})
        self.assertEqual(api._insert_transactions(booked)['reconciled'], 1)
        self.assertEqual(api._insert_transactions(pending)['skipped_duplicates'], 1)
        self.assertEqual(api._insert_transactions(booked)['skipped_duplicates'], 1)
        rows = self.conn.execute('SELECT * FROM transactions').fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]['booked'], rows[0]['booked_manually'], rows[0]['category']),
                         (1, 0, 'Mobilität'))

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
        self.tx('2025-10-01',-120,'Finanzen & Versicherung','Insurance')
        self.tx('2026-01-01',-120,'Finanzen & Versicherung','Insurance')
        self.tx('2026-04-01',-120,'Finanzen & Versicherung','Insurance')
        item=api._recurring_items(self.conn,as_of='2026-04-15')[0]
        self.assertEqual(item['yearly_cost'],480)
        self.assertEqual(item['next_expected'],'2026-07-01')
        self.assertFalse(api._recurring_items(self.conn,as_of='2026-11-01')[0]['active'])

    def test_recurring_rejects_repeat_purchases_without_schedule(self):
        cases = {
            'Only twice': [('2026-07-10', -20), ('2026-08-10', -20)],
            'Irregular months': [('2026-04-10', -20), ('2026-06-10', -20), ('2026-07-10', -20)],
            'Multiple purchases': [('2026-06-10', -20), ('2026-06-11', -20),
                                   ('2026-07-10', -20), ('2026-08-10', -20)],
            'Random dates': [('2026-06-02', -20), ('2026-07-15', -20), ('2026-08-28', -20)],
            'Volatile amounts': [('2026-06-10', -10), ('2026-07-10', -80), ('2026-08-10', -25)],
            'Large final purchase': [('2026-05-10', -20), ('2026-06-10', -20),
                                     ('2026-07-10', -20), ('2026-08-10', -200)],
            'Recently stable retailer': [('2026-05-10', -80), ('2026-06-10', -20),
                                        ('2026-07-10', -20), ('2026-08-10', -20)],
        }
        for merchant, entries in cases.items():
            for day, amount in entries:
                self.tx(day, amount, 'Einkaufen', merchant)
        self.assertEqual(api._recurring_items(self.conn, as_of='2026-09-01'), [])
        forecast = api._fixed_forecast_context(self.conn, periods.parse('2026-09'), .1, '2026-09-01')
        self.assertEqual(forecast['pending'], [])
        with patch('app.main.date') as today:
            today.today.return_value = date(2026, 9, 1)
            self.assertEqual(api.stats_recurring()['total_monthly'], 0)

    def test_recurring_accepts_calendar_schedule_and_moderate_variation(self):
        for day in ['2026-06-10', '2026-07-13', '2026-08-10']:
            self.tx(day, -20, 'Unterhaltung', 'Weekend shift')
        for day in ['2026-01-31', '2026-02-28', '2026-03-31']:
            self.tx(day, -30, 'Unterhaltung', 'Month end')
        for day, amount in [('2026-06-10', -90), ('2026-07-10', -100), ('2026-08-10', -110)]:
            self.tx(day, amount, 'Wohnen & Nebenkosten', 'Utilities')
        items = {r['merchant_name']: r for r in api._recurring_items(self.conn, as_of='2026-09-01')}
        self.assertEqual(set(items), {'Weekend shift', 'Month end', 'Utilities'})
        self.assertEqual(items['Weekend shift']['type'], 'fix')
        self.assertEqual(items['Month end']['type'], 'fix')
        self.assertEqual(items['Month end']['typical_day'], 31)
        self.assertEqual(items['Utilities']['type'], 'variabel')
        upcoming = api._upcoming_payments(list(items.values()), date(2026, 9, 1), 20)
        self.assertEqual([r['merchant_name'] for r in upcoming], ['Weekend shift'])

    def test_recurring_respects_manual_choice_for_short_history(self):
        for day in ['2026-07-10', '2026-08-10']:
            self.tx(day, -20, 'Unterhaltung', 'New subscription')
        self.assertEqual(api._recurring_items(self.conn, as_of='2026-09-01'), [])
        api.set_recurring_type('New subscription', 'Unterhaltung', {'type': 'fix'})
        item = api._recurring_items(self.conn, as_of='2026-09-01')[0]
        self.assertTrue(item['type_overridden'])
        self.assertEqual(item['type'], 'fix')
        api.clear_recurring_type('New subscription', 'Unterhaltung')
        self.assertEqual(api._recurring_items(self.conn, as_of='2026-09-01'), [])

    def test_recurring_keeps_excluded_false_positive_reachable(self):
        for day in ['2026-07-02', '2026-08-20']:
            self.tx(day, -20, 'Einkaufen', 'Old false positive')
        key = {'source': 'test', 'account_name': 'test',
               'merchant_name': 'Old false positive', 'category': 'Einkaufen'}
        api.set_recurring_exclusion({**key, 'excluded': True})
        item = api._recurring_items(self.conn, as_of='2026-09-01')[0]
        self.assertTrue(item['excluded'])
        self.assertFalse(item['active'])
        api.set_recurring_exclusion({**key, 'excluded': False})
        self.assertEqual(api._recurring_items(self.conn, as_of='2026-09-01'), [])

    def test_upcoming_includes_today_and_repeats_within_window(self):
        for day in ['2026-06-25', '2026-07-25', '2026-08-25']:
            self.tx(day, -20, 'Unterhaltung', 'Streaming')
        items = api._recurring_items(self.conn, as_of='2026-09-25')
        self.assertEqual(items[0]['next_expected'], '2026-09-25')
        upcoming = api._upcoming_payments(items, date(2026, 9, 25), 45)
        self.assertEqual([r['expected'] for r in upcoming], ['2026-09-25', '2026-10-25'])
        self.assertEqual([r['days_until'] for r in upcoming], [0, 30])

    def test_upcoming_omits_inactive_and_variable_items(self):
        for day in ['2026-01-01', '2026-02-01']:
            self.tx(day, -15, 'Unterhaltung', 'Old subscription')
        for day, amount in [('2026-07-10', -10), ('2026-08-10', -25)]:
            self.tx(day, amount, 'Unterhaltung', 'Variable subscription')
        items = api._recurring_items(self.conn, as_of='2026-09-01')
        self.assertEqual(api._upcoming_payments(items, date(2026, 9, 1), 45), [])

    def test_price_increase_keeps_stable_subscription_in_upcoming(self):
        for day, amount in [('2026-05-10', -10), ('2026-06-10', -10),
                            ('2026-07-10', -10), ('2026-08-10', -12)]:
            self.tx(day, amount, 'Unterhaltung', 'Streaming')
        items = api._recurring_items(self.conn, as_of='2026-09-01')
        self.assertEqual(items[0]['type'], 'fix')
        upcoming = api._upcoming_payments(items, date(2026, 9, 1), 30)
        self.assertEqual(upcoming[0]['expected'], '2026-09-10')
        self.assertEqual(upcoming[0]['amount'], 12)
        self.assertEqual(upcoming[0]['price_increase']['diff'], 2)

    def test_accounts_not_merged(self):
        for account,amount in [('one',-100),('two',-200)]:
            for day in ['2026-06-01','2026-07-01','2026-08-01']:
                self.tx(day,amount,account=account)
        self.assertEqual(len(api._recurring_items(self.conn,as_of='2026-08-15')),2)

    def test_recurring_exclusion_persists_and_can_be_restored(self):
        for day in ['2026-06-10', '2026-07-10', '2026-08-10']:
            self.tx(day, -20, 'Unterhaltung', 'Streaming')
        key = {'source': 'test', 'account_name': 'test',
               'merchant_name': 'Streaming', 'category': 'Unterhaltung'}
        api.set_recurring_type('Streaming', 'Unterhaltung', {'type': 'fix'})
        api.set_recurring_exclusion({**key, 'excluded': True})
        api.set_recurring_exclusion({**key, 'excluded': True})
        db.init_db()
        # Even additional imports must not re-enable a cancelled stream.
        self.tx('2026-08-11', -20, 'Unterhaltung', 'Streaming')
        items = api._recurring_items(self.conn, as_of='2026-09-01')
        self.assertTrue(items[0]['excluded'])
        self.assertFalse(items[0]['active'])
        self.assertEqual(api._upcoming_payments(items, date(2026, 9, 1), 45), [])
        forecast = api._fixed_forecast_context(self.conn, periods.parse('2026-09'), .1, '2026-09-01')
        self.assertEqual(forecast['pending'], [])
        with patch('app.main.date') as today:
            today.today.return_value = date(2026, 9, 1)
            summary = api.stats_recurring()
        self.assertEqual(summary['fixed_monthly'], 0)
        self.assertEqual(summary['total_yearly'], 0)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM transactions').fetchone()[0], 4)
        api.set_recurring_exclusion({**key, 'excluded': False})
        restored = api._recurring_items(self.conn, as_of='2026-09-01')[0]
        self.assertFalse(restored['excluded'])
        self.assertTrue(restored['active'])
        self.assertEqual(restored['type'], 'fix')
        self.assertEqual(len(api._upcoming_payments([restored], date(2026, 9, 1), 45)), 2)

    def test_recurring_exclusion_only_affects_selected_stream(self):
        for account, category, source in [('one', 'Unterhaltung', 'test'),
                                          ('two', 'Unterhaltung', 'test'),
                                          ('one', 'Einkaufen', 'test'),
                                          ('one', 'Unterhaltung', 'other')]:
            for day in ['2026-06-10', '2026-07-10', '2026-08-10']:
                self.tx(day, -20, category, 'Same merchant', account=account)
                self.conn.execute('UPDATE transactions SET source=? WHERE id=(SELECT MAX(id) FROM transactions)', (source,))
                self.conn.commit()
        api.set_recurring_exclusion({'source': 'test', 'account_name': 'one',
            'merchant_name': 'Same merchant', 'category': 'Unterhaltung', 'excluded': True})
        items = api._recurring_items(self.conn, as_of='2026-09-01')
        self.assertEqual(sum(r['excluded'] for r in items), 1)
        self.assertEqual(sum(r['active'] for r in items), 3)
        self.assertEqual(len(api._upcoming_payments(items, date(2026, 9, 1), 20)), 3)

    def test_recurring_exclusion_survives_category_rename(self):
        for day in ['2026-06-10', '2026-07-10', '2026-08-10']:
            self.tx(day, -20, 'Unterhaltung', 'Streaming')
        api.set_recurring_exclusion({'source': 'test', 'account_name': 'test',
            'merchant_name': 'Streaming', 'category': 'Unterhaltung', 'excluded': True})
        api.rename_category('Unterhaltung', {'name': 'Abos'})
        item = api._recurring_items(self.conn, as_of='2026-09-01')[0]
        self.assertEqual(item['category'], 'Abos')
        self.assertTrue(item['excluded'])
        self.assertFalse(item['active'])

    def test_recurring_exclusion_validation(self):
        key = {'source': 'test', 'account_name': 'test',
               'merchant_name': 'Missing', 'category': 'Unterhaltung'}
        for body in [{**key, 'excluded': 'false'}, {'excluded': True},
                     {**key, 'account_name': None, 'excluded': True}]:
            with self.assertRaises(HTTPException) as error:
                api.set_recurring_exclusion(body)
            self.assertEqual(error.exception.status_code, 400)
        with self.assertRaises(HTTPException) as error:
            api.set_recurring_exclusion({**key, 'excluded': True})
        self.assertEqual(error.exception.status_code, 404)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM recurring_exclusions').fetchone()[0], 0)

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

    def parsed_category_tx(self, day, description, merchant='Shop', amount=-10, account='test', source='test'):
        return {'source': source, 'account_name': account, 'date': day, 'transaction_date': day,
                'amount': amount, 'description': description, 'merchant_name': merchant,
                'category': categorize_from_comdirect('', description), 'subcategory': None,
                'city': None, 'country': None, 'transaction_type': 'Lastschrift', 'booked': 1,
                'import_hash': f'{source}|{account}|{day}|{merchant}|{description}|{amount}'}

    def test_learned_correction_precedes_rule_and_keeps_direction(self):
        api.create_rule({'pattern': 'amazon', 'category': 'Einkaufen'})
        api._insert_transactions([self.parsed_category_tx('2026-08-01', 'Amazon refund', 'Amazon', 10)])
        api.update_category(1, {'category': 'Einkommen'})
        result = api._insert_transactions([self.parsed_category_tx('2026-09-01', 'Amazon refund', 'Amazon', 10)])
        row = self.conn.execute('SELECT * FROM transactions WHERE id=2').fetchone()
        self.assertEqual((row['category'], row['category_origin']), ('Einkommen', 'learned_context'))
        self.assertEqual(result['unclassified'], 0)
        api.apply_rules_to_all()
        self.assertEqual(self.conn.execute('SELECT category FROM transactions WHERE id=2').fetchone()[0], 'Einkommen')
        api._insert_transactions([self.parsed_category_tx('2026-09-02', 'Amazon refund', 'Amazon', -10)])
        self.assertEqual(self.conn.execute('SELECT category FROM transactions WHERE id=3').fetchone()[0], 'Einkaufen')

    def seed_suggestion_examples(self, merchant='Local Store', category='Kinder', count=3):
        for i in range(count):
            tx = self.parsed_category_tx(f'2026-08-{i+1:02}', f'Order {i}', merchant)
            api._insert_transactions([tx])
            tx_id = self.conn.execute('SELECT MAX(id) FROM transactions').fetchone()[0]
            api.update_category(tx_id, {'category': category})

    def test_suggestions_only_for_new_relevant_import_rows(self):
        self.seed_suggestion_examples()
        unrelated = api._insert_transactions([self.parsed_category_tx('2026-10-01', 'New order', 'Other Store')])
        self.assertEqual(unrelated['suggestions'], [])
        tx = self.parsed_category_tx('2026-10-02', 'New order', 'Local Store')
        result = api._insert_transactions([tx, tx])
        self.assertEqual((result['inserted'], result['skipped_duplicates']), (1, 1))
        self.assertEqual(result['suggestions'], [{'merchant_name': 'Local Store', 'pattern': 'local store',
                                                'suggested_category': 'Kinder', 'count': 3}])
        self.assertEqual(api._insert_transactions([tx])['suggestions'], [])

    def test_suggestions_skip_learned_and_already_correct_rows(self):
        self.seed_suggestion_examples()
        learned = api._insert_transactions([self.parsed_category_tx('2026-10-01', 'Order 0', 'Local Store')])
        self.assertEqual(learned['suggestions'], [])
        self.assertEqual(self.conn.execute('SELECT category_origin FROM transactions ORDER BY id DESC').fetchone()[0], 'learned_context')
        tx = self.parsed_category_tx('2026-10-02', 'New order', 'Local Store')
        tx['category'] = 'Kinder'
        self.assertEqual(api._insert_transactions([tx])['suggestions'], [])

    def test_suggestions_require_distinct_consistent_examples(self):
        self.seed_suggestion_examples(count=2)
        # Editing one transaction repeatedly must not inflate the evidence.
        for category in ['Hobby', 'Kinder', 'Hobby', 'Kinder']:
            api.update_category(1, {'category': category})
        self.assertEqual(api._insert_transactions([self.parsed_category_tx('2026-10-01', 'New order', 'Local Store')])['suggestions'], [])
        self.seed_suggestion_examples(merchant='Mixed Store')
        api.update_category(4, {'category': 'Hobby'})
        self.assertEqual(api._insert_transactions([self.parsed_category_tx('2026-10-02', 'New order', 'Mixed Store')])['suggestions'], [])

    def test_suggestions_ignore_legacy_counter_and_payment_providers(self):
        self.conn.execute("INSERT INTO category_corrections VALUES ('Old Store', 'Kinder', 100)")
        self.conn.commit()
        self.assertEqual(api._insert_transactions([self.parsed_category_tx('2026-10-01', 'New order', 'Old Store')])['suggestions'], [])
        self.seed_suggestion_examples(merchant='PayPal Europe')
        self.assertEqual(api._insert_transactions([self.parsed_category_tx('2026-10-02', 'New order', 'PayPal Europe')])['suggestions'], [])

    def test_suggestions_skip_existing_normalized_rules_and_refunds(self):
        self.seed_suggestion_examples()
        refund = self.parsed_category_tx('2026-10-01', 'Refund', 'Local Store', amount=10)
        self.assertEqual(api._insert_transactions([refund])['suggestions'], [])
        api.create_rule({'pattern': ' LOCAL   STORE ', 'category': 'Kinder'})
        result = api._insert_transactions([self.parsed_category_tx('2026-10-02', 'New order', 'Local Store')])
        self.assertEqual(result['suggestions'], [])

    def test_suggestion_dismissal_persists_and_follows_category_rename(self):
        self.seed_suggestion_examples()
        body = {'merchant_name': ' LOCAL   STORE ', 'category': 'Kinder'}
        api.dismiss_import_suggestion(body)
        api.dismiss_import_suggestion(body)
        db.init_db()
        api.rename_category('Kinder', {'name': 'Familie'})
        self.assertEqual(api._insert_transactions([self.parsed_category_tx('2026-10-01', 'New order', 'Local Store')])['suggestions'], [])
        self.assertEqual(self.conn.execute('SELECT merchant_key, category FROM dismissed_rule_suggestions').fetchone()[:],
                         ('local store', 'Familie'))

    def test_suggestions_are_deduplicated_limited_and_use_full_pattern(self):
        merchants = [f'Local Store {i} with a long descriptive name beyond forty characters' for i in range(7)]
        for merchant in merchants:
            self.seed_suggestion_examples(merchant)
        imported = [self.parsed_category_tx('2026-10-01', 'New order', m) for m in merchants]
        imported.append(self.parsed_category_tx('2026-10-02', 'Another order', merchants[0]))
        suggestions = api._insert_transactions(imported)['suggestions']
        self.assertEqual(len(suggestions), 5)
        self.assertEqual(len({s['pattern'] for s in suggestions}), 5)
        self.assertEqual(suggestions[0]['pattern'], merchants[0].lower())

    def test_suggestions_exclude_merchants_with_split_purchases(self):
        self.seed_suggestion_examples()
        api.set_transaction_splits(1, {'splits': [
            {'category': 'Kinder', 'amount': 5}, {'category': 'Hobby', 'amount': 5}]})
        self.seed_suggestion_examples()
        self.assertEqual(api._insert_transactions([
            self.parsed_category_tx('2026-10-01', 'New order', 'Local Store')])['suggestions'], [])

    def test_suggestion_dismissal_validation(self):
        for body in [{}, {'merchant_name': 'Store', 'category': 'Unknown'}]:
            with self.assertRaises(HTTPException) as error:
                api.dismiss_import_suggestion(body)
            self.assertEqual(error.exception.status_code, 400)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM dismissed_rule_suggestions').fetchone()[0], 0)

    def test_batch_corrections_are_learned(self):
        for day, description, merchant in [('2026-08-01', 'Child clothes', 'Clothes shop'),
                                           ('2026-08-02', 'Dinner', 'Restaurant')]:
            api._insert_transactions([self.parsed_category_tx(day, description, merchant)])
        result = api.batch_update_category({'ids': [1, 2, 2, 999], 'category': 'Kinder'})
        self.assertEqual(result['updated'], 2)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM contextual_corrections').fetchone()[0], 2)
        api._insert_transactions([self.parsed_category_tx('2026-09-01', 'Child clothes', 'Clothes shop')])
        self.assertEqual(self.conn.execute('SELECT category FROM transactions WHERE id=3').fetchone()[0], 'Kinder')

    def test_learning_normalizes_formatting_but_keeps_meaningful_context(self):
        api._insert_transactions([self.parsed_category_tx('2026-08-01', 'Order 123\n CHILD', 'Store')])
        api.update_category(1, {'category': 'Kinder'})
        for day, description, merchant, account, source in [
            ('2026-09-01', '  order 123 child  ', 'STORE', 'test', 'test'),
            ('2026-09-02', 'Order 124 CHILD', 'Store', 'test', 'test'),
            ('2026-09-03', 'Order 123 ADULT', 'Store', 'test', 'test'),
            ('2026-09-04', 'Order 123 CHILD', 'Store', 'other', 'test'),
            ('2026-09-05', 'Order 123 CHILD', 'Store', 'test', 'other')]:
            api._insert_transactions([self.parsed_category_tx(day, description, merchant, account=account, source=source)])
        self.assertEqual([r[0] for r in self.conn.execute('SELECT category FROM transactions ORDER BY id')],
                         ['Kinder', 'Kinder', 'Sonstiges', 'Sonstiges', 'Sonstiges', 'Sonstiges'])

    def test_learning_without_merchant_and_from_older_batch_edits(self):
        api._insert_transactions([self.parsed_category_tx('2026-08-01', 'School payment', '')])
        # Simulate the old batch endpoint, which did not save a learning row.
        self.conn.execute("UPDATE transactions SET category='Kinder', category_origin='manual', category_locked=1 WHERE id=1")
        self.conn.commit()
        api._insert_transactions([self.parsed_category_tx('2026-09-01', 'School payment', '')])
        self.assertEqual(self.conn.execute('SELECT category FROM transactions WHERE id=2').fetchone()[0], 'Kinder')
        api.update_category(1, {'category': 'Hobby'})
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM contextual_corrections').fetchone()[0], 1)

    def test_conflicting_contexts_and_splits_are_not_learned(self):
        for day in ['2026-08-01', '2026-08-02']:
            api._insert_transactions([self.parsed_category_tx(day, 'Same description', 'Store')])
        api.update_category(1, {'category': 'Kinder'})
        api.update_category(2, {'category': 'Kleidung'})
        api._insert_transactions([self.parsed_category_tx('2026-09-01', 'Same description', 'Store')])
        self.assertEqual(self.conn.execute('SELECT category FROM transactions WHERE id=3').fetchone()[0], 'Sonstiges')
        api._insert_transactions([self.parsed_category_tx('2026-08-03', 'Mixed basket', 'Store')])
        api.update_category(4, {'category': 'Kinder'})
        api.set_transaction_splits(4, {'splits': [{'category': 'Kinder', 'amount': 5}, {'category': 'Kleidung', 'amount': 5}]})
        api._insert_transactions([self.parsed_category_tx('2026-09-03', 'Mixed basket', 'Store')])
        self.assertEqual(self.conn.execute('SELECT category FROM transactions WHERE id=5').fetchone()[0], 'Sonstiges')

    def test_specific_rule_precedes_payment_provider_unless_priority_is_explicit(self):
        api.create_rule({'pattern': 'paypal', 'category': 'Einkaufen'})
        api.create_rule({'pattern': 'cafe', 'category': 'Restaurant & Cafe'})
        self.assertEqual(api._apply_db_rules('PAYPAL *CAFE', '', self.conn), 'Restaurant & Cafe')
        api.create_rule({'pattern': 'paypal', 'category': 'Hobby', 'priority': 5})
        self.assertEqual(api._apply_db_rules('PAYPAL *CAFE', '', self.conn), 'Hobby')
        api.create_rule({'pattern': 'paypal *cafe', 'category': 'Restaurant & Cafe', 'priority': 5})
        self.assertEqual(api._apply_db_rules('PAYPAL *CAFE', '', self.conn), 'Restaurant & Cafe')

    def test_settlements_stay_protected_from_rules_and_learning(self):
        tx = self.parsed_category_tx('2026-08-01', 'Kartenabrechnung', 'Kartenabrechnung', 10)
        tx['category'] = 'Überweisung'
        api.create_rule({'pattern': 'kartenabrechnung', 'category': 'Einkommen'})
        api._insert_transactions([tx])
        api.apply_rules_to_all()
        self.assertEqual(self.conn.execute('SELECT category FROM transactions WHERE id=1').fetchone()[0], 'Überweisung')

    def test_import_unclassified_uses_final_categories_and_omits_duplicates(self):
        tx = self.parsed_category_tx('2026-08-01', 'Unknown school', 'School')
        api.create_rule({'pattern': 'school', 'category': 'Kinder'})
        result = api._insert_transactions([tx])
        self.assertEqual(result['unclassified'], 0)
        api.delete_rule(1)
        result = api._insert_transactions([tx])
        self.assertEqual((result['inserted'], result['skipped_duplicates'], result['unclassified']), (0, 1, 0))
        result = api._insert_transactions([self.parsed_category_tx('2026-08-02', 'Unidentified purchase')])
        self.assertEqual(result['unclassified'], 1)

    def test_builtin_categories_avoid_partial_words_and_provider_guesses(self):
        cases = [('Espresso Café', 'Restaurant & Cafe'), ('VisaBonus-Cashback', 'Sonstiges'),
                 ('VISA Amazon', 'Einkaufen'), ('PAYPAL unknown', 'Sonstiges'),
                 ('Riverty payment', 'Sonstiges'), ('Ref abo2xyz', 'Sonstiges'),
                 ('Musikschule', 'Kinder'), ('ZALANDO', 'Kleidung'), ('H&M', 'Kleidung'),
                 ('Zara', 'Kleidung'), ('ESSO Tankstelle', 'Mobilität'),
                 ('PAYPAL Netflix', 'Unterhaltung'), ('DM', 'Gesundheit'),
                 ('Deutsche   Bahn', 'Mobilität')]
        for description, expected in cases:
            with self.subTest(description=description):
                self.assertEqual(categorize_from_comdirect('', description), expected)

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
