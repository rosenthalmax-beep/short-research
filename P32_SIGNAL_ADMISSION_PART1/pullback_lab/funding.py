"""Funding rates in BASE currency; no missing historical rate becomes zero."""
import csv
import math
from datetime import datetime, time, timedelta
from pathlib import Path
from . import core as e

MODES = ('ZERO_FINANCING_CONTROL', 'FROZEN_14OCT2026_RATES', 'ADVERSE_3PCT_ANNUAL', 'ADVERSE_6PCT_ANNUAL')


class MissingHistoricalRate(Exception):
    pass


def rollovers(start, end):
    day = e.dt(start).astimezone(e.NY).date() - timedelta(days=1)
    through = e.dt(end).astimezone(e.NY).date()
    while day <= through:
        t = datetime.combine(day, time(17), e.NY).astimezone(e.UTC)
        if e.dt(start) <= t <= e.dt(end):
            yield t
        day += timedelta(days=1)


class Funding:
    def __init__(self, schedules, historical_file=None):
        self.schedules = schedules
        self.historical = {}
        self.coverage = set()
        if historical_file and Path(historical_file).is_file():
            with Path(historical_file).open(newline='', encoding='utf-8-sig') as f:
                for row in csv.DictReader(f):
                    day = datetime.strptime(row['ny_date'], '%Y-%m-%d').date().isoformat()
                    pair = row['pair']
                    e.require(pair in e.PAIRS, 'Invalid historical funding pair')
                    key = (day, pair)
                    e.require(key not in self.historical, 'Duplicate historical funding row')
                    rates = [float(row[k]) for k in ('long_rate', 'short_rate', 'charged_days')]
                    e.require(all(math.isfinite(v) for v in rates) and abs(rates[0]) <= 1 and abs(rates[1]) <= 1 and 0 <= rates[2] <= 14, 'Invalid funding units/rate/day multiplier')
                    e.require(bool(row['source'].strip()) and row['basis'] == 'FINALIZED_DAILY_RATE', 'Historical rates need a source and FINALIZED_DAILY_RATE basis')
                    e.require(row.get('rate_basis') == 'ANNUAL_ONE_DAY_BASE_NOTIONAL', 'Normalize annual base-notional rates separately from charged_days; do not triple an already-multiplied rate')
                    self.historical[key] = dict(long_rate=rates[0], short_rate=rates[1], days=rates[2], source=row['source'])

    def terms(self, mode, pair, side, when):
        local = e.dt(when).astimezone(e.NY)
        date = local.date().isoformat()
        if mode == 'HISTORICAL_FILE':
            self.coverage.add((date, pair))
            row = self.historical.get((date, pair))
            if row is None:
                raise MissingHistoricalRate(date + ' ' + pair)
            return dict(annual_rate=row['long_rate' if side == 'BUY' else 'short_rate'],
                        charged_days=row['days'], source=row['source'],
                        historical_rate_supplied=True, historical_rate_independently_verified=False,
                        holiday_schedule_assumed=False)
        e.require(mode in MODES, 'Unknown funding mode')
        schedule = self.schedules[pair]
        days = float(schedule['days'][e.DAYS[local.weekday()]])
        rate = 0. if mode == MODES[0] else float(schedule['long_rate' if side == 'BUY' else 'short_rate']) if mode == MODES[1] else -.03 if mode == MODES[2] else -.06
        return dict(annual_rate=rate, charged_days=days, source=mode,
                    historical_rate_supplied=False, historical_rate_independently_verified=False,
                    holiday_schedule_assumed=True)

    def coverage_report(self):
        requested = sorted(self.coverage)
        return [dict(ny_date=d, pair=p, supplied=(d, p) in self.historical,
                     source=self.historical.get((d, p), {}).get('source'),
                     independent_verification=False) for d, p in requested]
