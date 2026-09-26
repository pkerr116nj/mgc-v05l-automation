"""Local-only integrity inventory for the frozen NDXP Phase 1/2 inputs."""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import math
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from .ndxp_multidte_data import load_candidates, parse_osi, NEW_YORK, weekdays_between

WINDOW = '2021-09-24_2026-09-24'
DEGRADED = {'2024-06-03', '2025-10-22'}
MISSING = {'2026-09-22', '2026-09-23'}


def partition(day):
    day = str(day)
    if '2021-09-24' <= day <= '2024-12-31': return 'development'
    if '2025-01-01' <= day <= '2025-12-31': return 'validation'
    if '2026-01-01' <= day <= '2026-09-24': return 'final_holdout'
    raise ValueError(f'Outside frozen research window: {day}')


def development_only(rows):
    return [r for r in rows if partition(r['session_date']) == 'development']


def quality_flags(day):
    return {'degraded': str(day) in DEGRADED, 'missing_path_session': str(day) in MISSING}


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''): h.update(block)
    return h.hexdigest()


def manifest(root):
    return {str(p.relative_to(root)): {'bytes': p.stat().st_size, 'sha256': digest(p)}
            for p in sorted(root.rglob('*')) if p.is_file() and 'deep_dive_v1' not in p.relative_to(root).parts}


def dump(path, value):
    path.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + '\n')


def inventory(root):
    before = manifest(root)
    candidates = load_candidates(root / f'candidates_{WINDOW}.csv')
    sessions = {str(c.session_date) for c in candidates}
    invalid = []
    keys = Counter()
    for i, c in enumerate(candidates):
        keys[(str(c.session_date), c.calendar_dte, c.target_credit)] += 1
        expected = ((c.short_bid+c.short_ask)-(c.long_bid+c.long_ask))/2
        legs = [parse_osi(s) for s in (c.short_symbol, c.long_symbol)]
        if not (c.calendar_dte == (c.expiration-c.session_date).days in (2,3)
                and c.short_strike-c.long_strike == 10 and c.target_credit in (5,6,7)
                and abs(c.mid_credit-expected) < 1e-9
                and all(x[:3] == ('NDXP',c.expiration,'P') for x in legs)
                and [x[3] for x in legs] == [c.short_strike,c.long_strike]): invalid.append(i)
    counts = Counter(); quote_days = Counter(); bad = 0; lo = None; hi = None
    with (root / f'paths_{WINDOW}.csv').open(newline='') as f:
        for r in csv.DictReader(f):
            counts[r['session_date']] += 1
            t = datetime.fromisoformat(r['quote_time']).astimezone(NEW_YORK)
            quote_days[str(t.date())] += 1
            lo = min(lo,t) if lo else t; hi = max(hi,t) if hi else t
            b,a = float(r['bid']),float(r['ask'])
            bad += not (math.isfinite(b) and math.isfinite(a) and 0 <= b <= a and a > 0)
    failures = json.loads((root / f'paths_{WINDOW}.failures.json').read_text())
    discovery_failures = json.loads((root / f'candidates_{WINDOW}.failures.json').read_text())
    discovery = sorted(p.name[:10] for p in (root/'discovery_cache').rglob('*.dbn.zst'))
    missing = sorted(sessions-set(counts))
    report = {
        'phase': 1, 'remote_requests': 0, 'timezone': 'America/New_York',
        'input_manifest': before,
        'candidate_rows': len(candidates), 'candidate_sessions': len(sessions),
        'candidate_date_range': [min(sessions),max(sessions)],
        'candidate_groups': {f'{d}DTE_credit_{t:g}': sum(c.calendar_dte==d and c.target_credit==t for c in candidates) for d in (2,3) for t in (5,6,7)},
        'invalid_candidate_indices': invalid, 'duplicate_candidate_keys': [str(k) for k,v in keys.items() if v>1],
        'path_rows': sum(counts.values()), 'path_sessions': len(counts),
        'path_session_date_range': [min(counts),max(counts)], 'quote_time_range': [lo,hi],
        'path_rows_by_session': dict(sorted(counts.items())), 'invalid_quote_rows': bad,
        'missing_path_sessions': missing, 'path_failures': failures,
        'missing_matches_expected': set(missing)==MISSING=={x['session'] for x in failures},
        'discovery_cache_files': len(discovery), 'discovery_cache_date_range': [min(discovery),max(discovery)],
        'discovery_failures': discovery_failures,
        'discovery_requested_weekdays': len(weekdays_between(date(2021,9,24),date(2026,9,24))),
        'discovery_successful_sessions': len(weekdays_between(date(2021,9,24),date(2026,9,24)))-len(discovery_failures),
        'discovery_unique_cached_dates': len(set(discovery)),
        'discovery_success_evidence': '1,305 requested weekdays minus 51 saved failures = 1,254; DBN cache filenames may duplicate dates.',
        'degraded_dates': {d: {'source': 'governing specification; original condition-warning logs not found locally', 'path_quote_rows': quote_days[d], 'candidate_rows': sum(str(c.session_date)==d for c in candidates)} for d in sorted(DEGRADED)},
        'caveats': ['No settlement source verified; terminal quote is a proxy, not settlement.', 'Discovery success count derives from requested weekdays minus saved failures; DBN contents not re-decoded.'],
    }
    report['raw_inputs_unchanged'] = before == manifest(root)
    if invalid or not report['raw_inputs_unchanged']: raise ValueError('Input integrity check failed')
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('--root',type=Path,default=Path('output/ndxp_multidte'))
    a=p.parse_args(); out=a.root/'deep_dive_v1'; out.mkdir(exist_ok=True)
    result=inventory(a.root); dump(out/'data_quality_report.json',result)
    print(json.dumps({k:result[k] for k in ('candidate_rows','candidate_sessions','path_rows','path_sessions','missing_path_sessions','discovery_cache_files','raw_inputs_unchanged')}))

if __name__=='__main__': main()
