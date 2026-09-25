"""Backtest targeted 2DTE/3DTE NDXP short put verticals from cached CBBO-1m."""
from __future__ import annotations

import argparse, csv, json, math, statistics
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Sequence

from ndxp_multidte_data import Candidate, load_candidates

@dataclass(frozen=True)
class Outcome:
    session_date: date
    expiration: date
    dte_calendar: int
    target_credit: float
    entry_credit: float
    short_strike: float
    long_strike: float
    exit_rule: str
    exit_time: datetime | None
    exit_debit: float
    qty: int
    gross_pnl: float
    fees: float
    net_pnl: float
    max_loss: float
    return_on_max_risk: float
    min_debit_seen: float
    max_debit_seen: float
    time_to_min_minutes: float | None
    hit_target: bool


def load_paths(path: Path):
    by_symbol = defaultdict(list)
    with path.open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            bid=float(r["bid"]); ask=float(r["ask"])
            if bid < 0 or ask <= 0 or bid > ask: continue
            by_symbol[r["symbol"]].append((datetime.fromisoformat(r["quote_time"]), bid, ask))
    for rows in by_symbol.values(): rows.sort()
    return by_symbol


def pair_series(c: Candidate, paths):
    s = {t.replace(second=0, microsecond=0):(b,a) for t,b,a in paths.get(c.short_symbol, [])}
    l = {t.replace(second=0, microsecond=0):(b,a) for t,b,a in paths.get(c.long_symbol, [])}
    out=[]
    for t in sorted(set(s)&set(l)):
        if t < c.entry_time.replace(second=0,microsecond=0): continue
        sb,sa=s[t]; lb,la=l[t]
        mid=((sb+sa)/2)-((lb+la)/2)
        natural=sa-lb  # debit to buy back: short leg ask - long leg bid
        if 0 <= mid <= 10 and 0 <= natural <= 10:
            out.append((t,mid,natural))
    return out


def evaluate(candidates: Sequence[Candidate], paths, *, qty=20, fee_side=1.324, exit_targets=(3.0,2.0,1.0,0.5), fill_haircut=0.25):
    out=[]
    for c in candidates:
        series=pair_series(c,paths)
        if not series: continue
        entry=max(0.01, min(9.99, c.mid_credit-fill_haircut))
        debits=[x[2] for x in series]
        min_idx=min(range(len(series)), key=lambda i: series[i][2]); max_debit=max(debits)
        rules=[(f"target_{x:g}",x) for x in exit_targets] + [("eod_or_last",None)]
        for name,target in rules:
            chosen=None
            if target is not None:
                chosen=next((row for row in series if row[2] <= target),None)
            if chosen is None: chosen=series[-1]
            t,mid,natural=chosen
            exit_debit=natural
            gross=(entry-exit_debit)*100*qty
            fees=fee_side*qty*2
            max_loss=(10-entry)*100*qty + fees
            out.append(Outcome(c.session_date,c.expiration,c.dte_calendar,c.target_credit,entry,c.short_strike,c.long_strike,name,t,exit_debit,qty,gross,fees,gross-fees,max_loss,(gross-fees)/max_loss,series[min_idx][2],max_debit,(series[min_idx][0]-c.entry_time).total_seconds()/60, target is not None and chosen[2] <= target))
    return out


def summarize(rows):
    groups=defaultdict(list)
    for r in rows: groups[(r.dte_calendar,r.target_credit,r.exit_rule)].append(r)
    out=[]
    for key,trades in sorted(groups.items()):
        pnl=[x.net_pnl for x in trades]; wins=[x for x in pnl if x>0]; losses=[x for x in pnl if x<0]
        out.append({"dte":key[0],"target_credit":key[1],"exit_rule":key[2],"trades":len(trades),"win_rate":len(wins)/len(trades),"avg_pnl":statistics.fmean(pnl),"median_pnl":statistics.median(pnl),"total_pnl":sum(pnl),"profit_factor":sum(wins)/abs(sum(losses)) if losses else None,"avg_min_debit":statistics.fmean(x.min_debit_seen for x in trades),"target_hit_rate":sum(x.hit_target for x in trades)/len(trades),"avg_return_on_max_risk":statistics.fmean(x.return_on_max_risk for x in trades),"worst_trade":min(pnl),"best_trade":max(pnl)})
    return out


def write_csv(path, rows):
    if not rows: return
    with path.open("w",newline="",encoding="utf-8") as fh:
        w=csv.DictWriter(fh, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--candidates",type=Path,required=True); p.add_argument("--paths",type=Path,required=True); p.add_argument("--output-dir",type=Path,required=True)
    p.add_argument("--qty",type=int,default=20); p.add_argument("--fee-side",type=float,default=1.324); p.add_argument("--entry-haircut",type=float,default=0.25); p.add_argument("--exit-targets",default="3,2,1,0.5")
    a=p.parse_args(argv)
    candidates=load_candidates(a.candidates); paths=load_paths(a.paths)
    targets=tuple(float(x) for x in a.exit_targets.split(","))
    rows=evaluate(candidates,paths,qty=a.qty,fee_side=a.fee_side,exit_targets=targets,fill_haircut=a.entry_haircut)
    summary=summarize(rows); a.output_dir.mkdir(parents=True,exist_ok=True)
    write_csv(a.output_dir/"outcomes.csv",[{k:(v.isoformat() if hasattr(v,"isoformat") else v) for k,v in asdict(r).items()} for r in rows]); write_csv(a.output_dir/"summary.csv",summary)
    report={"counts":{"candidates":len(candidates),"outcomes":len(rows)},"assumptions":{"qty":a.qty,"fee_per_contract_side":a.fee_side,"entry_mid_haircut":a.entry_haircut,"exit_uses_natural_debit":True,"targets":targets},"summary":summary}
    (a.output_dir/"report.json").write_text(json.dumps(report,indent=2,default=str)+"\n")
    print(json.dumps(report["counts"]))
    return 0
if __name__=="__main__": raise SystemExit(main())
