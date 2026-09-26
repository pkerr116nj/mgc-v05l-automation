"""Fixed execution sensitivity summaries; no fitting or parameter selection."""
from collections import defaultdict
from statistics import fmean, median


def summarize_trades(rows, keys=('calendar_dte','target_credit','exit_target','slippage','mode')):
    groups=defaultdict(list)
    for row in rows: groups[tuple(row[k] for k in keys)].append(row)
    result=[]
    for key,group in sorted(groups.items()):
        group.sort(key=lambda r:(r['session_date'],r['entry_time'],r['candidate_id']))
        pnl=[r['net_pnl'] for r in group]; wins=sum(p for p in pnl if p>0); losses=-sum(p for p in pnl if p<0)
        equity=peak=drawdown=0.; streak=longest=0
        for value in pnl:
            equity+=value; peak=max(peak,equity); drawdown=max(drawdown,peak-equity)
            streak=streak+1 if value<0 else 0; longest=max(longest,streak)
        result.append(dict(zip(keys,key), trades=len(group),win_rate=sum(p>0 for p in pnl)/len(pnl),
                           target_hit_rate=fmean(r['hit_target'] for r in group),avg_pnl=fmean(pnl),median_pnl=median(pnl),
                           total_pnl=sum(pnl),profit_factor=wins/losses if losses else None,
                           avg_return_on_max_risk=fmean(r['net_pnl']/r['max_risk'] for r in group),worst_trade=min(pnl),
                           max_drawdown=drawdown,longest_losing_streak=longest,
                           missing_next_minute=sum(r.get('missing_next_minute',False) for r in group)))
    return result
