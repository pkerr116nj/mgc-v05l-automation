"""Replay runner for the current v0.5l build."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
import json

from ..domain.enums import OrderIntentType, PositionSide, StrategyStatus
from ..domain.events import FillReceivedEvent, OrderIntentCreatedEvent, ServiceStartupEvent
from ..domain.models import Bar, StrategyState
from .container import ApplicationContainer
from .replay_reporting import build_session_lookup, build_summary_metrics, build_trade_ledger
from .strategy_runtime_registry import StandaloneStrategyRuntimeInstance


@dataclass(frozen=True)
class ReplayStrategySummary:
    standalone_strategy_id: str
    strategy_family: str
    instrument: str
    processed_bars: int
    order_intents: int
    fills: int
    entries: int
    exits: int
    long_entries: int
    short_entries: int
    final_position_side: PositionSide
    final_strategy_status: StrategyStatus
    realized_pnl: str | None
    unrealized_pnl: str | None
    cumulative_pnl: str | None
    pnl_unavailable_reason: str | None


@dataclass(frozen=True)
class ReplayAggregateSummary:
    standalone_strategy_count: int
    strategy_count: int
    standalone_strategy_ids: tuple[str, ...]
    processed_bars: int
    order_intents: int
    fills: int
    entries: int
    exits: int
    long_entries: int
    short_entries: int
    realized_pnl: str | None
    unrealized_pnl: str | None
    cumulative_pnl: str | None
    pnl_unavailable_reason: str | None


@dataclass(frozen=True)
class ReplayRunSummary:
    processed_bars: int
    order_intents: int
    fills: int
    long_entries: int
    short_entries: int
    exits: int
    final_position_side: PositionSide
    final_strategy_status: StrategyStatus
    primary_standalone_strategy_id: str | None
    per_strategy_summaries: tuple[ReplayStrategySummary, ...]
    aggregate_portfolio_summary: ReplayAggregateSummary


class ReplayCoordinator:
    """Coordinate replay execution across standalone runtime identities."""

    def __init__(self, container: ApplicationContainer) -> None:
        self._container = container

    def run_bars(self, bars: Iterable[Bar]) -> ReplayRunSummary:
        event_counts: dict[str, dict[str, int]] = {}
        for bar in bars:
            for standalone_strategy_id, routed_events in self._container.strategy_runtime_registry.process_bar(bar).items():
                counts = event_counts.setdefault(
                    standalone_strategy_id,
                    {
                        "order_intents": 0,
                        "fills": 0,
                        "entries": 0,
                        "exits": 0,
                        "long_entries": 0,
                        "short_entries": 0,
                    },
                )
                for event in routed_events:
                    if isinstance(event, OrderIntentCreatedEvent):
                        counts["order_intents"] += 1
                        if event.intent_type == OrderIntentType.BUY_TO_OPEN:
                            counts["entries"] += 1
                            counts["long_entries"] += 1
                        elif event.intent_type == OrderIntentType.SELL_TO_OPEN:
                            counts["entries"] += 1
                            counts["short_entries"] += 1
                        else:
                            counts["exits"] += 1
                    elif isinstance(event, FillReceivedEvent):
                        counts["fills"] += 1

        per_strategy = tuple(
            self._build_strategy_summary(instance, event_counts.get(instance.definition.standalone_strategy_id, {}))
            for instance in self._container.strategy_runtime_registry.instances
            if instance.strategy_engine is not None
        )

        aggregate = self._build_aggregate_summary(per_strategy)
        primary = self._container.strategy_runtime_registry.primary_engine_instance()
        primary_state = primary.strategy_engine.state if primary is not None and primary.strategy_engine is not None else None
        return ReplayRunSummary(
            processed_bars=aggregate.processed_bars,
            order_intents=aggregate.order_intents,
            fills=aggregate.fills,
            long_entries=aggregate.long_entries,
            short_entries=aggregate.short_entries,
            exits=aggregate.exits,
            final_position_side=primary_state.position_side if primary_state is not None else PositionSide.FLAT,
            final_strategy_status=primary_state.strategy_status if primary_state is not None else StrategyStatus.READY,
            primary_standalone_strategy_id=primary.definition.standalone_strategy_id if primary is not None else None,
            per_strategy_summaries=per_strategy,
            aggregate_portfolio_summary=aggregate,
        )

    def _build_strategy_summary(
        self,
        instance: StandaloneStrategyRuntimeInstance,
        counts: dict[str, int],
    ) -> ReplayStrategySummary:
        assert instance.strategy_engine is not None
        assert instance.repositories is not None
        state = instance.strategy_engine.state
        realized_pnl, unrealized_pnl, cumulative_pnl, unavailable_reason = self._build_strategy_pnl_summary(
            instance,
            state=state,
        )
        return ReplayStrategySummary(
            standalone_strategy_id=instance.definition.standalone_strategy_id,
            strategy_family=instance.definition.strategy_family,
            instrument=instance.definition.instrument,
            processed_bars=instance.repositories.processed_bars.count(),
            order_intents=int(counts.get("order_intents", 0)),
            fills=int(counts.get("fills", 0)),
            entries=int(counts.get("entries", 0)),
            exits=int(counts.get("exits", 0)),
            long_entries=int(counts.get("long_entries", 0)),
            short_entries=int(counts.get("short_entries", 0)),
            final_position_side=state.position_side,
            final_strategy_status=state.strategy_status,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            cumulative_pnl=cumulative_pnl,
            pnl_unavailable_reason=unavailable_reason,
        )

    def _build_strategy_pnl_summary(
        self,
        instance: StandaloneStrategyRuntimeInstance,
        *,
        state: StrategyState,
    ) -> tuple[str | None, str | None, str | None, str | None]:
        assert instance.settings is not None
        assert instance.repositories is not None
        point_value = instance.definition.point_value
        if point_value is None:
            return (
                None,
                None,
                None,
                "Point value is unavailable for this standalone strategy in the replay config, so replay P/L cannot be priced exactly.",
            )

        order_intent_rows = instance.repositories.order_intents.list_all()
        fill_rows = instance.repositories.fills.list_all()
        bars = instance.repositories.bars.list_recent(
            symbol=instance.definition.instrument,
            timeframe=instance.settings.timeframe,
            limit=max(instance.repositories.processed_bars.count(), 1),
        )
        ledger = build_trade_ledger(
            order_intent_rows,
            fill_rows,
            build_session_lookup(bars),
            point_value=point_value,
            bars=bars,
        )
        realized_pnl_decimal = build_summary_metrics(ledger).total_net_pnl
        latest_close = bars[-1].close if bars else None
        unrealized_decimal: Decimal | None = None
        if latest_close is not None and state.entry_price is not None and state.internal_position_qty:
            quantity = Decimal(abs(state.internal_position_qty))
            if state.position_side is PositionSide.LONG:
                unrealized_decimal = (latest_close - state.entry_price) * quantity * point_value
            elif state.position_side is PositionSide.SHORT:
                unrealized_decimal = (state.entry_price - latest_close) * quantity * point_value
            else:
                unrealized_decimal = Decimal("0")
        elif state.position_side is PositionSide.FLAT:
            unrealized_decimal = Decimal("0")

        cumulative_decimal = realized_pnl_decimal + unrealized_decimal if unrealized_decimal is not None else None
        unavailable_reason = None
        if unrealized_decimal is None:
            unavailable_reason = "Final replay mark is unavailable for the open position, so unrealized and cumulative P/L are unavailable."
        return (
            str(realized_pnl_decimal),
            str(unrealized_decimal) if unrealized_decimal is not None else None,
            str(cumulative_decimal) if cumulative_decimal is not None else None,
            unavailable_reason,
        )

    def _build_aggregate_summary(self, per_strategy: tuple[ReplayStrategySummary, ...]) -> ReplayAggregateSummary:
        realized_values = [Decimal(row.realized_pnl) for row in per_strategy if row.realized_pnl is not None]
        unrealized_values = [Decimal(row.unrealized_pnl) for row in per_strategy if row.unrealized_pnl is not None]
        cumulative_values = [Decimal(row.cumulative_pnl) for row in per_strategy if row.cumulative_pnl is not None]
        realized_available = len(realized_values) == len(per_strategy)
        unrealized_available = len(unrealized_values) == len(per_strategy)
        cumulative_available = len(cumulative_values) == len(per_strategy)

        unavailable_reason = None
        if not realized_available:
            unavailable_reason = "One or more standalone strategies do not have explicit replay point-value metadata, so aggregate priced P/L is partial."
        elif not unrealized_available:
            unavailable_reason = "One or more standalone strategies are missing a final replay mark for open-position unrealized P/L."
        elif not cumulative_available:
            unavailable_reason = "One or more standalone strategies are missing cumulative replay P/L inputs."

        return ReplayAggregateSummary(
            standalone_strategy_count=len(per_strategy),
            strategy_count=len(per_strategy),
            standalone_strategy_ids=tuple(row.standalone_strategy_id for row in per_strategy),
            processed_bars=sum(row.processed_bars for row in per_strategy),
            order_intents=sum(row.order_intents for row in per_strategy),
            fills=sum(row.fills for row in per_strategy),
            entries=sum(row.entries for row in per_strategy),
            exits=sum(row.exits for row in per_strategy),
            long_entries=sum(row.long_entries for row in per_strategy),
            short_entries=sum(row.short_entries for row in per_strategy),
            realized_pnl=str(sum(realized_values, Decimal("0"))) if realized_available else None,
            unrealized_pnl=str(sum(unrealized_values, Decimal("0"))) if unrealized_available else None,
            cumulative_pnl=str(sum(cumulative_values, Decimal("0"))) if cumulative_available else None,
            pnl_unavailable_reason=unavailable_reason,
        )


class StrategyServiceRunner:
    """Replay-first strategy runner for CSV-based research and validation."""

    def __init__(self, container: ApplicationContainer) -> None:
        self._container = container

    def bootstrap(self) -> ServiceStartupEvent:
        return ServiceStartupEvent(source="app.runner")

    def run_replay(self, csv_path: str | Path) -> ReplayRunSummary:
        return self.run_bars(self._container.replay_feed.iter_csv(csv_path))

    def run_bars(self, bars: Iterable[Bar]) -> ReplayRunSummary:
        return ReplayCoordinator(self._container).run_bars(bars)


def replay_summary_payload(summary: ReplayRunSummary) -> dict[str, object]:
    """Return a JSON-ready replay summary payload."""
    return asdict(summary)


def write_replay_summary_json(summary: ReplayRunSummary, output_path: str | Path) -> Path:
    """Write a replay summary JSON artifact."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(replay_summary_payload(summary), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def render_replay_summary_markdown(summary: ReplayRunSummary) -> str:
    """Render a compact human-readable replay summary."""
    aggregate = summary.aggregate_portfolio_summary
    lines = [
        "# Replay Summary",
        "",
        "## Aggregate",
        f"- Primary standalone strategy: `{summary.primary_standalone_strategy_id or 'NONE'}`",
        f"- Standalone strategies replayed: `{aggregate.standalone_strategy_count}`",
        f"- Strategy IDs: `{', '.join(aggregate.standalone_strategy_ids) if aggregate.standalone_strategy_ids else 'NONE'}`",
        f"- Processed bars: `{aggregate.processed_bars}`",
        f"- Order intents: `{aggregate.order_intents}`",
        f"- Fills: `{aggregate.fills}`",
        f"- Entries: `{aggregate.entries}`",
        f"- Exits: `{aggregate.exits}`",
        f"- Realized P/L: `{aggregate.realized_pnl if aggregate.realized_pnl is not None else 'UNAVAILABLE'}`",
        f"- Unrealized P/L: `{aggregate.unrealized_pnl if aggregate.unrealized_pnl is not None else 'UNAVAILABLE'}`",
        f"- Cumulative P/L: `{aggregate.cumulative_pnl if aggregate.cumulative_pnl is not None else 'UNAVAILABLE'}`",
    ]
    if aggregate.pnl_unavailable_reason:
        lines.append(f"- P/L note: {aggregate.pnl_unavailable_reason}")

    lines.extend(["", "## Per Strategy"])
    for row in summary.per_strategy_summaries:
        lines.extend(
            [
                f"### `{row.standalone_strategy_id}`",
                f"- Family: `{row.strategy_family}`",
                f"- Instrument: `{row.instrument}`",
                f"- Processed bars: `{row.processed_bars}`",
                f"- Order intents: `{row.order_intents}`",
                f"- Fills: `{row.fills}`",
                f"- Entries: `{row.entries}`",
                f"- Exits: `{row.exits}`",
                f"- Final position: `{row.final_position_side.value}`",
                f"- Final strategy status: `{row.final_strategy_status.value}`",
                f"- Realized P/L: `{row.realized_pnl if row.realized_pnl is not None else 'UNAVAILABLE'}`",
                f"- Unrealized P/L: `{row.unrealized_pnl if row.unrealized_pnl is not None else 'UNAVAILABLE'}`",
                f"- Cumulative P/L: `{row.cumulative_pnl if row.cumulative_pnl is not None else 'UNAVAILABLE'}`",
            ]
        )
        if row.pnl_unavailable_reason:
            lines.append(f"- P/L note: {row.pnl_unavailable_reason}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_replay_summary_markdown(summary: ReplayRunSummary, output_path: str | Path) -> Path:
    """Write a compact replay markdown report."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_replay_summary_markdown(summary), encoding="utf-8")
    return path
