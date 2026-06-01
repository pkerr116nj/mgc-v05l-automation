"""Read-only broker-backed fill evidence resolver for Track B PAPER adoption.

The resolver never queries IBKR and never fabricates executions from broker
positions.  It only joins already-written artifacts and returns broker-backed
fill evidence when an exact execution id can be proven for the submitted order
identity.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from .track_b_submit_intent_ownership import DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL


RESOLVED = "BROKER_BACKED_FILL_EVIDENCE_RESOLVED"
MISSING_EXEC_ID = "REVIEW_REQUIRED_MISSING_EXEC_ID"
AMBIGUOUS_EXECUTION = "REVIEW_REQUIRED_AMBIGUOUS_EXECUTION"


@dataclass(frozen=True)
class BrokerFillEvidenceRequest:
    trade_id: str | None = None
    submit_intent_id: str | None = None
    order_id: str | int | None = None
    client_id: str | int | None = None
    perm_id: str | int | None = None
    con_id: str | int | None = None
    local_symbol: str | None = None
    account_id: str | None = None
    side: str | None = None
    action: str | None = None
    qty: str | int | float | Decimal | None = None
    symbol: str | None = None


@dataclass(frozen=True)
class BrokerFillEvidenceResult:
    classification: str
    broker_backed_evidence_valid: bool
    evidence: dict[str, Any] | None = None
    matches: tuple[dict[str, Any], ...] = ()
    rejected: tuple[dict[str, Any], ...] = ()
    searched_paths: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()

    @property
    def exec_id(self) -> str | None:
        if not self.evidence:
            return None
        return _text(self.evidence.get("exec_id") or self.evidence.get("execution_id")) or None


def resolve_broker_backed_fill_evidence(
    *,
    repo_root: Path,
    request: BrokerFillEvidenceRequest,
    extra_source_paths: Sequence[Path] = (),
) -> BrokerFillEvidenceResult:
    """Resolve exact broker-backed fill evidence for one PAPER submit identity."""

    paths = _candidate_paths(repo_root=repo_root, extra_source_paths=extra_source_paths)
    searched: list[str] = []
    candidates: list[dict[str, Any]] = []
    for path in paths:
        payloads = _read_payloads(path)
        if not payloads:
            continue
        searched.append(str(path))
        for payload in payloads:
            candidates.extend(_candidates_from_payload(payload, source_path=path))

    matches: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for candidate in candidates:
        match, reason = _candidate_matches_request(candidate, request)
        if match:
            matches.append(_normalized_evidence(candidate, request))
        else:
            rejected.append(
                {
                    "source_artifact_path": candidate.get("source_artifact_path"),
                    "source": candidate.get("source"),
                    "exec_id": candidate.get("exec_id") or candidate.get("execution_id"),
                    "perm_id": candidate.get("perm_id"),
                    "reason": reason,
                }
            )

    with_exec = [row for row in matches if _text(row.get("exec_id") or row.get("execution_id"))]
    unique_by_exec: dict[str, dict[str, Any]] = {}
    for row in with_exec:
        unique_by_exec.setdefault(_text(row.get("exec_id") or row.get("execution_id")), row)
    if len(unique_by_exec) == 1:
        evidence = next(iter(unique_by_exec.values()))
        return BrokerFillEvidenceResult(
            classification=RESOLVED,
            broker_backed_evidence_valid=True,
            evidence=evidence,
            matches=tuple(with_exec),
            rejected=tuple(rejected),
            searched_paths=tuple(searched),
            reason_codes=("BROKER_BACKED_FILL_EXEC_ID_RESOLVED",),
        )
    if len(unique_by_exec) > 1:
        return BrokerFillEvidenceResult(
            classification=AMBIGUOUS_EXECUTION,
            broker_backed_evidence_valid=False,
            matches=tuple(with_exec),
            rejected=tuple(rejected),
            searched_paths=tuple(searched),
            reason_codes=("MULTIPLE_MATCHING_EXEC_IDS",),
        )
    return BrokerFillEvidenceResult(
        classification=MISSING_EXEC_ID,
        broker_backed_evidence_valid=False,
        matches=tuple(matches),
        rejected=tuple(rejected),
        searched_paths=tuple(searched),
        reason_codes=("EXEC_ID_NOT_FOUND",),
    )


def _candidate_paths(*, repo_root: Path, extra_source_paths: Sequence[Path]) -> tuple[Path, ...]:
    roots = [
        repo_root / DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL,
        repo_root / "outputs" / "reports" / "ibkr_runtime_route_dispatch",
        repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes",
        repo_root / "outputs" / "reports" / "ibkr_read_only_verification",
        repo_root / "outputs" / "track_b_execution_core" / "paper_trade_ledger",
        repo_root / "outputs" / "track_b_execution_core" / "track_b_strategy_managed_paper_lifecycle",
    ]
    paths: list[Path] = []
    for item in extra_source_paths:
        path = _repo_path(repo_root, item)
        if path.is_file():
            paths.append(path)
    for root in roots:
        if root.is_file():
            paths.append(root)
            continue
        if not root.is_dir():
            continue
        for pattern in (
            "**/ibkr_paper_strategy_bridge_report.json",
            "**/filled_bridge_result_latest.json",
            "**/filled_bridge_results.jsonl",
            "**/*execution*.json",
            "**/*execution*.jsonl",
            "**/*fill*.json",
            "**/*fill*.jsonl",
            "**/*lifecycle*.json",
        ):
            try:
                paths.extend(root.glob(pattern))
            except OSError:
                continue
    unique: dict[str, Path] = {}
    for path in paths:
        unique.setdefault(str(path), path)
    return tuple(unique.values())


def _repo_path(repo_root: Path, path: Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else repo_root / candidate


def _read_payloads(path: Path) -> list[Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return []
    if path.suffix == ".jsonl":
        rows: list[Any] = []
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return rows
    try:
        return [json.loads(text)]
    except json.JSONDecodeError:
        return []


def _candidates_from_payload(payload: Any, *, source_path: Path) -> list[dict[str, Any]]:
    if not isinstance(payload, Mapping):
        return []
    source_path_text = str(source_path)
    candidates: list[dict[str, Any]] = []
    if source_path.name == "ibkr_paper_strategy_bridge_report.json":
        candidates.extend(_bridge_report_candidates(payload, source_path_text))
    candidates.extend(_recursive_execution_candidates(payload, source_path_text, context={}))
    return candidates


def _bridge_report_candidates(report: Mapping[str, Any], source_path: str) -> list[dict[str, Any]]:
    delegated = _mapping(report.get("delegated_result"))
    delegated_report = _mapping(delegated.get("report"))
    lifecycle = _mapping(
        delegated_report.get("submit_cancel_lifecycle")
        or delegated_report.get("lifecycle")
        or delegated.get("submit_cancel_lifecycle")
    )
    latest_status = _mapping(lifecycle.get("latest_order_status"))
    intent = _mapping(report.get("intent"))
    metadata = _mapping(report.get("caller_metadata"))
    environment = _mapping(report.get("environment"))
    contract = _first_mapping(
        _mapping(report.get("qualified_contract_report")).get("qualified_contract"),
        _mapping(report.get("exact_contract_report")).get("exact_contract"),
        _mapping(_mapping(delegated_report.get("preview_payload")).get("contract")),
    )
    context = {
        "source": "bridge_execution_report",
        "source_artifact_path": source_path,
        "account_id": report.get("selected_account_id") or environment.get("account_id") or metadata.get("account_id"),
        "symbol": contract.get("symbol") or intent.get("symbol") or report.get("symbol"),
        "local_symbol": contract.get("local_symbol") or metadata.get("local_symbol"),
        "con_id": contract.get("con_id") or contract.get("qualified_contract_identifier") or metadata.get("con_id"),
        "expiry": contract.get("expiry") or intent.get("contract_month") or report.get("contract_month"),
        "action": intent.get("action") or report.get("action") or metadata.get("intent_action"),
        "qty": intent.get("quantity") or report.get("quantity") or metadata.get("quantity"),
        "order_id": latest_status.get("order_id") or lifecycle.get("submitted_order_id") or lifecycle.get("order_id"),
        "client_id": latest_status.get("client_id") or lifecycle.get("client_id") or environment.get("client_id"),
        "perm_id": latest_status.get("perm_id") or lifecycle.get("perm_id"),
        "trade_id": metadata.get("trade_id"),
        "submit_intent_id": intent.get("order_intent_id") or report.get("order_intent_id"),
    }
    rows = []
    for execution in _bridge_execution_rows(lifecycle):
        rows.append({**context, **execution, "exec_id": execution.get("execution_id") or execution.get("exec_id")})
    return rows


def _bridge_execution_rows(lifecycle: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in (
        lifecycle.get("executions_after_submit"),
        _mapping(lifecycle.get("fill_verification")).get("executions_after_submit"),
    ):
        if isinstance(source, list):
            rows.extend(dict(item) for item in source if isinstance(item, Mapping))
    return rows


def _recursive_execution_candidates(payload: Any, source_path: str, context: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(payload, Mapping):
        next_context = _merge_context(context, payload)
        if _looks_like_execution_or_fill(payload):
            rows.append({**next_context, **dict(payload), "source_artifact_path": source_path, "source": "artifact_execution_or_fill"})
        for value in payload.values():
            rows.extend(_recursive_execution_candidates(value, source_path, next_context))
    elif isinstance(payload, list):
        for value in payload:
            rows.extend(_recursive_execution_candidates(value, source_path, context))
    return rows


def _merge_context(context: Mapping[str, Any], row: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(context)
    for key in (
        "trade_id",
        "submit_intent_id",
        "ownership_intent_id",
        "order_intent_id",
        "order_id",
        "broker_order_id",
        "client_id",
        "perm_id",
        "account_id",
        "account",
        "con_id",
        "conId",
        "local_symbol",
        "localSymbol",
        "symbol",
        "action",
        "side",
        "qty",
        "quantity",
        "price",
        "fill_price",
        "fill_timestamp",
        "executed_at",
    ):
        if row.get(key) not in (None, ""):
            merged.setdefault(key, row.get(key))
    return merged


def _looks_like_execution_or_fill(row: Mapping[str, Any]) -> bool:
    return bool(
        row.get("exec_id")
        or row.get("execution_id")
        or row.get("execId")
        or (row.get("perm_id") and ("fill" in json.dumps(row, default=str).lower() or row.get("order_id") or row.get("broker_order_id")))
    )


def _candidate_matches_request(candidate: Mapping[str, Any], request: BrokerFillEvidenceRequest) -> tuple[bool, str]:
    if request.trade_id and _has_text(candidate, "trade_id") and _text(candidate.get("trade_id")) != _text(request.trade_id):
        return False, "trade_id_mismatch"
    if request.submit_intent_id:
        candidate_intent_ids = {
            _text(candidate.get("submit_intent_id")),
            _text(candidate.get("ownership_intent_id")),
            _text(candidate.get("order_intent_id")),
        }
        candidate_intent_ids.discard("")
        if candidate_intent_ids and _text(request.submit_intent_id) not in candidate_intent_ids:
            return False, "submit_intent_id_mismatch"
    if not _optional_text_match(candidate, request.order_id, "order_id", "broker_order_id", "submitted_order_id"):
        return False, "order_id_mismatch"
    if not _optional_int_match(candidate, request.client_id, "client_id"):
        return False, "client_id_mismatch"
    if not _optional_int_match(candidate, request.perm_id, "perm_id"):
        return False, "perm_id_mismatch"
    if not _optional_text_match(candidate, request.account_id, "account_id", "account"):
        return False, "account_mismatch"
    if not (_has_any(candidate, "account_id", "account") or not request.account_id):
        return False, "account_missing"
    if not _optional_int_match(candidate, request.con_id, "con_id", "conId"):
        return False, "con_id_mismatch"
    if not _optional_text_match(candidate, request.local_symbol, "local_symbol", "localSymbol"):
        return False, "local_symbol_mismatch"
    if request.local_symbol and request.con_id and not _has_any(candidate, "local_symbol", "localSymbol", "con_id", "conId"):
        return False, "contract_identity_missing"
    if not _optional_qty_match(candidate, request.qty):
        return False, "qty_mismatch"
    if not _optional_action_match(candidate, request.action):
        return False, "action_mismatch"
    return True, "matched"


def _normalized_evidence(candidate: Mapping[str, Any], request: BrokerFillEvidenceRequest) -> dict[str, Any]:
    exec_id = _text(candidate.get("exec_id") or candidate.get("execution_id") or candidate.get("execId"))
    perm_id = _text(candidate.get("perm_id") or request.perm_id)
    return {
        "trade_id": _text(candidate.get("trade_id") or request.trade_id) or None,
        "submit_intent_id": _text(candidate.get("submit_intent_id") or candidate.get("ownership_intent_id") or request.submit_intent_id) or None,
        "order_id": _text(candidate.get("order_id") or candidate.get("broker_order_id") or request.order_id) or None,
        "client_id": _text(candidate.get("client_id") or request.client_id) or None,
        "perm_id": perm_id or None,
        "exec_id": exec_id or None,
        "execution_id": exec_id or None,
        "account_id": _text(candidate.get("account_id") or candidate.get("account") or request.account_id) or None,
        "con_id": _text(candidate.get("con_id") or candidate.get("conId") or request.con_id) or None,
        "local_symbol": _text(candidate.get("local_symbol") or candidate.get("localSymbol") or request.local_symbol) or None,
        "symbol": _text(candidate.get("symbol") or request.symbol) or None,
        "action": _normalize_action(candidate.get("action") or request.action) or None,
        "qty": _text(candidate.get("qty") or candidate.get("quantity") or request.qty) or None,
        "price": _text(candidate.get("price") or candidate.get("fill_price") or candidate.get("avg_fill_price")) or None,
        "fill_timestamp": _text(candidate.get("fill_timestamp") or candidate.get("executed_at") or candidate.get("updated_at")) or None,
        "source": candidate.get("source"),
        "source_artifact_path": candidate.get("source_artifact_path"),
    }


def _optional_text_match(candidate: Mapping[str, Any], expected: object, *keys: str) -> bool:
    expected_text = _text(expected).upper()
    values = [_text(candidate.get(key)).upper() for key in keys if _text(candidate.get(key))]
    return not expected_text or not values or expected_text in values


def _optional_int_match(candidate: Mapping[str, Any], expected: object, *keys: str) -> bool:
    expected_int = _int_text(expected)
    values = [_int_text(candidate.get(key)) for key in keys if _int_text(candidate.get(key))]
    return not expected_int or not values or expected_int in values


def _optional_qty_match(candidate: Mapping[str, Any], expected: object) -> bool:
    expected_qty = _decimal(expected)
    values = [_decimal(candidate.get(key)) for key in ("qty", "quantity", "filled", "shares") if _decimal(candidate.get(key)) is not None]
    return expected_qty is None or not values or any(abs(value) == abs(expected_qty) for value in values if value is not None)


def _optional_action_match(candidate: Mapping[str, Any], expected: object) -> bool:
    expected_action = _normalize_action(expected)
    values = [
        _normalize_action(candidate.get(key))
        for key in ("action", "side", "order_action")
        if _normalize_action(candidate.get(key))
    ]
    return not expected_action or not values or expected_action in values


def _normalize_action(value: object) -> str:
    text = _text(value).upper()
    if text in {"BOT", "BUY", "BUY_TO_OPEN", "BUY_TO_CLOSE", "LONG"}:
        return "BUY"
    if text in {"SLD", "SELL", "SELL_TO_OPEN", "SELL_TO_CLOSE", "SHORT"}:
        return "SELL"
    return text


def _has_text(candidate: Mapping[str, Any], key: str) -> bool:
    return bool(_text(candidate.get(key)))


def _has_any(candidate: Mapping[str, Any], *keys: str) -> bool:
    return any(_text(candidate.get(key)) for key in keys)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _first_mapping(*values: object) -> dict[str, Any]:
    for value in values:
        if isinstance(value, Mapping):
            return dict(value)
    return {}


def _text(value: object) -> str:
    return str(value or "").strip()


def _int_text(value: object) -> str:
    text = _text(value)
    if not text:
        return ""
    try:
        return str(int(Decimal(text)))
    except (InvalidOperation, ValueError):
        return text


def _decimal(value: object) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
