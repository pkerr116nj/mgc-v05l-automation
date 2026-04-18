from __future__ import annotations

from pathlib import Path

from mgc_v05l.app.probationary_runtime import _load_probationary_paper_lane_specs
from mgc_v05l.app.strategy_identity import build_standalone_strategy_identity
from mgc_v05l.app.strategy_runtime_registry import build_standalone_strategy_definitions
from mgc_v05l.config_models import load_settings_from_files


def test_strategy_identity_supports_explicit_id_without_instrument_suffix() -> None:
    identity = build_standalone_strategy_identity(
        instrument="MGC",
        source_family="ACTIVE_TREND_PARTICIPATION",
        explicit_root="ATP_COMPANION_V1",
        explicit_id="atp_companion_v1__paper_gc_asia_us",
        explicit_label="ATP Companion v1 / GC / paper",
        identity_components=["paper", "gc", "asia_us"],
    )

    assert identity["standalone_strategy_id"] == "atp_companion_v1__paper_gc_asia_us"
    assert identity["standalone_strategy_root"] == "atp_companion_v1"
    assert identity["standalone_strategy_label"] == "ATP Companion v1 / GC / paper"
    assert identity["identity_components"] == ["paper", "gc", "asia_us"]
    assert identity["legacy_instrument_derived_identity"] is False


def test_strategy_identity_legacy_fallback_remains_root_plus_instrument() -> None:
    identity = build_standalone_strategy_identity(
        instrument="MGC",
        source_family="LEGACY_RUNTIME",
        lane_name="legacy_runtime",
    )

    assert identity["standalone_strategy_id"] == "legacy_runtime__MGC"
    assert identity["identity_components"] == ["MGC"]
    assert identity["legacy_instrument_derived_identity"] is True


def test_runtime_registry_respects_explicit_standalone_strategy_id_from_overlay() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    settings = load_settings_from_files(
        [
            repo_root / "config" / "base.yaml",
            repo_root / "config" / "live.yaml",
            repo_root / "config" / "probationary_pattern_engine.yaml",
            repo_root / "config" / "probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us.yaml",
        ]
    )

    definitions = build_standalone_strategy_definitions(settings, runtime_lanes=settings.probationary_paper_lane_specs)

    assert len(definitions) == 1
    definition = definitions[0]
    assert definition.standalone_strategy_id == "atp_companion_v1__paper_gc_asia_us"
    assert definition.instrument == "GC"
    assert definition.identity_components == ("paper", "gc", "asia_us")


def test_frozen_atp_benchmark_uses_explicit_benchmark_identity() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    settings = load_settings_from_files(
        [
            repo_root / "config" / "base.yaml",
            repo_root / "config" / "live.yaml",
            repo_root / "config" / "probationary_pattern_engine.yaml",
            repo_root / "config" / "probationary_pattern_engine_paper_atp_companion_v1_asia_us.yaml",
        ]
    )

    definitions = build_standalone_strategy_definitions(settings, runtime_lanes=settings.probationary_paper_lane_specs)

    assert len(definitions) == 1
    definition = definitions[0]
    assert definition.standalone_strategy_id == "atp_companion_v1__benchmark_mgc_asia_us"
    assert definition.identity_components == ("benchmark", "mgc", "asia_us")


def test_probationary_runtime_lane_specs_preserve_explicit_identity_fields() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    settings = load_settings_from_files(
        [
            repo_root / "config" / "base.yaml",
            repo_root / "config" / "live.yaml",
            repo_root / "config" / "probationary_pattern_engine.yaml",
            repo_root / "config" / "probationary_pattern_engine_paper_atp_companion_v1_pl_asia_us.yaml",
        ]
    )

    lane_specs = _load_probationary_paper_lane_specs(settings)
    lane = next(spec for spec in lane_specs if spec.lane_id == "atp_companion_v1_pl_asia_us")

    assert lane.symbol == "PL"
    assert lane.standalone_strategy_id == "atp_companion_v1__paper_pl_asia_us"
    assert lane.identity_components == ("paper", "pl", "asia_us")
    assert lane.observed_instruments == ("PL",)
