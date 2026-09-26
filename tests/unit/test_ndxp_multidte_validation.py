from datetime import date
import pytest
from mgc_v05l.research.ndxp_multidte_validation import partition, development_only, quality_flags, manifest

@pytest.mark.parametrize('day,expected', [('2021-09-24','development'),('2024-12-31','development'),('2025-01-01','validation'),('2025-12-31','validation'),('2026-01-01','final_holdout'),('2026-09-24','final_holdout')])
def test_partitions(day,expected):
    assert partition(date.fromisoformat(day)) == expected

def test_outside_partition():
    with pytest.raises(ValueError): partition('2027-01-01')

def test_holdout_excluded():
    assert development_only([{'session_date':'2024-12-31'},{'session_date':'2025-01-01'},{'session_date':'2026-01-01'}]) == [{'session_date':'2024-12-31'}]

def test_flags():
    assert quality_flags('2024-06-03')['degraded']
    assert quality_flags('2025-10-22')['degraded']
    assert quality_flags('2026-09-22')['missing_path_session']
    assert quality_flags('2026-09-23')['missing_path_session']
    assert not any(quality_flags('2024-01-02').values())

def test_manifest_immutable_and_excludes_derived(tmp_path):
    raw=tmp_path/'raw.csv'; raw.write_text('raw\n')
    before=manifest(tmp_path)
    out=tmp_path/'deep_dive_v1'; out.mkdir(); (out/'derived').write_text('derived')
    assert before == manifest(tmp_path)
    assert raw.read_text() == 'raw\n'
