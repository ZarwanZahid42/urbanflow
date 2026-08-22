from notebooks.utilities.gold_common import gold_replace_where


def test_gold_monthly_replacement_is_stable_for_retries():
    assert gold_replace_where(2026, 5) == gold_replace_where(2026, 5)


def test_gold_monthly_replacement_does_not_overlap_future_batches():
    assert gold_replace_where(2026, 5) != gold_replace_where(2026, 6)
