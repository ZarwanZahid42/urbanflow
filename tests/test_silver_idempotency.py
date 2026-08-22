from notebooks.utilities.silver_common import silver_replace_where


def test_monthly_replace_predicate_is_stable_for_retries():
    first = silver_replace_where(2026, 5)
    second = silver_replace_where(2026, 5)
    assert first == second == "source_year = 2026 AND source_month = 5"


def test_monthly_replace_predicates_do_not_overlap():
    assert silver_replace_where(2026, 5) != silver_replace_where(2026, 6)
