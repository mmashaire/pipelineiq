from pipelineiq.funnel_metrics import (
    compute_campaign_performance,
    compute_campaign_type_performance,
    compute_funnel_kpis,
    compute_region_performance,
    compute_segment_performance,
)
from pipelineiq.synthetic_data import GenerationConfig, generate_pipeline_data


def test_funnel_rates_are_within_bounds() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=500, n_campaigns=10, seed=13))
    kpis = compute_funnel_kpis(data).iloc[0]

    rate_columns = [
        "open_rate",
        "click_to_open_rate",
        "mql_rate",
        "sql_rate",
        "conversion_rate",
        "won_opportunity_rate",
    ]

    for col in rate_columns:
        assert 0.0 <= float(kpis[col]) <= 1.0


def test_campaign_performance_has_expected_columns() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=500, n_campaigns=10, seed=19))
    perf = compute_campaign_performance(data)

    expected_columns = {
        "campaign_id",
        "campaign_name",
        "campaign_type",
        "sends",
        "opens",
        "clicks",
        "conversions",
        "won_opportunities",
        "revenue",
        "open_rate",
        "click_to_open_rate",
        "conversion_rate",
        "revenue_per_send",
    }

    assert expected_columns.issubset(set(perf.columns))
    assert len(perf) == data["campaigns"].shape[0]


def test_segment_performance_has_expected_columns_and_all_segments() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=700, n_campaigns=12, seed=31))
    perf = compute_segment_performance(data)

    expected_columns = {
        "segment",
        "contacts",
        "sends",
        "opens",
        "clicks",
        "mqls",
        "sqls",
        "conversions",
        "won_opportunities",
        "revenue",
        "open_rate",
        "click_to_open_rate",
        "conversion_rate",
        "revenue_per_contact",
    }

    assert expected_columns.issubset(set(perf.columns))
    assert set(perf["segment"]) == {"SMB", "Mid-Market", "Enterprise", "Inactive"}


def test_segment_performance_shows_inactive_segment_as_lower_value() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=900, n_campaigns=12, seed=37))
    perf = compute_segment_performance(data).set_index("segment")

    inactive = float(perf.loc["Inactive", "revenue_per_contact"])
    enterprise = float(perf.loc["Enterprise", "revenue_per_contact"])

    assert inactive <= enterprise


def test_region_performance_has_expected_columns_and_regions() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=700, n_campaigns=12, seed=41))
    perf = compute_region_performance(data)

    expected_columns = {
        "region",
        "contacts",
        "sends",
        "opens",
        "clicks",
        "conversions",
        "won_opportunities",
        "revenue",
        "open_rate",
        "conversion_rate",
        "revenue_per_contact",
    }

    assert expected_columns.issubset(set(perf.columns))
    assert set(perf["region"]) == {"Nordics", "DACH", "UKI", "North America"}


def test_campaign_type_performance_has_expected_columns_and_types() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=700, n_campaigns=12, seed=43))
    perf = compute_campaign_type_performance(data)

    expected_columns = {
        "campaign_type",
        "campaigns",
        "sends",
        "opens",
        "clicks",
        "conversions",
        "won_opportunities",
        "revenue",
        "open_rate",
        "click_to_open_rate",
        "conversion_rate",
        "revenue_per_campaign",
    }

    assert expected_columns.issubset(set(perf.columns))
    assert set(perf["campaign_type"]).issubset({"Newsletter", "Webinar", "Case Study", "Product Launch"})
