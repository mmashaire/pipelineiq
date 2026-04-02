from pipelineiq.funnel_metrics import (
    compute_campaign_attribution,
    compute_campaign_performance,
    compute_campaign_type_performance,
    compute_funnel_kpis,
    compute_monthly_trends,
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


def test_monthly_trends_has_expected_columns_and_sorted_months() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=700, n_campaigns=12, seed=47))
    trends = compute_monthly_trends(data)

    expected_columns = {
        "month",
        "sends",
        "opens",
        "clicks",
        "conversions",
        "won_deals",
        "revenue",
        "open_rate",
        "click_to_open_rate",
        "conversion_rate",
        "revenue_per_send",
    }

    assert expected_columns.issubset(set(trends.columns))
    assert trends["month"].tolist() == sorted(trends["month"].tolist())


def test_monthly_trends_reconcile_with_source_totals() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=900, n_campaigns=12, seed=53))
    trends = compute_monthly_trends(data)

    events = data["email_events"]
    opens = int((events["event_type"] == "open").sum()) if not events.empty else 0
    clicks = int((events["event_type"] == "click").sum()) if not events.empty else 0

    assert int(trends["sends"].sum()) == len(data["email_sends"])
    assert int(trends["opens"].sum()) == opens
    assert int(trends["clicks"].sum()) == clicks
    assert int(trends["conversions"].sum()) == len(data["conversions"])
    assert int(trends["won_deals"].sum()) == len(data["revenue_events"])
    assert float(trends["revenue"].sum()) == float(data["revenue_events"]["revenue_amount"].sum())

    rate_columns = ["open_rate", "click_to_open_rate", "conversion_rate"]
    for col in rate_columns:
        assert ((trends[col] >= 0.0) & (trends[col] <= 1.0)).all()

    assert (trends["revenue_per_send"] >= 0.0).all()


def test_campaign_attribution_has_expected_columns() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=700, n_campaigns=12, seed=59))
    attribution = compute_campaign_attribution(data)

    expected_columns = {
        "campaign_id",
        "campaign_name",
        "campaign_type",
        "won_deals",
        "attributed_revenue",
        "average_deal_size",
        "revenue_share",
        "latest_revenue_month",
        "attribution_model",
    }

    assert expected_columns.issubset(set(attribution.columns))
    assert set(attribution["attribution_model"]) == {"last_touch"}


def test_campaign_attribution_reconciles_with_won_revenue() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=900, n_campaigns=12, seed=61))
    attribution = compute_campaign_attribution(data)
    revenue_events = data["revenue_events"]

    assert int(attribution["won_deals"].sum()) == len(revenue_events)
    assert round(float(attribution["attributed_revenue"].sum()), 2) == round(
        float(revenue_events["revenue_amount"].sum()), 2
    )

    total_revenue = float(revenue_events["revenue_amount"].sum())
    if total_revenue > 0:
        assert abs(float(attribution["revenue_share"].sum()) - 1.0) <= 1e-6
    else:
        assert float(attribution["revenue_share"].sum()) == 0.0
