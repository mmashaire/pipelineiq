import pandas as pd

from pipelineiq.recommendations import build_recommendation_summary


def test_build_recommendation_summary_returns_expected_shape() -> None:
    funnel_kpis = pd.DataFrame(
        [{"click_to_open_rate": 0.22, "conversion_rate": 0.04, "revenue_per_send": 18.0}]
    )
    campaign_perf = pd.DataFrame(
        [
            {"campaign_name": "Campaign 01", "open_rate": 0.42, "conversion_rate": 0.015, "revenue": 8500.0},
            {"campaign_name": "Campaign 02", "open_rate": 0.31, "conversion_rate": 0.06, "revenue": 12500.0},
        ]
    )
    segment_perf = pd.DataFrame(
        [
            {"segment": "Enterprise", "revenue_per_contact": 160.0, "open_rate": 0.41, "conversion_rate": 0.08},
            {"segment": "Inactive", "revenue_per_contact": 12.0, "open_rate": 0.09, "conversion_rate": 0.01},
        ]
    )

    recommendations = build_recommendation_summary(funnel_kpis, campaign_perf, segment_perf)

    expected_columns = {"priority", "focus_area", "finding", "recommended_action", "evidence"}
    assert expected_columns.issubset(recommendations.columns)
    assert len(recommendations) >= 3


def test_build_recommendation_summary_highlights_business_actions() -> None:
    funnel_kpis = pd.DataFrame(
        [{"click_to_open_rate": 0.11, "conversion_rate": 0.018, "revenue_per_send": 9.5}]
    )
    campaign_perf = pd.DataFrame(
        [
            {"campaign_name": "Webinar Push", "open_rate": 0.47, "conversion_rate": 0.012, "revenue": 7200.0},
            {"campaign_name": "Case Study Follow-Up", "open_rate": 0.29, "conversion_rate": 0.055, "revenue": 14300.0},
        ]
    )
    segment_perf = pd.DataFrame(
        [
            {"segment": "Enterprise", "revenue_per_contact": 180.0, "open_rate": 0.44, "conversion_rate": 0.09},
            {"segment": "Inactive", "revenue_per_contact": 8.0, "open_rate": 0.07, "conversion_rate": 0.005},
        ]
    )

    recommendations = build_recommendation_summary(funnel_kpis, campaign_perf, segment_perf)
    combined_text = " ".join(recommendations["finding"].astype(str).str.lower()) + " " + " ".join(
        recommendations["recommended_action"].astype(str).str.lower()
    )

    assert "enterprise" in combined_text
    assert "inactive" in combined_text or "re-engagement" in combined_text
    assert "webinar push" in combined_text
    assert "landing" in combined_text or "cta" in combined_text or "follow-up" in combined_text
