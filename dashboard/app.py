"""PipelineIQ dashboard.

This Streamlit app turns the processed synthetic CRM outputs into a compact,
decision-oriented view for technical and business users.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from pipelineiq.recommendations import build_recommendation_summary

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data" / "processed"
REQUIRED_FILES = [
    "funnel_kpis.csv",
    "monthly_trends.csv",
    "campaign_attribution.csv",
    "campaign_performance.csv",
    "segment_performance.csv",
    "region_performance.csv",
    "campaign_type_performance.csv",
    "email_sends.csv",
    "email_events.csv",
    "lead_progression.csv",
    "conversions.csv",
    "revenue_events.csv",
]


def _format_currency(value: float) -> str:
    return f"€{float(value):,.0f}"


def _format_rate(value: float) -> str:
    return f"{float(value):.1%}"


@st.cache_data(show_spinner=False)
def load_processed_data() -> dict[str, pd.DataFrame]:
    """Load the processed CSV outputs needed by the dashboard."""

    missing_files = [name for name in REQUIRED_FILES if not (DATA_DIR / name).exists()]
    if missing_files:
        missing_list = ", ".join(missing_files)
        raise FileNotFoundError(
            f"Missing processed files: {missing_list}. Run `python pipelines/run_pipeline.py` first."
        )

    return {
        "funnel_kpis": pd.read_csv(DATA_DIR / "funnel_kpis.csv"),
        "monthly_trends": pd.read_csv(DATA_DIR / "monthly_trends.csv"),
        "campaign_attribution": pd.read_csv(DATA_DIR / "campaign_attribution.csv"),
        "campaign_performance": pd.read_csv(DATA_DIR / "campaign_performance.csv"),
        "segment_performance": pd.read_csv(DATA_DIR / "segment_performance.csv"),
        "region_performance": pd.read_csv(DATA_DIR / "region_performance.csv"),
        "campaign_type_performance": pd.read_csv(DATA_DIR / "campaign_type_performance.csv"),
        "email_sends": pd.read_csv(DATA_DIR / "email_sends.csv"),
        "email_events": pd.read_csv(DATA_DIR / "email_events.csv"),
        "lead_progression": pd.read_csv(DATA_DIR / "lead_progression.csv"),
        "conversions": pd.read_csv(DATA_DIR / "conversions.csv"),
        "revenue_events": pd.read_csv(DATA_DIR / "revenue_events.csv"),
    }


def build_funnel_snapshot(data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Create a compact funnel table with true stage counts from processed data."""

    events = data["email_events"]
    leads = data["lead_progression"]

    return pd.DataFrame(
        [
            {"stage": "Sends", "count": len(data["email_sends"])},
            {"stage": "Opens", "count": int((events["event_type"] == "open").sum())},
            {"stage": "Clicks", "count": int((events["event_type"] == "click").sum())},
            {"stage": "MQL", "count": int((leads["stage"] == "MQL").sum())},
            {"stage": "SQL", "count": int((leads["stage"] == "SQL").sum())},
            {"stage": "Conversions", "count": len(data["conversions"])},
            {"stage": "Won deals", "count": len(data["revenue_events"])},
        ]
    )


def main() -> None:
    """Render the PipelineIQ business dashboard."""

    st.set_page_config(page_title="PipelineIQ Dashboard", layout="wide")
    st.title("PipelineIQ — Lead-to-Revenue Dashboard")
    st.caption(
        "Synthetic CRM and campaign data only. Built to show business-facing analytics judgment, not fake production access."
    )

    try:
        data = load_processed_data()
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.code("python pipelines/run_pipeline.py")
        return

    funnel_kpis = data["funnel_kpis"]
    monthly_trends = data["monthly_trends"].sort_values("month", ignore_index=True)
    campaign_attribution = data["campaign_attribution"].sort_values(
        "attributed_revenue", ascending=False, ignore_index=True
    )
    campaign_perf = data["campaign_performance"].sort_values("revenue", ascending=False, ignore_index=True)
    segment_perf = data["segment_performance"].sort_values("revenue", ascending=False, ignore_index=True)
    region_perf = data["region_performance"].sort_values("revenue", ascending=False, ignore_index=True)
    campaign_type_perf = data["campaign_type_performance"].sort_values("revenue", ascending=False, ignore_index=True)
    recommendations = build_recommendation_summary(funnel_kpis, campaign_perf, segment_perf)
    funnel_snapshot = build_funnel_snapshot(data)

    kpi_row = funnel_kpis.iloc[0]
    best_campaign = campaign_perf.iloc[0]
    best_segment = segment_perf.sort_values("revenue_per_contact", ascending=False, ignore_index=True).iloc[0]
    best_region = region_perf.iloc[0]
    best_format = campaign_type_perf.iloc[0]

    st.subheader("What matters right now")
    st.markdown(
        f"- **Best campaign by revenue:** `{best_campaign['campaign_name']}` at {_format_currency(best_campaign['revenue'])}\n"
        f"- **Best segment to prioritize:** `{best_segment['segment']}` with {_format_currency(best_segment['revenue_per_contact'])} revenue per contact\n"
        f"- **Best region by revenue:** `{best_region['region']}` at {_format_currency(best_region['revenue'])}\n"
        f"- **Best campaign format:** `{best_format['campaign_type']}` with {_format_currency(best_format['revenue_per_campaign'])} revenue per campaign\n"
        f"- **Current revenue efficiency:** {_format_currency(kpi_row['revenue_per_send'])} per send"
    )

    metric_cols = st.columns(4)
    metric_cols[0].metric("Total revenue", _format_currency(kpi_row["total_revenue"]))
    metric_cols[1].metric("Open rate", _format_rate(kpi_row["open_rate"]))
    metric_cols[2].metric("Click-to-open", _format_rate(kpi_row["click_to_open_rate"]))
    metric_cols[3].metric("Conversion rate", _format_rate(kpi_row["conversion_rate"]))

    st.subheader("Momentum over time")
    trend_col, trend_detail_col = st.columns(2)

    with trend_col:
        st.line_chart(monthly_trends.set_index("month")[["sends", "opens", "conversions", "won_deals"]])

    with trend_detail_col:
        trend_view = monthly_trends[["month", "open_rate", "conversion_rate", "revenue"]].copy()
        trend_view["open_rate"] = trend_view["open_rate"].map(_format_rate)
        trend_view["conversion_rate"] = trend_view["conversion_rate"].map(_format_rate)
        trend_view["revenue"] = trend_view["revenue"].map(_format_currency)
        st.dataframe(trend_view, hide_index=True, use_container_width=True)

    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Funnel drop-off")
        st.bar_chart(funnel_snapshot.set_index("stage"))
        st.dataframe(funnel_snapshot, hide_index=True, use_container_width=True)

    with right_col:
        st.subheader("Recommended next actions")
        for _, row in recommendations.iterrows():
            st.markdown(
                f"**{row['priority']} — {row['focus_area']}**  \n"
                f"{row['finding']}  \n"
                f"**Action:** {row['recommended_action']}  \n"
                f"*Evidence:* {row['evidence']}"
            )

    campaign_col, segment_col = st.columns(2)

    with campaign_col:
        st.subheader("Top campaigns")
        campaign_view = campaign_perf.head(8)[
            ["campaign_name", "campaign_type", "sends", "open_rate", "conversion_rate", "revenue"]
        ].copy()
        campaign_view["open_rate"] = campaign_view["open_rate"].map(_format_rate)
        campaign_view["conversion_rate"] = campaign_view["conversion_rate"].map(_format_rate)
        campaign_view["revenue"] = campaign_view["revenue"].map(_format_currency)
        st.dataframe(campaign_view, hide_index=True, use_container_width=True)

    with segment_col:
        st.subheader("Segment value")
        segment_view = segment_perf[
            ["segment", "contacts", "open_rate", "conversion_rate", "revenue_per_contact", "revenue"]
        ].copy()
        segment_view["open_rate"] = segment_view["open_rate"].map(_format_rate)
        segment_view["conversion_rate"] = segment_view["conversion_rate"].map(_format_rate)
        segment_view["revenue_per_contact"] = segment_view["revenue_per_contact"].map(_format_currency)
        segment_view["revenue"] = segment_view["revenue"].map(_format_currency)
        st.dataframe(segment_view, hide_index=True, use_container_width=True)

    attribution_col, attribution_note_col = st.columns(2)

    with attribution_col:
        st.subheader("Attributed revenue drivers")
        attribution_view = campaign_attribution.head(6)[
            ["campaign_name", "campaign_type", "won_deals", "attributed_revenue", "revenue_share"]
        ].copy()
        attribution_view["attributed_revenue"] = attribution_view["attributed_revenue"].map(_format_currency)
        attribution_view["revenue_share"] = attribution_view["revenue_share"].map(_format_rate)
        st.dataframe(attribution_view, hide_index=True, use_container_width=True)

    with attribution_note_col:
        st.subheader("Attribution assumption")
        st.write(
            "Won revenue is attributed with a simple last-touch model. In this project, that means revenue is "
            "credited to the campaign attached to the conversion record, which represents the most recent clicked "
            "campaign in the synthetic funnel."
        )
        top_attribution = campaign_attribution.iloc[0]
        st.markdown(
            f"**Top attributed revenue source:** `{top_attribution['campaign_name']}`  \n"
            f"**Model:** `{top_attribution['attribution_model']}`  \n"
            f"**Won deals:** {int(top_attribution['won_deals'])}  \n"
            f"**Revenue share:** {_format_rate(top_attribution['revenue_share'])}"
        )

    region_col, format_col = st.columns(2)

    with region_col:
        st.subheader("Region performance")
        region_view = region_perf[["region", "contacts", "open_rate", "conversion_rate", "revenue_per_contact", "revenue"]].copy()
        region_view["open_rate"] = region_view["open_rate"].map(_format_rate)
        region_view["conversion_rate"] = region_view["conversion_rate"].map(_format_rate)
        region_view["revenue_per_contact"] = region_view["revenue_per_contact"].map(_format_currency)
        region_view["revenue"] = region_view["revenue"].map(_format_currency)
        st.dataframe(region_view, hide_index=True, use_container_width=True)

    with format_col:
        st.subheader("Campaign type performance")
        format_view = campaign_type_perf[
            ["campaign_type", "campaigns", "sends", "open_rate", "conversion_rate", "revenue_per_campaign", "revenue"]
        ].copy()
        format_view["open_rate"] = format_view["open_rate"].map(_format_rate)
        format_view["conversion_rate"] = format_view["conversion_rate"].map(_format_rate)
        format_view["revenue_per_campaign"] = format_view["revenue_per_campaign"].map(_format_currency)
        format_view["revenue"] = format_view["revenue"].map(_format_currency)
        st.dataframe(format_view, hide_index=True, use_container_width=True)

    st.subheader("Why this matters")
    st.write(
        "This view is designed to answer practical questions: which campaigns are worth repeating, "
        "which segments or regions deserve more budget, and where the funnel still leaks demand."
    )


if __name__ == "__main__":
    main()
