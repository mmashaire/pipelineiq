"""Funnel and campaign metrics for PipelineIQ."""

from __future__ import annotations

from typing import Dict

import pandas as pd


def _rate(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Return a safe rate series that avoids divide-by-zero errors."""

    return (numerator / denominator.clip(lower=1)).astype(float)


def _compute_contact_dimension_performance(data: Dict[str, pd.DataFrame], dimension: str) -> pd.DataFrame:
    """Compute contact-based performance metrics grouped by a contact attribute."""

    contacts = data["contacts"][["contact_id", dimension]].copy()
    sends = data["email_sends"][["send_id", "contact_id"]].copy()
    events = data["email_events"][["send_id", "event_type"]].copy()
    leads = data["lead_progression"][["contact_id", "stage"]].copy()
    conversions = data["conversions"][["conversion_id", "contact_id"]].copy()
    opportunities = data["opportunities"][["conversion_id", "opportunity_id", "stage"]].copy()
    revenue = data["revenue_events"][["opportunity_id", "revenue_amount"]].copy()

    base = contacts.groupby(dimension, as_index=False).agg(contacts=("contact_id", "nunique"))

    sends_with_dimension = sends.merge(contacts, on="contact_id", how="left")
    sends_agg = sends_with_dimension.groupby(dimension, as_index=False).agg(sends=("send_id", "count"))

    if events.empty:
        event_agg = pd.DataFrame(columns=[dimension, "opens", "clicks"])
    else:
        event_agg = (
            sends_with_dimension.merge(events, on="send_id", how="inner")
            .pivot_table(
                index=dimension,
                columns="event_type",
                values="send_id",
                aggfunc="count",
                fill_value=0,
            )
            .reset_index()
            .rename_axis(None, axis=1)
            .rename(columns={"open": "opens", "click": "clicks"})
        )

    mql_agg = (
        leads.loc[leads["stage"] == "MQL"]
        .merge(contacts, on="contact_id", how="left")
        .groupby(dimension, as_index=False)
        .agg(mqls=("contact_id", "count"))
    )
    sql_agg = (
        leads.loc[leads["stage"] == "SQL"]
        .merge(contacts, on="contact_id", how="left")
        .groupby(dimension, as_index=False)
        .agg(sqls=("contact_id", "count"))
    )
    conv_agg = (
        conversions.merge(contacts, on="contact_id", how="left")
        .groupby(dimension, as_index=False)
        .agg(conversions=("conversion_id", "count"))
    )

    won_revenue = opportunities.loc[opportunities["stage"] == "Won"].merge(
        conversions, on="conversion_id", how="left"
    )
    won_revenue = won_revenue.merge(contacts, on="contact_id", how="left")
    if revenue.empty:
        won_agg = won_revenue.groupby(dimension, as_index=False).agg(won_opportunities=("opportunity_id", "count"))
        won_agg["revenue"] = 0.0
    else:
        won_agg = won_revenue.merge(revenue, on="opportunity_id", how="left").groupby(dimension, as_index=False).agg(
            won_opportunities=("opportunity_id", "count"),
            revenue=("revenue_amount", "sum"),
        )

    perf = base.merge(sends_agg, on=dimension, how="left")
    perf = perf.merge(event_agg, on=dimension, how="left")
    perf = perf.merge(mql_agg, on=dimension, how="left")
    perf = perf.merge(sql_agg, on=dimension, how="left")
    perf = perf.merge(conv_agg, on=dimension, how="left")
    perf = perf.merge(won_agg, on=dimension, how="left")
    perf = perf.fillna(0)

    perf["open_rate"] = _rate(perf["opens"], perf["sends"])
    perf["click_to_open_rate"] = _rate(perf["clicks"], perf["opens"])
    perf["conversion_rate"] = _rate(perf["conversions"], perf["sends"])
    perf["revenue_per_contact"] = _rate(perf["revenue"], perf["contacts"])

    return perf.sort_values("revenue", ascending=False, ignore_index=True)


def compute_funnel_kpis(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Compute end-to-end funnel KPIs from generated tables."""

    sends = data["email_sends"]
    events = data["email_events"]
    leads = data["lead_progression"]
    conversions = data["conversions"]
    revenue = data["revenue_events"]

    total_sends = len(sends)
    opens = int((events["event_type"] == "open").sum()) if not events.empty else 0
    clicks = int((events["event_type"] == "click").sum()) if not events.empty else 0

    mql_contacts = int((leads["stage"] == "MQL").sum()) if not leads.empty else 0
    sql_contacts = int((leads["stage"] == "SQL").sum()) if not leads.empty else 0
    conversions_count = len(conversions)
    won_count = len(revenue)
    total_revenue = float(revenue["revenue_amount"].sum()) if not revenue.empty else 0.0

    def _safe_ratio(num: float, den: float) -> float:
        return float(num / den) if den else 0.0

    return pd.DataFrame(
        [
            {
                "total_sends": total_sends,
                "open_rate": _safe_ratio(opens, total_sends),
                "click_to_open_rate": _safe_ratio(clicks, opens),
                "mql_rate": _safe_ratio(mql_contacts, total_sends),
                "sql_rate": _safe_ratio(sql_contacts, total_sends),
                "conversion_rate": _safe_ratio(conversions_count, total_sends),
                "won_opportunity_rate": _safe_ratio(won_count, max(conversions_count, 1)),
                "revenue_per_send": _safe_ratio(total_revenue, total_sends),
                "total_revenue": total_revenue,
            }
        ]
    )


def compute_campaign_performance(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Compute campaign-level performance with engagement and revenue signals."""

    sends = data["email_sends"][ ["send_id", "campaign_id"] ].copy()
    campaigns = data["campaigns"][ ["campaign_id", "campaign_name", "campaign_type"] ].copy()
    events = data["email_events"].copy()
    conversions = data["conversions"][ ["conversion_id", "campaign_id"] ].copy()
    opportunities = data["opportunities"][ ["conversion_id", "opportunity_id", "stage"] ].copy()
    revenue_events = data["revenue_events"][ ["opportunity_id", "revenue_amount"] ].copy()

    sends_agg = sends.groupby("campaign_id", as_index=False).agg(sends=("send_id", "count"))

    if events.empty:
        event_agg = pd.DataFrame(columns=["campaign_id", "opens", "clicks"])
    else:
        event_agg = (
            events.pivot_table(
                index="campaign_id",
                columns="event_type",
                values="send_id",
                aggfunc="count",
                fill_value=0,
            )
            .reset_index()
            .rename_axis(None, axis=1)
            .rename(columns={"open": "opens", "click": "clicks"})
        )

    conv_agg = conversions.groupby("campaign_id", as_index=False).agg(conversions=("conversion_id", "count"))

    won_revenue = opportunities.loc[opportunities["stage"] == "Won"].merge(
        revenue_events, on="opportunity_id", how="left"
    )
    won_revenue = won_revenue.merge(conversions, on="conversion_id", how="left")
    rev_agg = won_revenue.groupby("campaign_id", as_index=False).agg(
        won_opportunities=("opportunity_id", "count"),
        revenue=("revenue_amount", "sum"),
    )

    perf = campaigns.merge(sends_agg, on="campaign_id", how="left")
    perf = perf.merge(event_agg, on="campaign_id", how="left")
    perf = perf.merge(conv_agg, on="campaign_id", how="left")
    perf = perf.merge(rev_agg, on="campaign_id", how="left")
    perf = perf.fillna(0)

    def _rate(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
        return (numerator / denominator.clip(lower=1)).astype(float)

    perf["open_rate"] = _rate(perf["opens"], perf["sends"])
    perf["click_to_open_rate"] = _rate(perf["clicks"], perf["opens"])
    perf["conversion_rate"] = _rate(perf["conversions"], perf["sends"])
    perf["revenue_per_send"] = _rate(perf["revenue"], perf["sends"])

    return perf.sort_values("revenue", ascending=False, ignore_index=True)


def compute_segment_performance(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Compute segment-level performance to support targeting decisions."""

    return _compute_contact_dimension_performance(data, dimension="segment")


def compute_region_performance(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Compute region-level performance to compare geographic value and engagement."""

    return _compute_contact_dimension_performance(data, dimension="region")


def compute_campaign_type_performance(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Compute performance grouped by campaign type to compare content formats."""

    campaigns = data["campaigns"][["campaign_id", "campaign_type"]].copy()
    sends = data["email_sends"][["send_id", "campaign_id", "campaign_type"]].copy()
    events = data["email_events"][["send_id", "event_type"]].copy()
    conversions = data["conversions"][["conversion_id", "campaign_id"]].copy()
    opportunities = data["opportunities"][["conversion_id", "opportunity_id", "stage"]].copy()
    revenue = data["revenue_events"][["opportunity_id", "revenue_amount"]].copy()

    base = campaigns.groupby("campaign_type", as_index=False).agg(campaigns=("campaign_id", "nunique"))
    sends_agg = sends.groupby("campaign_type", as_index=False).agg(sends=("send_id", "count"))

    if events.empty:
        event_agg = pd.DataFrame(columns=["campaign_type", "opens", "clicks"])
    else:
        event_agg = (
            sends.merge(events, on="send_id", how="inner")
            .pivot_table(
                index="campaign_type",
                columns="event_type",
                values="send_id",
                aggfunc="count",
                fill_value=0,
            )
            .reset_index()
            .rename_axis(None, axis=1)
            .rename(columns={"open": "opens", "click": "clicks"})
        )

    conv_agg = conversions.merge(campaigns, on="campaign_id", how="left").groupby("campaign_type", as_index=False).agg(
        conversions=("conversion_id", "count")
    )

    won_revenue = opportunities.loc[opportunities["stage"] == "Won"].merge(
        conversions, on="conversion_id", how="left"
    )
    won_revenue = won_revenue.merge(campaigns, on="campaign_id", how="left")
    if revenue.empty:
        won_agg = won_revenue.groupby("campaign_type", as_index=False).agg(
            won_opportunities=("opportunity_id", "count")
        )
        won_agg["revenue"] = 0.0
    else:
        won_agg = won_revenue.merge(revenue, on="opportunity_id", how="left").groupby(
            "campaign_type", as_index=False
        ).agg(
            won_opportunities=("opportunity_id", "count"),
            revenue=("revenue_amount", "sum"),
        )

    perf = base.merge(sends_agg, on="campaign_type", how="left")
    perf = perf.merge(event_agg, on="campaign_type", how="left")
    perf = perf.merge(conv_agg, on="campaign_type", how="left")
    perf = perf.merge(won_agg, on="campaign_type", how="left")
    perf = perf.fillna(0)

    perf["open_rate"] = _rate(perf["opens"], perf["sends"])
    perf["click_to_open_rate"] = _rate(perf["clicks"], perf["opens"])
    perf["conversion_rate"] = _rate(perf["conversions"], perf["sends"])
    perf["revenue_per_campaign"] = _rate(perf["revenue"], perf["campaigns"])

    return perf.sort_values("revenue", ascending=False, ignore_index=True)
