"""Data quality checks for PipelineIQ processed tables."""

from __future__ import annotations

from typing import Dict

import pandas as pd


def _build_check_result(check_name: str, status: str, detail: str, failed_rows: int) -> dict[str, str | int]:
    return {
        "check_name": check_name,
        "status": status,
        "detail": detail,
        "failed_rows": failed_rows,
    }


def run_data_quality_checks(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Run compact business-facing quality checks over the generated pipeline data."""

    contacts = data["contacts"]
    campaigns = data["campaigns"]
    sends = data["email_sends"]
    events = data["email_events"]
    leads = data["lead_progression"]
    conversions = data["conversions"]
    opportunities = data["opportunities"]
    revenue = data["revenue_events"]

    results: list[dict[str, str | int]] = []

    duplicate_contacts = int(contacts["contact_id"].duplicated().sum())
    results.append(
        _build_check_result(
            check_name="unique_contact_ids",
            status="pass" if duplicate_contacts == 0 else "fail",
            detail="Each contact should appear once in the contacts table.",
            failed_rows=duplicate_contacts,
        )
    )

    duplicate_campaigns = int(campaigns["campaign_id"].duplicated().sum())
    results.append(
        _build_check_result(
            check_name="unique_campaign_ids",
            status="pass" if duplicate_campaigns == 0 else "fail",
            detail="Each campaign should appear once in the campaigns table.",
            failed_rows=duplicate_campaigns,
        )
    )

    orphan_sends = int((~sends["contact_id"].isin(contacts["contact_id"])).sum()) + int(
        (~sends["campaign_id"].isin(campaigns["campaign_id"])).sum()
    )
    results.append(
        _build_check_result(
            check_name="send_foreign_keys",
            status="pass" if orphan_sends == 0 else "fail",
            detail="Every send should map to a valid contact and campaign.",
            failed_rows=orphan_sends,
        )
    )

    orphan_events = int((~events["send_id"].isin(sends["send_id"])).sum()) if not events.empty else 0
    results.append(
        _build_check_result(
            check_name="event_send_links",
            status="pass" if orphan_events == 0 else "fail",
            detail="Every email event should map back to an existing send.",
            failed_rows=orphan_events,
        )
    )

    orphan_conversions = int((~conversions["contact_id"].isin(contacts["contact_id"])).sum()) + int(
        (~conversions["campaign_id"].isin(campaigns["campaign_id"])).sum()
    )
    results.append(
        _build_check_result(
            check_name="conversion_foreign_keys",
            status="pass" if orphan_conversions == 0 else "fail",
            detail="Every conversion should map to a valid contact and campaign.",
            failed_rows=orphan_conversions,
        )
    )

    orphan_revenue = int((~revenue["opportunity_id"].isin(opportunities["opportunity_id"])).sum()) if not revenue.empty else 0
    results.append(
        _build_check_result(
            check_name="revenue_opportunity_links",
            status="pass" if orphan_revenue == 0 else "fail",
            detail="Every revenue event should map to an existing opportunity.",
            failed_rows=orphan_revenue,
        )
    )

    if not leads.empty:
        lead_stage_rank = {"Lead": 1, "MQL": 2, "SQL": 3}
        ranked_leads = leads.assign(stage_rank=leads["stage"].map(lead_stage_rank).fillna(0))
        stage_order_failures = int(
            ranked_leads.sort_values(["contact_id", "stage_timestamp"])
            .groupby("contact_id")["stage_rank"]
            .apply(lambda series: (series.diff().fillna(0) < 0).sum())
            .sum()
        )
    else:
        stage_order_failures = 0
    results.append(
        _build_check_result(
            check_name="lead_stage_order",
            status="pass" if stage_order_failures == 0 else "fail",
            detail="Lead stages should progress in order without moving backward in time.",
            failed_rows=stage_order_failures,
        )
    )

    if not conversions.empty and not leads.empty:
        latest_sql = (
            leads.loc[leads["stage"] == "SQL", ["contact_id", "stage_timestamp"]]
            .groupby("contact_id", as_index=False)
            .agg(sql_timestamp=("stage_timestamp", "max"))
        )
        conversion_order = conversions.merge(latest_sql, on="contact_id", how="left")
        conversion_before_sql = int(
            (
                conversion_order["sql_timestamp"].notna()
                & (pd.to_datetime(conversion_order["conversion_timestamp"]) < pd.to_datetime(conversion_order["sql_timestamp"]))
            ).sum()
        )
    else:
        conversion_before_sql = 0
    results.append(
        _build_check_result(
            check_name="conversion_after_sql",
            status="pass" if conversion_before_sql == 0 else "fail",
            detail="Conversions should not occur before the recorded SQL stage for the same contact.",
            failed_rows=conversion_before_sql,
        )
    )

    if not revenue.empty:
        revenue_with_stage = revenue.merge(opportunities[["opportunity_id", "stage"]], on="opportunity_id", how="left")
        non_won_revenue = int((revenue_with_stage["stage"] != "Won").sum())
        negative_revenue = int((revenue["revenue_amount"] < 0).sum())
    else:
        non_won_revenue = 0
        negative_revenue = 0
    results.append(
        _build_check_result(
            check_name="revenue_only_on_won_deals",
            status="pass" if non_won_revenue == 0 else "fail",
            detail="Revenue should only exist for opportunities marked as won.",
            failed_rows=non_won_revenue,
        )
    )
    results.append(
        _build_check_result(
            check_name="non_negative_revenue",
            status="pass" if negative_revenue == 0 else "fail",
            detail="Revenue amounts should never be negative.",
            failed_rows=negative_revenue,
        )
    )

    return pd.DataFrame(results)