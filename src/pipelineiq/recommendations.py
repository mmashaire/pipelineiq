"""Decision-support recommendations for PipelineIQ.

This module turns campaign, segment, and funnel metrics into a short list of
practical next actions a business user could take.
"""

from __future__ import annotations

import pandas as pd

LOW_OPEN_RATE_THRESHOLD = 0.12
LOW_CLICK_TO_OPEN_THRESHOLD = 0.16
LOW_CONVERSION_THRESHOLD = 0.025


def _safe_pct(value: float) -> str:
    return f"{float(value):.1%}"


def _safe_currency(value: float) -> str:
    return f"€{float(value):,.0f}"


def build_recommendation_summary(
    funnel_kpis: pd.DataFrame,
    campaign_perf: pd.DataFrame,
    segment_perf: pd.DataFrame,
) -> pd.DataFrame:
    """Build a short decision-support summary from the current KPI tables.

    Returns:
        DataFrame with one row per recommendation and columns for priority,
        focus area, business finding, recommended action, and supporting evidence.
    """

    recommendations: list[dict[str, str]] = []

    if not segment_perf.empty:
        ranked_segments = segment_perf.sort_values(
            ["revenue_per_contact", "conversion_rate"],
            ascending=[False, False],
            ignore_index=True,
        )
        top_segment = ranked_segments.iloc[0]
        recommendations.append(
            {
                "priority": "High",
                "focus_area": "Targeting",
                "finding": (
                    f"{top_segment['segment']} is the strongest audience by revenue per contact."
                ),
                "recommended_action": (
                    f"Prioritize {top_segment['segment']} in the next campaign wave and reuse the offers "
                    "that already convert with this audience."
                ),
                "evidence": (
                    f"Revenue/contact {_safe_currency(top_segment['revenue_per_contact'])}; "
                    f"conversion rate {_safe_pct(top_segment['conversion_rate'])}."
                ),
            }
        )

        weakest_segment = segment_perf.sort_values(
            ["revenue_per_contact", "open_rate"],
            ascending=[True, True],
            ignore_index=True,
        ).iloc[0]
        weak_segment_name = str(weakest_segment["segment"])
        if weak_segment_name == "Inactive" or float(weakest_segment["open_rate"]) <= LOW_OPEN_RATE_THRESHOLD:
            recommendations.append(
                {
                    "priority": "High",
                    "focus_area": "Re-engagement",
                    "finding": (
                        f"{weak_segment_name} is underperforming and shows weak email engagement."
                    ),
                    "recommended_action": (
                        f"Run a re-engagement sequence for {weak_segment_name} or suppress cold contacts "
                        "from broad sends until the audience quality improves."
                    ),
                    "evidence": (
                        f"Open rate {_safe_pct(weakest_segment['open_rate'])}; "
                        f"revenue/contact {_safe_currency(weakest_segment['revenue_per_contact'])}."
                    ),
                }
            )

    if not campaign_perf.empty:
        campaign_candidate = campaign_perf.sort_values(
            ["open_rate", "conversion_rate"],
            ascending=[False, True],
            ignore_index=True,
        ).iloc[0]
        recommendations.append(
            {
                "priority": "Medium",
                "focus_area": "Campaign optimization",
                "finding": (
                    f"{campaign_candidate['campaign_name']} earns attention but loses momentum after the open."
                ),
                "recommended_action": (
                    f"Keep the subject line approach from {campaign_candidate['campaign_name']}, but improve the CTA, "
                    "landing page, and follow-up flow to lift post-click conversion."
                ),
                "evidence": (
                    f"Open rate {_safe_pct(campaign_candidate['open_rate'])}; "
                    f"conversion rate {_safe_pct(campaign_candidate['conversion_rate'])}; "
                    f"revenue {_safe_currency(campaign_candidate['revenue'])}."
                ),
            }
        )

    if not funnel_kpis.empty:
        kpis = funnel_kpis.iloc[0]
        click_to_open_rate = float(kpis.get("click_to_open_rate", 0.0))
        conversion_rate = float(kpis.get("conversion_rate", 0.0))
        revenue_per_send = float(kpis.get("revenue_per_send", 0.0))

        if click_to_open_rate < LOW_CLICK_TO_OPEN_THRESHOLD:
            finding = "Too few opens are turning into clicks."
            action = "Tighten the CTA, message-to-offer match, and send-level targeting before scaling volume."
        elif conversion_rate < LOW_CONVERSION_THRESHOLD:
            finding = "Interest is visible, but the conversion step is still too weak."
            action = "Review the landing experience, form friction, and follow-up timing to recover more demand."
        else:
            finding = "The core funnel is converting at a healthy baseline."
            action = "Scale the best-performing segments and campaigns without adding unnecessary complexity."

        recommendations.append(
            {
                "priority": "Medium",
                "focus_area": "Funnel health",
                "finding": finding,
                "recommended_action": action,
                "evidence": (
                    f"Click-to-open {_safe_pct(click_to_open_rate)}; "
                    f"conversion rate {_safe_pct(conversion_rate)}; "
                    f"revenue/send {_safe_currency(revenue_per_send)}."
                ),
            }
        )

    if not recommendations:
        recommendations.append(
            {
                "priority": "Medium",
                "focus_area": "Data quality",
                "finding": "Recommendation logic could not find usable KPI inputs.",
                "recommended_action": "Re-run the pipeline and confirm the processed CSV outputs are available.",
                "evidence": "No campaign, segment, or funnel metrics were supplied.",
            }
        )

    return pd.DataFrame(recommendations)
