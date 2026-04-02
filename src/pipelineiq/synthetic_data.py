"""Synthetic CRM and campaign data generation for PipelineIQ.

The generator creates related tables that mimic a lead-to-revenue flow while
remaining fully safe for public sharing.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict

import numpy as np
import pandas as pd


SEGMENTS = ["SMB", "Mid-Market", "Enterprise", "Inactive"]
REGIONS = ["Nordics", "DACH", "UKI", "North America"]
INDUSTRIES = ["SaaS", "Manufacturing", "Retail", "Professional Services"]
CAMPAIGN_TYPES = ["Newsletter", "Webinar", "Case Study", "Product Launch"]
ENGAGEMENT_BANDS = ["Low", "Medium", "High"]


@dataclass(frozen=True)
class GenerationConfig:
    """Settings for synthetic data generation.

    Attributes:
        n_contacts: Number of contacts to generate.
        n_campaigns: Number of campaigns to generate.
        seed: Random seed for deterministic output.
    """

    n_contacts: int = 2500
    n_campaigns: int = 14
    seed: int = 42


def _segment_weight(segment: str) -> float:
    return {
        "SMB": 0.55,
        "Mid-Market": 0.62,
        "Enterprise": 0.48,
        "Inactive": 0.18,
    }[segment]


def _engagement_band(segment: str, rng: np.random.Generator) -> str:
    weights = {
        "SMB": [0.22, 0.55, 0.23],
        "Mid-Market": [0.18, 0.50, 0.32],
        "Enterprise": [0.16, 0.46, 0.38],
        "Inactive": [0.74, 0.22, 0.04],
    }[segment]
    return str(rng.choice(ENGAGEMENT_BANDS, p=weights))


def _engagement_multiplier(engagement_band: str) -> float:
    return {
        "Low": 0.74,
        "Medium": 1.0,
        "High": 1.24,
    }[engagement_band]


def _campaign_profile(campaign_type: str, rng: np.random.Generator) -> Dict[str, str]:
    segment_options = {
        "Newsletter": ["All", "SMB", "Mid-Market"],
        "Webinar": ["Mid-Market", "Enterprise"],
        "Case Study": ["Mid-Market", "Enterprise", "SMB"],
        "Product Launch": ["SMB", "Mid-Market", "Enterprise"],
    }[campaign_type]
    return {
        "target_segment": str(rng.choice(segment_options)),
        "target_region": str(rng.choice(REGIONS)),
        "target_industry": str(rng.choice(INDUSTRIES)),
    }


def _audience_fit_multiplier(
    contact_segment: str,
    contact_region: str,
    contact_industry: str,
    campaign_segment: str,
    campaign_region: str,
    campaign_industry: str,
) -> float:
    multiplier = 1.0
    if campaign_segment != "All":
        multiplier *= 1.35 if contact_segment == campaign_segment else 0.58
    if contact_region == campaign_region:
        multiplier *= 1.12
    if contact_industry == campaign_industry:
        multiplier *= 1.18
    return multiplier


def _open_rate(segment: str, campaign_type: str, engagement_band: str, audience_fit: float) -> float:
    base = {
        "Newsletter": 0.31,
        "Webinar": 0.38,
        "Case Study": 0.34,
        "Product Launch": 0.29,
    }[campaign_type]
    lift = {
        "SMB": 0.00,
        "Mid-Market": 0.03,
        "Enterprise": 0.02,
        "Inactive": -0.16,
    }[segment]
    engagement_lift = {
        "Low": -0.07,
        "Medium": 0.0,
        "High": 0.08,
    }[engagement_band]
    fit_lift = 0.04 if audience_fit >= 1.2 else -0.03 if audience_fit < 0.85 else 0.0
    return max(0.02, min(0.85, base + lift + engagement_lift + fit_lift))


def _click_given_open_rate(segment: str, campaign_type: str, engagement_band: str, audience_fit: float) -> float:
    base = {
        "Newsletter": 0.16,
        "Webinar": 0.29,
        "Case Study": 0.24,
        "Product Launch": 0.18,
    }[campaign_type]
    lift = {
        "SMB": 0.01,
        "Mid-Market": 0.03,
        "Enterprise": 0.02,
        "Inactive": -0.08,
    }[segment]
    engagement_lift = {
        "Low": -0.05,
        "Medium": 0.0,
        "High": 0.06,
    }[engagement_band]
    fit_lift = 0.03 if audience_fit >= 1.2 else -0.02 if audience_fit < 0.85 else 0.0
    return max(0.01, min(0.75, base + lift + engagement_lift + fit_lift))


def _conversion_given_click_rate(segment: str, campaign_type: str, engagement_band: str, audience_fit: float) -> float:
    base = {
        "Newsletter": 0.08,
        "Webinar": 0.15,
        "Case Study": 0.13,
        "Product Launch": 0.11,
    }[campaign_type]
    lift = {
        "SMB": -0.01,
        "Mid-Market": 0.02,
        "Enterprise": 0.03,
        "Inactive": -0.05,
    }[segment]
    engagement_lift = {
        "Low": -0.03,
        "Medium": 0.0,
        "High": 0.04,
    }[engagement_band]
    fit_lift = 0.02 if audience_fit >= 1.2 else -0.01 if audience_fit < 0.85 else 0.0
    return max(0.005, min(0.55, base + lift + engagement_lift + fit_lift))


def generate_pipeline_data(config: GenerationConfig) -> Dict[str, pd.DataFrame]:
    """Generate related CRM/funnel tables.

    Returns a dictionary of DataFrames keyed by table name.
    """

    rng = np.random.default_rng(config.seed)

    contact_ids = np.arange(1, config.n_contacts + 1)
    segments = rng.choice(SEGMENTS, size=config.n_contacts, p=[0.34, 0.32, 0.2, 0.14])
    regions = rng.choice(REGIONS, size=config.n_contacts, p=[0.36, 0.24, 0.2, 0.2])
    industries = rng.choice(INDUSTRIES, size=config.n_contacts)

    contacts = pd.DataFrame(
        {
            "contact_id": contact_ids,
            "segment": segments,
            "region": regions,
            "industry": industries,
            "engagement_band": [_engagement_band(segment, rng) for segment in segments],
            "created_at": pd.Timestamp("2025-01-01")
            + pd.to_timedelta(rng.integers(0, 200, size=config.n_contacts), unit="D"),
        }
    )

    campaign_ids = np.arange(1, config.n_campaigns + 1)
    campaign_types = rng.choice(CAMPAIGN_TYPES, size=config.n_campaigns, p=[0.4, 0.22, 0.2, 0.18])

    campaign_profiles = [_campaign_profile(campaign_type, rng) for campaign_type in campaign_types]
    campaigns = pd.DataFrame(
        {
            "campaign_id": campaign_ids,
            "campaign_name": [f"Campaign {i:02d}" for i in campaign_ids],
            "campaign_type": campaign_types,
            "target_segment": [profile["target_segment"] for profile in campaign_profiles],
            "target_region": [profile["target_region"] for profile in campaign_profiles],
            "target_industry": [profile["target_industry"] for profile in campaign_profiles],
            "launch_date": pd.Timestamp("2025-02-01")
            + pd.to_timedelta(rng.integers(0, 120, size=config.n_campaigns), unit="D"),
        }
    ).sort_values("launch_date", ignore_index=True)

    send_rows = []
    send_id = 1
    for _, campaign in campaigns.iterrows():
        ctype = campaign["campaign_type"]
        for _, contact in contacts.iterrows():
            audience_fit = _audience_fit_multiplier(
                contact_segment=contact["segment"],
                contact_region=contact["region"],
                contact_industry=contact["industry"],
                campaign_segment=campaign["target_segment"],
                campaign_region=campaign["target_region"],
                campaign_industry=campaign["target_industry"],
            )
            type_weight = {
                "Newsletter": 1.08,
                "Webinar": 0.78,
                "Case Study": 0.82,
                "Product Launch": 0.74,
            }[ctype]
            prob_send = (
                _segment_weight(contact["segment"])
                * _engagement_multiplier(contact["engagement_band"])
                * type_weight
                * audience_fit
                * rng.uniform(0.72, 0.98)
            )
            prob_send = min(0.94, max(0.02, prob_send))
            if rng.random() < prob_send:
                send_rows.append(
                    {
                        "send_id": send_id,
                        "campaign_id": int(campaign["campaign_id"]),
                        "contact_id": int(contact["contact_id"]),
                        "send_timestamp": campaign["launch_date"]
                        + pd.to_timedelta(int(rng.integers(0, 72)), unit="h"),
                        "campaign_type": ctype,
                        "segment": contact["segment"],
                        "region": contact["region"],
                        "industry": contact["industry"],
                        "engagement_band": contact["engagement_band"],
                    }
                )
                send_id += 1

    email_sends = pd.DataFrame(send_rows)

    event_rows = []
    for _, send in email_sends.iterrows():
        campaign = campaigns.loc[campaigns["campaign_id"] == send["campaign_id"]].iloc[0]
        audience_fit = _audience_fit_multiplier(
            contact_segment=send["segment"],
            contact_region=send["region"],
            contact_industry=send["industry"],
            campaign_segment=campaign["target_segment"],
            campaign_region=campaign["target_region"],
            campaign_industry=campaign["target_industry"],
        )
        p_open = _open_rate(send["segment"], send["campaign_type"], send["engagement_band"], audience_fit)
        opened = rng.random() < p_open
        if opened:
            open_ts = send["send_timestamp"] + pd.to_timedelta(int(rng.integers(1, 48)), unit="h")
            event_rows.append(
                {
                    "send_id": int(send["send_id"]),
                    "contact_id": int(send["contact_id"]),
                    "campaign_id": int(send["campaign_id"]),
                    "event_type": "open",
                    "event_timestamp": open_ts,
                }
            )
            p_click = _click_given_open_rate(
                send["segment"],
                send["campaign_type"],
                send["engagement_band"],
                audience_fit,
            )
            clicked = rng.random() < p_click
            if clicked:
                click_ts = open_ts + pd.to_timedelta(int(rng.integers(1, 24)), unit="h")
                event_rows.append(
                    {
                        "send_id": int(send["send_id"]),
                        "contact_id": int(send["contact_id"]),
                        "campaign_id": int(send["campaign_id"]),
                        "event_type": "click",
                        "event_timestamp": click_ts,
                    }
                )

    email_events = pd.DataFrame(event_rows)

    clicked_pairs = set(
        tuple(x)
        for x in email_events.loc[email_events["event_type"] == "click", ["contact_id", "campaign_id"]].to_numpy()
    )

    lead_rows = []
    conv_rows = []
    opp_rows = []
    rev_rows = []

    now = datetime(2025, 9, 1)
    conversion_id = 1
    opportunity_id = 1
    revenue_id = 1

    for contact_id, segment, region, industry, engagement_band in contacts[
        ["contact_id", "segment", "region", "industry", "engagement_band"]
    ].itertuples(index=False):
        lead_rows.append(
            {
                "contact_id": int(contact_id),
                "stage": "Lead",
                "stage_timestamp": now - timedelta(days=int(rng.integers(150, 240))),
            }
        )

        contact_clicks = [pair for pair in clicked_pairs if pair[0] == int(contact_id)]
        if not contact_clicks:
            continue

        lead_rows.append(
            {
                "contact_id": int(contact_id),
                "stage": "MQL",
                "stage_timestamp": now - timedelta(days=int(rng.integers(70, 140))),
            }
        )

        # Use the most recent clicked campaign to drive conversion likelihood.
        _, campaign_id = contact_clicks[-1]
        campaign = campaigns.loc[campaigns["campaign_id"] == campaign_id].iloc[0]
        audience_fit = _audience_fit_multiplier(
            contact_segment=segment,
            contact_region=region,
            contact_industry=industry,
            campaign_segment=campaign["target_segment"],
            campaign_region=campaign["target_region"],
            campaign_industry=campaign["target_industry"],
        )
        p_convert = _conversion_given_click_rate(segment, campaign["campaign_type"], engagement_band, audience_fit)

        if rng.random() < p_convert:
            lead_rows.append(
                {
                    "contact_id": int(contact_id),
                    "stage": "SQL",
                    "stage_timestamp": now - timedelta(days=int(rng.integers(20, 80))),
                }
            )
            conv_rows.append(
                {
                    "conversion_id": conversion_id,
                    "contact_id": int(contact_id),
                    "campaign_id": int(campaign_id),
                    "conversion_timestamp": now - timedelta(days=int(rng.integers(15, 70))),
                    "conversion_type": "Demo Booked",
                }
            )

            base_deal_size = {
                "SMB": 6500,
                "Mid-Market": 15000,
                "Enterprise": 28500,
                "Inactive": 4000,
            }[segment]
            industry_multiplier = {
                "SaaS": 1.12,
                "Manufacturing": 1.06,
                "Retail": 0.88,
                "Professional Services": 0.94,
            }[industry]
            region_multiplier = {
                "Nordics": 1.04,
                "DACH": 1.08,
                "UKI": 1.0,
                "North America": 1.15,
            }[region]
            opp_value = float(rng.normal(base_deal_size * industry_multiplier * region_multiplier, 4500))
            opp_value = max(1800.0, round(opp_value, 2))
            win_rate = {
                "SMB": 0.42,
                "Mid-Market": 0.49,
                "Enterprise": 0.52,
                "Inactive": 0.18,
            }[segment]
            if engagement_band == "High":
                win_rate += 0.05
            elif engagement_band == "Low":
                win_rate -= 0.05
            opp_rows.append(
                {
                    "opportunity_id": opportunity_id,
                    "conversion_id": conversion_id,
                    "contact_id": int(contact_id),
                    "stage": "Won" if rng.random() < min(0.8, max(0.05, win_rate)) else "Lost",
                    "expected_value": opp_value,
                }
            )

            if opp_rows[-1]["stage"] == "Won":
                rev_rows.append(
                    {
                        "revenue_event_id": revenue_id,
                        "opportunity_id": opportunity_id,
                        "contact_id": int(contact_id),
                        "revenue_amount": round(opp_value * rng.uniform(0.92, 1.08), 2),
                        "revenue_timestamp": now - timedelta(days=int(rng.integers(1, 30))),
                    }
                )
                revenue_id += 1

            conversion_id += 1
            opportunity_id += 1

    lead_progression = pd.DataFrame(lead_rows)
    conversions = pd.DataFrame(conv_rows)
    opportunities = pd.DataFrame(opp_rows)
    revenue_events = pd.DataFrame(rev_rows)

    return {
        "contacts": contacts,
        "campaigns": campaigns,
        "email_sends": email_sends,
        "email_events": email_events,
        "lead_progression": lead_progression,
        "conversions": conversions,
        "opportunities": opportunities,
        "revenue_events": revenue_events,
    }
