from pipelineiq.synthetic_data import GenerationConfig, generate_pipeline_data


def test_generation_contains_expected_tables() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=400, n_campaigns=8, seed=7))
    expected = {
        "contacts",
        "campaigns",
        "email_sends",
        "email_events",
        "lead_progression",
        "conversions",
        "opportunities",
        "revenue_events",
    }
    assert set(data.keys()) == expected


def test_not_every_contact_gets_every_campaign() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=350, n_campaigns=10, seed=11))
    sends = data["email_sends"]
    unique_pairs = sends[["contact_id", "campaign_id"]].drop_duplicates().shape[0]
    theoretical_max = data["contacts"].shape[0] * data["campaigns"].shape[0]
    assert unique_pairs < theoretical_max


def test_clicks_do_not_exceed_opens() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=450, n_campaigns=10, seed=21))
    events = data["email_events"]
    opens = int((events["event_type"] == "open").sum())
    clicks = int((events["event_type"] == "click").sum())
    assert clicks <= opens


def test_inactive_segment_engages_less_than_active_segments() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=1200, n_campaigns=12, seed=29))

    sends = data["email_sends"][ ["send_id", "contact_id"] ].merge(
        data["contacts"][ ["contact_id", "segment"] ],
        on="contact_id",
        how="left",
    )
    events = data["email_events"][ ["send_id", "event_type"] ]

    send_summary = sends.groupby("segment", as_index=False).agg(sends=("send_id", "count"))
    open_summary = (
        sends.merge(events.loc[events["event_type"] == "open", ["send_id"]], on="send_id", how="inner")
        .groupby("segment", as_index=False)
        .agg(opens=("send_id", "count"))
    )

    summary = send_summary.merge(open_summary, on="segment", how="left").fillna(0)
    summary["open_rate"] = summary["opens"] / summary["sends"]

    inactive_rate = float(summary.loc[summary["segment"] == "Inactive", "open_rate"].iloc[0])
    active_average = float(
        summary.loc[summary["segment"].isin(["SMB", "Mid-Market", "Enterprise"]), "open_rate"].mean()
    )

    assert inactive_rate < active_average
