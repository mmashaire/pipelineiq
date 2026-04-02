import pandas as pd

from pipelineiq.data_quality import run_data_quality_checks
from pipelineiq.synthetic_data import GenerationConfig, generate_pipeline_data


def test_data_quality_checks_pass_on_generated_baseline() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=500, n_campaigns=10, seed=67))
    checks = run_data_quality_checks(data)

    assert not checks.empty
    assert set(checks["status"]) == {"pass"}


def test_data_quality_checks_flag_orphan_send_records() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=300, n_campaigns=8, seed=71))
    broken_send = data["email_sends"].iloc[[0]].copy()
    broken_send.loc[:, "contact_id"] = 999999
    data["email_sends"] = pd.concat([data["email_sends"], broken_send], ignore_index=True)

    checks = run_data_quality_checks(data).set_index("check_name")

    assert checks.loc["send_foreign_keys", "status"] == "fail"
    assert int(checks.loc["send_foreign_keys", "failed_rows"]) >= 1


def test_data_quality_checks_flag_non_won_revenue() -> None:
    data = generate_pipeline_data(GenerationConfig(n_contacts=500, n_campaigns=10, seed=73))

    if data["opportunities"].empty:
        raise AssertionError("Expected opportunities in generated baseline data")

    lost_opportunity_id = data["opportunities"].iloc[0]["opportunity_id"]
    data["opportunities"].loc[data["opportunities"].index[0], "stage"] = "Lost"

    synthetic_revenue = pd.DataFrame(
        [{"revenue_event_id": 999999, "opportunity_id": lost_opportunity_id, "contact_id": int(data["opportunities"].iloc[0]["contact_id"]), "revenue_amount": 1000.0, "revenue_timestamp": pd.Timestamp("2025-08-01")}]
    )
    data["revenue_events"] = pd.concat([data["revenue_events"], synthetic_revenue], ignore_index=True)

    checks = run_data_quality_checks(data).set_index("check_name")

    assert checks.loc["revenue_only_on_won_deals", "status"] == "fail"
    assert int(checks.loc["revenue_only_on_won_deals", "failed_rows"]) >= 1