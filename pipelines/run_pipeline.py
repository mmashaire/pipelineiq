"""Run the PipelineIQ v1 synthetic pipeline end-to-end."""

from __future__ import annotations

from pathlib import Path

import duckdb

from pipelineiq.funnel_metrics import (
    compute_campaign_performance,
    compute_campaign_type_performance,
    compute_funnel_kpis,
    compute_region_performance,
    compute_segment_performance,
)
from pipelineiq.recommendations import build_recommendation_summary
from pipelineiq.synthetic_data import GenerationConfig, generate_pipeline_data


def main() -> None:
    config = GenerationConfig(n_contacts=2500, n_campaigns=14, seed=42)
    data = generate_pipeline_data(config)

    output_dir = Path("data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)

    for name, df in data.items():
        df.to_csv(output_dir / f"{name}.csv", index=False)

    funnel_kpis = compute_funnel_kpis(data)
    campaign_perf = compute_campaign_performance(data)
    segment_perf = compute_segment_performance(data)
    region_perf = compute_region_performance(data)
    campaign_type_perf = compute_campaign_type_performance(data)
    action_recs = build_recommendation_summary(funnel_kpis, campaign_perf, segment_perf)

    funnel_kpis.to_csv(output_dir / "funnel_kpis.csv", index=False)
    campaign_perf.to_csv(output_dir / "campaign_performance.csv", index=False)
    segment_perf.to_csv(output_dir / "segment_performance.csv", index=False)
    region_perf.to_csv(output_dir / "region_performance.csv", index=False)
    campaign_type_perf.to_csv(output_dir / "campaign_type_performance.csv", index=False)
    action_recs.to_csv(output_dir / "recommended_actions.csv", index=False)

    db_path = output_dir / "pipelineiq.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        for name, df in data.items():
            conn.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM df")
        conn.execute("CREATE OR REPLACE TABLE funnel_kpis AS SELECT * FROM funnel_kpis")
        conn.execute("CREATE OR REPLACE TABLE campaign_performance AS SELECT * FROM campaign_perf")
        conn.execute("CREATE OR REPLACE TABLE segment_performance AS SELECT * FROM segment_perf")
        conn.execute("CREATE OR REPLACE TABLE region_performance AS SELECT * FROM region_perf")
        conn.execute("CREATE OR REPLACE TABLE campaign_type_performance AS SELECT * FROM campaign_type_perf")
        conn.execute("CREATE OR REPLACE TABLE recommended_actions AS SELECT * FROM action_recs")

    print("PipelineIQ v1 pipeline completed.")
    print(f"Rows generated: contacts={len(data['contacts'])}, sends={len(data['email_sends'])}, events={len(data['email_events'])}")
    print(f"Total revenue: {funnel_kpis.loc[0, 'total_revenue']:.2f}")


if __name__ == "__main__":
    main()
