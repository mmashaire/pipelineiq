# PipelineIQ Overview

PipelineIQ is a public portfolio project that simulates a realistic CRM and campaign data flow:

campaign -> send -> engagement -> lead progression -> conversion -> revenue

The goal is to show practical business understanding, not only coding output.

## What v1 Includes

- Synthetic CRM-style data generation with realistic drop-offs
- Campaign targeting fields for segment, region, and industry
- Contact engagement bands so some audiences are easier to activate than others
- Funnel, campaign, segment, region, and campaign-type metrics
- Recommendation outputs for next-best business actions
- A lightweight Streamlit dashboard for quick, easy inspection
- Local DuckDB + CSV outputs for transparent inspection
- Tests for data realism and metric sanity

## Why It Matters

This setup helps answer business questions quickly:

- Which campaigns and segments drive conversions?
- Where are leads dropping out of the funnel?
- Which segments and regions deserve the next campaign budget?
- Which segments create the strongest revenue per contact?
- Which campaign types are worth repeating?
- What should the business do next based on the current results?
