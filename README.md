# Analytics Pipeline

A cross-pipeline data analytics asset that unifies order transaction records with machine learning order value predictions into a central data warehouse table for downstream business intelligence and reporting.

Built on top of [dagster-workshop-multi](https://github.com/dagster-workshop/dagster-workshop-multi), a multi-container Dagster workshop — see that repo's README for the base architecture (`pipeline_products`, `pipeline_fx`, `pipeline_ml`).

## What I built

- **Track:** Track B: cross-pipeline analytics
- **Data source:** Internal PostgreSQL data warehouse populated by `pipeline_products` (`orders` table) and `pipeline_ml` (`order_value_predictions` table).
- **Key assets:** 
  - `cross_pipeline_summary`: Performs an inner join between transactional order records and ML model predictions, persisting the consolidated result back into the warehouse.
- **Quality gate:** 
  - `check_summary_has_rows`: Verifies that the merged summary table contains at least one row (`row_count > 0`) to prevent downstream reporting issues caused by empty joins or upstream pipeline ingestion failures.

## Architecture
