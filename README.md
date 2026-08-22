# UrbanFlow

UrbanFlow is a portfolio data engineering platform for analyzing New York City mobility patterns from real NYC Taxi and Limousine Commission (TLC) trip records and a planned real weather source. The project is designed to demonstrate a production-style, end-to-end analytics workflow without using synthetic production data.

## Planned architecture

Real data sources → Azure Data Factory → Azure Data Lake Storage Gen2 → Bronze → Azure Databricks/PySpark and Delta Lake → Silver → Gold → Snowflake → dbt/SQL → Power BI

UrbanFlow follows a Medallion Architecture. Azure is the project's only cloud platform; AWS is not part of the design.

## Technology stack

- Python, SQL, PySpark, and Delta Lake
- Microsoft Azure, ADLS Gen2, Azure Data Factory, and Azure Databricks
- Snowflake and dbt
- GitHub Actions and Docker
- Power BI

## Current status

Phase 1, project foundation, is initialized. The repository currently contains the planned directory structure, project configuration, and design documentation. No data pipelines, cloud resources, datasets, warehouse objects, dashboards, or external integrations have been implemented yet.

See [`docs/Phases.md`](docs/Phases.md) for the implementation roadmap and [`docs/Architecture.md`](docs/Architecture.md) for the intended architecture.
