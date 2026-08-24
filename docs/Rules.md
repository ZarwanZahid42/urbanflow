# UrbanFlow Engineering Rules

## Code quality

- Keep modules small, cohesive, typed where practical, and free of unused code.
- Prefer straightforward implementations over premature abstraction.
- Add a dependency only when it solves a current requirement.
- Run the relevant formatter, linter, and tests before merging changes.

## Naming conventions

- Use `snake_case` for Python functions, variables, modules, tables, columns, and SQL models.
- Use `PascalCase` for Python classes and `UPPER_SNAKE_CASE` for constants.
- Use descriptive domain names and avoid unexplained abbreviations.
- Name storage paths and jobs consistently by layer, source, entity, and purpose.

## Python style

- Target Python 3.11 or later and follow PEP 8.
- Use type hints on public functions and concise docstrings for non-obvious behavior.
- Separate I/O, transformation logic, and configuration.
- Do not embed environment-specific paths, identifiers, or credentials in code.

## SQL style

- Use lowercase `snake_case` identifiers and explicit column lists.
- Write readable CTE-based transformations and qualify ambiguous columns.
- Avoid `select *` in production models.
- Document business logic and use deterministic ordering where results depend on order.

## Secrets and environment variables

- Never commit passwords, tokens, keys, connection strings, or populated `.env` files.
- Local secrets belong in ignored environment files; cloud secrets belong in approved secret stores.
- Commit placeholders only in `.env.example`.
- Fail clearly when required configuration is absent; never silently use insecure defaults.

## Data quality

- Define checks for schema, uniqueness, nullability, accepted values, ranges, and relationships.
- Quarantine invalid records when recovery or investigation is valuable.
- Record quality results and thresholds by run and dataset.
- Reconcile row counts and key metrics between material processing boundaries.

## Idempotency

- Every pipeline step must be safe to retry for the same batch or partition.
- Use deterministic keys and merge/overwrite semantics appropriate to the dataset.
- Do not append blindly where a retry could duplicate data.

## Incremental processing

- Parameterize processing by an explicit watermark, batch, or bounded date range.
- Persist successful watermarks only after downstream completion.
- Support deliberate backfills without disturbing unrelated partitions.

## Logging

- Emit structured logs with run ID, component, dataset, stage, and status.
- Record start/end times, source identifiers, row counts, and error context.
- Never log secrets or unnecessarily expose sensitive configuration.

## Error handling

- Fail fast on invalid configuration and critical schema violations.
- Use bounded retries only for transient failures.
- Preserve actionable error context and distinguish rejected data from infrastructure failures.
- Do not suppress exceptions without recording and handling the failure state.

## Testing

- Unit-test deterministic transformation and utility logic.
- Add integration tests at storage, orchestration, and warehouse boundaries when those components exist.
- Test critical dbt constraints and business relationships.
- Include regression tests for every corrected defect where practical.

## dbt engineering (Phase 8)

- Treat the committed Phase 7 `URBANFLOW.ANALYTICS` tables as immutable upstream source contracts for dbt.
- Declare upstream relations with explicit `source()` definitions and express model dependencies with `ref()`; do not bypass lineage with hardcoded relation names in model SQL.
- Keep dbt credentials, private keys, passwords, tokens, account identifiers, and populated connection profiles out of Git. `profiles.yml` must remain external or ignored and read sensitive values from environment variables or an approved secret manager.
- Never place secrets in `dbt_project.yml`, schema YAML, model SQL, macros, tests, logs, generated documentation, or tracked example configuration.
- Do not mutate Phase 7 LANDING, ANALYTICS, or AUDIT tables, weaken their contracts, or rewrite validated source data merely to make a dbt test pass.
- Keep staging models thin and deterministic. Do not duplicate transformations already correctly owned by Databricks Silver/Gold without a documented analytical or warehouse-specific reason.
- Keep the current staging and mart relations as views and `int_trip_enriched` ephemeral unless live workload evidence justifies a different persisted design. Do not introduce incremental state without a tested unique key and bounded retry/backfill contract.
- Build marts only for a declared business grain and consumer need; use explicit columns, stable keys, and idempotent materializations. Incremental models require a documented unique key and bounded retry/backfill behavior.
- Test source and model nullability, uniqueness, accepted values, relationships, freshness where meaningful, and documented business rules. Reconcile important measures with the Phase 7 upstream evidence.
- Generate and review dbt lineage/documentation, but do not commit generated `target/`, `logs/`, package directories, or local profile artifacts.
- Snowflake grants, warehouse usage, authentication, environment setup, and any cloud changes require explicit manual acknowledgement and least privilege.

## Documentation and portfolio accuracy

- Keep architecture, setup, operational steps, schemas, metrics, phase status, and decisions aligned with implementation and live evidence.
- Update `Memory.md` when an architectural or consequential implementation decision is made.
- Never claim that a planned, evaluated, or partially prepared feature is implemented, deployed, scheduled, or validated.
- Never fabricate cloud identifiers, resources, permissions, execution histories, screenshots, counts, or test results.
- Label production-oriented design separately from continuously operating production deployment.
- Preserve the implemented Python → ADLS Gen2 → Databricks → Snowflake → dbt architecture as the primary project narrative.
- Treat Azure Data Factory, Power BI, CI/CD deployment, infrastructure automation, centralized monitoring, and downstream weather enrichment as deferred until both repository implementation and validation evidence exist.
- Phase 9 is a completed scope review with ADF deferred. Do not resume ADF or create ADF resources automatically.
- Do not start Phase 10 without explicit user instruction.

## Validated contract preservation

- Preserve immutable source objects and Bronze source-fidelity behavior.
- Preserve the `source_year` / `source_month` retry boundary and exact partition-replacement semantics.
- Preserve deterministic trip keys, structured rejection evidence, and retained financial-adjustment/null-passenger semantics.
- Preserve Snowflake LANDING validation, transactional ANALYTICS replacement, AUDIT evidence, reconciliation, and two-pass idempotency.
- Preserve `URBANFLOW.ANALYTICS` as the governed read-only dbt source and `URBANFLOW.DBT_DEV` as the separate dbt target.
- Do not change a validated upstream contract merely to make documentation, a downstream model, or a test easier.
- Re-run affected tests and reconciliation checks whenever a contract intentionally changes.

## Git conventions

- Use focused branches and small, coherent commits with imperative messages.
- Do not commit generated artifacts, local data, raw datasets, private keys, populated profiles, secrets, or environment-specific state.`r`n- Never commit dbt `target/`, `logs/`, `dbt_packages/`, or generated documentation output.
- Review diffs and ensure tests pass before opening or merging a pull request.
- Commit at meaningful phase milestones rather than committing incomplete scaffolding by default.

## Cloud-resource safety

- Do not create, modify, or delete Azure, Databricks, Snowflake, Power BI, or other external resources without explicit instruction.
- Inspect the target environment, subscription/account, region, and resource names before approved changes.
- Prefer least privilege, tagging, cost controls, and reversible changes.
- Never introduce AWS services unless the project scope is explicitly changed.

## Data and dependency constraints

- Never use fake data as production input or present synthetic results as real analysis.
- Small test fixtures may be introduced only when needed for automated tests and must be clearly identified.
- Do not add unnecessary dependencies or install packages globally.
- Pin or constrain dependencies when implementation begins and review their licenses and security posture.
