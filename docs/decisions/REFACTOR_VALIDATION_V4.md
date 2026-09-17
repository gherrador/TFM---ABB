# Refactor validation — Silver / Gold

Validation performed against the complete historical dataset:

- Bronze source files: 3
- June 2025 Bronze rows: 40,500
- September 2025 Bronze rows: 67,993
- November 2025 Bronze rows: 64,718
- Total Bronze rows: 173,211
- Bronze source columns: 75
- Silver source fields selected: 24
- Silver final columns after lineage, DQ, and derived fields: 31
- Valid Silver rows: 173,211
- Quarantine rows: 0
- `source_result_status`: 166,304 OK / 6,907 NOK (reference only)
- `torque_spec_status`: 165,325 IN_SPEC / 4,240 OUT_OF_SPEC / 3,646 NOT_APPLICABLE
- Torque-applicable events: 169,565
- Gold `dim_process`: 311 rows
- Gold `dim_date`: 64 rows
- Gold `fact_process_daily`: 5,545 rows
- Gold event reconciliation: 173,211

Automated unit tests: 24 passed.

The complete Bronze-to-Silver-to-Gold pipeline was executed against the three
available historical source files. Silver and Gold outputs reconcile exactly with
the 173,211 Bronze input records, and no historical records were routed to
Quarantine.

Notebooks 02 -> 03 -> 04 provide the intended validation path for the transformation,
Gold modelling, and PostgreSQL serving layers. Schema-version migration automatically
rebuilds stale Silver, Gold, and PostgreSQL outputs when an incompatible contract is
detected.