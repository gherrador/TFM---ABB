# Semantic refactor checklist — final analytical contract

1. Bronze remains immutable (75 raw columns).
2. Silver projects exactly 24 source business fields.
3. Confirm hierarchy: FCT -> LIN/WKA -> TU -> PS -> STEP -> RES.
4. Confirm `PS_Comment -> process_step_name` and STEP fields represent sub-step context.
5. Confirm only `RES_ID`, `RES_Report`, `RES_DateTime`, `RES_FinalTorque` are promoted from RES.
6. Confirm `RES_VIN` / product-traceability KPI logic is absent.
7. Confirm `source_result_status` is reference metadata, not the torque quality KPI.
8. Confirm torque conformity is derived from final torque vs min/max limits.
9. Gold fact contains only event, applicability, conformity and torque statistics.
10. PostgreSQL serving view matches Gold.
11. Run notebooks 02 -> 03 -> 04 after migration; auto-version migration handles stale outputs.
12. Refresh Power BI and repair only measures/visuals affected by removed columns.
