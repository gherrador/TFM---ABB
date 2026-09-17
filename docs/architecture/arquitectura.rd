BRONZE CSV
   |
   v
SILVER
- UTC canonical timestamps
- FCT -> LIN/WKA -> TU -> PS -> STEP hierarchy
- DQ + Quarantine
- lineage
- torque_applicable / torque_spec_status
   |
   v
GOLD
- dim_process (configuration-aware TU + PS + STEP)
- dim_date
- fact_process_daily
   |
   v
POSTGRESQL analytics
- incremental ETL state
- vw_process_daily_kpis
   |
   v
POWER BI
