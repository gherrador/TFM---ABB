from __future__ import annotations

import pandas as pd


# Granularidad analítica reutilizable para agregaciones orientadas a torque.
# PS es el Process Step / operación; STEP es el subpaso dentro de ese PS.
TORQUE_ANALYSIS_GROUP_COLUMNS = [
    "tightening_unit_id",
    "process_step_id",
    "process_step_name",
    "substep_id",
    "substep_type",
]


def torque_applicability_mask(df: pd.DataFrame) -> pd.Series:
    """Devuelve si cada evento válido puede utilizarse en analítica orientada a torque."""

    return (
        df["target_torque"].notna()
        & df["target_torque"].gt(0)
        & df["min_torque"].notna()
        & df["max_torque"].notna()
        & df["min_torque"].le(df["target_torque"])
        & df["target_torque"].le(df["max_torque"])
        & df["applied_torque"].notna()
        & df["applied_torque"].gt(0)
    )


def add_torque_classification(df: pd.DataFrame) -> pd.DataFrame:
    """Añade la semántica torque_applicable e IN_SPEC/OUT_OF_SPEC/NOT_APPLICABLE."""

    result = df.copy()
    applicable = torque_applicability_mask(result)

    result["torque_applicable"] = applicable.astype(bool)
    result["torque_spec_status"] = pd.Series(
        "NOT_APPLICABLE", index=result.index, dtype="string"
    )

    in_spec = (
        applicable
        & result["applied_torque"].ge(result["min_torque"])
        & result["applied_torque"].le(result["max_torque"])
    )

    result.loc[in_spec, "torque_spec_status"] = "IN_SPEC"
    result.loc[applicable & ~in_spec, "torque_spec_status"] = "OUT_OF_SPEC"
    return result
