import pandas as pd


def apply_data_quality_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica reglas de calidad de datos a nivel de fila y anota los eventos inválidos."""

    result = df.copy()
    result["dq_is_valid"] = True
    result["dq_error_codes"] = ""

    # DQ001 - El timestamp del evento es obligatorio.
    mask = result["event_timestamp"].isna()
    result.loc[mask, "dq_is_valid"] = False
    _append_error(result, mask, "DQ001_INVALID_TIMESTAMP")

    # DQ002 - El número de serie físico de la herramienta se conserva para análisis futuros de equipamiento.
    mask = result["tool_serial_number"].isna() | (
        result["tool_serial_number"].str.strip() == ""
    )
    result.loc[mask, "dq_is_valid"] = False
    _append_error(result, mask, "DQ002_MISSING_TOOL")

    # DQ003 - El torque medido no puede ser negativo cuando está presente.
    mask = result["applied_torque"].notna() & (result["applied_torque"] < 0)
    result.loc[mask, "dq_is_valid"] = False
    _append_error(result, mask, "DQ003_NEGATIVE_TORQUE")

    # DQ004 - Los límites de especificación deben ser coherentes cuando todos están disponibles.
    mask = (
        result["min_torque"].notna()
        & result["target_torque"].notna()
        & result["max_torque"].notna()
        & (
            (result["min_torque"] > result["target_torque"])
            | (result["target_torque"] > result["max_torque"])
        )
    )
    result.loc[mask, "dq_is_valid"] = False
    _append_error(result, mask, "DQ004_INVALID_TORQUE_RANGE")

    # DQ005 - El estado de resultado del origen se conserva como metadata fuente. Se valida su
    # dominio, pero no se utiliza para derivar KPIs de conformidad de torque.
    allowed_status = {"OK", "NOK"}
    mask = result["source_result_status"].notna() & ~result[
        "source_result_status"
    ].isin(allowed_status)
    result.loc[mask, "dq_is_valid"] = False
    _append_error(result, mask, "DQ005_INVALID_SOURCE_STATUS")

    return result


def _append_error(df: pd.DataFrame, mask: pd.Series, error_code: str) -> None:
    current = df.loc[mask, "dq_error_codes"]
    df.loc[mask, "dq_error_codes"] = current.apply(
        lambda value: error_code if value == "" else f"{value}|{error_code}"
    )
