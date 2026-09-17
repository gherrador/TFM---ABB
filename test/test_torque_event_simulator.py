from __future__ import annotations

import random

from iot.torque_event_simulator import (
    SimulatorProbabilities,
    build_dq_test_events,
    build_torque_event,
)


CONFIG = {
    "factory_id": "F1",
    "factory_name": "Factory",
    "line_id": "L1",
    "line_name": "Line",
    "work_area_id": "W1",
    "work_area_name": "Area",
    "tightening_unit_id": "TU1",
    "tightening_unit_name": "TU1",
    "process_step_id": "PS1",
    "process_step_number": "1",
    "process_step_name": "Tighten",
    "substep_id": "S1",
    "substep_number": "1",
    "substep_type": "TIGHTENING",
    "target_torque": 10.0,
    "min_torque": 9.0,
    "max_torque": 11.0,
    "tool_id": "T1",
    "tool_number": "1",
    "tool_serial_number": "SER1",
}


def test_generated_in_spec_event_is_inside_limits() -> None:
    event = build_torque_event(
        [CONFIG],
        SimulatorProbabilities(existing_oos=0.0, new_tu_event=0.0, new_tu_oos=0.0),
        random.Random(42),
    )
    assert event["source_result_status"] == "OK"
    assert event["min_torque"] <= event["applied_torque"] <= event["max_torque"]


def test_generated_oos_event_is_outside_limits() -> None:
    event = build_torque_event(
        [CONFIG],
        SimulatorProbabilities(existing_oos=1.0, new_tu_event=0.0, new_tu_oos=0.0),
        random.Random(42),
    )
    assert event["source_result_status"] == "NOK"
    assert (
        event["applied_torque"] < event["min_torque"]
        or event["applied_torque"] > event["max_torque"]
    )


def test_non_applicable_config_is_not_fabricated_as_oos() -> None:
    config = dict(CONFIG)
    config.update(
        {
            "process_step_name": "Aflojar",
            "target_torque": 0.0,
            "min_torque": 0.0,
            "max_torque": 0.0,
            "_historical_weight": 1,
        }
    )
    event = build_torque_event(
        [config],
        SimulatorProbabilities(existing_oos=1.0, new_tu_event=0.0, new_tu_oos=0.0),
        random.Random(42),
    )
    assert event["source_result_status"] == "OK"
    assert event["applied_torque"] == 0.0


def test_historical_weights_affect_configuration_selection() -> None:
    frequent = dict(CONFIG)
    frequent["_historical_weight"] = 1000

    rare = dict(CONFIG)
    rare.update(
        {
            "tightening_unit_id": "TU-RARE",
            "tightening_unit_name": "TU-RARE",
            "process_step_id": "PS-RARE",
            "_historical_weight": 1,
        }
    )

    rng = random.Random(1234)
    events = [
        build_torque_event(
            [frequent, rare],
            SimulatorProbabilities(
                existing_oos=0.0,
                new_tu_event=0.0,
                new_tu_oos=0.0,
            ),
            rng,
        )
        for _ in range(250)
    ]
    rare_count = sum(event["tightening_unit_id"] == "TU-RARE" for event in events)
    assert rare_count <= 2

def test_dq_test_events_cover_all_six_quarantine_rules() -> None:
    events = build_dq_test_events([CONFIG], random.Random(42))
    assert len(events) == 6
    assert events[0]["result_id"] is None
    assert events[1]["event_timestamp"] is None
    assert events[2]["tightening_unit_id"] is None
    assert events[3]["process_step_id"] is None
    assert events[4]["substep_id"] is None
    assert events[5]["min_torque"] > events[5]["max_torque"]
    assert all(event["process_step_name"].startswith("DQ_TEST_") for event in events)

