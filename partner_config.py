"""Partner configuration registry — loads and manages per-company defaults."""

import json
import os

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "partner_configs", "default.json")


def load_configs():
    """Load all partner configs from JSON file."""
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def save_configs(configs):
    """Save partner configs back to JSON file."""
    with open(CONFIG_PATH, "w") as f:
        json.dump(configs, f, indent=2)


def get_config(company_id):
    """Get config for a specific company. Returns defaults if not found."""
    configs = load_configs()
    company_id_str = str(company_id)
    if company_id_str in configs:
        return configs[company_id_str]
    # Return a blank default config
    return {
        "name": "",
        "default_position_id": 29,
        "default_position_tiering_id": None,
        "default_break_length": 30,
        "default_parking": 2,
        "default_attire": "",
        "default_location_instructions": "",
        "default_position_instructions": "",
        "default_creator_id": None,
        "default_contact_id": None,
        "clock_out_task_ids": [],
        "special_requirement_ids": [],
        "training_ids": [],
        "certification_ids": [],
        "needs_booking_group": False,
        "is_task_based": False,
        "store_number_field": "Store #",
        "adjusted_base_rate": None,
        "column_mapping": {
            "store_number": "Store #",
            "address": "Address",
            "city": "City",
            "state": "State",
            "zip": "Zip",
            "start_date": "Start Date",
            "start_time": "Start Time",
            "end_time": "End Time",
            "break_length": "Break",
            "quantity": "# of Workers",
            "team_lead": "Team Lead",
        },
    }


def save_config(company_id, config_data):
    """Save/update config for a specific company."""
    configs = load_configs()
    configs[str(company_id)] = config_data
    save_configs(configs)
