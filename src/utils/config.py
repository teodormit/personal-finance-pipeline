"""
Config loading for account-filter presets.

Real account display names are personal data, so they live in a gitignored
`config/accounts.yaml` rather than in source. This module reads that file and
returns the presets mapping consumed by the incremental loader and the inspect
script. A committed `config/accounts.example.yaml` documents the structure with
fictional names — copy it to `config/accounts.yaml` on a new machine.
"""

from pathlib import Path

import yaml

# repo root = src/utils/config.py -> parents[2]
_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"
ACCOUNTS_FILE = _CONFIG_DIR / "accounts.yaml"
EXAMPLE_FILE = _CONFIG_DIR / "accounts.example.yaml"


def load_account_presets() -> dict:
    """Return the account-filter presets from ``config/accounts.yaml``.

    Structure (see ``config/accounts.example.yaml``)::

        presets:
          <name>:
            allowed_accounts: [ ... ]
            account_end_dates: { "<account>": "YYYY-MM-DD" }

    Returns the mapping under the top-level ``presets:`` key (a bare mapping of
    preset-name -> preset is also accepted for forward-compatibility).

    Raises:
        FileNotFoundError: if ``config/accounts.yaml`` is missing, with a message
            pointing at the committed example file.
    """
    if not ACCOUNTS_FILE.exists():
        raise FileNotFoundError(
            f"Account config not found: {ACCOUNTS_FILE}\n"
            f"Copy the committed template and edit it with your account names:\n"
            f"    cp config/accounts.example.yaml config/accounts.yaml"
        )

    with ACCOUNTS_FILE.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}

    # Support both `presets: {...}` and a bare top-level mapping.
    presets = data.get("presets", data) if isinstance(data, dict) else {}
    if not isinstance(presets, dict) or not presets:
        raise ValueError(
            f"No presets found in {ACCOUNTS_FILE}. "
            f"Expected a 'presets:' mapping — see {EXAMPLE_FILE.name}."
        )
    return presets
