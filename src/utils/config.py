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
    """Return the account-filter presets.

    Resolution order:
      1. ``config/accounts.yaml`` (gitignored, real account names) if present.
      2. ``config/accounts.example.yaml`` (committed template, fictional names)
         as a fallback — so a fresh clone and CI can import the loader and run
         the suite without a personal config. A notice is printed when the
         fallback is used: the fictional accounts will filter out real data, so
         a real run without a personal config surfaces an obvious "kept 0 rows".

    Structure (see ``config/accounts.example.yaml``)::

        presets:
          <name>:
            allowed_accounts: [ ... ]
            account_end_dates: { "<account>": "YYYY-MM-DD" }

    Returns the mapping under the top-level ``presets:`` key (a bare mapping of
    preset-name -> preset is also accepted for forward-compatibility).

    Raises:
        FileNotFoundError: if neither the real config nor the template exists.
        ValueError: if the chosen file has no ``presets`` mapping.
    """
    if ACCOUNTS_FILE.exists():
        source = ACCOUNTS_FILE
    elif EXAMPLE_FILE.exists():
        source = EXAMPLE_FILE
        print(
            f"[config] {ACCOUNTS_FILE.name} not found — using template "
            f"{EXAMPLE_FILE.name} with fictional accounts. Copy it to "
            f"{ACCOUNTS_FILE.name} and edit it for real runs."
        )
    else:
        raise FileNotFoundError(
            f"No account config found. Expected {ACCOUNTS_FILE} or the "
            f"committed template {EXAMPLE_FILE}. Restore the template or run:\n"
            f"    cp config/accounts.example.yaml config/accounts.yaml"
        )

    with source.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}

    # Support both `presets: {...}` and a bare top-level mapping.
    presets = data.get("presets", data) if isinstance(data, dict) else {}
    if not isinstance(presets, dict) or not presets:
        raise ValueError(
            f"No presets found in {source}. "
            f"Expected a 'presets:' mapping — see {EXAMPLE_FILE.name}."
        )
    return presets
