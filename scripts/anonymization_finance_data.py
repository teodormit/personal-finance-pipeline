"""
Anonymized finance data export
================================

Exports silver + gold data as a CSV for public sharing (Tableau Public, portfolio).
Preserves all analytical columns so existing dashboards keep working.
Anonymizes at the value level only — pipeline-internal columns with no dashboard
value are dropped (source_raw_id, created_by).

Anonymization applied:
  1. description     → synthesized as "{Merchant} – {City}" using a subcategory-keyed
                       lookup dict; city is rule-based (Sofia default, foreign for
                       subscriptions/travel, seasonal Burgas in summer)
  2. payee           → frequency-ranked aliases ("Merchant #1", "Merchant #2", …)
  3. account_name    → frequency-ranked aliases ("Account #1", "Account #2", …)
  4. account_group   → NEW derived column; coarse top-3 grouping of account_name
                       ("Account Group #1/2/3", "Account Group #other")
  5. labels          → cleared to NULL (free-text user tags)
  6. amounts         → all monetary columns scaled by SCALE constant;
                       gold EUR stats (hist_avg/std) scaled consistently
  7. reason text     → euro figures embedded in notability_reason (e.g.
                       "avg €420") scaled by SCALE so they stay consistent
                       with the scaled amount columns

z-score columns are left unscaled: a z-score is scale-invariant, so scaling
the amount and its mean/std by the same factor leaves z unchanged. Scaling z
would only make it inconsistent with the visible scaled amounts.

transaction_hash is kept as-is — it is an opaque SHA-256 digest with no
human-readable personal data.

Output: data/exports/anonymized_<YYYY-MM-DD>.csv

Run:
  python scripts/anonymization_finance_data.py
  docker compose run --rm pipeline python scripts/anonymization_finance_data.py
"""

import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd

# Windows consoles default to cp1252, which cannot encode the '→'/'€' glyphs
# printed below; force UTF-8 so a successful run never crashes on output.
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

_project_root = Path(__file__).resolve().parent.parent
_src_path = _project_root / "src"
if str(_src_path) not in sys.path:
    sys.path.insert(0, str(_src_path))

from utils.db_connector import get_db_connector

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCALE = 0.73  # multiplier applied to all monetary and z-score columns

AMOUNT_COLS = (
    "amount", "amount_abs",
    "amount_eur", "amount_abs_eur",
    "amount_bgn", "amount_abs_bgn",
)

GOLD_EUR_STAT_COLS = ("hist_avg_amount_eur", "hist_std_amount_eur")

# Free-text reason columns may embed real euro figures (e.g. "avg €420");
# these are scaled to stay consistent with the scaled amount columns.
REASON_COLS = ("notability_reason", "save_potential_reason")

# Pure pipeline internals & source-system identifiers — no dashboard value
DROP_COLS = {"source_raw_id", "created_by", "source_record_id", "category_id"}

# ---------------------------------------------------------------------------
# Description synthesis — merchant names & city rules
# ---------------------------------------------------------------------------

# Keys matched as substrings of the lowercased subcategory.
# Ordering: longer/more-specific keys before shorter overlapping ones
# ("barber" before "bar"; "fast food" before "food"; "transport" before
# "sport"; "beauty" before "doctor"; "tv, stream" before "stream").
# Subcategory names come from BudgetBakers/Wallet and are capitalized in DB.
MERCHANT_NAMES: dict[str, list[str]] = {
    # Personal care
    "barber":      ["101 Barber Shop", "Men's Lounge", "Fade Brothers", "BarberStyle Sofia"],
    # Food & drink (specific → generic)
    "fast food":   ["McDonald's", "Speedy Burger", "French Tacos", "Rolling Dogs",
                    "Mekitsa & Co", "Happy Pancakes", "Blue Taste Poké"],
    "deliver":     ["Wolt", "Glovo", "foodpanda"],
    "coffee":      ["Dabov Specialty Coffee", "Father Coffee Vinyls", "Blend Coffee Lab",
                    "Single Origin Roasters", "Kaffeina", "Mocca House", "Black Sheep Coffee"],
    "bar, cafe":   ["Club Dom", "Holy Smokes Bar", "Exe Club", "Absolut Bar",
                    "Dabov Specialty Coffee", "Amora Coffee Shop", "Vinyl Underground"],
    "restaurant":  ["Manastirska Magernitsa", "Mamma Mia Trattoria", "Gozba", "Batlars Bistro",
                    "Osteria Tartufo", "La Bottega", "Happy Bar & Grill",
                    "El Asador", "Vapiano", "Victoria Restaurant"],
    "food":        ["Wolt", "Gozba", "Food Box", "Speedy Burger", "Manali"],
    # Alcohol & tobacco
    "alcohol":     ["Wine & More", "Alko DJ", "Metro Wines", "VinCo", "Premium Tabako"],
    "tobacco":     ["Top Tobacco", "Premium Tabako", "Trishur Tobacco", "Tabachki"],
    # Groceries
    "grocer":      ["Lidl", "T-Market", "Kaufland", "Billa", "Fantastico", "CBA Market"],
    # Shopping
    "online":      ["Amazon", "Temu", "eMag", "Ozone.bg", "Remixshop"],
    "cloth":       ["Reserved", "Zara", "H&M", "Pull&Bear", "Bershka", "Stradivarius", "Mango"],
    "electron":    ["Technopolis", "Electro Camel", "Media Markt", "Samsung Store"],
    "home":        ["IKEA", "Bauhaus", "Jysk", "Mr. Bricolage", "Praktiker"],
    "book":        ["Helikon Bookstore", "Ciela", "Orange Bookstore",
                    "Slaveykov Books", "Storytel"],
    "station":     ["Bauhaus", "Mr. Bricolage", "Office 1", "Decathlon"],
    # Culture & sport
    "culture":     ["Eventim Bulgaria", "TicketStation", "National Palace of Culture",
                    "Sofia Philharmonic", "Cinemax", "Cinema City"],
    "fitness":     ["Vibes Fitness", "Mega Gym", "Sportal Center Europa",
                    "Next Level Gym", "Genesis Fitness", "Pulse Fitness", "Smashers"],
    # Health
    "beauty":      ["Douglas", "Notino", "MAC Cosmetics", "Rituals", "Sephora"],
    "doctor":      ["Aleksandrovska Hospital", "Tokuda Hospital", "City Clinic", "MedHelp"],
    "drug":        ["Sopharmacy", "Profarma", "Benu Pharmacy", "Lily Drug Store",
                    "Remedium", "Dr. Max"],
    # Transport
    "transport":   ["CityGate Sofia Transit", "Metrostation Sofia", "Sofia Urban Mobility"],
    "taxi":        ["Bolt", "Uber", "FreeNow"],
    "fuel":        ["Lukoil", "Shell", "EKO", "OMV"],
    # Digital subscriptions
    "tv, stream":  ["Netflix", "HBO Max", "Disney+", "Apple TV+"],
    "software":    ["Google One", "Microsoft 365", "Adobe Creative Cloud",
                    "ChatGPT Plus", "Notion", "GitHub Pro", "Dropbox"],
    # Telecoms
    "internet":    ["Vivacom", "A1 Bulgaria", "Yettel", "Bulsatcom"],
    "phone":       ["A1 Bulgaria", "Yettel", "Vivacom"],
    # Travel
    "hotel":       ["Marriott Sofia", "Hilton Sofia", "Radisson Blu",
                    "Sense Hotel", "Park Inn by Radisson"],
    "flight":      ["Ryanair", "Wizz Air", "Bulgaria Air", "easyJet"],
    # Utilities & insurance
    "utilit":      ["CEZ Group", "Sofiyska Voda", "Toplofikacia Sofia", "ePay"],
    "insur":       ["Generali Insurance", "Allianz Bulgaria", "DZI", "Bulstrad"],
    # Gaming (late: "gaming" is not a substring risk but kept last for clarity)
    "gaming":      ["Blizzard Entertainment", "Steam", "PlayStation Store", "Epic Games"],
}

# EU billing cities for digital/subscription services (Dublin-weighted for Google/Meta)
_SUB_CITIES = ["Dublin", "Dublin", "Luxembourg", "Stockholm", "Dublin"]
# Cities assigned to travel subcategories and business trips
_TRAVEL_CITIES = [
    "Amsterdam", "Prague", "Vienna", "Berlin", "Barcelona",
    "London", "Edinburgh", "Paris", "Rome", "Istanbul",
]
_SUMMER_MONTHS = {6, 7, 8, 9}
# Subcategory substrings for dining/bar categories that plausibly occur in Burgas in summer
_OUTDOOR_KEYS = ("bar, cafe", "restaurant", "fast food", "food")


def _merchant_for(subcategory: str, tx_hash: str) -> str:
    seed = int(tx_hash[:8], 16)
    subcat = subcategory.lower()
    for key, names in MERCHANT_NAMES.items():
        if key in subcat:
            return names[seed % len(names)]
    return subcategory  # fallback: subcategory name itself


def _city_for(subcategory: str, month: int, tx_hash: str) -> str:
    seed = int(tx_hash[8:16], 16)
    subcat = subcategory.lower()

    # "Holiday, trips, hotels" / "Бизнес пътувания" (business trips, BG)
    if any(k in subcat for k in ("hotel", "trip", "flight", "business trip")):
        return _TRAVEL_CITIES[seed % len(_TRAVEL_CITIES)]

    # Digital services billed from EU HQs
    if any(k in subcat for k in ("tv, stream", "software", "internet")):
        return _SUB_CITIES[seed % len(_SUB_CITIES)]

    # ~17% of summer dining/bar transactions → Burgas (Black Sea coast)
    if month in _SUMMER_MONTHS and any(k in subcat for k in _OUTDOOR_KEYS):
        if seed % 6 == 0:
            return "Burgas"

    return "Sofia"


def _derive_description_cols(df: pd.DataFrame) -> pd.DataFrame:
    subcats = df["subcategory"].astype(str)
    hashes = df["transaction_hash"].astype(str)
    try:
        months = pd.to_datetime(df["transaction_date"]).dt.month
    except Exception:
        months = pd.Series(0, index=df.index)

    df["merchant"] = [_merchant_for(s, h) for s, h in zip(subcats, hashes)]
    df["city"] = [_city_for(s, m, h) for s, m, h in zip(subcats, months, hashes)]
    df["description"] = df["merchant"] + " – " + df["city"]
    return df


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

QUERY = """
SELECT
    t.*,
    n.notability_score,
    n.notability_label,
    n.notability_reason,
    n.hist_n_txns,
    n.hist_avg_amount_eur,
    n.hist_std_amount_eur,
    n.amount_z_score,
    n.is_new_subcategory,
    n.is_new_subcategory_max,
    s.save_potential_score,
    s.save_potential_label,
    s.save_potential_reason,
    s.avoidability,
    s.freq_ratio,
    s.freq_excess,
    s.amt_excess,
    s.month_txn_count,
    s.hist_avg_monthly_count
FROM silver.transactions t
LEFT JOIN gold.transaction_notability  n ON t.transaction_hash = n.transaction_hash
LEFT JOIN gold.transaction_save_potential s ON t.transaction_hash = s.transaction_hash
ORDER BY t.transaction_date, t.transaction_id
"""


# ---------------------------------------------------------------------------
# Anonymization helpers
# ---------------------------------------------------------------------------

_EUR_IN_TEXT_RE = re.compile(r"€(\d+(?:\.\d+)?)")


def _scale_euros_in_text(value):
    """Scale any '€<number>' figure embedded in a free-text string by SCALE."""
    if not isinstance(value, str):
        return value
    return _EUR_IN_TEXT_RE.sub(lambda m: f"€{float(m.group(1)) * SCALE:.0f}", value)


def _rank_aliases(series: pd.Series, prefix: str, max_rank: int = None) -> pd.Series:
    """Replace real values with frequency-ranked aliases; keep NULL/empty as NULL.

    If max_rank is set, everything beyond that rank is collapsed into
    '{prefix} #other' so the output has at most max_rank + 1 distinct values.
    """
    non_null = series.dropna()
    non_null = non_null[non_null.astype(str).str.strip() != ""]
    if non_null.empty:
        return series

    alias_map = {}
    for rank, val in enumerate(non_null.value_counts().index, start=1):
        if max_rank and rank > max_rank:
            alias_map[val] = f"{prefix} #other"
        else:
            alias_map[val] = f"{prefix} #{rank}"

    def _map(v):
        if pd.isna(v) or str(v).strip() == "":
            return None
        return alias_map.get(v, f"{prefix} #other")

    return series.map(_map)


def _anonymize(df: pd.DataFrame) -> pd.DataFrame:
    # description → synthesized "{Merchant} – {City}"; merchant and city as separate columns too
    if "subcategory" in df.columns and "transaction_hash" in df.columns:
        df = _derive_description_cols(df)

    # payee → "Merchant #N" by frequency rank
    if "payee" in df.columns:
        df["payee"] = _rank_aliases(df["payee"], "Merchant")

    # account_name → "Account #N"; account_group → coarse top-3 grouping of the aliases
    if "account_name" in df.columns:
        df["account_name"] = _rank_aliases(df["account_name"], "Account")
        df["account_group"] = _rank_aliases(df["account_name"], "Account Group", max_rank=3)

    # labels → clear free-text tags
    if "labels" in df.columns:
        df["labels"] = None

    # scale all monetary columns
    for col in AMOUNT_COLS + GOLD_EUR_STAT_COLS:
        if col in df.columns:
            df[col] = (pd.to_numeric(df[col], errors="coerce") * SCALE).round(2)

    # scale euro figures embedded in free-text reason columns
    for col in REASON_COLS:
        if col in df.columns:
            df[col] = df[col].map(_scale_euros_in_text)

    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    db = get_db_connector()

    print("Connecting to database and fetching data...")
    with db.connect() as conn:
        cursor = conn.cursor()
        cursor.execute(QUERY)
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()

    df = pd.DataFrame(rows, columns=cols)
    print(f"Fetched {len(df):,} rows, {len(df.columns)} columns.")

    # Drop pipeline-internal columns
    cols_to_drop = [c for c in DROP_COLS if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    # Anonymize values
    df = _anonymize(df)

    # Write output
    out_dir = _project_root / "data" / "exports"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"anonymized_{date.today().isoformat()}.csv"
    df.to_csv(out_path, index=False)

    print(f"Exported {len(df):,} rows → {out_path}")
    print(f"Columns ({len(df.columns)}): {list(df.columns)}")


if __name__ == "__main__":
    main()
