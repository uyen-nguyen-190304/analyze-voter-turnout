"""
ETL pipeline to standardize 2020 *county-level* presidential results

What this script does
---------------------
This is a single, reusable replacement for per-state cleaning notebooks

For each state under:  data/raw/2020/<STATE>/, it will:

1) EXTRACT
   - Read all CSVs matching:
       *general*.csv   (general election)
       *primary*.csv   (primary election, optional)

2) TRANSFORM (per stage)
   - Normalize column names to lowercase
   - Ensure a county column exists (renaming common aliases when needed)
   - Filter rows to presidential contests (office contains "president")
   - Detect vote columns robustly (with safeguards against numeric ID columns)
   - Compute a row-level votes_total (sum of vote columns)
   - Normalize party labels to a small set: dem/rep/lib/grn/ind/oth
   - Slugify candidate names into stable tokens for column names
   - Aggregate to COUNTY level (summing votes_total)
   - Pivot long -> wide:
       gen_dem_BIDEN, gen_rep_TRUMP, ...
       pri_dem_BIDEN, pri_rep_TRUMP, ... (if primary exists)

3) LOAD
   - Merge general + primary wide tables on county
    - Add totals:
        gen_total, rep_pri_total, dem_pri_total
   - Write one cleaned CSV per state:
       data/processed/2020/<STATE>.csv

4) WRITE A PIPELINE SUMMARY
   - After processing all states, writes:
       data/processed/2020/summary.csv
     containing diagnostics per state:
       - whether general/primary files existed
       - how many files for primary abd general
       - how many rows survived office filtering
       - how many counties in primary and general
       - how many counties left after inner merge
       - number of output columns
       - any errors encountered

Usage
-----
Process all states found in data/raw/2020:
    python election_etl_2020_county.py

Process a subset:
    python election_etl_2020_county.py --states GA AR NC
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


# =============================================================================
# Paths
# =============================================================================
# ROOT is computed relative to this file location
# parents[3] means "go up 3 directories". Adjust if needed
ROOT = Path(__file__).resolve().parents[3]
RAW_ROOT = ROOT / "data" / "raw" / "2020"
OUT_ROOT = ROOT / "data" / "processed" / "2020"


# =============================================================================
# Party normalization
# =============================================================================
PARTY_MAP: Dict[str, str] = {
    "dem": "dem",
    "democratic": "dem",
    "democrat": "dem",
    "dfldem": "dem",
    "rep": "rep",
    "gop": "rep",
    "republican": "rep",
    "lib": "lib",
    "libertarian": "lib",
    "grn": "grn",
    "green": "grn",
    "ind": "ind",
    "independent": "ind",
}

# Columns that are metadata (not votes). We exclude these from vote detection
# Add common numeric ID columns here to prevent accidental summing into votes_total
METADATA_COLS = {
    "state", "county", "county_name", "county_fips", "fips",
    "precinct", "precinct_id",
    "jurisdiction", "jurisdiction_name",
    "office", "race", "contest", "district",
    "party", "candidate", "candidate_id",
    "year", "election", "election_date",
}

# Common county column aliases we will attempt to rename to "county"
COUNTY_ALIASES = [
    "county",
    "county_name",
    "town",      # e.g., Connecticut uses "town" where county is absent
    "district",  # e.g., Arkansas uses "district" for county-equivalents
    "parish",
    "borough",
    "jurisdiction",
    "jurisdiction_name",
]

# Small county-name fixes for state-specific quirks (New York)
NY_COUNTY_FIXES = {
    "Schenectedy": "Schenectady",
    "St. Lawrence": "St Lawrence",
}

# Massachusetts town -> county lookup (from 2008 cleaning notebook)
MA_TOWN_TO_COUNTY = {
    # Barnstable County
    "Barnstable": "Barnstable", "Bourne": "Barnstable", "Brewster": "Barnstable",
    "Chatham": "Barnstable", "Dennis": "Barnstable", "Eastham": "Barnstable",
    "Falmouth": "Barnstable", "Harwich": "Barnstable", "Mashpee": "Barnstable",
    "Orleans": "Barnstable", "Provincetown": "Barnstable", "Sandwich": "Barnstable",
    "Truro": "Barnstable", "Wellfleet": "Barnstable", "Yarmouth": "Barnstable",

    # Berkshire County
    "Adams": "Berkshire", "Alford": "Berkshire", "Becket": "Berkshire",
    "Cheshire": "Berkshire", "Clarksburg": "Berkshire", "Dalton": "Berkshire",
    "Egremont": "Berkshire", "Florida": "Berkshire", "Great Barrington": "Berkshire",
    "Hancock": "Berkshire", "Hinsdale": "Berkshire", "Lanesborough": "Berkshire",
    "Lee": "Berkshire", "Lenox": "Berkshire", "Monterey": "Berkshire",
    "Mount Washington": "Berkshire", "New Ashford": "Berkshire",
    "New Marlborough": "Berkshire", "North Adams": "Berkshire", "N. Adams": "Berkshire",
    "Otis": "Berkshire", "Peru": "Berkshire", "Pittsfield": "Berkshire",
    "Richmond": "Berkshire", "Sandisfield": "Berkshire", "Savoy": "Berkshire",
    "Sheffield": "Berkshire", "Stockbridge": "Berkshire", "Tyringham": "Berkshire",
    "Washington": "Berkshire", "West Stockbridge": "Berkshire", "W. Stockbridge": "Berkshire",
    "Williamstown": "Berkshire", "Windsor": "Berkshire",

    # Bristol County
    "Attleboro": "Bristol", "Berkley": "Bristol", "Dartmouth": "Bristol",
    "Dighton": "Bristol", "Easton": "Bristol", "Fall River": "Bristol",
    "Fairhaven": "Bristol", "Freetown": "Bristol", "Mansfield": "Bristol",
    "New Bedford": "Bristol", "Norton": "Bristol", "North Attleborough": "Bristol",
    "N. Attleborough": "Bristol", "Raynham": "Bristol", "Rehoboth": "Bristol",
    "Seekonk": "Bristol", "Somerset": "Bristol", "Swansea": "Bristol",
    "Taunton": "Bristol", "Westport": "Bristol", "Acushnet": "Bristol",

    # Dukes County
    "Aquinnah": "Dukes", "Chilmark": "Dukes", "Edgartown": "Dukes",
    "Gosnold": "Dukes", "Oak Bluffs": "Dukes", "Tisbury": "Dukes",
    "West Tisbury": "Dukes", "W. Tisbury": "Dukes",

    # Essex County
    "Amesbury": "Essex", "Andover": "Essex", "Beverly": "Essex",
    "Boxford": "Essex", "Danvers": "Essex", "Essex": "Essex",
    "Georgetown": "Essex", "Gloucester": "Essex", "Groveland": "Essex",
    "Hamilton": "Essex", "Haverhill": "Essex", "Ipswich": "Essex",
    "Lawrence": "Essex", "Lynn": "Essex", "Lynnfield": "Essex",
    "Manchester-by-the-Sea": "Essex", "Marblehead": "Essex", "Merrimac": "Essex",
    "Methuen": "Essex", "Middleton": "Essex", "Nahant": "Essex",
    "Newbury": "Essex", "Newburyport": "Essex", "North Andover": "Essex",
    "N. Andover": "Essex", "Peabody": "Essex", "Rockport": "Essex",
    "Rowley": "Essex", "Salem": "Essex", "Salisbury": "Essex",
    "Saugus": "Essex", "Swampscott": "Essex", "Topsfield": "Essex",
    "West Newbury": "Essex", "W. Newbury": "Essex", "Wenham": "Essex",

    # Franklin County
    "Ashfield": "Franklin", "Bernardston": "Franklin", "Buckland": "Franklin",
    "Charlemont": "Franklin", "Colrain": "Franklin", "Conway": "Franklin",
    "Deerfield": "Franklin", "Erving": "Franklin", "Gill": "Franklin",
    "Greenfield": "Franklin", "Hawley": "Franklin", "Heath": "Franklin",
    "Leverett": "Franklin", "Leyden": "Franklin", "Monroe": "Franklin",
    "Montague": "Franklin", "New Salem": "Franklin", "Northfield": "Franklin",
    "Orange": "Franklin", "Rowe": "Franklin", "Shelburne": "Franklin",
    "Shutesbury": "Franklin", "Sunderland": "Franklin", "Warwick": "Franklin",
    "Wendell": "Franklin", "Whately": "Franklin",

    # Hampden County
    "Agawam": "Hampden", "Blandford": "Hampden", "Brimfield": "Hampden",
    "Chester": "Hampden", "Chicopee": "Hampden", "East Longmeadow": "Hampden",
    "E. Longmeadow": "Hampden", "Granville": "Hampden", "Hampden": "Hampden",
    "Holyoke": "Hampden", "Longmeadow": "Hampden", "Ludlow": "Hampden",
    "Monson": "Hampden", "Montgomery": "Hampden", "Palmer": "Hampden",
    "Russell": "Hampden", "Southwick": "Hampden", "Springfield": "Hampden",
    "Tolland": "Hampden", "Wales": "Hampden", "West Springfield": "Hampden",
    "W. Springfield": "Hampden", "Westfield": "Hampden", "Wilbraham": "Hampden",
    "Holland": "Hampden",

    # Hampshire County
    "Amherst": "Hampshire", "Belchertown": "Hampshire", "Chesterfield": "Hampshire",
    "Cummington": "Hampshire", "Easthampton": "Hampshire", "Goshen": "Hampshire",
    "Granby": "Hampshire", "Hadley": "Hampshire", "Hatfield": "Hampshire",
    "Huntington": "Hampshire", "Middlefield": "Hampshire", "Northampton": "Hampshire",
    "Pelham": "Hampshire", "Plainfield": "Hampshire", "South Hadley": "Hampshire",
    "S. Hadley": "Hampshire", "Southampton": "Hampshire", "Ware": "Hampshire",
    "Westhampton": "Hampshire", "Williamsburg": "Hampshire", "Worthington": "Hampshire",

    # Middlesex County
    "Acton": "Middlesex", "Arlington": "Middlesex", "Ashby": "Middlesex",
    "Ashland": "Middlesex", "Ayer": "Middlesex", "Bedford": "Middlesex",
    "Belmont": "Middlesex", "Billerica": "Middlesex", "Boxborough": "Middlesex",
    "Burlington": "Middlesex", "Cambridge": "Middlesex", "Carlisle": "Middlesex",
    "Chelmsford": "Middlesex", "Concord": "Middlesex", "Dracut": "Middlesex",
    "Dunstable": "Middlesex", "Everett": "Middlesex", "Framingham": "Middlesex",
    "Groton": "Middlesex", "Holliston": "Middlesex", "Hopkinton": "Middlesex",
    "Hudson": "Middlesex", "Lexington": "Middlesex", "Lincoln": "Middlesex",
    "Littleton": "Middlesex", "Lowell": "Middlesex", "Malden": "Middlesex",
    "Marlborough": "Middlesex", "Maynard": "Middlesex", "Medford": "Middlesex",
    "Melrose": "Middlesex", "Natick": "Middlesex", "Newton": "Middlesex",
    "North Reading": "Middlesex", "N. Reading": "Middlesex", "Pepperell": "Middlesex",
    "Reading": "Middlesex", "Sherborn": "Middlesex", "Shirley": "Middlesex",
    "Somerville": "Middlesex", "Stoneham": "Middlesex", "Stow": "Middlesex",
    "Sudbury": "Middlesex", "Tewksbury": "Middlesex", "Townsend": "Middlesex",
    "Tyngsborough": "Middlesex", "Wakefield": "Middlesex", "Waltham": "Middlesex",
    "Watertown": "Middlesex", "Wayland": "Middlesex", "Westford": "Middlesex",
    "Weston": "Middlesex", "Wilmington": "Middlesex", "Winchester": "Middlesex",
    "Woburn": "Middlesex",

    # Nantucket County
    "Nantucket": "Nantucket",

    # Norfolk County
    "Avon": "Norfolk", "Bellingham": "Norfolk", "Braintree": "Norfolk",
    "Brookline": "Norfolk", "Canton": "Norfolk", "Cohasset": "Norfolk",
    "Dedham": "Norfolk", "Dover": "Norfolk", "Foxborough": "Norfolk",
    "Franklin": "Norfolk", "Holbrook": "Norfolk", "Medfield": "Norfolk",
    "Medway": "Norfolk", "Milton": "Norfolk", "Needham": "Norfolk",
    "Norfolk": "Norfolk", "Norwood": "Norfolk", "Plainville": "Norfolk",
    "Quincy": "Norfolk", "Randolph": "Norfolk", "Sharon": "Norfolk",
    "Stoughton": "Norfolk", "Walpole": "Norfolk", "Wellesley": "Norfolk",
    "Westwood": "Norfolk", "Weymouth": "Norfolk", "Wrentham": "Norfolk",
    "Millis": "Norfolk",

    # Plymouth County
    "Abington": "Plymouth", "Bridgewater": "Plymouth",
    "East Bridgewater": "Plymouth", "E. Bridgewater": "Plymouth",
    "West Bridgewater": "Plymouth", "W. Bridgewater": "Plymouth",
    "Brockton": "Plymouth", "Carver": "Plymouth", "Duxbury": "Plymouth",
    "Halifax": "Plymouth", "Hanover": "Plymouth", "Hanson": "Plymouth",
    "Hingham": "Plymouth", "Hull": "Plymouth", "Kingston": "Plymouth",
    "Lakeville": "Plymouth", "Marion": "Plymouth", "Marshfield": "Plymouth",
    "Mattapoisett": "Plymouth", "Middleborough": "Plymouth",
    "Norwell": "Plymouth", "Pembroke": "Plymouth", "Plymouth": "Plymouth",
    "Plympton": "Plymouth", "Rochester": "Plymouth", "Rockland": "Plymouth",
    "Scituate": "Plymouth", "Wareham": "Plymouth", "Whitman": "Plymouth",

    # Suffolk County
    "Boston": "Suffolk", "Chelsea": "Suffolk", "Revere": "Suffolk", "Winthrop": "Suffolk",

    # Worcester County
    "Ashburnham": "Worcester", "Athol": "Worcester", "Auburn": "Worcester",
    "Barre": "Worcester", "Berlin": "Worcester", "Blackstone": "Worcester",
    "Bolton": "Worcester", "Boylston": "Worcester", "Brookfield": "Worcester",
    "Charlton": "Worcester", "Clinton": "Worcester", "Douglas": "Worcester",
    "Dudley": "Worcester", "East Brookfield": "Worcester", "E. Brookfield": "Worcester",
    "Fitchburg": "Worcester", "Gardner": "Worcester", "Grafton": "Worcester",
    "Hardwick": "Worcester", "Harvard": "Worcester", "Holden": "Worcester",
    "Hopedale": "Worcester", "Hubbardston": "Worcester", "Lancaster": "Worcester",
    "Leicester": "Worcester", "Leominster": "Worcester", "Lunenburg": "Worcester",
    "Mendon": "Worcester", "Milford": "Worcester", "Millbury": "Worcester",
    "Millville": "Worcester", "New Braintree": "Worcester", "Northborough": "Worcester",
    "North Brookfield": "Worcester", "N. Brookfield": "Worcester",
    "Northbridge": "Worcester", "Oakham": "Worcester", "Oxford": "Worcester",
    "Paxton": "Worcester", "Petersham": "Worcester", "Phillipston": "Worcester",
    "Princeton": "Worcester", "Royalston": "Worcester", "Rutland": "Worcester",
    "Shrewsbury": "Worcester", "Southborough": "Worcester", "Southbridge": "Worcester",
    "Spencer": "Worcester", "Sterling": "Worcester", "Sturbridge": "Worcester",
    "Sutton": "Worcester", "Templeton": "Worcester", "Upton": "Worcester",
    "Uxbridge": "Worcester", "Warren": "Worcester", "Webster": "Worcester",
    "West Boylston": "Worcester", "W. Boylston": "Worcester",
    "West Brookfield": "Worcester", "W. Brookfield": "Worcester",
    "Westminster": "Worcester", "Winchendon": "Worcester", "Worcester": "Worcester",
    "Westborough": "Worcester",
}


# =============================================================================
# Summary record
# =============================================================================
@dataclass
class StateRunSummary:
    """A compact diagnostic record for one state's run."""
    state: str
    raw_dir_exists: bool
    general_files: int
    primary_files: int

    # Post-filter row counts (after filtering to "president")
    general_pres_rows: int
    primary_pres_rows: int

    # County counts (after aggregation)
    general_counties: int
    primary_counties: int
    merged_counties: int

    # Output diagnostics
    output_cols: int

    # Error message if the state failed (empty if success)
    error: str = ""


# =============================================================================
# Utility: text normalization
# =============================================================================
def slugify_candidate(name: str) -> str:
    """
    Convert a candidate name into a column-safe token, preferring last name
    to match the 2008/2016 convention (e.g., "Barack Obama" -> "OBAMA")
    Falls back to a full-name slug if no alphabetic tokens are found
    """
    raw = str(name).strip()
    # Use only the first segment before "/" for multi-person tickets
    segment = raw.split("/")[0]
    tokens = re.findall(r"[A-Za-z]+", segment)
    if tokens:
        last = tokens[-1].upper()
        return last
    # Fallback: full slug
    slug = re.sub(r"[^A-Z0-9]+", "_", raw.upper())
    return slug.strip("_")


def normalize_party(party: object) -> str:
    """
    Standardize party labels to a compact code:
      dem, rep, lib, grn, ind, oth
    """
    if party is None or (isinstance(party, float) and np.isnan(party)):
        return "oth"
    key = str(party).strip().lower()
    return PARTY_MAP.get(key, key if key in {"dem", "rep"} else "oth")


def normalize_county_name(x: object) -> str:
    """
    Standardize county-like geography names.

    - Strips common suffix words like "County", "Parish", "Borough"
    - Normalizes whitespace and casing
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    s = str(x).strip()

    # Remove common suffixes (case-insensitive)
    s = re.sub(r"\b(County|Parish|Borough|Census Area|City and Borough)\b", "", s, flags=re.IGNORECASE)

    # Normalize punctuation a bit
    s = s.replace("&", "and")
    s = re.sub(r"[^\w\s\.-]", "", s)

    # Collapse spaces and title-case
    s = re.sub(r"\s+", " ", s).strip()
    return s.title()


def ensure_county_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure a 'county' column exists by renaming common aliases

    - If 'county' already exists: no-op.
    - Else: searches COUNTY_ALIASES and renames the first match to 'county'
    - If no alias found: raises KeyError.
    """
    if "county" in df.columns:
        return df

    for alt in COUNTY_ALIASES:
        if alt in df.columns:
            return df.rename(columns={alt: "county"})

    raise KeyError(
        "No county-like column found. Expected one of: "
        + ", ".join(COUNTY_ALIASES)
        + ". You may need to add another alias for this state's raw format."
    )


# =============================================================================
# Vote-column detection (safer version)
# =============================================================================
def detect_vote_columns(df: pd.DataFrame) -> List[str]:
    """
    Detect columns that represent vote counts, with safeguards

    Returns:
      vote_cols

    Strategy:
    1) If 'votes' exists -> use ONLY 'votes' (most reliable)
    2) Else, use columns with vote-like names: vote|ballot|count|total
       (excluding METADATA_COLS).
    3) Else, fallback to numeric dtype columns excluding METADATA_COLS
       (least safe; used only when name-based detection finds nothing)
    4) Validate each candidate column by coercing to numeric and requiring
       at least one numeric value.
    """
    cols = list(df.columns)

    # 1) Prefer explicit 'votes'
    if "votes" in cols:
        df["votes"] = pd.to_numeric(df["votes"], errors="coerce")
        return ["votes"]

    # 2) Name-based detection (conservative)
    vote_name_pattern = re.compile(r"(vote|votes|ballot|count|total)", re.IGNORECASE)
    candidate_cols: List[str] = []
    for col in cols:
        if col in METADATA_COLS:
            continue
        if vote_name_pattern.search(col):
            candidate_cols.append(col)

    # 3) Fallback: numeric dtype columns (excluding metadata)
    if not candidate_cols:
        for col in cols:
            if col in METADATA_COLS:
                continue
            if pd.api.types.is_numeric_dtype(df[col]):
                candidate_cols.append(col)

    # 4) Validate by coercion
    vote_cols: List[str] = []
    for col in candidate_cols:
        coerced = pd.to_numeric(df[col], errors="coerce")
        if coerced.notna().any():
            df[col] = coerced
            vote_cols.append(col)

    return vote_cols


# =============================================================================
# Extract: read multiple CSVs
# =============================================================================
def load_state_frames(files: Sequence[Path]) -> pd.DataFrame:
    """
    Read a list of CSV files and concatenate them.

    If files is empty, returns an empty dataframe.
    """
    frames: List[pd.DataFrame] = []
    for path in files:
        try:
            df = pd.read_csv(path)
        except pd.errors.ParserError:
            # Fallback for ragged rows (e.g., TX with extra delimiters)
            df = pd.read_csv(path, engine="python", on_bad_lines="skip")
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# =============================================================================
# Transform: standardize to long schema, then aggregate to county
# =============================================================================
def prepare_long_and_aggregate_county(
    df: pd.DataFrame,
    office_filter: str,
) -> Tuple[pd.DataFrame, List[str], str, int]:
    """
    Convert raw election data to standardized long format *and* aggregate to county level.

    Returns:
      (county_long, vote_cols, pres_rows)

    county_long columns:
      county | party_std | candidate_clean | votes_total
    """
    if df.empty:
        empty = pd.DataFrame(columns=["county", "party_std", "candidate_clean", "votes_total"])
        return empty, [], 0

    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    # Required columns
    if "office" not in df.columns:
        raise KeyError("Expected column 'office' not found.")
    for col in ("party", "candidate"):
        if col not in df.columns:
            raise KeyError(f"Expected column '{col}' not found in raw data.")

    # County column (rename aliases if needed)
    df = ensure_county_column(df)

    # Filter to presidential contests
    pres = df[df["office"].astype(str).str.contains(office_filter, case=False, na=False)].copy()
    pres_rows = int(pres.shape[0])
    if pres.empty:
        empty = pd.DataFrame(columns=["county", "party_std", "candidate_clean", "votes_total"])
        return empty, [], 0

    # Drop administrative rows like registered voters / ballots-cast summaries
    admin_patterns = ("registered voters", "ballots cast", "total votes cast", "blanks")
    mask_admin = pres["candidate"].astype(str).str.contains("|".join(admin_patterns), case=False, na=False)
    pres = pres[~mask_admin]

    # Detect vote columns and compute votes_total
    vote_cols = detect_vote_columns(pres)

    pres["votes_total"] = 0
    if vote_cols:
        pres[vote_cols] = pres[vote_cols].fillna(0)
        pres["votes_total"] = pres[vote_cols].sum(axis=1)

    # Standardize fields
    pres["county"] = pres["county"].apply(normalize_county_name)
    pres["party_std"] = pres["party"].apply(normalize_party)
    pres["candidate_clean"] = pres["candidate"].apply(slugify_candidate)

    pres["votes_total"] = pd.to_numeric(pres["votes_total"], errors="coerce").fillna(0).astype(int)

    # Drop non-geographic county placeholders
    bad_counties = {"", "total", "totals", "total votes", "statewide", "nan"}
    mask_bad_exact = pres["county"].str.lower().isin(bad_counties)
    mask_bad_prefix = pres["county"].str.lower().str.startswith("pct.")
    pres = pres[~(mask_bad_exact | mask_bad_prefix)]

    # Keep standardized long columns
    long_df = pres[["county", "party_std", "candidate_clean", "votes_total"]].copy()

    # Aggregate to county level (the key change!)
    county_long = (
        long_df
        .groupby(["county", "party_std", "candidate_clean"], as_index=False)["votes_total"]
        .sum()
    )

    return county_long, vote_cols, pres_rows


# =============================================================================
# Transform: pivot county long -> wide
# =============================================================================
def pivot_wide(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Pivot county long format into wide format columns."""
    if df.empty:
        return pd.DataFrame(columns=["county"])

    pivot = (
        df.pivot_table(
            index="county",
            columns=["party_std", "candidate_clean"],
            values="votes_total",
            aggfunc="sum",
            fill_value=0,
        )
        .sort_index(axis=1)
    )

    pivot.columns = [f"{prefix}_{p}_{c}" for p, c in pivot.columns]
    return pivot.reset_index()


def add_totals(df: pd.DataFrame) -> pd.DataFrame:
    """Add totals (general + party-specific primary totals)."""
    out = df.copy()

    gen_cols = [c for c in out.columns if c.startswith("gen_")]
    pri_rep_cols = [c for c in out.columns if c.startswith("pri_rep_")]
    pri_dem_cols = [c for c in out.columns if c.startswith("pri_dem_")]

    out["gen_total"] = out[gen_cols].sum(axis=1) if gen_cols else 0
    # Match 2008/2016 naming convention used downstream.
    # Only add primary totals when we actually detected primary presidential data.
    if pri_rep_cols or pri_dem_cols:
        out["rep_pri_total"] = out[pri_rep_cols].sum(axis=1) if pri_rep_cols else 0
        out["dem_pri_total"] = out[pri_dem_cols].sum(axis=1) if pri_dem_cols else 0

    return out

# =============================================================================
# Orchestration: process one state + produce diagnostics
# =============================================================================
def clean_state(state: str) -> Tuple[Optional[Path], StateRunSummary]:
    """Clean one state and return (output_path_or_None, summary_record)."""
    state = state.upper()
    state_dir = RAW_ROOT / state

    summary = StateRunSummary(
        state=state,
        raw_dir_exists=state_dir.exists(),
        general_files=0,
        primary_files=0,
        general_pres_rows=0,
        primary_pres_rows=0,
        general_counties=0,
        primary_counties=0,
        merged_counties=0,
        output_cols=0,
        error="",
    )

    if not state_dir.exists():
        summary.error = f"No raw directory for state {state}: {state_dir}"
        return None, summary

    general_files = sorted(state_dir.glob("*general*.csv"))
    primary_files = sorted(state_dir.glob("*primary*.csv"))
    summary.general_files = len(general_files)
    summary.primary_files = len(primary_files)

    try:
        general_raw = load_state_frames(general_files)
        primary_raw = load_state_frames(primary_files)

        # State-specific enrichment before county checks
        if state == "MA":
            for df in (general_raw, primary_raw):
                if not df.empty and "town" in df.columns:
                    df["county"] = df["town"].map(MA_TOWN_TO_COUNTY)
                    df["county"] = df["county"].fillna(df["town"])
        if state == "CT":
            # General: use district code as the geography key
            if not general_raw.empty and "district" in general_raw.columns:
                general_raw["county"] = general_raw["district"].astype(str)
            # Primary: keep using town as the geography key
            if not primary_raw.empty and "town" in primary_raw.columns:
                primary_raw["county"] = primary_raw["town"]
        if state == "NY":
            for df in (general_raw, primary_raw):
                if not df.empty and "county" in df.columns:
                    df["county"] = df["county"].replace(NY_COUNTY_FIXES)
                    # Drop clearly bad county codes
                    df = df[df["county"].astype(str).str.strip().ne("Č")]

        general_long, gen_vote_cols, gen_pres_rows = prepare_long_and_aggregate_county(
            general_raw, office_filter="president"
        )
        primary_long, pri_vote_cols, pri_pres_rows = prepare_long_and_aggregate_county(
            primary_raw, office_filter="president"
        )

        # If neither general nor primary has presidential data, skip this state
        if gen_pres_rows == 0 and pri_pres_rows == 0:
            summary.error = "No presidential rows in general or primary data; skipped."
            return None, summary

        summary.general_pres_rows = gen_pres_rows
        summary.primary_pres_rows = pri_pres_rows

        general_wide = pivot_wide(general_long, prefix="gen")
        primary_wide = pivot_wide(primary_long, prefix="pri") if pri_pres_rows > 0 else pd.DataFrame(columns=["county"])

        if not primary_wide.empty:
            merged = pd.merge(general_wide, primary_wide, on="county", how="inner").fillna(0)
        else:
            merged = general_wide.copy()

        merged = add_totals(merged)

        summary.general_counties = int(general_wide.shape[0]) if not general_wide.empty else 0
        summary.primary_counties = int(primary_wide.shape[0]) if not primary_wide.empty else 0
        summary.merged_counties = int(merged.shape[0])
        summary.output_cols = int(merged.shape[1])

        OUT_ROOT.mkdir(parents=True, exist_ok=True)
        out_path = OUT_ROOT / f"{state}.csv"
        merged.to_csv(out_path, index=False)

        return out_path, summary

    except Exception as exc:
        import traceback
        traceback.print_exc()
        summary.error = str(exc)
        return None, summary


# =============================================================================
# Discover available states
# =============================================================================
def discover_states(selected: Optional[Iterable[str]] = None) -> List[str]:
    """List state codes present under data/raw/2020 (optionally restricted)"""
    if not RAW_ROOT.exists():
        return []
    states = [p.name.upper() for p in RAW_ROOT.iterdir() if p.is_dir()]
    if selected:
        selected_upper = {s.upper() for s in selected}
        states = [s for s in states if s in selected_upper]
    return sorted(states)


# =============================================================================
# CLI entry point
# =============================================================================
def main(argv: Optional[Sequence[str]] = None) -> None:
    """
    Run the ETL for all states (or a subset) and write per-state outputs + summary.csv.
    """
    parser = argparse.ArgumentParser(description="Clean 2020 county-level presidential data.")
    parser.add_argument(
        "--states",
        nargs="*",
        help="Optional list of state codes to process (defaults to all in raw/2020).",
    )
    args = parser.parse_args(argv)

    states = discover_states(args.states)
    if not states:
        raise SystemExit("No states found to process under data/raw/2020.")

    summaries: List[StateRunSummary] = []

    for state in states:
        out_path, summary = clean_state(state)
        summaries.append(summary)
        if out_path is not None:
            print(f"[ok] {state}: wrote {out_path}")
        else:
            print(f"[error] {state}: {summary.error}")

    # Write the run summary
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    summary_path = OUT_ROOT / "summary.csv"

    df_summary = pd.DataFrame([asdict(s) for s in summaries])

    ordered_cols = [
        "state",
        "raw_dir_exists",
        "general_files",
        "primary_files",
        "general_pres_rows",
        "primary_pres_rows",
        "general_counties",
        "primary_counties",
        "merged_counties",
        "output_cols",
        "error",
    ]
    df_summary = df_summary[[c for c in ordered_cols if c in df_summary.columns]]

    df_summary.to_csv(summary_path, index=False)
    print(f"[ok] wrote run summary: {summary_path}")


if __name__ == "__main__":
    main()
