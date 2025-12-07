# Analyze Voter Turnout

This project studies how U.S. presidential primary dynamics relate to general-election outcomes at the precinct level. We combine cleaned per-state precinct data with exploratory notebooks that quantify primary competitiveness (entropy), compare primary vs. general vote shares, and fit simple turnout/voting-chance models.

## Repository Layout
- `data/` – Cleaned and intermediate datasets (e.g., `processed/2008`, `processed/2016`, and merged state CSVs).
- `scripts/` – Jupyter notebooks for each stage:
  - `0_data_processing/`: per-state cleaning notebooks (2008, 2016).
  - `1_exploratory_analysis/eda.ipynb`: merges DEM/REP totals, computes shares, and builds primary vs. general plots.
  - `2_modeling/entropy.ipynb`: computes Shannon entropy by state/party and plots competitiveness over the primary calendar.
  - `2_modeling/voting_chance.ipynb`: early modeling of turnout/voting probability.
- `figures/` – Saved plots from notebooks.

## Data Notes
- Processed data live under `data/processed/<year>/<STATE>.csv`, with standardized primary/general columns (e.g., `pri_dem_*`, `pri_rep_*`, `gen_dem_*`, `gen_rep_*`, plus totals).
- Merged convenience files such as `data/processed/2016/merged.csv` are produced in the exploratory stage.
- Raw inputs are under `data/raw/<year>/<STATE>/` and mirror source files from election archives.

## Current Findings (snapshot)
- Primary entropy: GOP primaries tend to show higher entropy (more fragmented fields) than DEM primaries in both 2008 and 2016, especially early in the calendar.
- Entropy over time: entropy generally declines as the season progresses, reflecting candidate consolidation; late states (e.g., CA, MT, OR) exhibit lower entropy than Super Tuesday states.
- Primary vs. general: per-state scatterplots show how REP primary share relates to REP general share; complacency/overperformance patterns vary by state.
- Voting chance modeling: initial fits explore precinct-level turnout probabilities using cleaned totals.

## How to Reproduce Locally
1) Create and activate a virtual environment (Python 3.10+ recommended):
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -U pip
```
2) Install project requirements (if a requirements file is added) or use `pip install -r requirements.txt` when available.

3) Run notebooks in order:
   - Clean data: `scripts/0_data_processing/<STATE>_cleaning.ipynb` (produces `data/processed/...`).
   - Exploratory analysis: `scripts/1_exploratory_analysis/eda.ipynb` (builds merged datasets and primary vs. general plots).
   - Modeling: `scripts/2_modeling/entropy.ipynb` and `scripts/2_modeling/voting_chance.ipynb`.

4) Figures are saved under `figures/` (or within notebook output directories).

## Next Steps
- Add a lightweight dependency file (`requirements.txt` or `environment.yml`) to pin versions.
- Fill in missing states/years and rerun cleaning notebooks for broader coverage.
- Extend modeling: richer turnout models, uncertainty estimates, and validation on held-out states.
- Write automated scripts (in addition to notebooks) to batch-generate plots and merged datasets.
