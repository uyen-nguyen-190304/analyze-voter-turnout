# Analyze Voter Turnout

This repository analyzes whether patterns consistent with **voter complacency** appear *within* U.S. presidential election cycles. Using county-level election data from 2008, 2016, and 2020, the project studies how **primary-election dynamics** relate to **general-election participation** later in the same year.

The analysis is **within-cycle and descriptive**: it does not compare elections to each other or attempt causal identification. Instead, it asks whether primary competitiveness and dominance are systematically associated with turnout and vote-share behavior in the general election.

## Key Results

**Across all three election cycles (2008, 2016, 2020):**

* **Primary dominance is negatively associated with general-election gains.**
  States where Republicans performed strongly in the primary tend to show *smaller* increases (or losses) in Republican general-election vote share relative to the primary.

* **Turnout expands asymmetrically from primary to general election.**
  Using a general-election turnout multiplier:

  * **Primary losers** show increasing turnout expansion as primaries become more lopsided.
  * **Primary winners** exhibit flat or declining turnout growth as their primary advantage increases.

* **This asymmetry is strongest in 2008 and 2020**, and weaker but still present in 2016.

* **Primary uncertainty declines over time in most cycles**, as measured by Shannon entropy, but patterns vary substantially by party and year, especially in 2016 and 2020.

Taken together, these repeated patterns are **consistent with voter complacency** operating on the advantaged side of the primary, though the results are descriptive and sensitive to data limitations.

## Repository Structure

```
analyze-voter-turnout/
├── data/
│   ├── raw/              # OpenElections source files
│   └── processed/        # Cleaned, standardized state-level outputs
├── scripts/
│   ├── 0_data_processing/
│   │   ├── 2008/          # Per-state cleaning notebooks
│   │   └── 2020/          # Automated ETL pipeline
│   ├── 1_exploratory_analysis/
│   └── 2_modeling/
│       ├── multipliers/  # Turnout multiplier models
│       └── entropy/      # Shannon entropy analysis
├── figures/
├── REPORT.md
└── README.md
```

## Data

* **Source:** OpenElections GitHub repositories
* **Unit of analysis:** County (or county-equivalent geographic units)
* **Election cycles:** 2008, 2016, 2020
* **Parties:** Democratic and Republican presidential contests only


## Data Processing

### 2008

* One cleaning notebook per state
* Normalized headers, geography, and party labels
* Filtered to presidential races
* Aggregated to county level
* Output: one CSV per state

### 2016

* Uses pre-cleaned precinct-level data from prior research
* Aggregated to county level during exploratory analysis

### 2020

* Cleaned via a single automated ETL pipeline
* Handles inconsistent schemas, geography labels, and malformed files
* Removes statewide totals and aggregates to county-equivalents
* Pipeline is **cycle-specific** and includes some state-level fixes

## Exploratory Analysis

* Computes Republican primary and general vote shares by state
* Defines **vote-share overperformance** as
  [
  \text{REP general share} - \text{REP primary share}
  ]
* Produces weighted multi-state scatter plots:

  * One point per state
  * Bubble size proportional to general-election turnout
  * Linear trend summarizes cross-state patterns

## Modeling: General-Election Multipliers

For each party:
[
r = \frac{\text{General-Election Turnout}}{\text{Primary Turnout}}
]

* Parties are relabeled as **primary winner** or **primary loser** within each state-year
* Winner and loser multipliers are plotted against primary margin
* Separate linear trends share a common intercept
* Reveals consistent winner–loser asymmetry across cycles


## Shannon Entropy (Primary Competitiveness)

* Computes Shannon entropy from candidate vote-share distributions
* Analyzed as a **time series over the primary calendar**
* Used to contextualize the primary environment rather than directly test turnout behavior
* Shows expected decline in 2008, mixed patterns in 2016 and 2020


## Limitations

* Descriptive analysis only (no causal inference)
* Turnout multipliers sensitive to uncontested primaries
* State coverage varies by cycle, especially in 2020
* ETL pipeline is not yet generalized beyond current years


## Full Report

A detailed discussion of methods, figures, and interpretation is available in the full project report in the repo.
