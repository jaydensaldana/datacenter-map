# U.S. Data Center Landscape Map

Interactive choropleth map of U.S. data center infrastructure with automated federal data feeds, hosted on GitHub Pages.

## How It Works

The map merges **manual data** you maintain in a Google Sheet with **automated data** pulled from federal APIs. A GitHub Action runs monthly (or on-demand) to fetch the latest data and rebuild the map.

### Your Google Sheet
| Tab | What You Enter | How to Update |
|-----|---------------|---------------|
| State DC History | Monthly DC count per state | Find the current month's column, enter counts |
| State Tax Incentive | Incentive details (Y/N, type, thresholds) | Edit when legislation changes |
| Total Regulations | State regulatory burden count | Update annually |
| Regional Market History | Monthly DC count per city/market | Find the current month's column, enter counts |

**Row-safe:** States and markets are matched by name, not row position. Add, remove, or reorder rows freely.

**Month-matching logic:** The script finds the rightmost populated month column as "current," then looks up the same month 1, 3, and 5 years earlier for YoY % change. For example, if your latest data is in the "Apr 2026" column, it compares against Apr 2025, Apr 2023, and Apr 2021.

### Automated Data (no API keys required)
| Source | Data | Refresh |
|--------|------|---------|
| [BLS QCEW](https://www.bls.gov/cew/) | Employment (NAICS 518210, 5415, 236220) | Monthly Action |
| [Census PEP](https://www.census.gov/programs-surveys/popest.html) | State population | Monthly Action |
| [EIA](https://www.eia.gov/electricity/data/state/) | Electricity prices (RES/COM/IND) | Monthly Action |

### Computed by Script
- Rank (Total DCs)
- Rank (Per Capita)
- DCs per 100K population
- YoY % change: DC counts, employment, population (1yr, 3yr, 5yr)

## Setup

### 1. Push to GitHub
```bash
git init && git add . && git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/<you>/datacenter-map.git
git push -u origin main
```

### 2. Enable GitHub Pages
Settings → Pages → Source: `main` branch, `/ (root)` → Save

### 3. (Optional) Set Google Sheet ID as a repo variable
Settings → Secrets and variables → Actions → Variables → New:
- Name: `GOOGLE_SHEET_ID`
- Value: `1SWUPHFJT3K3phN8bB2iuPnyEgFCT1XfT`

### 4. Run first build
Actions tab → "Update Data Center Map" → Run workflow

### 5. Ongoing
- Edit Google Sheet → trigger Action manually (or wait for monthly auto-run)
- Map at: `https://<you>.github.io/datacenter-map/`

## Map Layers
1. **Tax Incentive Programs** — Green/Yellow/Red
2. **DC Count YoY Change** — 1yr growth rate with 3yr/5yr in hover
3. **Total Regulations** — Regulatory burden heat map
4. **DCs Per Capita** — Per 100K population
5. **Population Growth** — 1yr change with 3yr/5yr in hover
6. **State Rankings** — Rank by total DC count

Bubble overlay shows total DC count (always visible).
