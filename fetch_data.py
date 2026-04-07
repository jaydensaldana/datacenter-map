"""
fetch_data.py — Automated data pipeline for USA Data Center Map
================================================================
Reads manual data from Google Sheet, fetches federal data, merges,
and writes data/combined.json for the map builder.

DESIGN: All data keyed by state name. Rows can be added/reordered
in the Google Sheet without breaking anything. Month columns are
identified by header text (e.g., "Apr 2026").

SOURCES:
  - Google Sheet:  DC counts, tax incentives, regulations, regional markets
  - EIA EPM 5.6.A: Average price of electricity by state/sector
  - BLS QCEW:      Employment by NAICS (518210, 5415, 236220)
  - Census PEP:    State population estimates
"""

import requests, csv, io, json, time, os, re
from datetime import datetime

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "1ZtNh_GJCEzZ3zNxIjJad09ihbwT5UDBPfy7wK2f1gWo")
CURRENT_YEAR = datetime.now().year
CHANGE_WINDOWS = [1, 3, 5]
MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
MONTH_RE = re.compile(r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})$')

STATE_ABBREVS = {
    'Alabama':'AL','Alaska':'AK','Arizona':'AZ','Arkansas':'AR','California':'CA',
    'Colorado':'CO','Connecticut':'CT','Delaware':'DE','Florida':'FL','Georgia':'GA',
    'Hawaii':'HI','Idaho':'ID','Illinois':'IL','Indiana':'IN','Iowa':'IA','Kansas':'KS',
    'Kentucky':'KY','Louisiana':'LA','Maine':'ME','Maryland':'MD','Massachusetts':'MA',
    'Michigan':'MI','Minnesota':'MN','Mississippi':'MS','Missouri':'MO','Montana':'MT',
    'Nebraska':'NE','Nevada':'NV','New Hampshire':'NH','New Jersey':'NJ',
    'New Mexico':'NM','New York':'NY','North Carolina':'NC','North Dakota':'ND',
    'Ohio':'OH','Oklahoma':'OK','Oregon':'OR','Pennsylvania':'PA','Rhode Island':'RI',
    'South Carolina':'SC','South Dakota':'SD','Tennessee':'TN','Texas':'TX','Utah':'UT',
    'Vermont':'VT','Virginia':'VA','Washington':'WA','West Virginia':'WV',
    'Wisconsin':'WI','Wyoming':'WY'
}
ABBREV_TO_STATE = {v: k for k, v in STATE_ABBREVS.items()}
STATE_FIPS = {
    'AL':'01','AK':'02','AZ':'04','AR':'05','CA':'06','CO':'08','CT':'09','DE':'10',
    'FL':'12','GA':'13','HI':'15','ID':'16','IL':'17','IN':'18','IA':'19','KS':'20',
    'KY':'21','LA':'22','ME':'23','MD':'24','MA':'25','MI':'26','MN':'27','MS':'28',
    'MO':'29','MT':'30','NE':'31','NV':'32','NH':'33','NJ':'34','NM':'35','NY':'36',
    'NC':'37','ND':'38','OH':'39','OK':'40','OR':'41','PA':'42','RI':'44','SC':'45',
    'SD':'46','TN':'47','TX':'48','UT':'49','VT':'50','VA':'51','WA':'53','WV':'54',
    'WI':'55','WY':'56'
}
FIPS_TO_ABBREV = {v: k for k, v in STATE_FIPS.items()}


def safe_int(v):
    if v is None or v == '': return None
    try: return int(float(str(v).replace(',','').strip()))
    except: return None

def safe_float(v):
    if v is None or v in ('', '--', 'N/A', 'NA'): return None
    try: return float(str(v).replace(',','').replace('$','').strip())
    except: return None

def parse_month(label):
    m = MONTH_RE.match(label.strip())
    if not m: return None
    return (int(m.group(2)), MONTH_NAMES.index(m.group(1)) + 1)

def pct_change(cur, hist):
    if cur is None or hist is None or hist == 0: return None
    return (cur - hist) / hist


# ═══════════════════════════════════════════
# GOOGLE SHEETS
# ═══════════════════════════════════════════
def fetch_sheet(name):
    enc = requests.utils.quote(name)
    urls = [
        f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/gviz/tq?tqx=out:csv&sheet={enc}",
        f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&sheet={enc}",
    ]
    for url in urls:
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200 and len(r.text) > 10:
                reader = csv.reader(io.StringIO(r.text))
                headers = [h.strip().strip('"') for h in next(reader)]
                rows = []
                for row in reader:
                    if len(row) >= 1 and row[0].strip():
                        rows.append({h: (row[i].strip() if i < len(row) else '') for i, h in enumerate(headers)})
                print(f"  '{name}': {len(rows)} rows")
                return rows, headers
        except Exception:
            continue
    print(f"  ERROR: Could not fetch '{name}'")
    return [], []

def read_dc_history(rows, headers):
    month_cols = {}
    for h in headers:
        p = parse_month(h)
        if p: month_cols[p] = h
    history, latest = {}, (2020, 1)
    for row in rows:
        state = row.get('State', '').strip()
        if state not in STATE_ABBREVS: continue
        history[state] = {}
        for ym, hdr in month_cols.items():
            v = safe_int(row.get(hdr, ''))
            if v is not None:
                history[state][ym] = v
                if ym > latest: latest = ym
    print(f"  DC History: {len(history)} states, latest={MONTH_NAMES[latest[1]-1]} {latest[0]}")
    return history, latest

def read_markets(rows, headers):
    month_cols = {}
    for h in headers:
        p = parse_month(h)
        if p: month_cols[p] = h
    markets, latest = {}, (2020, 1)
    for row in rows:
        state = row.get('State', '').strip()
        city = row.get('Market/City', '').strip()
        if not state or not city: continue
        markets[(state, city)] = {}
        for ym, hdr in month_cols.items():
            v = safe_int(row.get(hdr, ''))
            if v is not None:
                markets[(state, city)][ym] = v
                if ym > latest: latest = ym
    print(f"  Markets: {len(markets)} entries")
    return markets, latest

def read_tax(rows, headers):
    d = {}
    for row in rows:
        s = row.get('State', '').strip()
        if s not in STATE_ABBREVS: continue
        d[s] = {k: row.get(k, '').strip() for k in
                ['Tax Incentive?','Incentive Type','Min Capital Investment',
                 'Job Creation Req','Incentive Description']}
    return d

def read_regs(rows, headers):
    d = {}
    for row in rows:
        s = row.get('State', '').strip()
        if s not in STATE_ABBREVS: continue
        d[s] = safe_int(row.get('Total Regulations', ''))
    return d


# ═══════════════════════════════════════════
# EIA — Table 5.6.A: Average Price of Electricity by State by Sector
# ═══════════════════════════════════════════
def fetch_eia():
    """
    Download EIA Electric Power Monthly Table 5.6.A.
    Returns: {state: {(year, month): {'res': cents, 'com': cents, 'ind': cents}}}
    Also tracks the most recent (year, month) of data found.
    """
    print("\n--- EIA Table 5.6.A: Electricity Prices ---")
    import openpyxl

    # Try multiple URL patterns. The Electric Power Monthly publishes monthly,
    # and the table_5_06_a.xlsx file is updated each month with the latest data.
    candidate_urls = [
        "https://www.eia.gov/electricity/monthly/xls/table_5_06_a.xlsx",
        "https://www.eia.gov/electricity/monthly/epm_table_grapher.php?t=epmt_5_6_a",
    ]

    for url in candidate_urls:
        print(f"  Trying: {url}")
        try:
            r = requests.get(url, timeout=60, allow_redirects=True)
            if r.status_code != 200:
                print(f"    Status {r.status_code}")
                continue
            ct = r.headers.get('content-type', '').lower()
            if 'sheet' not in ct and 'excel' not in ct and 'octet' not in ct:
                # Got HTML, not the Excel file
                print(f"    Got {ct}, not a spreadsheet")
                continue
            with open('data/eia_raw.xlsx', 'wb') as f:
                f.write(r.content)
            print(f"    Downloaded {len(r.content):,} bytes")

            wb = openpyxl.load_workbook('data/eia_raw.xlsx', data_only=True)
            ws = wb.active

            # Table 5.6.A structure:
            #   Top rows: title text
            #   Header rows include "Census Division and State", then sector headers
            #     spanning multiple sub-columns for current month, year ago, etc.
            #   Each state name appears in column A (or B), with prices in cents/kWh
            #     in subsequent columns.
            # Specifically: Residential / Commercial / Industrial / Transportation / All Sectors
            # Each section has columns for "current month", "1 year ago", and "% change"

            # Find the title row to extract the month/year
            title_text = ''
            for rr in range(1, 8):
                for cc in range(1, 5):
                    v = ws.cell(row=rr, column=cc).value
                    if v and 'table 5.6' in str(v).lower():
                        title_text = str(v)
                        # Get full title from this cell or adjacent
                        for cc2 in range(1, ws.max_column + 1):
                            v2 = ws.cell(row=rr, column=cc2).value
                            if v2 and len(str(v2)) > len(title_text):
                                title_text = str(v2)
                        break
                if title_text:
                    break

            print(f"    Title: {title_text[:100]}")

            # Extract month and year from title
            # Format example: "Average Price of Electricity to Ultimate Customers
            # by End-Use Sector, by State, January 2026 and 2025"
            data_year, data_month = None, None
            month_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})', title_text)
            if month_match:
                month_full = month_match.group(1)
                month_short = month_full[:3]
                data_month = MONTH_NAMES.index(month_short) + 1
                data_year = int(month_match.group(2))
                print(f"    Data month: {month_short} {data_year}")

            # Find the data start row by scanning for "New England" or "Alabama" or similar
            data_start = None
            for rr in range(1, 20):
                v1 = ws.cell(row=rr, column=1).value
                if v1 and str(v1).strip() in ('New England', 'Alabama', 'Connecticut'):
                    data_start = rr
                    break

            if not data_start:
                print(f"    Could not find data start row")
                continue

            print(f"    Data starts at row {data_start}")

            # Now identify which columns hold residential/commercial/industrial
            # For Table 5.6.A, the structure is typically:
            # Col A: state name
            # Col B-D: Residential (current, prior year, % change)
            # Col E-G: Commercial
            # Col H-J: Industrial
            # Col K-M: Transportation
            # Col N-P: All sectors
            # We want column B (residential current), E (commercial current), H (industrial current)
            # And from prior year (column C, F, I) for YoY computation
            #
            # But to be robust, let's scan header rows above data_start to figure out which columns are which sector

            # Find sector header row (residential, commercial, industrial)
            sector_row = None
            for rr in range(1, data_start):
                row_text = ' '.join(str(ws.cell(row=rr, column=c).value or '').lower()
                                    for c in range(1, ws.max_column + 1))
                if 'residential' in row_text and 'commercial' in row_text:
                    sector_row = rr
                    break

            if not sector_row:
                print("    Could not find sector header row")
                continue

            # Map sector → starting column
            sector_cols = {}
            for c in range(1, ws.max_column + 1):
                v = str(ws.cell(row=sector_row, column=c).value or '').strip().lower()
                if v.startswith('residential') and 'res' not in sector_cols:
                    sector_cols['res'] = c
                elif v.startswith('commercial') and 'com' not in sector_cols:
                    sector_cols['com'] = c
                elif v.startswith('industrial') and 'ind' not in sector_cols:
                    sector_cols['ind'] = c

            print(f"    Sector columns: {sector_cols}")

            if 'res' not in sector_cols:
                print("    Couldn't find Residential column")
                continue

            # Identify the row immediately below sector_row that has subheaders like
            # "Jan 2026" / "Jan 2025" / "% change" — but for our purposes the FIRST
            # column under each sector header is always "current month".
            # So sector_cols['res'] = the residential current column,
            # sector_cols['res'] + 1 = residential prior year column

            # Parse data rows
            result = {}
            ym_current = (data_year, data_month) if data_year and data_month else (CURRENT_YEAR, 1)
            ym_prior = (ym_current[0] - 1, ym_current[1])

            for rr in range(data_start, ws.max_row + 1):
                state_val = ws.cell(row=rr, column=1).value
                if not state_val: continue
                state_str = str(state_val).strip()
                if state_str not in STATE_ABBREVS:
                    continue

                if state_str not in result:
                    result[state_str] = {}

                # Read current and prior year for each sector
                current_data = {}
                prior_data = {}
                for sec_key, base_col in sector_cols.items():
                    cur_val = safe_float(ws.cell(row=rr, column=base_col).value)
                    prior_val = safe_float(ws.cell(row=rr, column=base_col + 1).value)
                    if cur_val is not None:
                        current_data[sec_key] = cur_val
                    if prior_val is not None:
                        prior_data[sec_key] = prior_val

                if current_data:
                    result[state_str][ym_current] = current_data
                if prior_data:
                    result[state_str][ym_prior] = prior_data

            print(f"    Parsed {len(result)} states")
            if result:
                # Show a sample for verification
                sample_state = next(iter(result.keys()))
                print(f"    Sample [{sample_state}]: {result[sample_state]}")
                return result, ym_current

        except Exception as e:
            print(f"    Error: {e}")
            import traceback
            traceback.print_exc()
            continue

    print("  EIA: No data retrieved")
    return {}, None


# ═══════════════════════════════════════════
# BLS QCEW — fetch ALL years from latest-5 to latest
# ═══════════════════════════════════════════
def fetch_qcew_one(naics, year):
    url = f"https://data.bls.gov/cew/data/api/{year}/a/industry/{str(naics).replace('-','_')}.csv"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200: return None
    except: return None
    result = {}
    for row in csv.DictReader(io.StringIO(r.text)):
        af = row.get('area_fips','')
        oc = row.get('own_code','')
        if not af.endswith('000') or af == '00000' or oc not in ('0','5'):
            continue
        ab = FIPS_TO_ABBREV.get(af[:2])
        if not ab: continue
        sn = ABBREV_TO_STATE[ab]
        try:
            v = int(row.get('annual_avg_emplvl','0'))
            if sn not in result or v > result[sn]: result[sn] = v
        except: pass
    return result

def fetch_all_qcew():
    print("\n--- BLS QCEW employment ---")
    codes = {'dc_ops':'518210', 'comp_sys':'5415', 'nonres_build':'236220'}

    # Fetch every year from CURRENT_YEAR-6 to CURRENT_YEAR-1
    # so 1yr/3yr/5yr lookups always have a comparison year
    years_to_try = list(range(CURRENT_YEAR - 6, CURRENT_YEAR))

    out = {}
    for label, naics in codes.items():
        out[label] = {}
        for y in years_to_try:
            print(f"  {naics}/{y}", end='', flush=True)
            d = fetch_qcew_one(naics, y)
            if d:
                out[label][y] = d
                print(f" → {len(d)} states")
            else:
                print(" → no data")
            time.sleep(0.4)
    return out


# ═══════════════════════════════════════════
# CENSUS
# ═══════════════════════════════════════════
def fetch_census():
    print("\n--- Census population ---")
    for v in range(CURRENT_YEAR, CURRENT_YEAR - 4, -1):
        url = f"https://www2.census.gov/programs-surveys/popest/datasets/2020-{v}/state/totals/NST-EST{v}-ALLDATA.csv"
        print(f"  Trying vintage {v}...")
        try:
            r = requests.get(url, timeout=30)
            if r.status_code != 200: continue
            result = {}
            for row in csv.DictReader(io.StringIO(r.text)):
                s = row.get('NAME','').strip()
                if s not in STATE_ABBREVS: continue
                result[s] = {}
                for y in range(2020, v + 1):
                    val = safe_int(row.get(f'POPESTIMATE{y}',''))
                    if val: result[s][y] = val
            if result:
                print(f"  Vintage {v}: {len(result)} states")
                return result
        except: pass
    return {}


# ═══════════════════════════════════════════
# COMBINE
# ═══════════════════════════════════════════
def build_combined(dc_hist, lat_dc, tax, regs, qcew, census, eia, eia_ym, mkts, lat_mkt):
    census_yr = max(next(iter(census.values()), {}).keys(), default=None) if census else None

    # EIA latest year/month
    eia_label = ''
    if eia_ym:
        eia_label = f"{MONTH_NAMES[eia_ym[1]-1]} {eia_ym[0]}"

    state_list = []
    for state in sorted(dc_hist.keys()):
        cur_dcs = dc_hist[state].get(lat_dc)
        pop = census.get(state, {}).get(census_yr) if census_yr else None
        pc = round((cur_dcs/pop)*100000, 2) if cur_dcs and pop and pop > 0 else None

        e = {
            'state': state,
            'abbrev': STATE_ABBREVS.get(state, ''),
            'dcs': cur_dcs,
            'dcs_per_100k': pc,
            'population': pop,
            'census_year': census_yr,
            'dcs_month': f"{MONTH_NAMES[lat_dc[1]-1]} {lat_dc[0]}",
            'eia_month': eia_label,
            'tax_incentive': tax.get(state, {}).get('Tax Incentive?', ''),
            'incentive_type': tax.get(state, {}).get('Incentive Type', ''),
            'min_investment': tax.get(state, {}).get('Min Capital Investment', ''),
            'job_creation_req': tax.get(state, {}).get('Job Creation Req', ''),
            'incentive_desc': tax.get(state, {}).get('Incentive Description', ''),
            'total_regulations': regs.get(state),
        }

        # DC YoY % changes
        for w in CHANGE_WINDOWS:
            comp = dc_hist[state].get((lat_dc[0]-w, lat_dc[1]))
            e[f'dcs_{w}yr_ago'] = comp
            e[f'dcs_{w}yr_pct'] = pct_change(cur_dcs, comp)

        # Employment + % changes (latest year + 1/3/5 year ago)
        for lbl in qcew:
            yrs = sorted(qcew[lbl].keys(), reverse=True)
            if yrs:
                ly = yrs[0]
                cur_emp = qcew[lbl].get(ly, {}).get(state)
                e[f'{lbl}_current'] = cur_emp
                e[f'{lbl}_year'] = ly
                for w in CHANGE_WINDOWS:
                    h = qcew[lbl].get(ly - w, {}).get(state)
                    e[f'{lbl}_{w}yr_pct'] = pct_change(cur_emp, h)
                    e[f'{lbl}_{w}yr_year'] = ly - w if h else None

        # Population % changes
        if census_yr and state in census:
            for w in CHANGE_WINDOWS:
                h = census[state].get(census_yr - w)
                e[f'pop_{w}yr_pct'] = pct_change(pop, h)

        # Electricity rates + % changes
        # eia[state] is a dict of (year, month) → {'res':, 'com':, 'ind':}
        if eia_ym and state in eia and eia_ym in eia[state]:
            current_rates = eia[state][eia_ym]
            for sec in ['res', 'com', 'ind']:
                cur_rate = current_rates.get(sec)
                e[f'{sec}_rate'] = cur_rate
                # 1yr % change: same month, prior year
                ym_1yr = (eia_ym[0] - 1, eia_ym[1])
                hist_1yr = eia.get(state, {}).get(ym_1yr, {}).get(sec)
                e[f'{sec}_1yr_pct'] = pct_change(cur_rate, hist_1yr)
                # 3yr and 5yr — Table 5.6.A only has current + prior year,
                # so 3yr and 5yr will be None for now
                e[f'{sec}_3yr_pct'] = None
                e[f'{sec}_5yr_pct'] = None

        state_list.append(e)

    # Rankings
    state_list.sort(key=lambda x: x.get('dcs') or 0, reverse=True)
    for i, s in enumerate(state_list, 1): s['rank_total'] = i
    by_pc = sorted(state_list, key=lambda x: x.get('dcs_per_100k') or 0, reverse=True)
    for i, s in enumerate(by_pc, 1): s['rank_per_capita'] = i

    # Markets
    mkt_list = []
    for (state, city), hist in mkts.items():
        cur = hist.get(lat_mkt)
        m = {
            'state': state, 'city': city, 'dcs': cur,
            'month': f"{MONTH_NAMES[lat_mkt[1]-1]} {lat_mkt[0]}",
        }
        for w in CHANGE_WINDOWS:
            comp = hist.get((lat_mkt[0]-w, lat_mkt[1]))
            m[f'dcs_{w}yr_ago'] = comp
            m[f'dcs_{w}yr_pct'] = pct_change(cur, comp)
        mkt_list.append(m)

    out = {
        'generated': datetime.now().isoformat(),
        'sheet_id': GOOGLE_SHEET_ID,
        'latest_dc_month': f"{MONTH_NAMES[lat_dc[1]-1]} {lat_dc[0]}",
        'latest_mkt_month': f"{MONTH_NAMES[lat_mkt[1]-1]} {lat_mkt[0]}",
        'latest_eia_month': eia_label,
        'change_windows': CHANGE_WINDOWS,
        'states': state_list,
        'markets': mkt_list,
    }
    with open('data/combined.json','w') as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\n  Output: {len(state_list)} states, {len(mkt_list)} markets")
    return out


# ═══════════════════════════════════════════
def main():
    print("="*60)
    print("USA Data Center Map — Data Pipeline")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*60)
    os.makedirs('data', exist_ok=True)

    print("\n--- Google Sheet ---")
    dc_rows, dc_h = fetch_sheet('State DC History')
    dc_hist, lat_dc = read_dc_history(dc_rows, dc_h)
    tax_rows, tax_h = fetch_sheet('State Tax Incentive')
    tax = read_tax(tax_rows, tax_h)
    reg_rows, reg_h = fetch_sheet('Total Regulations')
    regs = read_regs(reg_rows, reg_h)
    mkt_rows, mkt_h = fetch_sheet('Regional Market History')
    mkts, lat_mkt = read_markets(mkt_rows, mkt_h)

    eia, eia_ym = fetch_eia()
    qcew = fetch_all_qcew()
    census = fetch_census()

    print("\n--- Building combined output ---")
    build_combined(dc_hist, lat_dc, tax, regs, qcew, census, eia, eia_ym, mkts, lat_mkt)

    with open('data/last_updated.json','w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'sheet_id': GOOGLE_SHEET_ID,
        }, f, indent=2)
    print("\n" + "="*60 + "\nDONE\n" + "="*60)


if __name__ == '__main__':
    main()
