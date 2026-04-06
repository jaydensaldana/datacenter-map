"""
fetch_data.py — Automated data pipeline for USA Data Center Map
================================================================
Reads manual data from a Google Sheet, fetches automated data from
federal sources, merges everything, and writes combined JSON.

DESIGN PRINCIPLES:
  - All manual data is keyed by STATE NAME, never by row position.
    Rows can be added, removed, or reordered without breaking anything.
  - Month columns are identified by header text (e.g., "Jun 2025"),
    never by column index.
  - The script scans right-to-left to find the latest populated month,
    then looks up the same month in prior years for YoY % change.

SOURCES:
  Google Sheet (manual):       DC counts, tax incentives, regulations, regional markets
  EIA (auto, no key):          Electricity prices by state/sector
  BLS QCEW (auto, no key):    Employment by NAICS
  Census (auto, no key):       Population estimates

OUTPUT:  data/combined.json    (consumed by build_map.py)
================================================================
"""

import requests, csv, io, json, time, os, re
from datetime import datetime
from collections import OrderedDict

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "1SWUPHFJT3K3phN8bB2iuPnyEgFCT1XfT")
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
    if v is None or v in ('', '--'): return None
    try: return float(str(v).replace(',','').strip())
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
        except Exception as e:
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
        key = (state, city)
        markets[key] = {}
        for ym, hdr in month_cols.items():
            v = safe_int(row.get(hdr, ''))
            if v is not None:
                markets[key][ym] = v
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
# FEDERAL DATA
# ═══════════════════════════════════════════
def fetch_eia():
    print("\n--- EIA electricity prices ---")
    url = "https://www.eia.gov/electricity/data/state/avgprice_annual.xlsx"
    try:
        r = requests.get(url, timeout=60); r.raise_for_status()
        with open('data/eia_raw.xlsx','wb') as f: f.write(r.content)
        import openpyxl
        wb = openpyxl.load_workbook('data/eia_raw.xlsx', data_only=True)
        ws = wb.active
        hrow = None
        for rr in range(1, 20):
            for cc in range(1, ws.max_column+1):
                v = ws.cell(row=rr, column=cc).value
                if v and 'state' in str(v).lower(): hrow = rr; break
            if hrow: break
        if not hrow: return {}
        hdrs = {c: str(ws.cell(row=hrow,column=c).value).strip() for c in range(1,ws.max_column+1) if ws.cell(row=hrow,column=c).value}
        result = {}
        for rr in range(hrow+1, ws.max_row+1):
            s = ws.cell(row=rr,column=1).value
            if not s or str(s).strip() not in STATE_ABBREVS: continue
            s = str(s).strip()
            result[s] = {hdrs[c]: safe_float(ws.cell(row=rr,column=c).value) for c in hdrs if c > 1}
        print(f"  {len(result)} states")
        return result
    except Exception as e:
        print(f"  ERROR: {e}"); return {}

def fetch_qcew_one(naics, year):
    url = f"https://data.bls.gov/cew/data/api/{year}/a/industry/{str(naics).replace('-','_')}.csv"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200: return None
    except: return None
    result = {}
    for row in csv.DictReader(io.StringIO(r.text)):
        af = row.get('area_fips',''); oc = row.get('own_code','')
        if not af.endswith('000') or af=='00000' or oc not in ('0','5'): continue
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
    codes = {'dc_ops':'518210','comp_sys':'5415','nonres_build':'236220'}
    ly = CURRENT_YEAR - 1
    yrs = {ly} | {ly - w for w in CHANGE_WINDOWS}
    out = {}
    for label, naics in codes.items():
        out[label] = {}
        for y in sorted(yrs):
            print(f"  {naics}/{y}", end='')
            d = fetch_qcew_one(naics, y)
            if d: out[label][y] = d; print(f" → {len(d)} states")
            else: print(" → no data")
            time.sleep(0.5)
    return out

def fetch_census():
    print("\n--- Census population ---")
    for v in range(CURRENT_YEAR, CURRENT_YEAR-3, -1):
        url = f"https://www2.census.gov/programs-surveys/popest/datasets/2020-{v}/state/totals/NST-EST{v}-ALLDATA.csv"
        try:
            r = requests.get(url, timeout=30)
            if r.status_code != 200: continue
            result = {}
            for row in csv.DictReader(io.StringIO(r.text)):
                s = row.get('NAME','').strip()
                if s not in STATE_ABBREVS: continue
                result[s] = {}
                for y in range(2020, v+1):
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
def build_combined(dc_hist, lat_dc, tax, regs, qcew, census, eia, mkts, lat_mkt):
    # Rankings
    census_yr = max(next(iter(census.values()), {}).keys(), default=None) if census else None

    state_list = []
    for state in sorted(dc_hist.keys()):
        cur_dcs = dc_hist[state].get(lat_dc)
        pop = census.get(state, {}).get(census_yr) if census_yr else None
        pc = round((cur_dcs/pop)*100000, 2) if cur_dcs and pop and pop > 0 else None
        e = {'state': state, 'abbrev': STATE_ABBREVS.get(state,''),
             'dcs': cur_dcs, 'dcs_per_100k': pc, 'population': pop,
             'census_year': census_yr,
             'dcs_month': f"{MONTH_NAMES[lat_dc[1]-1]} {lat_dc[0]}",
             'tax_incentive': tax.get(state,{}).get('Tax Incentive?',''),
             'incentive_type': tax.get(state,{}).get('Incentive Type',''),
             'min_investment': tax.get(state,{}).get('Min Capital Investment',''),
             'job_creation_req': tax.get(state,{}).get('Job Creation Req',''),
             'incentive_desc': tax.get(state,{}).get('Incentive Description',''),
             'total_regulations': regs.get(state)}
        for w in CHANGE_WINDOWS:
            comp = dc_hist[state].get((lat_dc[0]-w, lat_dc[1]))
            e[f'dcs_{w}yr_ago'] = comp
            e[f'dcs_{w}yr_pct'] = pct_change(cur_dcs, comp)
        for lbl in qcew:
            yrs = sorted(qcew[lbl].keys(), reverse=True)
            if yrs:
                ly = yrs[0]; e[f'{lbl}_current'] = qcew[lbl].get(ly,{}).get(state)
                e[f'{lbl}_year'] = ly
                for w in CHANGE_WINDOWS:
                    h = qcew[lbl].get(ly-w,{}).get(state)
                    e[f'{lbl}_{w}yr_pct'] = pct_change(e[f'{lbl}_current'], h)
        if census_yr and state in census:
            for w in CHANGE_WINDOWS:
                h = census[state].get(census_yr - w)
                e[f'pop_{w}yr_pct'] = pct_change(pop, h)
        state_list.append(e)

    # Compute rankings
    state_list.sort(key=lambda x: x.get('dcs') or 0, reverse=True)
    for i, s in enumerate(state_list, 1): s['rank_total'] = i
    by_pc = sorted(state_list, key=lambda x: x.get('dcs_per_100k') or 0, reverse=True)
    for i, s in enumerate(by_pc, 1): s['rank_per_capita'] = i

    # Markets
    mkt_list = []
    for (state, city), hist in mkts.items():
        cur = hist.get(lat_mkt)
        m = {'state': state, 'city': city, 'dcs': cur,
             'month': f"{MONTH_NAMES[lat_mkt[1]-1]} {lat_mkt[0]}"}
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

    eia = fetch_eia()
    qcew = fetch_all_qcew()
    census = fetch_census()

    print("\n--- Building combined output ---")
    build_combined(dc_hist, lat_dc, tax, regs, qcew, census, eia, mkts, lat_mkt)

    with open('data/last_updated.json','w') as f:
        json.dump({'timestamp': datetime.now().isoformat(), 'sheet_id': GOOGLE_SHEET_ID}, f, indent=2)
    print("\n" + "="*60 + "\nDONE\n" + "="*60)

if __name__ == '__main__':
    main()
