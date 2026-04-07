"""
build_map.py — Interactive Plotly choropleth with multi-toggle controls
Reads data/combined.json, writes index.html
"""

import json, os
from datetime import datetime

CENTROIDS = {
    'AL':(32.8,-86.8),'AK':(64.2,-152.5),'AZ':(34.0,-111.1),'AR':(35.2,-91.8),
    'CA':(36.8,-119.4),'CO':(39.1,-105.4),'CT':(41.6,-72.7),'DE':(39.0,-75.5),
    'FL':(27.8,-81.8),'GA':(33.0,-83.6),'HI':(19.9,-155.6),'ID':(44.2,-114.4),
    'IL':(40.3,-89.0),'IN':(40.3,-86.1),'IA':(42.0,-93.2),'KS':(38.5,-98.8),
    'KY':(37.8,-84.3),'LA':(30.5,-91.2),'ME':(45.3,-69.4),'MD':(39.0,-76.6),
    'MA':(42.4,-71.4),'MI':(44.3,-84.5),'MN':(46.4,-94.6),'MS':(32.7,-89.7),
    'MO':(38.6,-91.8),'MT':(46.8,-110.4),'NE':(41.1,-98.3),'NV':(38.8,-116.4),
    'NH':(43.2,-71.6),'NJ':(40.1,-74.5),'NM':(34.8,-106.2),'NY':(43.0,-75.0),
    'NC':(35.6,-79.8),'ND':(47.5,-100.5),'OH':(40.4,-82.7),'OK':(35.0,-97.1),
    'OR':(43.8,-120.6),'PA':(41.2,-77.2),'RI':(41.6,-71.5),'SC':(33.9,-81.2),
    'SD':(44.3,-99.4),'TN':(35.5,-86.6),'TX':(31.0,-100.0),'UT':(39.3,-111.1),
    'VT':(44.6,-72.7),'VA':(37.8,-78.2),'WA':(47.8,-120.7),'WV':(38.5,-80.5),
    'WI':(43.8,-89.9),'WY':(43.1,-107.6)
}

# Color scales — exactly as user specified
GREEN_SCALE = [
    [0.0, '#edf8e9'],
    [0.2, '#bae4b3'],
    [0.4, '#74c476'],
    [0.6, '#31a354'],
    [0.8, '#006d2c'],
    [1.0, '#00441b'],
]

BLUE_SCALE = [
    [0.0, '#f7fbff'],
    [0.25, '#c6dbef'],
    [0.5, '#6baed6'],
    [0.75, '#2171b5'],
    [1.0, '#08306b'],
]

YLGNBU_SCALE = [
    [0.000, 'rgb(255,255,217)'],
    [0.125, 'rgb(237,248,177)'],
    [0.250, 'rgb(199,233,180)'],
    [0.375, 'rgb(127,205,187)'],
    [0.500, 'rgb(65,182,196)'],
    [0.625, 'rgb(29,145,192)'],
    [0.750, 'rgb(34,94,168)'],
    [0.875, 'rgb(37,52,148)'],
    [1.000, 'rgb(8,29,88)'],
]

# Tax incentive: 3 discrete tiles
TAX_DISCRETE = [
    [0.0,    '#ffa07a'],  # 0 = No incentive
    [0.333,  '#ffa07a'],
    [0.334,  '#ffffb3'],  # 1 = Local only
    [0.666,  '#ffffb3'],
    [0.667,  '#90ee90'],  # 2 = State program
    [1.0,    '#90ee90'],
]

# Diverging scale for % changes (red = decrease, green = increase)
RDYLGN_SCALE = 'RdYlGn'


def fmt_pct(v):
    if v is None: return 'N/A'
    return f"{v*100:+.1f}%"

def fmt_num(v):
    if v is None: return 'N/A'
    if isinstance(v, (int, float)):
        if v == int(v): return f"{int(v):,}"
        return f"{v:,.2f}"
    return str(v)

def safe_num(v, default=0):
    if v is None: return default
    try: return float(v)
    except: return default


def build():
    print("Building map...")
    with open('data/combined.json') as f:
        data = json.load(f)

    states = data['states']
    states.sort(key=lambda s: s.get('rank_total') or 999)
    abbrevs = [s['abbrev'] for s in states]
    updated = data.get('generated', '')[:16].replace('T', ' ')
    dc_month = data.get('latest_dc_month', '')
    eia_month = data.get('latest_eia_month', '')

    sample = states[0] if states else {}
    print(f"  Sample keys: {sorted(sample.keys())[:30]}")

    # ─── Helper: build a choropleth trace ───
    def choro(z, text, colorscale, bar_title, visible=False, zmin=None, zmax=None,
              tickvals=None, ticktext=None, showscale=None):
        cb = {'title': {'text': bar_title}, 'x': 1.0, 'len': 0.6, 'thickness': 15}
        if tickvals is not None:
            cb['tickvals'] = tickvals
        if ticktext is not None:
            cb['ticktext'] = ticktext
        t = {
            'type': 'choropleth',
            'locationmode': 'USA-states',
            'locations': abbrevs,
            'z': z,
            'text': text,
            'hoverinfo': 'text',
            'colorscale': colorscale,
            'colorbar': cb,
            'visible': visible,
            'showscale': visible if showscale is None else showscale,
        }
        if zmin is not None: t['zmin'] = zmin
        if zmax is not None: t['zmax'] = zmax
        return t

    traces = []
    trace_map = {}
    bubble_map = {}

    def register(layer, sector, metric, trace):
        idx = len(traces)
        traces.append(trace)
        trace_map.setdefault(layer, {}).setdefault(sector, {})[metric] = idx

    # ════════════════════════════════════════════
    # LAYER: Tax Incentives (DISCRETE)
    # ════════════════════════════════════════════
    z, txt = [], []
    for s in states:
        ti = (s.get('tax_incentive') or '').strip().upper()
        ti_lower = ti.lower()
        # Map to 0/1/2
        if ti == 'Y':
            cat = 2  # State program
            cat_label = 'State Program'
        elif 'local' in ti_lower:
            cat = 1
            cat_label = 'General/Local Programs'
        else:
            cat = 0
            cat_label = 'No Incentive'
        z.append(cat)
        txt.append(
            f"<b>{s['state']}</b><br>"
            f"<b>Category:</b> {cat_label}<br>"
            f"Incentive: {s.get('tax_incentive') or 'N/A'}<br>"
            f"Type: {s.get('incentive_type') or 'N/A'}<br>"
            f"Min Investment: {s.get('min_investment') or 'N/A'}<br>"
            f"Job Req: {s.get('job_creation_req') or 'N/A'}"
        )

    register('tax', 'none', 'none', choro(
        z, txt, TAX_DISCRETE, 'Tax Incentive',
        visible=True, zmin=0, zmax=2,
        tickvals=[0, 1, 2],
        ticktext=['No Incentive', 'Local Only', 'State Program'],
    ))

    # ════════════════════════════════════════════
    # LAYER: Total Regulations (Green scale)
    # ════════════════════════════════════════════
    z = [safe_num(s.get('total_regulations')) for s in states]
    txt = [
        f"<b>{s['state']}</b><br>Total Regulations: {fmt_num(s.get('total_regulations'))}<br>"
        f"<i>Source: QuantGov State RegData</i>"
        for s in states
    ]
    register('regs', 'none', 'none', choro(z, txt, GREEN_SCALE, 'Regulations'))

    # ════════════════════════════════════════════
    # LAYER: DCs Per Capita
    # ════════════════════════════════════════════
    census_yr = sample.get('census_year', '')
    z = [safe_num(s.get('dcs_per_100k')) for s in states]
    txt = [
        f"<b>{s['state']}</b><br>"
        f"DCs per 100K: {fmt_num(s.get('dcs_per_100k'))}<br>"
        f"Total DCs ({dc_month}): {fmt_num(s.get('dcs'))}<br>"
        f"Population ({census_yr}): {fmt_num(s.get('population'))}"
        for s in states
    ]
    register('per_capita', 'none', 'none', choro(z, txt, YLGNBU_SCALE, 'DCs per 100K'))

    # ════════════════════════════════════════════
    # LAYER: State Rankings
    # ════════════════════════════════════════════
    z = [safe_num(s.get('rank_total'), 50) for s in states]
    txt = [
        f"<b>{s['state']}</b><br>"
        f"Rank (Total): #{s.get('rank_total') or 'N/A'}<br>"
        f"Rank (Per Capita): #{s.get('rank_per_capita') or 'N/A'}<br>"
        f"Total DCs ({dc_month}): {fmt_num(s.get('dcs'))}"
        for s in states
    ]
    register('rank', 'none', 'none', choro(z, txt, BLUE_SCALE, 'Rank'))

    # ════════════════════════════════════════════
    # LAYER: Population
    # ════════════════════════════════════════════
    for met_key, met_label in [
        ('current', 'Population'),
        ('1yr', '1-Year % Change'),
        ('3yr', '3-Year % Change'),
        ('5yr', '5-Year % Change'),
    ]:
        if met_key == 'current':
            z = [safe_num(s.get('population')) for s in states]
            scale = BLUE_SCALE
            bar = f'Population ({census_yr})'
        else:
            z = [safe_num(s.get(f'pop_{met_key}_pct')) * 100 for s in states]
            scale = RDYLGN_SCALE
            bar = met_label
        txt = [
            f"<b>{s['state']}</b><br>"
            f"Population ({census_yr}): {fmt_num(s.get('population'))}<br>"
            f"1yr Change: {fmt_pct(s.get('pop_1yr_pct'))}<br>"
            f"3yr Change: {fmt_pct(s.get('pop_3yr_pct'))}<br>"
            f"5yr Change: {fmt_pct(s.get('pop_5yr_pct'))}"
            for s in states
        ]
        register('population', 'none', met_key, choro(z, txt, scale, bar))

    # ════════════════════════════════════════════
    # LAYER: Electricity Rates (3 sectors × 4 metrics)
    # ════════════════════════════════════════════
    sectors_elec = [
        ('res', 'Residential'),
        ('com', 'Commercial'),
        ('ind', 'Industrial'),
    ]
    metrics_time = [
        ('current', 'Current Rate'),
        ('1yr', '1-Year % Change'),
        ('3yr', '3-Year % Change'),
        ('5yr', '5-Year % Change'),
    ]

    for sec_key, sec_label in sectors_elec:
        for met_key, met_label in metrics_time:
            if met_key == 'current':
                z = [safe_num(s.get(f'{sec_key}_rate')) for s in states]
                scale = YLGNBU_SCALE
                bar = f'¢/kWh ({eia_month})' if eia_month else '¢/kWh'
            else:
                z = [safe_num(s.get(f'{sec_key}_{met_key}_pct')) * 100 for s in states]
                scale = RDYLGN_SCALE
                bar = met_label
            txt = [
                f"<b>{s['state']}</b><br>"
                f"<b>{sec_label} Rate ({eia_month or 'latest'})</b><br>"
                f"Current: {fmt_num(s.get(f'{sec_key}_rate'))} ¢/kWh<br>"
                f"1yr Change: {fmt_pct(s.get(f'{sec_key}_1yr_pct'))}<br>"
                f"3yr Change: {fmt_pct(s.get(f'{sec_key}_3yr_pct'))}<br>"
                f"5yr Change: {fmt_pct(s.get(f'{sec_key}_5yr_pct'))}"
                for s in states
            ]
            register('electricity', sec_key, met_key, choro(z, txt, scale, bar))

    # ════════════════════════════════════════════
    # LAYER: Employment (3 NAICS × 4 metrics)
    # ════════════════════════════════════════════
    naics = [
        ('dc_ops', 'Data Center Ops (NAICS 518210)'),
        ('comp_sys', 'Computer Sys Design (NAICS 5415)'),
        ('nonres_build', 'Non-Res Builders (NAICS 236220)'),
    ]
    for naics_key, naics_label in naics:
        for met_key, met_label in metrics_time:
            data_year = sample.get(f'{naics_key}_year', '')
            if met_key == 'current':
                z = [safe_num(s.get(f'{naics_key}_current')) for s in states]
                scale = BLUE_SCALE
                bar = f'Employment ({data_year})' if data_year else 'Employment'
            else:
                z = [safe_num(s.get(f'{naics_key}_{met_key}_pct')) * 100 for s in states]
                scale = RDYLGN_SCALE
                bar = met_label
            txt = [
                f"<b>{s['state']}</b><br>"
                f"<b>{naics_label}</b><br>"
                f"Current ({s.get(f'{naics_key}_year', 'N/A')}): {fmt_num(s.get(f'{naics_key}_current'))}<br>"
                f"1yr Change: {fmt_pct(s.get(f'{naics_key}_1yr_pct'))}<br>"
                f"3yr Change: {fmt_pct(s.get(f'{naics_key}_3yr_pct'))}<br>"
                f"5yr Change: {fmt_pct(s.get(f'{naics_key}_5yr_pct'))}"
                for s in states
            ]
            register('employment', naics_key, met_key, choro(z, txt, scale, bar))

    # ════════════════════════════════════════════
    # BUBBLES (independent control)
    # ════════════════════════════════════════════
    lats = [CENTROIDS.get(s['abbrev'], (0, 0))[0] for s in states]
    lons = [CENTROIDS.get(s['abbrev'], (0, 0))[1] for s in states]

    def make_bubble(values, hover_text, color, name):
        max_val = max((abs(v) for v in values if v is not None), default=1) or 1
        sizes = []
        for v in values:
            if v is None or v == 0:
                sizes.append(8)
            else:
                normalized = abs(v) / max_val
                sized = 8 + (normalized ** 0.5) * 52
                sizes.append(min(max(sized, 8), 60))
        return {
            'type': 'scattergeo',
            'locationmode': 'USA-states',
            'lat': lats,
            'lon': lons,
            'text': hover_text,
            'hoverinfo': 'text',
            'name': name,
            'showlegend': False,
            'marker': {
                'size': sizes,
                'color': color,
                'line': {'color': 'rgba(20,30,50,1)', 'width': 1.5},
                'sizemode': 'diameter',
                'opacity': 0.75,
            },
            'visible': False,
        }

    # Total DC bubble
    vals = [safe_num(s.get('dcs')) for s in states]
    txt = [
        f"<b>{s['abbrev']} — {s['state']}</b><br>"
        f"Total DCs ({dc_month}): {fmt_num(s.get('dcs'))}<br>"
        f"1yr: {fmt_pct(s.get('dcs_1yr_pct'))} · "
        f"3yr: {fmt_pct(s.get('dcs_3yr_pct'))} · "
        f"5yr: {fmt_pct(s.get('dcs_5yr_pct'))}"
        for s in states
    ]
    b = make_bubble(vals, txt, 'rgba(31, 78, 121, 0.75)', 'Total DCs')
    b['visible'] = True
    bubble_map['total'] = len(traces)
    traces.append(b)

    # 1yr/3yr/5yr % change bubbles
    for window in ['1yr', '3yr', '5yr']:
        field = f'dcs_{window}_pct'
        vals = [safe_num(s.get(field)) for s in states]
        txt = [
            f"<b>{s['abbrev']} — {s['state']}</b><br>"
            f"DC {window} Change: {fmt_pct(s.get(field))}<br>"
            f"Current ({dc_month}): {fmt_num(s.get('dcs'))}<br>"
            f"{window} ago: {fmt_num(s.get(f'dcs_{window}_ago'))}"
            for s in states
        ]
        bubble_map[window] = len(traces)
        traces.append(make_bubble(vals, txt, 'rgba(193, 39, 45, 0.75)', f'DC {window} Change'))

    # ════════════════════════════════════════════
    # LAYOUT
    # ════════════════════════════════════════════
    layout = {
        'title': {
            'text': f'<b>U.S. Data Center Landscape</b><br>'
                    f'<span style="font-size:13px;color:#666">DC Counts as of {dc_month} · Updated {updated}</span>',
            'x': 0.5,
            'font': {'size': 22, 'family': 'Georgia, serif'},
        },
        'geo': {
            'scope': 'usa',
            'projection': {'type': 'albers usa'},
            'showlakes': True,
            'lakecolor': 'rgb(220,230,240)',
            'bgcolor': 'rgba(0,0,0,0)',
            'showland': True,
            'landcolor': 'rgb(248,248,245)',
            'subunitcolor': 'rgb(180,180,180)',
        },
        'margin': {'l': 10, 'r': 10, 't': 100, 'b': 10},
        'paper_bgcolor': '#ffffff',
    }

    # ════════════════════════════════════════════
    # HTML
    # ════════════════════════════════════════════
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>U.S. Data Center Map</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: Georgia, 'Times New Roman', serif; background: #f5f5f0; color: #333; }}
.controls {{
    display: flex; flex-wrap: wrap; gap: 20px;
    padding: 14px 24px;
    background: #ffffff;
    border-bottom: 1px solid #e0e0e0;
    box-shadow: 0 2px 4px rgba(0,0,0,0.04);
}}
.control-group {{ display: flex; flex-direction: column; gap: 6px; }}
.control-group label {{
    font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em;
    color: #666; font-weight: bold; font-family: 'Helvetica Neue', sans-serif;
}}
.control-group select {{
    padding: 7px 12px; font-size: 13px; font-family: Georgia, serif;
    border: 1px solid #c0c0c0; border-radius: 4px; background: #fafafa;
    cursor: pointer; min-width: 180px;
}}
.control-group select:hover {{ background: #f0f0f0; }}
.control-group select:disabled {{ background: #ececec; color: #aaa; cursor: not-allowed; }}
.control-group.bubble-group {{
    border-left: 2px solid #1f4e79; padding-left: 18px; margin-left: 6px;
}}
.control-group.bubble-group label {{ color: #1f4e79; }}
#map {{ width: 100%; height: 78vh; }}
.footer {{
    text-align: center; padding: 14px 20px; font-size: 12px;
    color: #888; border-top: 1px solid #ddd; background: #fff;
}}
.footer a {{ color: #2c5282; text-decoration: none; }}
.footer a:hover {{ text-decoration: underline; }}
</style>
</head>
<body>

<div class="controls">
    <div class="control-group">
        <label for="layer-select">Map Layer</label>
        <select id="layer-select">
            <option value="tax">Tax Incentive Programs</option>
            <option value="regs">Total Regulations</option>
            <option value="per_capita">DCs Per Capita</option>
            <option value="rank">State Rankings</option>
            <option value="population">Population</option>
            <option value="electricity">Electricity Rates</option>
            <option value="employment">Employment</option>
        </select>
    </div>

    <div class="control-group">
        <label for="sector-select">Sector / Category</label>
        <select id="sector-select" disabled>
            <option value="none">—</option>
        </select>
    </div>

    <div class="control-group">
        <label for="metric-select">Metric</label>
        <select id="metric-select" disabled>
            <option value="none">—</option>
        </select>
    </div>

    <div class="control-group bubble-group">
        <label for="bubble-select">Bubble Overlay (DC Counts)</label>
        <select id="bubble-select">
            <option value="total">Total Data Centers</option>
            <option value="1yr">1-Year % Change</option>
            <option value="3yr">3-Year % Change</option>
            <option value="5yr">5-Year % Change</option>
            <option value="off">Hide Bubbles</option>
        </select>
    </div>
</div>

<div id="map"></div>

<div class="footer">
<b>Sources:</b>
<a href="https://www.eia.gov/electricity/monthly/" target="_blank">EIA EPM 5.6.A</a> ·
<a href="https://www.bls.gov/cew/" target="_blank">BLS QCEW</a> ·
<a href="https://www.census.gov/programs-surveys/popest.html" target="_blank">Census PEP</a> ·
<a href="https://www.quantgov.org/state-regdata" target="_blank">QuantGov</a>
<br>DC counts: publicly listed operational facilities. Use dropdowns to switch layers and bubble metrics.
<br>Last updated: {updated}
</div>

<script>
const TRACES = {json.dumps(traces)};
const TRACE_MAP = {json.dumps(trace_map)};
const BUBBLE_MAP = {json.dumps(bubble_map)};
const LAYOUT = {json.dumps(layout)};

const SECTOR_OPTIONS = {{
    electricity: [
        {{value: 'res', label: 'Residential'}},
        {{value: 'com', label: 'Commercial'}},
        {{value: 'ind', label: 'Industrial'}},
    ],
    employment: [
        {{value: 'dc_ops', label: 'Data Center Operations (NAICS 518210)'}},
        {{value: 'comp_sys', label: 'Computer Systems Design (NAICS 5415)'}},
        {{value: 'nonres_build', label: 'Non-Residential Builders (NAICS 236220)'}},
    ],
}};

const METRIC_OPTIONS = {{
    electricity: [
        {{value: 'current', label: 'Current Rate'}},
        {{value: '1yr', label: '1-Year % Change'}},
        {{value: '3yr', label: '3-Year % Change'}},
        {{value: '5yr', label: '5-Year % Change'}},
    ],
    employment: [
        {{value: 'current', label: 'Current Employment'}},
        {{value: '1yr', label: '1-Year % Change'}},
        {{value: '3yr', label: '3-Year % Change'}},
        {{value: '5yr', label: '5-Year % Change'}},
    ],
    population: [
        {{value: 'current', label: 'Current Population'}},
        {{value: '1yr', label: '1-Year % Change'}},
        {{value: '3yr', label: '3-Year % Change'}},
        {{value: '5yr', label: '5-Year % Change'}},
    ],
}};

Plotly.newPlot('map', TRACES, LAYOUT, {{responsive: true, displayModeBar: true}});

const layerSel = document.getElementById('layer-select');
const sectorSel = document.getElementById('sector-select');
const metricSel = document.getElementById('metric-select');
const bubbleSel = document.getElementById('bubble-select');

function populateSelect(selectEl, options) {{
    selectEl.innerHTML = '';
    options.forEach(opt => {{
        const o = document.createElement('option');
        o.value = opt.value;
        o.textContent = opt.label;
        selectEl.appendChild(o);
    }});
}}

function updateMap() {{
    const layer = layerSel.value;
    const sector = sectorSel.value;
    const metric = metricSel.value;
    const bubble = bubbleSel.value;

    let layerTraceIdx = null;
    if (TRACE_MAP[layer]) {{
        if (TRACE_MAP[layer][sector] && TRACE_MAP[layer][sector][metric] !== undefined) {{
            layerTraceIdx = TRACE_MAP[layer][sector][metric];
        }} else if (TRACE_MAP[layer]['none'] && TRACE_MAP[layer]['none']['none'] !== undefined) {{
            layerTraceIdx = TRACE_MAP[layer]['none']['none'];
        }} else if (TRACE_MAP[layer]['none'] && TRACE_MAP[layer]['none'][metric] !== undefined) {{
            layerTraceIdx = TRACE_MAP[layer]['none'][metric];
        }}
    }}

    let bubbleTraceIdx = null;
    if (bubble !== 'off' && BUBBLE_MAP[bubble] !== undefined) {{
        bubbleTraceIdx = BUBBLE_MAP[bubble];
    }}

    const visibility = TRACES.map((t, i) => {{
        if (i === layerTraceIdx) return true;
        if (i === bubbleTraceIdx) return true;
        return false;
    }});
    const showscale = TRACES.map((t, i) => i === layerTraceIdx);

    Plotly.restyle('map', {{ visible: visibility, showscale: showscale }});
}}

function onLayerChange() {{
    const layer = layerSel.value;
    const hasSectors = SECTOR_OPTIONS[layer];
    const hasMetrics = METRIC_OPTIONS[layer];

    if (hasSectors) {{
        sectorSel.disabled = false;
        populateSelect(sectorSel, hasSectors);
    }} else {{
        sectorSel.disabled = true;
        sectorSel.innerHTML = '<option value="none">—</option>';
    }}

    if (hasMetrics) {{
        metricSel.disabled = false;
        populateSelect(metricSel, hasMetrics);
    }} else {{
        metricSel.disabled = true;
        metricSel.innerHTML = '<option value="none">—</option>';
    }}

    updateMap();
}}

layerSel.addEventListener('change', onLayerChange);
sectorSel.addEventListener('change', updateMap);
metricSel.addEventListener('change', updateMap);
bubbleSel.addEventListener('change', updateMap);

onLayerChange();
</script>
</body>
</html>"""

    with open('index.html', 'w') as f:
        f.write(html)

    print(f"  index.html: {len(html):,} bytes, {len(traces)} traces")
    print(f"  Layers: {list(trace_map.keys())}")


if __name__ == '__main__':
    build()
