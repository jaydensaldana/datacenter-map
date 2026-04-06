"""
build_map.py — Generate interactive Plotly choropleth from data/combined.json
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

def fmt_pct(v):
    if v is None: return 'N/A'
    return f"{v*100:+.1f}%"

def build():
    print("Building map...")
    with open('data/combined.json') as f:
        data = json.load(f)

    states = data['states']
    updated = data.get('generated','')[:16].replace('T',' ')
    dc_month = data.get('latest_dc_month','')

    # Sort states by rank for consistent ordering
    states.sort(key=lambda s: s.get('rank_total') or 999)
    abbrevs = [s['abbrev'] for s in states]

    def choro(z, text, colorscale, bar_title, visible=False):
        return {
            'type':'choropleth','locationmode':'USA-states',
            'locations':abbrevs,'z':z,'text':text,'hoverinfo':'text',
            'colorscale':colorscale,
            'colorbar':{'title':bar_title,'x':1.0,'len':0.5},
            'visible':visible,
        }

    traces = []

    # L0: Tax Incentives
    z = [2 if s['tax_incentive']=='Y' else 1 if 'local' in s['tax_incentive'].lower() else 0 for s in states]
    txt = [f"<b>{s['state']}</b><br>Incentive: {s['tax_incentive']}<br>"
           f"Type: {s['incentive_type']}<br>Min Investment: {s['min_investment']}" for s in states]
    traces.append(choro(z, txt, [[0,'#e74c3c'],[0.5,'#f39c12'],[1,'#27ae60']], 'Incentive', True))

    # L1: DC Count YoY Change (1yr)
    z = [(s.get('dcs_1yr_pct') or 0)*100 for s in states]
    txt = [f"<b>{s['state']}</b><br>Current DCs: {s['dcs'] or 'N/A'} ({dc_month})<br>"
           f"1yr ago: {s.get('dcs_1yr_ago') or 'N/A'} → {fmt_pct(s.get('dcs_1yr_pct'))}<br>"
           f"3yr: {fmt_pct(s.get('dcs_3yr_pct'))} · 5yr: {fmt_pct(s.get('dcs_5yr_pct'))}" for s in states]
    traces.append(choro(z, txt, 'RdYlGn', '1yr % Chg'))

    # L2: Regulations
    z = [s.get('total_regulations') or 0 for s in states]
    txt = [f"<b>{s['state']}</b><br>Regulations: {s['total_regulations']:,}" if s.get('total_regulations')
           else f"<b>{s['state']}</b><br>N/A" for s in states]
    traces.append(choro(z, txt, 'Reds', 'Regulations'))

    # L3: DCs Per Capita
    z = [s.get('dcs_per_100k') or 0 for s in states]
    txt = [f"<b>{s['state']}</b><br>DCs per 100K: {s['dcs_per_100k']}<br>"
           f"Total DCs: {s['dcs'] or 0}<br>Pop: {s['population']:,}" if s.get('population')
           else f"<b>{s['state']}</b><br>N/A" for s in states]
    traces.append(choro(z, txt, 'Viridis', 'Per 100K'))

    # L4: Population Growth (1yr)
    z = [(s.get('pop_1yr_pct') or 0)*100 for s in states]
    txt = [f"<b>{s['state']}</b><br>Population: {s.get('population') or 'N/A':,}<br>"
           f"1yr: {fmt_pct(s.get('pop_1yr_pct'))}<br>"
           f"3yr: {fmt_pct(s.get('pop_3yr_pct'))} · 5yr: {fmt_pct(s.get('pop_5yr_pct'))}"
           if s.get('population') else f"<b>{s['state']}</b><br>N/A" for s in states]
    traces.append(choro(z, txt, 'Bluered', '1yr Pop %'))

    # L5: Rankings (total)
    z = [s.get('rank_total') or 50 for s in states]
    txt = [f"<b>{s['state']}</b><br>Rank (Total): #{s.get('rank_total')}<br>"
           f"Rank (Per Capita): #{s.get('rank_per_capita')}<br>"
           f"DCs: {s['dcs'] or 0} · Per 100K: {s.get('dcs_per_100k') or 'N/A'}" for s in states]
    traces.append(choro(z, txt, 'YlGnBu_r', 'Rank'))

    # Bubble overlay: DC count
    bubble = {
        'type':'scattergeo','locationmode':'USA-states',
        'lat':[CENTROIDS.get(s['abbrev'],(0,0))[0] for s in states],
        'lon':[CENTROIDS.get(s['abbrev'],(0,0))[1] for s in states],
        'text':[f"<b>{s['abbrev']}</b><br>{s['dcs'] or 0} DCs" for s in states],
        'hoverinfo':'text',
        'marker':{
            'size':[max((s['dcs'] or 0)*0.08, 3) for s in states],
            'color':'rgba(52,73,94,0.6)',
            'line':{'color':'rgba(52,73,94,1)','width':1},
            'sizemode':'area',
        },
        'visible':True,
    }
    traces.append(bubble)

    layer_names = [
        'Tax Incentive Programs',
        'DC Count YoY Change',
        'Total Regulations',
        'DCs Per Capita',
        'Population Growth',
        'State Rankings',
    ]

    n_choro = len(layer_names)
    buttons = []
    for i, name in enumerate(layer_names):
        vis = [False]*(n_choro+1)
        vis[i] = True; vis[n_choro] = True
        buttons.append({'label':name,'method':'update','args':[{'visible':vis}]})

    layout = {
        'title':{
            'text':f'<b>U.S. Data Center Landscape</b><br>'
                   f'<span style="font-size:13px;color:#666">Data as of {dc_month} · '
                   f'Updated {updated}</span>',
            'x':0.5,'font':{'size':20,'family':'Georgia, serif'},
        },
        'geo':{'scope':'usa','projection':{'type':'albers usa'},
               'showlakes':True,'lakecolor':'rgb(220,230,240)','bgcolor':'rgba(0,0,0,0)'},
        'updatemenus':[{
            'buttons':buttons,'direction':'down','showactive':True,
            'x':0.01,'y':0.95,'xanchor':'left','yanchor':'top',
            'bgcolor':'#f8f9fa','bordercolor':'#dee2e6','font':{'size':12},
        }],
        'margin':{'l':10,'r':10,'t':80,'b':10},
        'paper_bgcolor':'#ffffff',
    }

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>U.S. Data Center Map</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:Georgia,'Times New Roman',serif;background:#f5f5f0}}
#map{{width:100%;height:85vh}}
.footer{{text-align:center;padding:12px;font-size:12px;color:#888;border-top:1px solid #ddd;background:#fff}}
.footer a{{color:#2c5282;text-decoration:none}}
.footer a:hover{{text-decoration:underline}}
</style>
</head>
<body>
<div id="map"></div>
<div class="footer">
<b>Sources:</b>
<a href="https://www.eia.gov/electricity/data/state/" target="_blank">EIA</a> ·
<a href="https://www.bls.gov/cew/" target="_blank">BLS QCEW</a> ·
<a href="https://www.census.gov/programs-surveys/popest.html" target="_blank">Census</a> ·
<a href="https://www.quantgov.org/state-regdata" target="_blank">QuantGov</a>
<br>DC counts: publicly listed operational facilities. Blue text = manual input.
Hover over states for details. Use dropdown to switch layers.
<br>Last updated: {updated}
</div>
<script>
Plotly.newPlot('map',{json.dumps(traces)},{json.dumps(layout)},{{responsive:true}});
</script>
</body>
</html>"""

    with open('index.html','w') as f: f.write(html)
    print(f"  index.html: {len(html):,} bytes, {n_choro} layers + bubbles")

if __name__ == '__main__':
    build()
