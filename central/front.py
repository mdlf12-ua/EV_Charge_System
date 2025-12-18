import os
import requests
from flask import Flask, jsonify, Response

API_CENTRAL_BASE = os.getenv("API_CENTRAL_BASE", "https://api_central:8000")
CA_CERT = os.getenv("CA_CERT", "/app/certs/certificado_CA.crt")
FRONT_PORT = int(os.getenv("FRONT_PORT", 8080))

app = Flask(__name__)

@app.get("/api/cps")
def proxy_cps():
    if API_CENTRAL_BASE.lower().startswith("https://"):
        verify = CA_CERT if os.path.exists(CA_CERT) else True
    else:
        verify = True

    r = requests.get(f"{API_CENTRAL_BASE}/cps", timeout=5, verify=verify)
    return Response(
        r.content,
        status=r.status_code,
        content_type=r.headers.get("content-type", "application/json")
    )


HTML = r"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>EV Charging - Monitor</title>
  <style>
    :root{
      --bg:#0b1020; --card:#121a33; --card2:#0f1730;
      --text:#e8ecff; --muted:#aab2d5;
      --ok:#34d399; --warn:#fbbf24; --bad:#fb7185; --info:#60a5fa;
      --border: rgba(255,255,255,.10);
    }
    *{box-sizing:border-box}
    body{
      margin:0; font-family: ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial;
      background: radial-gradient(1200px 600px at 20% 0%, #1b2a6a 0%, rgba(27,42,106,0) 60%),
                  radial-gradient(900px 600px at 90% 10%, #3b1b6a 0%, rgba(59,27,106,0) 55%),
                  var(--bg);
      color:var(--text);
    }
    header{
      position: sticky; top:0; z-index:10;
      backdrop-filter: blur(10px);
      background: rgba(11,16,32,.65);
      border-bottom: 1px solid var(--border);
    }
    .wrap{max-width:1100px; margin:0 auto; padding:18px;}
    .row{display:flex; gap:14px; flex-wrap:wrap; align-items:center; justify-content:space-between;}
    h1{font-size:18px; margin:0; letter-spacing:.2px;}
    .subtitle{color:var(--muted); font-size:13px; margin-top:2px}
    .pill{
      display:inline-flex; align-items:center; gap:8px;
      padding:8px 10px; border:1px solid var(--border); border-radius:999px;
      background: rgba(255,255,255,.03);
      color:var(--muted); font-size:13px;
    }
    .dot{width:10px; height:10px; border-radius:50%;}
    .controls{
      display:flex; gap:10px; flex-wrap:wrap; align-items:center;
    }
    input{
      background: rgba(255,255,255,.04);
      border: 1px solid var(--border);
      color: var(--text);
      padding:10px 12px; border-radius:12px;
      outline:none; width:260px;
    }
    button{
      background: linear-gradient(180deg, rgba(96,165,250,.25), rgba(96,165,250,.12));
      border: 1px solid rgba(96,165,250,.35);
      color: var(--text);
      padding:10px 12px; border-radius:12px;
      cursor:pointer;
    }
    button:hover{filter:brightness(1.1)}
    main{padding:18px;}
    .grid{
      display:grid; grid-template-columns: repeat(12, 1fr);
      gap:14px;
    }
    .card{
      grid-column: span 6;
      background: linear-gradient(180deg, rgba(18,26,51,.95), rgba(15,23,48,.92));
      border: 1px solid var(--border);
      border-radius: 18px;
      padding:14px 14px 12px;
      box-shadow: 0 12px 30px rgba(0,0,0,.25);
    }
    @media (max-width: 900px){ .card{grid-column: span 12;} input{width:100%} }
    .top{
      display:flex; align-items:flex-start; justify-content:space-between; gap:10px;
    }
    .id{font-weight:700; letter-spacing:.3px}
    .loc{color:var(--muted); margin-top:4px; font-size:13px}
    .badges{display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end}
    .badge{
      display:inline-flex; align-items:center; gap:7px;
      padding:7px 10px; border-radius:999px;
      border:1px solid var(--border);
      font-size:12px; color:var(--text);
      background: rgba(255,255,255,.04);
      white-space:nowrap;
    }
    .badge .dot{width:8px; height:8px}
    .kpis{
      display:grid; grid-template-columns: repeat(3, 1fr); gap:10px;
      margin-top:12px;
    }
    .kpi{
      background: rgba(255,255,255,.03);
      border:1px solid var(--border);
      border-radius:14px;
      padding:10px;
    }
    .kpi .label{color:var(--muted); font-size:12px}
    .kpi .val{font-size:16px; font-weight:650; margin-top:6px}
    .footer{
      margin-top:10px; color:var(--muted); font-size:12px;
      display:flex; justify-content:space-between; gap:10px; flex-wrap:wrap;
    }
    .muted{color:var(--muted)}
    .error{
      margin-top:12px; padding:12px 14px; border-radius:14px;
      border:1px solid rgba(251,113,133,.35);
      background: rgba(251,113,133,.10);
      color: #ffd7df;
    }
    table{
      width:100%; border-collapse: collapse; margin-top:12px; overflow:hidden;
      border:1px solid var(--border); border-radius:14px;
    }
    th, td{padding:10px 10px; border-bottom:1px solid var(--border); font-size:13px}
    th{color:var(--muted); text-align:left; font-weight:600; background: rgba(255,255,255,.03)}
    tr:last-child td{border-bottom:none}
  </style>
</head>
<body>
<header>
  <div class="wrap">
    <div class="row">
      <div>
        <h1>⚡ EV Charging · Monitorización</h1>
        <div class="subtitle">Vista en tiempo real de Charging Points (CPs) desde API_Central</div>
      </div>
      <div class="controls">
        <input id="q" placeholder="Buscar por ID / ciudad / estado..." />
        <button id="refresh">Actualizar</button>
        <span class="pill"><span id="apiDot" class="dot" style="background:var(--warn)"></span><span id="apiStatus">Conectando…</span></span>
      </div>
    </div>
  </div>
</header>

<main class="wrap">
  <div class="row" style="margin-bottom:12px">
    <span class="pill">CPs: <b id="count" style="color:var(--text)">0</b></span>
    <span class="pill">Auto-refresh: <b id="auto" style="color:var(--text)">ON</b> (cada <span id="sec">4</span>s)</span>
  </div>

  <div id="err" class="error" style="display:none"></div>

  <div id="grid" class="grid"></div>

  <div style="margin-top:18px" class="muted">
    Tip: abre <b>/health</b> para comprobar API. Aquí el front llama a <b>/api/cps</b> (proxy).
  </div>
</main>

<script>
  const grid = document.getElementById('grid');
  const q = document.getElementById('q');
  const err = document.getElementById('err');
  const count = document.getElementById('count');
  const apiStatus = document.getElementById('apiStatus');
  const apiDot = document.getElementById('apiDot');

  const POLL_SECONDS = 4;
  document.getElementById('sec').textContent = POLL_SECONDS;

  function dotColor(kind){
    if(kind==='ok') return 'var(--ok)';
    if(kind==='bad') return 'var(--bad)';
    if(kind==='warn') return 'var(--warn)';
    return 'var(--info)';
  }

  function badge(label, kind){
    return `
      <span class="badge">
        <span class="dot" style="background:${dotColor(kind)}"></span>
        ${label}
      </span>
    `;
  }

  function estadoKind(estado){
    estado = (estado||'').toUpperCase();
    if(estado === 'ACTIVADO') return 'ok';
    if(estado === 'SUMINISTRANDO' || estado === 'AUTORIZADO') return 'info';
    if(estado === 'PARADO') return 'warn';
    if(estado === 'AVERIADO' || estado === 'DESCONECTADO') return 'bad';
    return 'info';
  }

  function fmt(n){
    if(n === null || n === undefined) return '—';
    const x = Number(n);
    if(Number.isNaN(x)) return String(n);
    return x.toFixed(2);
  }

  function render(cps){
    const query = (q.value||'').trim().toLowerCase();
    let filtered = cps;

    if(query){
      filtered = cps.filter(cp => {
        const s = [
          cp.ID, cp.Ubicacion, cp.ESTADO,
          cp.CONDUCTOR_ID
        ].join(' ').toLowerCase();
        return s.includes(query);
      });
    }

    count.textContent = filtered.length;

    grid.innerHTML = filtered.map(cp => {
      const estado = (cp.ESTADO || 'DESCONOCIDO').toUpperCase();
      const alerta = Number(cp.ALERTA_METEO || 0) === 1;

      const meteoBadge = alerta
        ? badge('ALERTA METEO', 'bad')
        : badge('Meteo OK', 'ok');

      return `
        <div class="card">
          <div class="top">
            <div>
              <div class="id">${cp.ID || '—'}</div>
              <div class="loc">📍 ${cp.Ubicacion || 'Sin ubicación'}</div>
            </div>
            <div class="badges">
              ${badge('Estado: ' + estado, estadoKind(estado))}
              ${meteoBadge}
            </div>
          </div>

          <div class="kpis">
            <div class="kpi">
              <div class="label">Precio (€/kWh)</div>
              <div class="val">${fmt(cp.PRECIO)}</div>
            </div>
            <div class="kpi">
              <div class="label">Consumo (kWh)</div>
              <div class="val">${fmt(cp.CONSUMO_KW)}</div>
            </div>
            <div class="kpi">
              <div class="label">Importe (€)</div>
              <div class="val">${fmt(cp.IMPORTE_EU)}</div>
            </div>
          </div>

          <div class="footer">
            <span>👤 Conductor: <b>${cp.CONDUCTOR_ID || '—'}</b></span>
            <span class="muted">Fuente: API_Central</span>
          </div>
        </div>
      `;
    }).join('');
  }

  async function fetchCps(){
    err.style.display = 'none';
    try{
      const r = await fetch('/api/cps', {cache:'no-store'});
      if(!r.ok) throw new Error('HTTP ' + r.status);
      const data = await r.json();
      apiStatus.textContent = 'API OK';
      apiDot.style.background = dotColor('ok');
      render(Array.isArray(data) ? data : []);
    }catch(e){
      apiStatus.textContent = 'API ERROR';
      apiDot.style.background = dotColor('bad');
      err.textContent = 'No puedo obtener /api/cps: ' + e;
      err.style.display = 'block';
      grid.innerHTML = '';
      count.textContent = '0';
    }
  }

  document.getElementById('refresh').addEventListener('click', fetchCps);
  q.addEventListener('input', () => fetchCps());

  fetchCps();
  setInterval(fetchCps, POLL_SECONDS * 1000);
</script>
</body>
</html>
"""

@app.get("/")
def index():
    return Response(HTML, mimetype="text/html")

@app.get("/health")
def front_health():
    return jsonify({"status": "ok", "service": "EV_Front"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=FRONT_PORT, debug=False)
