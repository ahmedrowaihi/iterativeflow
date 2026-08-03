/** @internal */
export const UI = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>iterativeflow</title>
<style>
  :root { color-scheme: light dark; --bg:#fff; --fg:#111; --mut:#666; --line:#e5e5e5; --card:#fafafa; --accent:#3b6ef5; }
  @media (prefers-color-scheme: dark) { :root { --bg:#0e0e11; --fg:#eaeaea; --mut:#999; --line:#26262c; --card:#17171c; } }
  * { box-sizing: border-box; }
  body { margin:0; font:14px/1.5 ui-sans-serif,system-ui,sans-serif; background:var(--bg); color:var(--fg); }
  header { padding:16px 20px; border-bottom:1px solid var(--line); display:flex; align-items:center; gap:14px; }
  header h1 { font-size:16px; margin:0; font-weight:650; }
  .stats { display:flex; gap:8px; flex-wrap:wrap; margin-left:auto; }
  .pill { padding:3px 9px; border:1px solid var(--line); border-radius:999px; font-size:12px; color:var(--mut); }
  .pill b { color:var(--fg); }
  main { display:grid; grid-template-columns:1fr; gap:0; }
  .bar { padding:10px 20px; display:flex; gap:8px; align-items:center; border-bottom:1px solid var(--line); flex-wrap:wrap; }
  select,button { font:inherit; background:var(--card); color:var(--fg); border:1px solid var(--line); border-radius:7px; padding:5px 10px; cursor:pointer; }
  button.act { background:var(--accent); color:#fff; border-color:transparent; }
  table { width:100%; border-collapse:collapse; }
  th,td { text-align:left; padding:9px 20px; border-bottom:1px solid var(--line); font-size:13px; }
  th { color:var(--mut); font-weight:500; }
  tr.run { cursor:pointer; }
  tr.run:hover td { background:var(--card); }
  code { font:12px ui-monospace,monospace; color:var(--mut); }
  .s { font-size:11px; padding:2px 7px; border-radius:999px; border:1px solid var(--line); }
  .s.done{color:#1a8f4a} .s.failed{color:#d33} .s.canceled{color:#996} .s.running,.s.pending{color:var(--accent)}
  .s.sleeping,.s.awaiting_signal,.s.awaiting_child,.s.retrying{color:#c80}
  #drawer { position:fixed; top:0; right:0; height:100%; width:min(560px,92vw); background:var(--bg); border-left:1px solid var(--line); transform:translateX(100%); transition:transform .15s; overflow:auto; padding:20px; }
  #drawer.open { transform:none; box-shadow:-8px 0 30px rgba(0,0,0,.15); }
  #drawer h2 { font-size:14px; margin:0 0 4px; }
  .row { display:flex; gap:8px; margin:14px 0; flex-wrap:wrap; }
  .kv { display:grid; grid-template-columns:110px 1fr; gap:4px 12px; font-size:13px; }
  .kv div:nth-child(odd){ color:var(--mut); }
  .timeline li { list-style:none; padding:6px 0; border-bottom:1px dashed var(--line); font-size:12px; display:flex; gap:10px; }
  .timeline { padding:0; margin:8px 0; }
  pre { background:var(--card); border:1px solid var(--line); border-radius:7px; padding:10px; overflow:auto; font-size:12px; }
  .x { margin-left:auto; }
</style>
</head>
<body>
<header>
  <h1>iterativeflow</h1>
  <div class="stats" id="stats"></div>
</header>
<div class="bar">
  <label>Status
    <select id="status">
      <option value="">all</option>
      <option>pending</option><option>running</option><option>sleeping</option>
      <option>awaiting_signal</option><option>awaiting_child</option><option>retrying</option>
      <option>done</option><option>failed</option><option>canceled</option>
    </select>
  </label>
  <button id="refresh">Refresh</button>
</div>
<main><table><thead><tr><th>Run</th><th>Flow</th><th>Status</th><th>Attempts</th></tr></thead>
<tbody id="rows"></tbody></table></main>
<div id="drawer"></div>
<script>
const api = (p,o) => fetch("api"+p,o).then(r=>r.json());
const esc = s => String(s).replace(/[&<>]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;"}[c]));
const badge = s => '<span class="s '+s+'">'+s+'</span>';

async function loadStats(){
  const h = await api("/health");
  document.getElementById("stats").innerHTML = Object.entries(h)
    .filter(([,n])=>n>0).map(([k,n])=>'<span class="pill"><b>'+n+'</b> '+k+'</span>').join("")
    || '<span class="pill">no runs</span>';
}
async function loadRuns(){
  const st = document.getElementById("status").value;
  const q = st ? "?status="+st : "";
  const { runs } = await api("/runs"+q);
  document.getElementById("rows").innerHTML = runs.map(r =>
    '<tr class="run" data-id="'+esc(r.id)+'"><td><code>'+esc(r.id.slice(0,8))+'</code></td>'+
    '<td>'+esc(r.name)+' v'+r.version+'</td><td>'+badge(r.status)+'</td><td>'+r.attempts+'</td></tr>'
  ).join("") || '<tr><td colspan="4" style="color:var(--mut)">no runs</td></tr>';
  for (const tr of document.querySelectorAll("tr.run"))
    tr.onclick = () => openRun(tr.dataset.id);
}
async function openRun(id){
  const d = await api("/runs/"+encodeURIComponent(id));
  const dr = document.getElementById("drawer");
  const steps = d.steps.map(s=>'<li><code>'+esc(s.cursorKey)+'</code> '+badge(s.status)+
    ' <span style="color:var(--mut)">'+esc(JSON.stringify(s.result ?? s.error ?? "")).slice(0,80)+'</span></li>').join("");
  const events = d.events.map(e=>'<li><code>'+esc(new Date(e.at).toISOString().slice(11,19))+'</code> '+esc(e.type)+'</li>').join("");
  dr.innerHTML =
    '<div class="row"><h2>'+esc(d.run.name)+' v'+d.run.version+'</h2><button class="x" onclick="closeRun()">close</button></div>'+
    '<div class="kv"><div>id</div><div><code>'+esc(d.run.id)+'</code></div>'+
    '<div>status</div><div>'+badge(d.run.status)+'</div>'+
    '<div>attempts</div><div>'+d.run.attempts+'</div>'+
    (d.run.tags?.length?'<div>tags</div><div>'+esc(d.run.tags.join(", "))+'</div>':'')+'</div>'+
    '<div class="row">'+
      '<button class="act" onclick="act(\\''+esc(id)+'\\',\\'retry\\')">Retry</button>'+
      '<button onclick="act(\\''+esc(id)+'\\',\\'cancel\\')">Cancel</button>'+
    '</div>'+
    (steps?'<h2>Steps</h2><ul class="timeline">'+steps+'</ul>':'')+
    (d.run.input!==undefined?'<h2>Input</h2><pre>'+esc(JSON.stringify(d.run.input,null,2))+'</pre>':'')+
    (d.run.output!==undefined?'<h2>Output</h2><pre>'+esc(JSON.stringify(d.run.output,null,2))+'</pre>':'')+
    (d.run.error?'<h2>Error</h2><pre>'+esc(JSON.stringify(d.run.error,null,2))+'</pre>':'')+
    (events?'<h2>Events</h2><ul class="timeline">'+events+'</ul>':'');
  dr.classList.add("open");
}
function closeRun(){ document.getElementById("drawer").classList.remove("open"); }
async function act(id,kind){ await api("/runs/"+encodeURIComponent(id)+"/"+kind,{method:"POST"}); await refresh(); openRun(id); }
async function refresh(){ await Promise.all([loadStats(),loadRuns()]); }
document.getElementById("refresh").onclick = refresh;
document.getElementById("status").onchange = loadRuns;
refresh();
setInterval(refresh, 4000);
</script>
</body>
</html>`;
