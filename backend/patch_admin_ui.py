# -*- coding: utf-8 -*-
"""Atomic, anchor-based patch that adds the in-app Admin panel to the prod index.html.
Checks md5 (abort if metrics-chat pushed), requires every anchor to match EXACTLY once,
applies all-or-nothing, backs up, then writes. Additive — all insertions live in the
profile/nav 'safe corridors', never the metrics-chat zones."""
import sys, time, hashlib, shutil

F = "/opt/screener/backend/static/index.html"
EXPECT_MD5 = "bff2181b64cff05e522a0b4d768d6621"

orig = open(F, encoding="utf-8").read()
cur = hashlib.md5(orig.encode("utf-8")).hexdigest()
if cur != EXPECT_MD5:
    print("MD5 CHANGED:", cur, "!= expected", EXPECT_MD5, "- metrics-chat pushed; ABORT (re-recon).")
    sys.exit(2)

REPL = []

# 1) dropdown "Админ" item (hidden by default; shown for is_admin) before logout
REPL.append(("dropdown-btn",
'''          <button type="button" class="tb-profile-item tb-profile-item--logout" id="tb-profile-menu-logout">''',
'''          <button type="button" class="tb-profile-item" id="tb-profile-menu-admin" style="display:none">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M12 2l8 4v6c0 5-3.5 8-8 10-4.5-2-8-5-8-10V6z"/></svg>
            <span>Админ</span>
          </button>
          <button type="button" class="tb-profile-item tb-profile-item--logout" id="tb-profile-menu-logout">'''))

# 2) dropdown handler: route admin item to the admin tab
REPL.append(("dropdown-handler",
'''      } else if (item.id === 'tb-profile-menu-invite') {
        openProfileTab('invite');
      }''',
'''      } else if (item.id === 'tb-profile-menu-invite') {
        openProfileTab('invite');
      } else if (item.id === 'tb-profile-menu-admin') {
        openProfileTab('admin');
      }'''))

# 3) profile tab strip: add Админ tab (hidden by default)
REPL.append(("tab",
'''          <button type="button" class="pf-tab" data-pf-tab="invite" id="pf-tab-invite">Invite a friend</button>''',
'''          <button type="button" class="pf-tab" data-pf-tab="invite" id="pf-tab-invite">Invite a friend</button>
          <button type="button" class="pf-tab" data-pf-tab="admin" id="pf-tab-admin" style="display:none">Админ</button>'''))

# 4) panel markup — inserted as a sibling pf-panel right before the invite panel
REPL.append(("panel",
'''      <div class="pf-panel" id="pf-panel-invite" data-pf-panel="invite">''',
'''      <div class="pf-panel" id="pf-panel-admin" data-pf-panel="admin">
        <div class="pf-card-wrap">
          <div class="pf-card"><div class="pf-section-title">Система</div><div class="pf-rows" id="adm-system"></div></div>
          <div class="pf-card"><div class="pf-section-title">Charts / WS / Redis</div><div class="pf-rows" id="adm-charts"></div></div>
          <div class="pf-card"><div class="pf-section-title">Свежесть фидов</div><div id="adm-ingest"></div></div>
          <div class="pf-card"><div class="pf-section-title">Сервисы</div><div id="adm-services" class="adm-svc-grid"></div></div>
          <div class="pf-row" style="opacity:.6"><span id="adm-updated"></span></div>
        </div>
      </div>
      <div class="pf-panel" id="pf-panel-invite" data-pf-panel="invite">'''))

# 5) CSS
REPL.append(("css",
'''.pf-admin-block.visible { display: block; }''',
'''.pf-admin-block.visible { display: block; }
.adm-bar{display:inline-block;width:84px;height:8px;border-radius:4px;background:rgba(255,255,255,.1);vertical-align:middle;margin-left:8px;overflow:hidden}
.adm-bar-fill{height:100%;border-radius:4px}
.adm-tbl{width:100%;border-collapse:collapse;font-size:12px}
.adm-tbl th,.adm-tbl td{text-align:left;padding:3px 8px;border-bottom:1px solid rgba(255,255,255,.06)}
.adm-dot{display:inline-block;width:9px;height:9px;border-radius:50%}
.adm-dot.ok{background:#3fb950}
.adm-dot.bad{background:#e5484d}
.adm-svc-grid{display:flex;flex-wrap:wrap;gap:6px}
.adm-svc{font-size:11px;padding:3px 8px;border-radius:6px;border:1px solid rgba(255,255,255,.1)}
.adm-svc.ok{color:#3fb950;border-color:rgba(63,185,80,.4)}
.adm-svc.bad{color:#e5484d;border-color:rgba(229,72,77,.5)}'''))

# 6) JS render + auto-refresh funcs (inserted before openProfileTab)
REPL.append(("js-funcs",
'''function openProfileTab(tab) {''',
'''function admBar(p){ p=Math.max(0,Math.min(100,p||0)); var c=p>=90?'#e5484d':p>=70?'#e0a800':'#3fb950'; return '<span class="adm-bar"><span class="adm-bar-fill" style="width:'+p+'%;background:'+c+'"></span></span>'; }
function admRow(l,v){ return '<div class="pf-row"><div class="pf-k">'+l+'</div><div class="pf-v">'+v+'</div></div>'; }
async function loadAdminMetrics(){
  var sys=document.getElementById('adm-system'); if(!sys) return;
  try{
    var email=(typeof getProfileEmail==='function')?getProfileEmail():'';
    var r=await fetch('/api/admin/metrics'+(email?('?email='+encodeURIComponent(email)):''));
    var m=await r.json();
    if(m.error||m.ok===false){ sys.innerHTML=admRow('Доступ',(m.error||'forbidden')); return; }
    var sy=m.system||{};
    sys.innerHTML=admRow('CPU',(sy.cpu_pct==null?'—':sy.cpu_pct)+'%'+admBar(sy.cpu_pct))
      +admRow('RAM',(sy.mem_pct==null?'—':sy.mem_pct)+'% ('+(sy.mem_used_mb==null?'—':sy.mem_used_mb)+' MB)'+admBar(sy.mem_pct))
      +admRow('Диск /opt',(sy.disk_pct==null?'—':sy.disk_pct)+'% — своб. '+(sy.disk_free_gb==null?'—':sy.disk_free_gb)+' GB'+admBar(sy.disk_pct));
    var c=m.charts||{},w=m.ws||{},rd=m.redis||{};
    document.getElementById('adm-charts').innerHTML=
       admRow('RAM-серии',(c.ram_series==null?'—':c.ram_series)+' / '+(c.ram_series_max==null?'—':c.ram_series_max))
      +admRow('Серий в БД',(c.db_series_total==null?'—':c.db_series_total.toLocaleString('ru')))
      +admRow('charts.db',(c.charts_db_bytes==null?'—':(c.charts_db_bytes/1073741824).toFixed(2)+' GB'))
      +admRow('WAL',(c.charts_db_wal_bytes==null?'—':(c.charts_db_wal_bytes/1048576).toFixed(0)+' MB'))
      +admRow('WS (py)',(w.py_clients==null?'—':w.py_clients)+' / subs '+(w.chart_subs==null?'—':w.chart_subs))
      +admRow('Redis',(rd.connected_clients==null?'—':rd.connected_clients)+' кл., '+(rd.used_memory_mb==null?'—':rd.used_memory_mb)+' MB');
    var ing=(m.ingest&&m.ingest.exchanges)||{};
    var rows=Object.keys(ing).sort().map(function(k){ var v=ing[k]; var d=v.stalled?'<span class="adm-dot bad"></span>':'<span class="adm-dot ok"></span>'; return '<tr><td>'+k+'</td><td>'+(v.age_s==null?'—':v.age_s.toFixed(0))+'s</td><td>'+(v.closed_total==null?'—':v.closed_total)+'</td><td>'+d+'</td></tr>'; }).join('');
    document.getElementById('adm-ingest').innerHTML='<table class="adm-tbl"><tr><th>Биржа</th><th>Возраст</th><th>Баров</th><th></th></tr>'+rows+'</table>';
    var sv=m.services||{};
    document.getElementById('adm-services').innerHTML=Object.keys(sv).map(function(k){ return '<span class="adm-svc '+(sv[k]==='active'?'ok':'bad')+'">'+k+'</span>'; }).join('');
    var up=document.getElementById('adm-updated'); if(up) up.textContent='Обновлено: '+new Date((m.ts||0)*1000).toLocaleTimeString('ru');
  }catch(e){ sys.innerHTML=admRow('Ошибка сети',String(e)); }
}
var _admTimer=null;
function startAdminAutoRefresh(){ stopAdminAutoRefresh(); loadAdminMetrics(); _admTimer=setInterval(loadAdminMetrics,5000); }
function stopAdminAutoRefresh(){ if(_admTimer){ clearInterval(_admTimer); _admTimer=null; } }

function openProfileTab(tab) {'''))

# 7) gate the Админ button + tab on is_admin (reuse renderReferralUI)
REPL.append(("admin-gate",
'''function renderReferralUI() {
  const st = _referralState;''',
'''function renderReferralUI() {
  const st = _referralState;
  try {
    const _isAdm = !!(st && st.is_admin);
    const _ab = document.getElementById('tb-profile-menu-admin'); if (_ab) _ab.style.display = _isAdm ? 'flex' : 'none';
    const _at = document.getElementById('pf-tab-admin'); if (_at) _at.style.display = _isAdm ? '' : 'none';
  } catch (e) {}'''))

# 8) setProfileTab: accept the 'admin' tab + drive auto-refresh
REPL.append(("setProfileTab",
'''function setProfileTab(tab) {
  const t = tab === 'invite' ? 'invite' : 'account';
  document.querySelectorAll('.pf-tab').forEach(el => {
    el.classList.toggle('active', el.dataset.pfTab === t);
  });
  document.querySelectorAll('.pf-panel').forEach(el => {
    el.classList.toggle('active', el.dataset.pfPanel === t);
  });
  const T = TRANSLATIONS[currentLang] || TRANSLATIONS.en;
  const pfPageTitle = document.getElementById('pf-page-title');
  const pfSectionTitle = document.getElementById('pf-section-title');
  if (t === 'invite') {
    if (pfPageTitle) pfPageTitle.textContent = T.invite_friend;
    loadReferralState();
  } else {
    if (pfPageTitle) pfPageTitle.textContent = T.profile;
    if (pfSectionTitle) pfSectionTitle.textContent = T.profile;
  }
}''',
'''function setProfileTab(tab) {
  const t = (tab === 'invite' || tab === 'admin') ? tab : 'account';
  document.querySelectorAll('.pf-tab').forEach(el => {
    el.classList.toggle('active', el.dataset.pfTab === t);
  });
  document.querySelectorAll('.pf-panel').forEach(el => {
    el.classList.toggle('active', el.dataset.pfPanel === t);
  });
  const T = TRANSLATIONS[currentLang] || TRANSLATIONS.en;
  const pfPageTitle = document.getElementById('pf-page-title');
  const pfSectionTitle = document.getElementById('pf-section-title');
  if (t === 'admin') {
    if (pfPageTitle) pfPageTitle.textContent = 'Админ';
    startAdminAutoRefresh();
  } else if (t === 'invite') {
    if (pfPageTitle) pfPageTitle.textContent = T.invite_friend;
    loadReferralState();
  } else {
    if (pfPageTitle) pfPageTitle.textContent = T.profile;
    if (pfSectionTitle) pfSectionTitle.textContent = T.profile;
  }
  if (t !== 'admin') stopAdminAutoRefresh();
}'''))

# ---- verify every anchor matches exactly once on the ORIGINAL (all-or-nothing) ----
for label, old, new in REPL:
    n = orig.count(old)
    if n != 1:
        print("ANCHOR FAIL [%s]: matched %d times (need 1) — ABORT, nothing written." % (label, n))
        sys.exit(3)

src = orig
for label, old, new in REPL:
    src = src.replace(old, new, 1)

for marker in ("pf-panel-admin", "tb-profile-menu-admin", "pf-tab-admin",
               "loadAdminMetrics", "startAdminAutoRefresh", "adm-svc-grid", "/api/admin/metrics"):
    if marker not in src:
        print("SANITY FAIL: marker missing:", marker); sys.exit(4)

bak = F + ".bak.adminui." + str(int(time.time()))
shutil.copy(F, bak)
open(F, "w", encoding="utf-8").write(src)
print("PATCHED OK | backup:", bak)
print("size: %d -> %d (+%d bytes)" % (len(orig), len(src), len(src) - len(orig)))
print("new md5:", hashlib.md5(src.encode("utf-8")).hexdigest())
