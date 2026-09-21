"""Idempotently inject the auth + role-gating UI into backend/static/index.html.

WHY a patch script: index.html is periodically redeployed/overwritten by the
metrics-chat workflow (clobber hazard — see memory). Direct edits get wiped, so
the auth UI lives here and is re-applied after any index.html redeploy:

    python backend/patch_auth_ui.py

Safe to run repeatedly: each patch is skipped if already present. Pairs with the
backend (backend/auth/ + /api/auth/* + /api/admin/* in main.py) and admin.html.

Free tier: Binance Futures only; sorts natr__1m / volume__1d / chg_5m only;
filters & settings (screener Settings menu, chart settings, params panel) = PRO.
Locked sections (densities/splash/arbitrage) render behind an undismissable PRO
overlay. Guests: any interaction → auth modal.
"""
from pathlib import Path
import sys

INDEX = Path(__file__).resolve().parent / "static" / "index.html"

# ── (name, old, new) replacement patches. Idempotent: skipped if `new` present. ──
PATCHES = []

def P(name, old, new):
    PATCHES.append((name, old, new))

# affiliate: send the stored ?ref code with registration so the signup is attributed
# to the referring partner (lifetime). Harmless on login (backend ignores extra fields).
P("register-ref",
  "body:JSON.stringify({email:email,password:password})",
  "body:JSON.stringify({email:email,password:password,ref:((function(){try{return localStorage.getItem('crypto_ref')||''}catch(e){return ''}})())})")

# 1) early-script role coercion ------------------------------------------------
P("early-coercion",
"""<body><script>
var _emb=((location.search.match(/[?&]embed=([a-z]+)/)||[])[1])||'';   /* Builder iframe embeds a single chromeless section */
var _s= _emb || localStorage.getItem('crypto_section')||'charts';
if(_s==='splash'||_s==='decorrelation')_s='charts';   /* Spike + Decorrelation sections disabled */
if(['charts','densities','screener','arbitrage','alerts','premium','listings','formations','profile'].indexOf(_s)<0)_s='charts';
document.body.classList.add('sec-'+_s);
if(_emb)document.body.classList.add('embed-mode');
if(_s==='listings'&&localStorage.getItem('crypto_ls_tab')==='delistings')document.body.classList.add('ls-tab-delistings');
if(localStorage.getItem('crypto_theme')!=='green')document.body.classList.add('theme-blue');
if(localStorage.getItem('crypto_ch_draw_collapsed')==='1')document.body.classList.add('ch-draw-collapsed');
</script>""",
"""<body><script>
/* Role-aware boot coercion (auth): non-paid users forced to Binance Futures +
   an allowed sort; guests land on the screener. /api/auth/me repairs after. */
var _emb=((location.search.match(/[?&]embed=([a-z]+)/)||[])[1])||'';   /* Builder iframe embeds a single chromeless section */
var _role=localStorage.getItem('crypto_user_role')||'guest';
var _paid=(_role==='pro'||_role==='admin');
if(!_paid){try{
  localStorage.setItem('crypto_scr_exch','binance_futures');
  localStorage.setItem('crypto_ch_exch','binance_futures');
  if(['natr__1m','volume__1d','chg_5m'].indexOf(localStorage.getItem('crypto_scr_sort'))<0)localStorage.setItem('crypto_scr_sort','natr__1m');
  if(_role==='guest'&&!_emb)localStorage.setItem('crypto_section','screener');
}catch(e){}}
var _s= _emb || localStorage.getItem('crypto_section')||'charts';
if(_s==='splash'||_s==='decorrelation')_s='charts';   /* Spike + Decorrelation sections disabled */
if(['charts','densities','screener','arbitrage','alerts','premium','listings','formations','profile'].indexOf(_s)<0)_s='charts';
if(_role==='guest'&&!_emb)_s='screener';
document.body.classList.add('sec-'+_s);
if(_emb)document.body.classList.add('embed-mode');
document.body.classList.add('plan-'+( _paid ? _role : (_role==='user'?'free':_role) ));
if(_s==='listings'&&localStorage.getItem('crypto_ls_tab')==='delistings')document.body.classList.add('ls-tab-delistings');
if(localStorage.getItem('crypto_theme')!=='green')document.body.classList.add('theme-blue');
if(localStorage.getItem('crypto_ch_draw_collapsed')==='1')document.body.classList.add('ch-draw-collapsed');
</script>""")

# 2) CSS block (before </head>) ------------------------------------------------
AUTH_CSS = """<style id="auth-styles">
/* Auth + role gating */
.tb-login-btn{ display:none; align-items:center; gap:7px; height:34px; padding:0 16px; border-radius:9px; border:1px solid var(--accent); cursor:pointer; background:transparent; color:var(--accent); font:600 13px/1 inherit; letter-spacing:-0.01em; white-space:nowrap; transition:opacity .12s ease; }
.tb-login-btn:hover{ opacity:.78; }
.tb-login-btn svg{ width:14px; height:14px; }
body.plan-guest .tb-login-btn{ display:inline-flex; }
body.plan-guest #tb-profile-wrap, body.plan-guest #ws-dropdown, body.plan-guest #tb-role-chip{ display:none !important; }
.tb-role-chip{ display:none; align-items:center; height:23px; padding:0 9px; border-radius:6px; font:700 10px/1 inherit; letter-spacing:.06em; text-transform:uppercase; white-space:nowrap; }
body.plan-pro .tb-role-chip{ display:inline-flex; background:var(--accent-dim); color:var(--accent); border:1px solid var(--accent); }
body.plan-admin .tb-role-chip{ display:inline-flex; background:rgba(139,92,246,.16); color:#c4a3ff; border:1px solid #8b5cf6; }
body.plan-free .tb-role-chip, body.plan-guest .tb-role-chip{ display:none; }
#auth-overlay{ display:none; position:fixed; inset:0; z-index:100050; background:rgba(2,7,4,.66); backdrop-filter:blur(7px); -webkit-backdrop-filter:blur(7px); align-items:center; justify-content:center; padding:20px; }
#auth-overlay.show{ display:flex; }
#auth-card{ width:100%; max-width:500px; background:var(--card-bg); border:1px solid var(--card-border); border-radius:18px; padding:34px 40px 28px; box-shadow:0 24px 80px rgba(0,0,0,.6); animation:authIn .16s ease-out; }
@keyframes authIn{ from{ opacity:0; transform:translateY(10px) scale(.98); } to{ opacity:1; transform:none; } }
#auth-title{ text-align:center; font:800 24px/1.25 inherit; color:var(--tb-btn-fg); margin:0 0 7px; }
#auth-sub{ text-align:center; font-size:13px; color:var(--muted2); margin:0 0 22px; }
.auth-tabs{ display:flex; gap:6px; padding:6px; margin-bottom:18px; background:var(--input-bg); border:1px solid var(--card-border); border-radius:12px; }
.auth-tab{ flex:1; height:44px; border:none; border-radius:9px; cursor:pointer; background:transparent; color:var(--muted2); font:700 13px/1 inherit; letter-spacing:.03em; text-transform:uppercase; transition:background .12s, color .12s; }
.auth-tab.active{ background:var(--accent); color:#06120a; }
.auth-field{ display:flex; align-items:center; gap:11px; height:50px; padding:0 16px; margin-bottom:12px; background:var(--input-bg); border:1px solid var(--card-border); border-radius:12px; transition:border-color .12s; }
.auth-field:focus-within{ border-color:var(--accent); }
.auth-field svg{ width:17px; height:17px; color:var(--muted2); flex:none; }
.auth-field input{ flex:1; background:transparent; border:none; outline:none; color:var(--tb-btn-fg); font-size:14.5px; min-width:0; }
.auth-field input::placeholder{ color:var(--muted2); }
#auth-submit{ width:100%; height:50px; margin-top:10px; border:none; border-radius:12px; cursor:pointer; background:var(--accent); color:#06120a; font:700 14.5px/1 inherit; letter-spacing:.01em; display:flex; align-items:center; justify-content:center; gap:9px; transition:filter .12s, opacity .12s; }
#auth-submit:hover{ filter:brightness(1.08); }
#auth-submit:disabled{ opacity:.6; cursor:default; }
#auth-submit svg{ width:17px; height:17px; }
#auth-error{ display:none; margin:13px 0 0; padding:10px 12px; border-radius:10px; background:rgba(204,68,68,.12); border:1px solid rgba(204,68,68,.4); color:#e88; font-size:12.5px; text-align:center; }
#auth-error.show{ display:block; }
.auth-foot{ text-align:center; margin:20px 0 0; font-size:13px; color:var(--muted2); }
.auth-foot a{ color:var(--accent); cursor:pointer; text-decoration:none; font-weight:600; }
.auth-foot a:hover{ text-decoration:underline; }
#plan-lock{ display:none; position:fixed; top:58px; left:0; right:0; bottom:0; z-index:9700; background:rgba(3,8,5,.42); backdrop-filter:blur(7px); -webkit-backdrop-filter:blur(7px); align-items:center; justify-content:center; padding:24px; }
#plan-lock.show{ display:flex; }
#plan-lock-card{ width:100%; max-width:430px; text-align:center; padding:30px 30px 26px; background:rgba(10,16,12,.86); border:1px solid var(--card-border); border-radius:18px; box-shadow:0 24px 80px rgba(0,0,0,.55); }
.plan-lock-ico{ width:58px; height:58px; margin:0 auto 16px; border-radius:50%; background:var(--accent-dim); border:1px solid var(--accent); color:var(--accent); display:flex; align-items:center; justify-content:center; }
.plan-lock-ico svg{ width:26px; height:26px; }
#plan-lock-title{ font:800 20px/1.3 inherit; color:var(--tb-btn-fg); margin:0 0 10px; }
#plan-lock-desc{ font-size:13px; line-height:1.55; color:var(--muted2); margin:0 0 20px; }
#plan-lock-buy{ height:42px; padding:0 26px; border:none; border-radius:10px; cursor:pointer; background:var(--accent); color:#06120a; font:700 14px/1 inherit; }
#plan-lock-buy:hover{ filter:brightness(1.08); }
#plan-lock-docs{ display:block; margin:14px auto 0; background:none; border:none; cursor:pointer; color:var(--muted2); font-size:12.5px; text-decoration:underline; }
#plan-lock-docs:hover{ color:var(--tb-btn-fg); }
#plan-toast{ position:fixed; left:50%; bottom:38px; transform:translateX(-50%) translateY(14px); z-index:100070; padding:11px 18px; border-radius:11px; pointer-events:none; background:rgba(12,18,13,.96); border:1px solid var(--accent); color:var(--tb-btn-fg); font-size:13px; box-shadow:0 10px 34px rgba(0,0,0,.5); opacity:0; transition:opacity .18s, transform .18s; }
#plan-toast.show{ opacity:1; transform:translateX(-50%) translateY(0); }
#plan-toast b{ color:var(--accent); }
.ch-scr-tf-chip.scr-locked{ opacity:.4; position:relative; }
.ch-scr-tf-chip.scr-locked::after{ content:"\\01F512"; font-size:8px; margin-left:3px; opacity:.9; }
.ch-exch-item.ch-exch-locked{ opacity:.45; }
.ch-exch-item.ch-exch-locked .ch-exch-count{ display:none; }
.ch-exch-lock{ margin-left:auto; font-size:11px; opacity:.85; }
</style>"""
# CSS is handled specially in main() (regex update-or-insert) so re-runs refresh
# the styles in place instead of duplicating the <style> block.

# 3) role chip before profile wrap ---------------------------------------------
P("role-chip",
"""      <div class="tb-profile-wrap" id="tb-profile-wrap">
        <button type="button" class="tb-btn tb-icon-btn" id="tb-profile-btn" title="Profile">""",
"""      <span class="tb-role-chip" id="tb-role-chip">PRO</span>

      <div class="tb-profile-wrap" id="tb-profile-wrap">
        <button type="button" class="tb-btn tb-icon-btn" id="tb-profile-btn" title="Profile">""")

# 4) login button before language wrap -----------------------------------------
P("login-button",
"""      <div class="sb-lang-wrap" id="sb-lang-wrap">
        <button type="button" class="tb-btn tb-lang-btn" id="sb-lang-btn" title="Language">""",
"""      <button type="button" class="tb-login-btn" id="tb-login-btn">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M15 3h4a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-4"/><polyline points="10 17 15 12 10 7"/><line x1="15" y1="12" x2="3" y2="12"/></svg>
        <span id="tb-login-lbl">Sign in</span>
      </button>

      <div class="sb-lang-wrap" id="sb-lang-wrap">
        <button type="button" class="tb-btn tb-lang-btn" id="sb-lang-btn" title="Language">""")

# 5) i18n keys (merge into TRANSLATIONS) ---------------------------------------
I18N = """};

Object.assign(TRANSLATIONS.en, {
  auth_login_tab: 'Sign in', auth_register_tab: 'Register',
  auth_create_title: 'Create new account', auth_create_sub: 'Get access to CRYPTO SCREENER features right now',
  auth_login_title: 'Welcome back', auth_login_sub: 'Sign in to your CRYPTO SCREENER account',
  auth_email_ph: 'Email', auth_pass_ph: 'Password',
  auth_btn_register: 'Register', auth_btn_login: 'Sign in',
  auth_have_acc: 'Already have an account?', auth_no_acc: "Don't have an account?",
  auth_link_login: 'Sign in', auth_link_register: 'Register',
  auth_err_invalid_email: 'Enter a valid email',
  auth_err_weak_password: 'Password must be at least 6 characters', auth_err_email_taken: 'This email is already registered',
  auth_err_invalid_credentials: 'Wrong email or password', auth_err_network: 'Network error, try again',
  login_btn: 'Sign in', role_pro: 'PRO', role_admin: 'ADMIN',
  gate_pro_feature: 'Available on <b>PRO</b>', gate_pro_exchange: 'Other exchanges are <b>PRO</b>',
  gate_pro_sort: 'This sort is <b>PRO</b>', gate_pro_settings: 'Filters &amp; settings are <b>PRO</b>',
  lock_buy: 'Get PRO', lock_docs: 'Learn more in the docs',
  lock_densities_title: 'Density map opens with PRO access',
  lock_densities_desc: 'All large limit orders on one screen: where the walls sit, how far from price, and how fast they melt. Spot the levels a big player defends and see where liquidity is shifting.',
  lock_splash_title: 'Volume Spikes open with PRO access',
  lock_splash_desc: 'Catch abnormal volume and trade bursts the moment they appear across every market - so you see momentum forming before the move.',
  lock_arbitrage_title: 'Arbitrage opens with PRO access',
  lock_arbitrage_desc: 'Cross-exchange price spreads in real time: where the same asset is cheaper or richer, how wide the gap is, and how long it lasts.',
  lock_alerts_title: 'Alerts open with PRO access',
  lock_alerts_desc: 'Set custom triggers on any metric — volatility, volume, open interest, price moves — and get notified 24/7 the instant a coin crosses your conditions, with the chart streamed in live.',
  lock_generic_title: 'This section opens with PRO access',
  lock_generic_desc: 'Upgrade to PRO to unlock all sections, every exchange, and the full set of screener tools.',
  prem_title: 'Unlock DIGASH PRO', prem_sub: 'Full access to every section, exchange and screener tool.',
  prem_f1: 'All exchanges in screener & charts', prem_f2: 'Every sorting metric & timeframe',
  prem_f3: 'Density map, Volume Spikes & Arbitrage', prem_f4: 'Filters, settings & advanced chart tools',
  prem_buy: 'Get PRO', prem_soon: 'Payment integration is coming soon - contact us to activate PRO.',
});
Object.assign(TRANSLATIONS.es, {
  auth_login_tab: 'Entrar', auth_register_tab: 'Registro',
  auth_create_title: 'Crear cuenta nueva', auth_create_sub: 'Obtén acceso a las funciones de CRYPTO SCREENER ahora mismo',
  auth_login_title: 'Bienvenido de nuevo', auth_login_sub: 'Entra en tu cuenta de CRYPTO SCREENER',
  auth_email_ph: 'Correo', auth_pass_ph: 'Contraseña',
  auth_btn_register: 'Registrarse', auth_btn_login: 'Entrar',
  auth_have_acc: '¿Ya tienes una cuenta?', auth_no_acc: '¿No tienes una cuenta?',
  auth_link_login: 'Entrar', auth_link_register: 'Registrarse',
  auth_err_invalid_email: 'Introduce un correo válido',
  auth_err_weak_password: 'La contraseña debe tener al menos 6 caracteres', auth_err_email_taken: 'Este correo ya está registrado',
  auth_err_invalid_credentials: 'Correo o contraseña incorrectos', auth_err_network: 'Error de red, inténtalo de nuevo',
  login_btn: 'Entrar', role_pro: 'PRO', role_admin: 'ADMIN',
  gate_pro_feature: 'Disponible en <b>PRO</b>', gate_pro_exchange: 'Otras bolsas son <b>PRO</b>',
  gate_pro_sort: 'Este orden es <b>PRO</b>', gate_pro_settings: 'Filtros y ajustes son <b>PRO</b>',
  lock_buy: 'Obtener PRO', lock_docs: 'Más información en la documentación',
  lock_densities_title: 'El mapa de densidades se abre con acceso PRO',
  lock_densities_desc: 'Todas las órdenes límite grandes en una pantalla: dónde están los muros, a qué distancia del precio y con qué rapidez se disuelven.',
  lock_splash_title: 'Los picos de volumen se abren con acceso PRO',
  lock_splash_desc: 'Detecta volúmenes y ráfagas de operaciones anómalos en cuanto aparecen en cada mercado.',
  lock_arbitrage_title: 'El arbitraje se abre con acceso PRO',
  lock_arbitrage_desc: 'Diferencias de precio entre bolsas en tiempo real: dónde el mismo activo está más barato o más caro.',
  lock_alerts_title: 'Las alertas se abren con acceso PRO',
  lock_alerts_desc: 'Crea disparadores personalizados sobre cualquier métrica — volatilidad, volumen, interés abierto, movimientos de precio — y recibe avisos 24/7 en cuanto una moneda cruza tus condiciones, con el gráfico en vivo.',
  lock_generic_title: 'Esta sección se abre con acceso PRO',
  lock_generic_desc: 'Mejora a PRO para desbloquear todas las secciones, todas las bolsas y el conjunto completo de herramientas.',
  prem_title: 'Desbloquea DIGASH PRO', prem_sub: 'Acceso completo a todas las secciones, bolsas y herramientas del screener.',
  prem_f1: 'Todas las bolsas en screener y gráficos', prem_f2: 'Todas las métricas y marcos temporales',
  prem_f3: 'Mapa de densidades, Picos y Arbitraje', prem_f4: 'Filtros, ajustes y herramientas avanzadas',
  prem_buy: 'Obtener PRO', prem_soon: 'La integración de pagos llegará pronto: contáctanos para activar PRO.',
});

let currentLang = localStorage.getItem('crypto_lang');"""
P("i18n", """};

let currentLang = localStorage.getItem('crypto_lang');""", I18N)

# 6) switchSection start gate ---------------------------------------------------
P("switch-start",
"""function switchSection(section) {""",
"""function switchSection(section) {
  if (window.PLAN && !PLAN.beforeSection(section)) return;""")

# 7) switchSection end overlay --------------------------------------------------
P("switch-end",
"""  requestAnimationFrame(() => window._syncChHeaderSearchPos?.());
}""",
"""  requestAnimationFrame(() => window._syncChHeaderSearchPos?.());
  if (window.PLAN) PLAN.afterSection(section);
}""")

# 8) exchange gate --------------------------------------------------------------
P("exch-gate",
"""  function _chSwitchExchange(id) {""",
"""  function _chSwitchExchange(id) {
    if (window.PLAN && !PLAN.canExchange(id)) { PLAN.toast(t('gate_pro_exchange')); PLAN.closeMenusSoft(); return; }""")

# 9) exchange-menu lock badge ---------------------------------------------------
P("exch-menu-lock",
"""      const isActive = ex.id === chExch;
      const count = _exchCounts[ex.id] || '';
      html += `<div class="ch-exch-item${isActive ? ' active' : ''}" data-exch-id="${ex.id}" data-name="${ex.label.toLowerCase()}">${_exchIconHtml(ex)}<span class="ch-exch-name">${ex.label}</span><span class="ch-exch-count">${count}</span></div>`;""",
"""      const isActive = ex.id === chExch;
      const count = _exchCounts[ex.id] || '';
      const locked = (window.PLAN && !PLAN.canExchange(ex.id));
      html += `<div class="ch-exch-item${isActive ? ' active' : ''}${locked ? ' ch-exch-locked' : ''}" data-exch-id="${ex.id}" data-name="${ex.label.toLowerCase()}">${_exchIconHtml(ex)}<span class="ch-exch-name">${ex.label}</span>${locked ? '<span class="ch-exch-lock">&#128274;</span>' : `<span class="ch-exch-count">${count}</span>`}</div>`;""")

# 10) sort-chip lock ------------------------------------------------------------
P("sort-chip-lock",
"""      p.tfs.forEach(tf => {
        const chip = document.createElement('button');
        chip.type = 'button';
        chip.className = 'ch-scr-tf-chip' + (cur.paramKey === p.key && cur.tf === tf ? ' active' : '');
        chip.textContent = SCR_TF_LABELS[tf] || tf;
        if (p.active) chip.addEventListener('click', () => _scrPickSort(p.sortKey(tf)));
        tfs.appendChild(chip);
      });""",
"""      p.tfs.forEach(tf => {
        const sk = p.sortKey(tf);
        const locked = (window.PLAN && !PLAN.canSort(sk));
        const chip = document.createElement('button');
        chip.type = 'button';
        chip.className = 'ch-scr-tf-chip' + (cur.paramKey === p.key && cur.tf === tf ? ' active' : '') + (locked ? ' scr-locked' : '');
        chip.textContent = SCR_TF_LABELS[tf] || tf;
        if (p.active) chip.addEventListener('click', () => { if (locked) { PLAN.toast(t('gate_pro_sort')); return; } _scrPickSort(sk); });
        tfs.appendChild(chip);
      });""")

# 11) _scrPickSort gate ---------------------------------------------------------
P("sort-pick-gate",
"""  function _scrPickSort(key) {
    if (chSortKey === key) chSortAsc = !chSortAsc;""",
"""  function _scrPickSort(key) {
    if (window.PLAN && !PLAN.canSort(key)) { PLAN.toast(t('gate_pro_sort')); return; }
    if (chSortKey === key) chSortAsc = !chSortAsc;""")

# 12) screener Settings menu gate (filters/settings = PRO) ----------------------
P("scr-settings-gate",
"""    _scrBindOnce(menuBtn, 'click', e => { e.stopPropagation(); _scrToggleMenu(menuBtn, menuMenu); });""",
"""    _scrBindOnce(menuBtn, 'click', e => { e.stopPropagation(); if (window.PLAN && !PLAN.isPaid()) { PLAN.toast(t('gate_pro_settings')); return; } _scrToggleMenu(menuBtn, menuMenu); });""")

# 13) chart-settings gate -------------------------------------------------------
P("chart-settings-gate",
"""    btn.addEventListener('click', e => {
      e.preventDefault();
      e.stopPropagation();
      if (overlay.classList.contains('open')) _closeChartSettingsModal(true);
      else _openChartSettingsModal();
    });""",
"""    btn.addEventListener('click', e => {
      e.preventDefault();
      e.stopPropagation();
      if (window.PLAN && !PLAN.isPaid()) { PLAN.toast(t('gate_pro_settings')); return; }
      if (overlay.classList.contains('open')) _closeChartSettingsModal(true);
      else _openChartSettingsModal();
    });""")

# 14) charts params-panel (Settings/Parameters) gate ----------------------------
P("params-panel-gate",
"""      wlMenuBtn.addEventListener('click', e => {
        e.preventDefault();
        e.stopPropagation();
        const panel = document.getElementById('ch-params-panel');
        if (panel) panel.classList.toggle('open');
      });""",
"""      wlMenuBtn.addEventListener('click', e => {
        e.preventDefault();
        e.stopPropagation();
        if (window.PLAN && !PLAN.isPaid()) { PLAN.toast(t('gate_pro_settings')); return; }
        const panel = document.getElementById('ch-params-panel');
        if (panel) panel.classList.toggle('open');
      });""")

# 15) tail: modal + lock + toast + controller (before </body>) ------------------
TAIL = r"""
<!-- Auth + role gating UI -->
<div id="auth-overlay"><div id="auth-card" role="dialog" aria-modal="true">
  <h2 id="auth-title"></h2>
  <p id="auth-sub"></p>
  <div class="auth-tabs">
    <button type="button" class="auth-tab" id="auth-tab-login"></button>
    <button type="button" class="auth-tab" id="auth-tab-register"></button>
  </div>
  <form id="auth-form" autocomplete="on">
    <div class="auth-field"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="3" y="5" width="18" height="14" rx="2"/><path d="M3 7l9 6 9-6"/></svg><input type="email" id="auth-email" autocomplete="email" spellcheck="false"></div>
    <div class="auth-field"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><rect x="4" y="11" width="16" height="10" rx="2"/><path d="M8 11V7a4 4 0 0 1 8 0v4"/></svg><input type="password" id="auth-password" autocomplete="current-password"></div>
    <div id="auth-error"></div>
    <button type="submit" id="auth-submit"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg><span id="auth-submit-lbl"></span></button>
  </form>
  <p class="auth-foot"><span id="auth-foot-q"></span> <a id="auth-foot-link"></a></p>
</div></div>
<div id="plan-lock"><div id="plan-lock-card">
  <div class="plan-lock-ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><rect x="4" y="11" width="16" height="10" rx="2"/><path d="M8 11V7a4 4 0 0 1 8 0v4"/></svg></div>
  <h3 id="plan-lock-title"></h3>
  <p id="plan-lock-desc"></p>
  <button type="button" id="plan-lock-buy"></button>
  <button type="button" id="plan-lock-docs"></button>
</div></div>
<div id="plan-toast"></div>
<script>
(function(){
  "use strict";
  var ALLOWED_SORTS = ['natr__1m','volume__1d','chg_5m'];
  var LOCKED_SECTIONS = ['densities','splash','arbitrage','alerts'];
  window.AUTH = { authenticated:false, email:'', username:'', role:'guest', isPro:false, isAdmin:false };
  try { var rc = localStorage.getItem('crypto_user_role');
    if (rc && rc !== 'guest') { AUTH.authenticated=true; AUTH.isAdmin=(rc==='admin'); AUTH.isPro=(rc==='pro'||rc==='admin'); AUTH.role=AUTH.isAdmin?'admin':(AUTH.isPro?'pro':'user'); AUTH.email=localStorage.getItem('crypto_user_email')||''; }
  } catch(e){}
  function T(k){ return (typeof t==='function') ? t(k) : k; }
  function $(id){ return document.getElementById(id); }
  var _tt=null;
  function toast(html){ var el=$('plan-toast'); if(!el)return; el.innerHTML=html; el.classList.add('show'); clearTimeout(_tt); _tt=setTimeout(function(){el.classList.remove('show');},2200); }
  var _mode='register';
  function renderMode(){ var reg=_mode==='register';
    $('auth-tab-login').classList.toggle('active',!reg); $('auth-tab-register').classList.toggle('active',reg);
    $('auth-title').textContent=reg?T('auth_create_title'):T('auth_login_title');
    $('auth-sub').textContent=reg?T('auth_create_sub'):T('auth_login_sub');
    $('auth-submit-lbl').textContent=reg?T('auth_btn_register'):T('auth_btn_login');
    $('auth-password').autocomplete=reg?'new-password':'current-password';
    $('auth-foot-q').textContent=reg?T('auth_have_acc'):T('auth_no_acc');
    $('auth-foot-link').textContent=reg?T('auth_link_login'):T('auth_link_register'); hideErr();
  }
  function localizeAuth(){ $('auth-tab-login').textContent=T('auth_login_tab'); $('auth-tab-register').textContent=T('auth_register_tab');
    $('auth-email').placeholder=T('auth_email_ph'); $('auth-password').placeholder=T('auth_pass_ph'); renderMode(); }
  function setMode(m){ _mode=(m==='login')?'login':'register'; renderMode(); }
  function openAuth(m){ setMode(m||'register'); $('auth-overlay').classList.add('show'); setTimeout(function(){$('auth-email').focus();},60); }
  function closeAuth(){ $('auth-overlay').classList.remove('show'); }
  function showErr(c){ var el=$('auth-error'),k='auth_err_'+c,m=T(k); if(m===k)m=T('auth_err_network'); el.textContent=m; el.classList.add('show'); }
  function hideErr(){ var el=$('auth-error'); if(el){el.classList.remove('show'); el.textContent='';} }
  function submitAuth(){ hideErr(); var email=$('auth-email').value.trim(), password=$('auth-password').value, btn=$('auth-submit'); btn.disabled=true;
    var path=_mode==='register'?'/api/auth/register':'/api/auth/login';
    fetch(path,{method:'POST',headers:{'Content-Type':'application/json'},credentials:'same-origin',body:JSON.stringify({email:email,password:password})})
      .then(function(r){ return r.json().then(function(d){ return {ok:r.ok,d:d}; }); })
      .then(function(res){ if(!res.ok||!res.d||!res.d.ok){ showErr((res.d&&res.d.error)||'network'); btn.disabled=false; return; }
        var u=res.d.user; try{ localStorage.setItem('crypto_user_email',u.email); localStorage.setItem('crypto_user_role',u.is_admin?'admin':(u.is_pro?'pro':'user')); }catch(e){} location.reload();
      }).catch(function(){ showErr('network'); btn.disabled=false; });
  }
  function fillLock(sec){ var has=LOCKED_SECTIONS.indexOf(sec)>=0;
    $('plan-lock-title').textContent=has?T('lock_'+sec+'_title'):T('lock_generic_title');
    $('plan-lock-desc').textContent=has?T('lock_'+sec+'_desc'):T('lock_generic_desc');
    $('plan-lock-buy').textContent=T('lock_buy'); $('plan-lock-docs').textContent=T('lock_docs'); }
  function buildPremium(){ var m=$('premium-main'); if(!m) return;
    var feats=['prem_f1','prem_f2','prem_f3','prem_f4'].map(function(k){return '<div>&#10003; '+T(k)+'</div>';}).join('');
    m.innerHTML='<div style="max-width:560px;margin:auto;text-align:center;padding:24px">'
      +'<div style="width:62px;height:62px;margin:0 auto 18px;border-radius:16px;background:var(--accent-dim);border:1px solid var(--accent);color:var(--accent);display:flex;align-items:center;justify-content:center"><svg width="30" height="30" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linejoin="round"><path d="M12 2l3 7h7l-5.5 4 2 7L12 16l-6.5 4 2-7L2 9h7z"/></svg></div>'
      +'<h2 style="font-size:26px;font-weight:800;color:var(--tb-btn-fg);margin:0 0 8px">'+T('prem_title')+'</h2>'
      +'<p style="color:var(--muted2);margin:0 0 22px">'+T('prem_sub')+'</p>'
      +'<div style="text-align:left;display:inline-block;margin:0 auto 22px;color:var(--tb-btn-fg);font-size:14px;line-height:2.1">'+feats+'</div>'
      +'<div><button type="button" id="prem-buy" style="height:46px;padding:0 30px;border:none;border-radius:11px;background:var(--accent);color:#06120a;font:700 15px/1 inherit;cursor:pointer">'+T('prem_buy')+'</button></div>'
      +'<p style="color:var(--muted2);font-size:12.5px;margin:16px 0 0">'+T('prem_soon')+'</p></div>';
    var b=$('prem-buy'); if(b) b.addEventListener('click',function(){ toast(T('prem_soon')); });
  }
  function planClass(){ return !AUTH.authenticated?'guest':(AUTH.isAdmin?'admin':(AUTH.isPro?'pro':'free')); }
  function ensureAdminMenu(){ var menu=$('tb-profile-menu'); if(!menu) return; var item=$('tb-profile-menu-admin');
    if(AUTH.isAdmin && !item){ item=document.createElement('button'); item.type='button'; item.className='tb-profile-item'; item.id='tb-profile-menu-admin';
      item.innerHTML='<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2l8 4v6c0 5-3.5 8-8 10-4.5-2-8-5-8-10V6z"/></svg><span>Admin</span>';
      item.addEventListener('click',function(e){ e.stopPropagation(); var w=$('tb-profile-wrap'); if(w)w.classList.remove('open'); window.open('/static/admin.html','_blank'); });
      menu.insertBefore(item, $('tb-profile-menu-logout')||null);
    }
    if(item) item.style.display=AUTH.isAdmin?'flex':'none';
  }
  function applyRole(){ var pc=planClass();
    document.body.classList.remove('plan-guest','plan-free','plan-pro','plan-admin'); document.body.classList.add('plan-'+pc);
    try{ localStorage.setItem('crypto_user_role', pc==='free'?'user':pc); }catch(e){}
    var chip=$('tb-role-chip'); if(chip) chip.textContent=AUTH.isAdmin?T('role_admin'):T('role_pro');
    var lbl=$('tb-login-lbl'); if(lbl) lbl.textContent=T('login_btn');
    if(AUTH.authenticated){ try{ localStorage.setItem('crypto_user_email',AUTH.email); }catch(e){} var ve=$('pf-val-email'); if(ve&&AUTH.email) ve.textContent=AUTH.email; }
    else { try{ localStorage.removeItem('crypto_user_email'); }catch(e){} }
    ensureAdminMenu();
  }
  window.PLAN = {
    isPaid:function(){ return !!(AUTH.isPro||AUTH.isAdmin); },
    canExchange:function(id){ return this.isPaid()||id==='binance_futures'; },
    canSort:function(k){ return this.isPaid()||ALLOWED_SORTS.indexOf(k)>=0; },
    beforeSection:function(s){ if(!AUTH.authenticated){ if(s==='screener') return true; openAuth('register'); return false; } return true; },
    afterSection:function(s){ var el=$('plan-lock'); if(!el) return; var need=AUTH.authenticated&&!this.isPaid()&&LOCKED_SECTIONS.indexOf(s)>=0; if(need){ fillLock(s); el.classList.add('show'); } else el.classList.remove('show'); },
    toast:toast, openAuth:openAuth, closeAuth:closeAuth,
    closeMenusSoft:function(){ document.querySelectorAll('.ch-scr-menu.open').forEach(function(m){m.classList.remove('open');}); },
    refresh:function(){ var self=this; return fetch('/api/auth/me',{credentials:'same-origin'}).then(function(r){return r.json();})
      .then(function(d){ if(d&&d.authenticated) AUTH={authenticated:true,email:d.email||'',username:d.username||'',role:d.role||'user',isPro:!!d.is_pro,isAdmin:!!d.is_admin};
        else AUTH={authenticated:false,email:'',username:'',role:'guest',isPro:false,isAdmin:false}; })
      .catch(function(){}).then(function(){ applyRole(); try{ if(window._scrRelocalize) window._scrRelocalize(); }catch(e){}
        var cur=(typeof activeSection!=='undefined')?activeSection:'screener'; self.afterSection(cur);
        if(!AUTH.authenticated && typeof switchSection==='function' && cur!=='screener') switchSection('screener'); }); },
    logout:function(){ var done=function(){ try{ localStorage.setItem('crypto_user_role','guest'); localStorage.removeItem('crypto_user_email'); localStorage.setItem('crypto_section','screener'); }catch(e){} location.reload(); }; fetch('/api/auth/logout',{method:'POST',credentials:'same-origin'}).then(done,done); },
  };
  function wire(){
    $('auth-tab-login').addEventListener('click',function(){ setMode('login'); });
    $('auth-tab-register').addEventListener('click',function(){ setMode('register'); });
    $('auth-foot-link').addEventListener('click',function(){ setMode(_mode==='register'?'login':'register'); });
    $('auth-form').addEventListener('submit',function(e){ e.preventDefault(); submitAuth(); });
    $('auth-overlay').addEventListener('mousedown',function(e){ if(e.target.id==='auth-overlay') closeAuth(); });
    document.addEventListener('keydown',function(e){ if(e.key==='Escape') closeAuth(); });
    var lb=$('tb-login-btn'); if(lb) lb.addEventListener('click',function(){ openAuth('login'); });
    var lg=$('tb-profile-menu-logout'); if(lg) lg.addEventListener('click',function(e){ e.stopPropagation(); window.PLAN.logout(); });
    var buy=$('plan-lock-buy'); if(buy) buy.addEventListener('click',function(){ if(typeof switchSection==='function') switchSection('premium'); });
    var docs=$('plan-lock-docs'); if(docs) docs.addEventListener('click',function(){ if(typeof switchSection==='function') switchSection('premium'); });
    localizeAuth(); buildPremium();
    if(typeof window.applyTranslations==='function' && !window.applyTranslations._authWrapped){ var orig=window.applyTranslations;
      window.applyTranslations=function(){ orig.apply(this,arguments); try{ localizeAuth(); buildPremium(); var c=$('tb-role-chip'); if(c) c.textContent=AUTH.isAdmin?T('role_admin'):T('role_pro'); }catch(e){} };
      window.applyTranslations._authWrapped=true; }
    document.addEventListener('click',function(e){ if(AUTH.authenticated) return; var tg=e.target;
      if(tg.closest('#auth-overlay,#tb-login-btn,#sb-theme-wrap,#sb-lang-wrap,#plan-toast,#mobile-nav-close')) return;
      if(tg.closest('#top-nav,#mobile-nav-drawer,#mobile-nav-toggle,#mobile-nav-overlay,#charts-main,#statsbar,#filterbar,#bottom-bar,#ws-dropdown,#tb-profile-wrap,#premium-main,#listings-main,#profile-main,#splash-main,#arb-main')){
        e.preventDefault(); e.stopPropagation(); openAuth('register'); }
    }, true);
    applyRole(); window.PLAN.refresh();
  }
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',wire); else wire();
})();
</script>

</body>"""
P("tail", "\n\n</body>", TAIL)


def main():
    import re
    if not INDEX.exists():
        print("ERROR: index.html not found at", INDEX); return 2
    content = INDEX.read_text(encoding="utf-8")

    # CSS: update existing <style id="auth-styles"> in place, else insert before </head>.
    m = re.search(r'<style id="auth-styles">.*?</style>', content, re.DOTALL)
    if m:
        css_status = "css:already" if m.group(0) == AUTH_CSS else "css:updated"
        content = content[:m.start()] + AUTH_CSS + content[m.end():]
    elif "</head>" in content:
        content = content.replace("</head>", AUTH_CSS + "\n</head>", 1)
        css_status = "css:applied"
    else:
        print("FAILED: no </head> and no existing auth-styles block"); return 1

    applied, skipped, failed = [], [], []
    for name, old, new in PATCHES:
        if new in content:
            skipped.append(name)
        elif old in content:
            content = content.replace(old, new, 1)
            applied.append(name)
        else:
            failed.append(name)
    if failed:
        print("FAILED (anchor not found) — index.html may have changed:", failed)
        print("Applied:", applied, "| Skipped(already):", skipped)
        return 1
    INDEX.write_text(content, encoding="utf-8")
    print(f"OK  {css_status}  applied={len(applied)} skipped={len(skipped)}  ({INDEX})")
    if applied:  print("   applied:", ", ".join(applied))
    if skipped:  print("   already:", ", ".join(skipped))
    return 0


if __name__ == "__main__":
    sys.exit(main())
