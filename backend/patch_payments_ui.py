"""Wire the Premium "Get PRO" button to the Heleket checkout in index.html.

Targets the live (glass-redesign) Premium page: the `_premBuy()` handler already
gates the purchase behind the consent checkboxes, so we only swap its terminal
"payment coming soon" toast for a real checkout that POSTs /api/pay/create and
redirects to Heleket. Also drops the now-stale "coming soon" note under the CTA,
and adds a return handler for /premium?pay=success.

Run on the box that owns index.html (server is usually ahead of local):

    python backend/patch_payments_ui.py

Idempotent: each patch is skipped if already applied.
"""
from pathlib import Path
import sys

INDEX = Path(sys.argv[1]) if len(sys.argv) > 1 else \
    Path(__file__).resolve().parent / "static" / "index.html"

PATCHES = []
def P(name, old, new): PATCHES.append((name, old, new))

# 1) _premBuy terminal action: was a "coming soon" toast → start the real checkout.
#    `this` is the clicked button (handler bound to both prem-buy / prem-buy2).
P("prembuy-checkout",
  "      toast(T('prem_soon'));",
  "      if(window.startProCheckout){ window.startProCheckout(this); } else { toast(T('prem_soon')); }")

# 2) Remove the stale "payment coming soon" note under the hero CTA.
P("drop-cta-note",
  "<div class=\"prem-cta-note\">'+T('prem_soon')+'</div>",
  "")

# 3) Checkout controller + return-from-payment handler (before </body>).
CHECKOUT = r"""
<!-- PRO checkout (Heleket) — injected by patch_payments_ui.py -->
<script id="pay-checkout-script">
(function(){
  "use strict";
  function toast(m){ try{ if(window.PLAN&&PLAN.toast) return PLAN.toast(m); }catch(e){} }
  function promo(){ try{ return (localStorage.getItem('crypto_promo')||'').trim(); }catch(e){ return ''; } }
  window.startProCheckout=function(btn){
    try{
      if(!(window.AUTH&&AUTH.authenticated)){ if(window.PLAN&&PLAN.openAuth) PLAN.openAuth('register'); return; }
      if(window.AUTH&&(AUTH.isPro||AUTH.isAdmin)){ toast('PRO уже активен'); return; }
    }catch(e){}
    var months=parseInt(btn&&btn.getAttribute('data-months'),10); if(!(months>0)) months=1;
    var old=btn?btn.innerHTML:''; if(btn){ btn.disabled=true; btn.style.opacity='0.7'; }
    function restore(){ if(btn){ btn.disabled=false; btn.style.opacity=''; btn.innerHTML=old; } }
    var body={ months:months, locale:(typeof currentLang==='string'?currentLang:'en') };
    var pc=promo(); if(pc) body.promo_code=pc;
    fetch('/api/pay/create',{method:'POST',headers:{'Content-Type':'application/json'},credentials:'same-origin',body:JSON.stringify(body)})
      .then(function(r){ return r.json().then(function(d){ return {ok:r.ok,d:d}; }); })
      .then(function(res){
        if(res.ok && res.d && res.d.ok && res.d.free){ toast('PRO активирован ✓'); try{ if(window.PLAN&&PLAN.refresh) PLAN.refresh(); }catch(e){} restore(); return; }
        if(res.ok && res.d && res.d.ok && res.d.url){ window.location.href=res.d.url; return; }
        var err=(res.d&&res.d.error)||'error';
        toast(err==='already_pro' ? 'PRO уже активен'
              : (err==='auth_required' ? 'Войдите, чтобы оформить PRO'
              : 'Ошибка оплаты, попробуйте ещё раз'));
        restore();
      }).catch(function(){ toast('Ошибка сети'); restore(); });
  };
  // Returning from Heleket (/premium?pay=success): the PRO grant arrives via the
  // webhook, so poll /api/auth/me a few times to reflect it without a manual reload.
  function onReturn(){
    try{
      if(!/[?&]pay=success/.test(location.search)) return;
      try{ var sp=new URLSearchParams(location.search); sp.delete('pay'); var qs=sp.toString(); history.replaceState(null,'',location.pathname+(qs?'?'+qs:'')+location.hash); }catch(e){}
      toast('Оплата получена — активируем PRO…');
      var n=0; var iv=setInterval(function(){ n++;
        if(window.PLAN&&PLAN.refresh){ PLAN.refresh().then(function(){
          try{ if(window.AUTH&&(AUTH.isPro||AUTH.isAdmin)){ clearInterval(iv); toast('PRO активирован ✓'); try{ if(window.Onboard&&Onboard.startPro&&localStorage.getItem('crypto_onb_pro')!=='1') Onboard.startPro(); }catch(e){} } }catch(e){}
        }); }
        if(n>=8) clearInterval(iv);
      }, 2500);
    }catch(e){}
  }
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',onReturn); else onReturn();
})();
</script>

</body>"""
P("checkout-script", "\n</body>", CHECKOUT)


def main():
    if not INDEX.exists():
        print("ERROR: index.html not found at", INDEX); return 2
    content = INDEX.read_text(encoding="utf-8")
    applied, skipped, failed = [], [], []
    for name, old, new in PATCHES:
        if new and new in content:
            skipped.append(name)
        elif old in content:
            content = content.replace(old, new, 1)
            applied.append(name)
        elif not new:
            # removal patch whose anchor is already gone → treat as done, not failed
            skipped.append(name)
        else:
            failed.append(name)
    if failed:
        print("FAILED (anchor not found) — index.html may have changed:", failed)
        print("Applied:", applied, "| Skipped(already):", skipped)
        return 1
    INDEX.write_text(content, encoding="utf-8")
    print(f"OK  applied={len(applied)} skipped={len(skipped)}  ({INDEX})")
    if applied: print("   applied:", ", ".join(applied))
    if skipped: print("   already:", ", ".join(skipped))
    return 0


if __name__ == "__main__":
    sys.exit(main())
