/* One read-only public Arcus batch for Charts, Screener and existing Arbitrage UI. */
(function () {
  'use strict';
  const notes = {
    en: 'Candles: Arcus model · Metrics: OKX Futures reference. 24h volume: OKX ticker; interval volume/spikes unavailable. Only 1m NATR carries a source timestamp.',
    ru: 'Свечи: модель Arcus · Метрики: референс OKX Futures. Объём 24ч: тикер OKX; интервальный объём/спайки недоступны. Время источника есть только у NATR 1m.',
    uk: 'Свічки: модель Arcus · Метрики: референс OKX Futures',
    es: 'Velas: modelo Arcus · Métricas: referencia OKX Futures',
    de: 'Kerzen: Arcus-Modell · Kennzahlen: OKX Futures-Referenz',
    fr: 'Bougies : modèle Arcus · Indicateurs : référence OKX Futures',
    pt: 'Velas: modelo Arcus · Métricas: referência OKX Futures',
    tr: 'Mumlar: Arcus modeli · Metrikler: OKX Futures referansı',
    zh: 'K线：Arcus 模型 · 指标：OKX Futures 参考',
    ja: 'ローソク足：Arcusモデル · 指標：OKX Futures参照'
  };
  let pending = false;
  let lastSourceAt = 0;
  function note() {
    const lang = (document.documentElement.lang || 'en').slice(0, 2);
    for (const id of ['ch-chart-arcus-note', 'ch-scr-arcus-note']) {
      const el = document.getElementById(id);
      if (el) {
        el.title = notes[lang] || notes.en;
        el.setAttribute('aria-label', el.title);
      }
    }
  }
  function unavailable() {
    window.dispatchEvent(new Event('arcus-unavailable'));
  }
  async function poll() {
    if (pending || document.hidden) return;
    if (!document.body.classList.contains('sec-arbitrage') &&
        !document.body.classList.contains('arcus-market') &&
        !(window._chHasArcusSlots && window._chHasArcusSlots())) return;
    pending = true;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);
    try {
      const response = await fetch('/api/arcus/snapshot', {cache:'no-store', signal:controller.signal});
      if (!response.ok) throw Error('Arcus batch unavailable');
      const snapshot = await response.json();
      if (!snapshot || snapshot.schemaVersion !== 1 || !Array.isArray(snapshot.markets) ||
          !snapshot.markets.some(row => row.fresh)) throw Error('Arcus batch stale');
      lastSourceAt = Date.now();
      note();
      window.dispatchEvent(new CustomEvent('arcus-snapshot', {detail:snapshot}));
    } catch (_) {
      unavailable();
    } finally {
      clearTimeout(timeout);
      pending = false;
    }
  }
  setInterval(poll, 2000);
  setInterval(() => {
    if (lastSourceAt && Date.now() - lastSourceAt > 6000) {
      lastSourceAt = 0;
      unavailable();
    }
  }, 1000);
  window.addEventListener('arcus-snapshot-request', poll);
  setTimeout(poll, 0);
})();
