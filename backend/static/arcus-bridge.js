/* Arcus public market feed: one cached batch, no browser-side model or admin API. */
(function () {
  'use strict';
  const labels = {
    en: ['Arcus — modeled market', 'Arcus — OKX reference', 'Arcus ↔ OKX · modeled market', 'Model mark versus OKX reference mark. Not a verified executable return.', 'Data unavailable', 'lag', 'recovery'],
    ru: ['Arcus — моделируемый рынок', 'Arcus — данные OKX', 'Arcus ↔ OKX · моделируемый рынок', 'Цена модели и референсная цена OKX. Не подтверждённая доходность сделки.', 'Данные недоступны', 'отставание', 'восстановление'],
    uk: ['Arcus — модельований ринок', 'Arcus — дані OKX', 'Arcus ↔ OKX · модельований ринок', 'Ціна моделі та референтна ціна OKX. Не підтверджений прибуток угоди.', 'Дані недоступні', 'відставання', 'відновлення'],
    es: ['Arcus — mercado simulado', 'Arcus — referencia OKX', 'Arcus ↔ OKX · mercado simulado', 'Precio modelado frente a la referencia OKX; no es un retorno ejecutable verificado.', 'Datos no disponibles', 'retraso', 'recuperación'],
    de: ['Arcus — modellierter Markt', 'Arcus — OKX-Referenz', 'Arcus ↔ OKX · modellierter Markt', 'Modellpreis gegenüber OKX-Referenz; kein bestätigbarer Handelsgewinn.', 'Daten nicht verfügbar', 'Verzögerung', 'Erholung'],
    fr: ['Arcus — marché simulé', 'Arcus — référence OKX', 'Arcus ↔ OKX · marché simulé', 'Prix modélisé contre référence OKX ; aucun rendement exécutable garanti.', 'Données indisponibles', 'retard', 'récupération'],
    pt: ['Arcus — mercado simulado', 'Arcus — referência OKX', 'Arcus ↔ OKX · mercado simulado', 'Preço modelado versus referência OKX; não é retorno de negociação verificado.', 'Dados indisponíveis', 'atraso', 'recuperação'],
    tr: ['Arcus — modellenmiş piyasa', 'Arcus — OKX referansı', 'Arcus ↔ OKX · modellenmiş piyasa', 'Model fiyatı ile OKX referansı; doğrulanmış işlem getirisi değildir.', 'Veri kullanılamıyor', 'gecikme', 'toparlanma'],
    zh: ['Arcus — 模拟市场', 'Arcus — OKX 参考', 'Arcus ↔ OKX · 模拟市场', '模型价格与 OKX 参考价格之差，不代表可实现的交易收益。', '数据不可用', '延迟', '恢复'],
    ja: ['Arcus — モデル市場', 'Arcus — OKX参照', 'Arcus ↔ OKX · モデル市場', 'モデル価格とOKX参照価格の差であり、実現可能な利益ではありません。', 'データを取得できません', '遅延', '回復']
  };
  const tr = () => labels[(document.documentElement.lang || 'en').slice(0, 2)] || labels.en;
  const money = value => Number(value).toLocaleString('en-US', {maximumFractionDigits: 8});
  let last = null;
  let pending = false;
  let selectedCoin = '';
  let selectedEvent = '';
  let manualClosed = false;
  let chartHandles = null;
  let loadedCoin = '';
  let lastSourceAt = 0;
  let detailAbort = null;
  let detailGeneration = 0;
  let detailLoading = false;
  let detailRetryAt = 0;
  let pricePending = false;

  function stopDetail() {
    detailGeneration++;
    if (detailAbort) { detailAbort.abort(); detailAbort = null; }
    detailLoading = false;
    detailRetryAt = 0;
    selectedCoin = '';
    selectedEvent = '';
    loadedCoin = '';
    if (chartHandles) {
      for (const handle of Object.values(chartHandles)) {
        try { handle.observer.disconnect(); handle.chart.remove(); } catch (_) {}
      }
      chartHandles = null;
    }
    const detail = document.getElementById('arcus-detail');
    if (detail) detail.hidden = true;
    document.body.classList.remove('arcus-arb-selected');
  }

  function status(message) {
    const node = document.getElementById('arcus-detail-status');
    if (!node) return;
    node.hidden = !message;
    node.textContent = message || '';
  }

  function ensureCharts() {
    if (window.LightweightCharts) return Promise.resolve(true);
    return new Promise(resolve => {
      const existing = document.querySelector('script[src="/static/lw-charts.js"]');
      if (!existing) {
        const script = document.createElement('script');
        script.src = '/static/lw-charts.js';
        script.onload = () => resolve(!!window.LightweightCharts);
        script.onerror = () => resolve(false);
        document.head.append(script);
      } else {
        let attempts = 0;
        const timer = setInterval(() => {
          if (window.LightweightCharts || ++attempts >= 100) {
            clearInterval(timer);
            resolve(!!window.LightweightCharts);
          }
        }, 50);
      }
    });
  }

  function makeChart(id, spread) {
    const root = document.getElementById(id);
    if (!root) return null;
    root.replaceChildren();
    const chart = LightweightCharts.createChart(root, {
      width: root.clientWidth || 500, height: root.clientHeight || 200,
      layout: {background:{color:'#0b1019'}, textColor:'#9097a9', attributionLogo:false},
      grid: {vertLines:{color:'#1b2330'}, horzLines:{color:'#1b2330'}},
      rightPriceScale:{borderColor:'#293241'},
      timeScale:{borderColor:'#293241', timeVisible:true, secondsVisible:spread},
      handleScroll:true, handleScale:true,
    });
    const series = spread
      ? chart.addSeries(LightweightCharts.LineSeries, {color:'#8b6cff', lineWidth:2,
          priceFormat:{type:'price', precision:3, minMove:.001}})
      : chart.addSeries(LightweightCharts.CandlestickSeries, {
          upColor:'#0A9D61', downColor:'#DA2647', borderUpColor:'#0A9D61',
          borderDownColor:'#DA2647', wickUpColor:'#0A9D61', wickDownColor:'#DA2647'});
    const observer = new ResizeObserver(() => {
      if (root.clientWidth && root.clientHeight) chart.resize(root.clientWidth, root.clientHeight);
    });
    observer.observe(root);
    return {chart, series, observer, lastTime:0, lastCandle:null};
  }

  function chartCandles(raw) {
    if (!Array.isArray(raw)) return [];
    const byTime = new Map();
    for (const item of raw) {
      const time = Math.floor(Number(item[0]) / 1000);
      const [open, high, low, close] = item.slice(1, 5).map(Number);
      if (time > 0 && [open, high, low, close].every(v => Number.isFinite(v) && v > 0))
        byTime.set(time, {time, open, high, low, close});
    }
    return [...byTime.values()].sort((a, b) => a.time - b.time);
  }

  async function loadDetail(row) {
    const coin = row.coin, event = row.event;
    const detail = document.getElementById('arcus-detail');
    if (!detail) return;
    const generation = ++detailGeneration;
    detailLoading = true;
    detailRetryAt = 0;
    loadedCoin = '';
    if (detailAbort) detailAbort.abort();
    detailAbort = new AbortController();
    const controller = detailAbort;
    const timeout = setTimeout(() => controller.abort(), 8000);
    detail.hidden = false;
    document.body.classList.add('arcus-arb-selected');
    status(tr()[4]);
    try {
      if (!await ensureCharts()) throw Error('charts unavailable');
      if (generation !== detailGeneration || selectedCoin !== coin || selectedEvent !== event) return;
      if (chartHandles) stopChartsOnly();
      await new Promise(requestAnimationFrame);
      if (generation !== detailGeneration || selectedCoin !== coin || selectedEvent !== event) return;
      chartHandles = {
        spread:makeChart('arcus-spread-chart', true),
        arcus:makeChart('arcus-mark-chart', false),
        okx:makeChart('arcus-reference-chart', false),
      };
      if (Object.values(chartHandles).some(handle => !handle)) throw Error('chart mount');
      const handles = chartHandles;
      const query = `symbol=${encodeURIComponent(row.arcusSymbol)}&interval=1m&limit=500`;
      const [eventReply, arcusReply, okxReply] = await Promise.all([
        fetch('/api/arcus/event?coin=' + encodeURIComponent(coin), {signal:controller.signal}),
        fetch('/api/charts/klines?' + query + '&exchange=arcus_futures', {signal:controller.signal}),
        fetch('/api/arcus/reference_klines?' + query, {signal:controller.signal}),
      ]);
      if (![eventReply, arcusReply, okxReply].every(reply => reply.ok)) throw Error('chart data');
      const [history, arcusRaw, okxRaw] = await Promise.all([eventReply.json(), arcusReply.json(), okxReply.json()]);
      if (generation !== detailGeneration || selectedCoin !== coin || selectedEvent !== event || chartHandles !== handles) return;
      if (history.event !== event || !history.active) throw Error('event closed');
      const byTime = new Map();
      for (const point of history.points || []) {
        const time = Math.floor(Number(point.time) / 1000), value = Number(point.differencePercent);
        if (time > 0 && Number.isFinite(value)) byTime.set(time, value);
      }
      const points = [];
      let previous = 0;
      for (const [time, value] of [...byTime.entries()].sort((a, b) => a[0] - b[0])) {
        if (previous && time - previous > 2) points.push({time:previous + 1}); // source-time gap
        points.push({time, value});
        previous = time;
      }
      if (!points.length && row.differencePercent != null) points.push({time:Math.floor(row.markTime / 1000), value:row.differencePercent});
      chartHandles.spread.series.setData(points);
      chartHandles.spread.lastTime = points.length ? points[points.length - 1].time : 0;
      for (const [kind, raw] of [['arcus', arcusRaw], ['okx', okxRaw]]) {
        const candles = chartCandles(raw);
        chartHandles[kind].series.setData(candles);
        chartHandles[kind].lastCandle = candles[candles.length - 1] || null;
        chartHandles[kind].chart.timeScale().fitContent();
      }
      chartHandles.spread.chart.timeScale().fitContent();
      loadedCoin = coin;
      status('');
      updateLive(row);
    } catch (_) {
      if (generation === detailGeneration && selectedCoin === coin && selectedEvent === event) {
        status(tr()[4]);
        detailRetryAt = Date.now() + 2000;
      }
    } finally {
      clearTimeout(timeout);
      if (detailAbort === controller) detailAbort = null;
      if (generation === detailGeneration) detailLoading = false;
    }
  }

  function stopChartsOnly() {
    if (!chartHandles) return;
    for (const handle of Object.values(chartHandles)) {
      try { handle.observer.disconnect(); handle.chart.remove(); } catch (_) {}
    }
    chartHandles = null;
  }

  function updateLive(row) {
    if (!chartHandles || loadedCoin !== row.coin || !row.fresh || row.differencePercent == null) return;
    const time = Math.floor(Math.max(row.markTime, row.referenceTime) / 1000);
    if (time >= chartHandles.spread.lastTime) {
      try {
        if (chartHandles.spread.lastTime && time - chartHandles.spread.lastTime > 3)
          chartHandles.spread.series.update({time:chartHandles.spread.lastTime + 1});
        chartHandles.spread.series.update({time, value:row.differencePercent});
        chartHandles.spread.lastTime = time;
      } catch (_) {}
    }
    const spreadTitle = document.getElementById('arcus-spread-title');
    if (spreadTitle) spreadTitle.textContent = row.arcusSymbol + ' · Arcus / ' + row.okxInstId + ' · ' +
      (row.differencePercent > 0 ? '+' : '') + row.differencePercent.toFixed(3) + '%';
    const markTitle = document.getElementById('arcus-mark-title');
    if (markTitle) markTitle.textContent = 'Arcus ' + money(row.mark) + ' · ' + (row.simulated ? tr()[0] : tr()[1]);
    const referenceTitle = document.getElementById('arcus-reference-title');
    if (referenceTitle) referenceTitle.textContent = row.okxInstId + ' · mark ' + money(row.reference) + ' · trading candles';
  }

  function notice(snapshot) {
    const text = !snapshot ? tr()[4] : snapshot.status === 'off' ? tr()[1] : tr()[0];
    for (const id of ['ch-chart-arcus-note', 'ch-scr-arcus-note']) {
      const node = document.getElementById(id);
      if (node) node.textContent = text;
    }
  }

  function render(snapshot, unavailable) {
    const root = document.getElementById('arcus-arb');
    if (!root) return;
    root.replaceChildren();
    if (unavailable) {
      root.hidden = false;
      const title = document.createElement('strong');
      title.textContent = tr()[2];
      const body = document.createElement('span');
      body.textContent = tr()[4];
      root.append(title, body);
      if (selectedCoin) status(tr()[4]);
      return;
    }
    const active = snapshot && snapshot.status !== 'off'
      ? snapshot.markets.filter(row => row.event && (row.phase === 'lag' || row.phase === 'recovery'))
      : [];
    const selected = active.find(row => row.coin === selectedCoin);
    const selectedSource = snapshot && snapshot.markets.find(row => row.coin === selectedCoin);
    if (selectedCoin && (snapshot?.status === 'off' || (selectedSource && selectedSource.fresh && !selected)))
      stopDetail(); // only a fresh terminal state (or status=off) closes the event
    root.hidden = !active.length;
    if (!active.length) { if (selectedCoin) status(tr()[4]); return; }
    let shouldLoad = false;
    if (!selectedCoin && !manualClosed) {
      selectedCoin = active[0].coin;
      selectedEvent = active[0].event;
      shouldLoad = true;
    } else if (selected && selected.event !== selectedEvent) {
      selectedEvent = selected.event;
      shouldLoad = true;
    }
    const header = document.createElement('div');
    header.className = 'arcus-arb-head';
    const title = document.createElement('strong');
    title.textContent = tr()[2];
    const note = document.createElement('span');
    note.textContent = tr()[3];
    header.append(title, note);
    root.append(header);
    const list = document.createElement('div');
    list.className = 'arcus-arb-list';
    active.sort((a, b) => (Number.isFinite(b.differencePercent) ? Math.abs(b.differencePercent) : -1) -
      (Number.isFinite(a.differencePercent) ? Math.abs(a.differencePercent) : -1));
    for (const row of active.slice(0, 24)) {
      const item = document.createElement('div');
      item.className = 'arcus-arb-item';
      if (row.coin === selectedCoin) item.classList.add('arcus-selected');
      const sym = document.createElement('b');
      sym.textContent = row.arcusSymbol;
      const phase = document.createElement('span');
      phase.textContent = row.phase === 'lag' ? tr()[5] : tr()[6];
      const prices = document.createElement('span');
      prices.textContent = row.fresh && row.mark != null && row.reference != null
        ? 'Arcus ' + money(row.mark) + ' / OKX ' + money(row.reference)
        : 'Arcus — / OKX —';
      prices.title = row.okxInstId + ' · ' + row.quote + '/' + row.referenceQuote;
      const diff = document.createElement('strong');
      if (row.fresh && Number.isFinite(row.differencePercent)) {
        diff.className = row.differencePercent >= 0 ? 'arcus-arb-positive' : 'arcus-arb-negative';
        diff.textContent = (row.differencePercent > 0 ? '+' : '') + row.differencePercent.toFixed(3) + '%';
      } else diff.textContent = '—';
      item.append(sym, phase, prices, diff);
      item.addEventListener('click', () => {
        if (selectedCoin === row.coin && selectedEvent === row.event) return;
        manualClosed = false;
        selectedCoin = row.coin;
        selectedEvent = row.event;
        render(snapshot, false);
        loadDetail(row);
      });
      list.append(item);
    }
    root.append(list);
    const current = active.find(row => row.coin === selectedCoin);
    if (current && current.fresh && (shouldLoad || (!loadedCoin && !detailLoading && Date.now() >= detailRetryAt)))
      loadDetail(current);
    else if (current && current.fresh) { updateLive(current); if (loadedCoin === current.coin) status(''); }
    else if (selectedCoin) status(tr()[4]);
  }

  function markUnavailable() {
    notice(null);
    if (last) render(last, true);
    window.dispatchEvent(new Event('arcus-unavailable'));
  }

  async function poll() {
    if (pending || document.hidden) return;
    const relevant = document.body.classList.contains('sec-arbitrage') ||
      document.body.classList.contains('arcus-market') ||
      (window._chHasArcusSlots && window._chHasArcusSlots());
    if (!relevant) return;
    pending = true;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);
    try {
      const response = await fetch('/api/arcus/snapshot', {cache:'no-store', signal:controller.signal});
      if (!response.ok) throw Error('Arcus batch ' + response.status);
      const snapshot = await response.json();
      if (!snapshot || snapshot.schemaVersion !== 1) throw Error('Arcus batch schema');
      if (!snapshot.markets.some(row => row.fresh)) throw Error('Arcus source stale');
      last = snapshot;
      lastSourceAt = Date.now();
      notice(snapshot);
      render(snapshot, false);
      window.dispatchEvent(new CustomEvent('arcus-snapshot', {detail:snapshot}));
    } catch (_) {
      markUnavailable(); // stale is unavailable, never a zero spread
    } finally {
      clearTimeout(timeout);
      pending = false;
    }
  }

  setInterval(poll, 2000);
  setInterval(() => {
    if (lastSourceAt && Date.now() - lastSourceAt > 6000) {
      lastSourceAt = 0;
      markUnavailable();
    }
  }, 1000);
  setInterval(async () => {
    if (pricePending || !selectedCoin || !last || document.hidden || !chartHandles || loadedCoin !== selectedCoin) return;
    const row = last.markets.find(m => m.coin === selectedCoin && m.fresh && m.event === selectedEvent);
    if (!row) return;
    const handles = chartHandles;
    const generation = detailGeneration;
    pricePending = true;
    const query = `symbol=${encodeURIComponent(row.arcusSymbol)}&interval=1m&limit=3`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 6000);
    try {
      const replies = await Promise.all([
        fetch('/api/charts/klines?' + query + '&exchange=arcus_futures', {signal:controller.signal}),
        fetch('/api/arcus/reference_klines?' + query, {signal:controller.signal}),
      ]);
      if (replies.some(reply => !reply.ok)) return;
      const [arcusRaw, okxRaw] = await Promise.all(replies.map(reply => reply.json()));
      if (selectedCoin !== row.coin || selectedEvent !== row.event ||
          chartHandles !== handles || detailGeneration !== generation) return;
      for (const [kind, raw] of [['arcus', arcusRaw], ['okx', okxRaw]]) {
        const bars = chartCandles(raw);
        if (!bars.length) continue;
        const handle = chartHandles[kind];
        for (const bar of bars) {
          if (handle.lastCandle && bar.time < handle.lastCandle.time) continue;
          handle.series.update(bar);
          handle.lastCandle = bar;
        }
      }
    } catch (_) {} finally { clearTimeout(timeout); pricePending = false; }
  }, 3000);
  const ordinary = document.getElementById('arb-feed');
  if (ordinary) ordinary.addEventListener('click', event => {
    if (event.target.closest('.arb-row')) { manualClosed = true; stopDetail(); }
  });
  window.addEventListener('arcus-snapshot-request', poll);
  setTimeout(poll, 0);
})();
