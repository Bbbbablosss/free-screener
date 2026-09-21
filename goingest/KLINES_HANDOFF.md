# KLINES MIGRATION & MULTI-EXCHANGE EXPANSION — HANDOFF

> Этот документ — единственная точка входа в задачу переписывания
> **graphs/klines** на Go и расширения покрытия до 25 бирж.
> Сначала прочитай этот файл целиком, потом смежные:
> [`goingest/HANDOFF.md`](HANDOFF.md) — про density (контекст архитектуры),
> память `vps-deployment.md` и `vps-linux-fixes.md`.

Дата создания: 2026-06-08. Обновлено: 2026-06-08 (после Phase 4 MVP + решение
о вариант А — Gateway).

---

## 0. С ЧЕГО НАЧАТЬ (TL;DR)

**Главная цель сейчас (Phase 5): написать `goingest-gateway` — Go WSocket-сервер
для браузеров клиентов.** Это сделает Python web почти ненужным в горячем пути.
Без этого — масштабирование до 25 бирж невозможно (Python web упирается в CPU
event loop). Подробности — раздел 5.

**Текущее состояние прода (2026-06-08):**
- ✅ Density-ингест: 5 Go-сервисов (bybit/binance/okx/gate/bitget),
  прибиты к ядрам 0+1 через `CPUAffinity`.
- ✅ Klines MVP: `goingest-klines` (один Go-процесс) работает на bybit
  (все ~1000 пар × 6 tf), пишет в `scr:klines` через **batched dedup
  flush 500мс** (`Bus.QueueKline`/`KlineFlushLoop`).
- ⚠️ Python web (`backend/charts/`) — ВРЕМЕННО получает события из шины и
  делает fanout клиентам. Это горячий путь, добавляет ~14% CPU при 1 бирже.
- ⚠️ Python WS-handlers в `live_klines.py` отключены для bybit через env
  `CHARTS_PY_DISABLED_EXCH=bybit` (drop-in в `screener.service.d/`).
- ✅ Warmer включён в режиме SQLite (Phase 6 готова к расширению), копит
  историю по 12 биржам, ~700k+ свечей и растёт.
- БД: `charts.db` SQLite, ~80 МБ.

**С чего НЕ начинать:**
- НЕ продолжать миграцию остальных 24 бирж klines пока Gateway не готов —
  при 25 биржах × ~1300 msg/sec через шину Python web задохнётся.
- НЕ ломать density-сервисы, их affinity, фикс `bus.py`.

**Архитектурное решение (2026-06-08):**
Делаем **вариант А — Gateway** (см. раздел 5). Полная миграция Python в Go
(вариант B) отвергнута: 40-55 ч работы ради ~10% доп. выигрыша.
Python остаётся для **холодных** задач: REST API, warmer, splash/arb.

---

## 1. ЗАЧЕМ ЭТО (мотивация)

1. **Снять нагрузку с Python web** — klines в Python съедают значительную долю
   CPU web-процесса; они также мешают broadcast'у плотностей и арбитража.
2. **Унификация архитектуры** — density уже на Go через шину, klines на Python
   через свои WS. После миграции — всё одинаково через Redis.
3. **Расширение арбитража** — `arb_detector` сейчас работает по 5 биржам density.
   После 25 бирж в klines (где идут trades) arb получит цены со всех 25 →
   значительно больше возможностей.
4. **Один процесс на 25 бирж** — экономия ресурсов, проще пиннинг к ядру.

---

## 2. АРХИТЕКТУРА

### Принцип разделения труда

```
┌─ Go (goingest-klines, 1 процесс) ────────────┐    ┌─ Python (web) ──────────────────────────┐
│                                                │    │                                          │
│ WS real-time на 25 бирж:                       │    │ ── приём из шины ────────────────────── │
│   ┌── каждое kline-update от биржи ──┐         │    │  scr:klines  → klines_cache.apply(...)  │
│   │                                   │         │ →  │  scr:trades  → klines_cache.on_trade(.)│
│   │ публикуется в:                    │         │ →  │                                          │
│   │  scr:klines  ← новый канал        │         │    │ ── REST history (warm.py) ────────────  │
│   │  scr:trades  ← уже существует     │         │    │  фоновый seeder по всем биржам          │
│   └──────────────────────────────────┘         │    │                                          │
│                                                │    │ ── броадкаст клиентам ──────────────────  │
│ (НЕ пишет в БД, НЕ делает REST)                │    │  WebSocket → throttle 50ms → клиент     │
└────────────────────────────────────────────────┘    │                                          │
                                                       │ ── БД (charts.db / postgres) ─────────  │
                                                       │  batched write закрытых свечей          │
                                                       └──────────────────────────────────────────┘
```

### Что НЕ меняется

- Тип хранилища: **SQLite сейчас (`charts.db`)**. См. секцию "БД и история".
- Python `klines_cache` (`backend/charts/service.py`) и его текущая логика
  `on_trade`/`push_live_candle`/`_update_price_buf`.
- Python `warm.py` REST-fetcher — расширяется новыми биржами, но логика
  background-сбора та же.
- Throttle между web и клиентом — 50мс (20 fps), уже стоит, не трогаем.
- Density-сервисы и их юниты, CPU affinity — не трогаем.

### Что добавляется

- Новый канал шины `scr:klines` (формат — см. раздел 4).
- Новый Go-процесс `goingest-klines` (новый systemd-юнит, новый pprof порт).
- 17 новых WS-коннекторов в Go (`goingest/klines_<exch>.go`) — для бирж
  которых сейчас нет в Python klines.
- 12+ новых REST-fetchers в Python (`backend/charts/fetcher.py`) для тех же
  новых бирж — чтобы warmer мог копить историю.
- В web (Python) — `bus.subscribe(CH_KLINES, klines_cache.apply_kline_event)`.

---

## 3. СПИСОК БИРЖ (25 = 24 если Lighter = Lighter DEX)

Уже работают в klines на Python (мигрируем как есть):
1. bybit
2. binance
3. okx
4. gate
5. bitget
6. mexc
7. bingx
8. kucoin
9. hyperliquid (через REST poll сейчас — нужно посмотреть, делать ли WS)

Новые (нужно реализовать WS + REST):
10. **HTX** (бывший Huobi) — крупная CEX, GZIP WS
11. **Kraken** — топ CEX, WS v2 OHLC channel
12. **Coinbase** — Advanced Trade WS, candles channel
13. **Bitfinex** — WS v2
14. **BitMEX** — первая perp-биржа, WS tradeBin
15. **Phemex** — WS kline
16. **BitMart** — WS kline
17. **AscendEX** — WS bar
18. **LBank** — WS kbar
19. **CoinW** — WS kline
20. **XT** — WS kline
21. **WEEX** — копирует Bybit API
22. **OrangeX** — копирует Bitget API style
23. **Toobit** — копирует Binance API
24. **Bitunix** — WS
25. **KCEX** — копия MEXC, WS
26. **JuCoin (JU)** — docs.jucoin.com, формат `kline@btc_usdt,5m`
27. **Lighter** (DEX, ZK rollup, WS API через их Hub) = Lighter DEX
28. **edgeX** (DEX, L2)
29. **Aster DEX** (DEX, WS API)
30. **Backpack** (молодая CEX, WS streams)
31. **Upbit** (Korea, **KRW**, не USDT, только графики, не в arb/density)
32. **Pionex** ← добавлен по запросу

Исключены / отложены:
- **OurBit** — нет публичной API-доки, WebFetch 403, WebSearch не находит.
  Вернуться когда найдём URL/доки.

**Итого: 30 в полном списке, 31 если считать Lighter и Lighter DEX как разные**.
Здесь "25 бирж от пользователя" + 8 уже существующих − пересечения. Точный счёт
после уточнений по дублям ниже.

Уточнения по бирж:
- **Hyperliquid** — DEX, есть WS, но в Python сейчас REST poll. Можно сразу на
  WS перевести при миграции.
- **mexc / kucoin / bingx** — уже в Python, при миграции — портируем WS на Go.
- **Lighter == Lighter DEX** — пользователь подтвердил, считать один продукт.
- **Aster DEX** — есть в Python `WARM_SOURCES` (`aster_futures/aster_spot`),
  но в `live_klines.py` коннектора нет. Только REST.

---

## 4. КОНТРАКТЫ ДАННЫХ

### Сообщения в шине

**Канал `scr:klines`** (новый):
```json
{
  "type": "kline_update",
  "exchange": "binance_futures",
  "symbol": "BTCUSDT",
  "tf": "1m",
  "candle": [1780605660000, "62500.5", "62550.0", "62480.0", "62540.0", "12.3456"]
}
```
- `candle` = `[ts_ms, open, high, low, close, volume]` (как в Python today)
- `exchange` = идентификатор в формате `<slug>_<futures|spot>` — совпадает с
  ключами `CHART_EXCH_MAP` в `backend/charts/constants.py`.
- Все числа — **строки** (как в текущем Python — сохраняет точность).
- Сообщение шлётся **на каждое обновление текущей свечи** + **при её закрытии**.

**Канал `scr:trades`** (уже существует, расширяется):
- goingest-klines публикует туда последнюю цену каждой пары всех 25 бирж.
- Текущий формат: `{"exch:sym:market": price, ...}` батчем раз в 500мс.
- Так arb получит цены со всех 25 бирж, не только density-5.

### Формат БД (без изменений)

```sql
CREATE TABLE chart_candles (
    key    TEXT NOT NULL,    -- "binance_futures:BTCUSDT:1m"
    ts_ms  BIGINT NOT NULL,
    open   TEXT NOT NULL,
    high   TEXT NOT NULL,
    low    TEXT NOT NULL,
    close  TEXT NOT NULL,
    volume TEXT NOT NULL,
    PRIMARY KEY (key, ts_ms)
);
CREATE TABLE series_meta (key TEXT PK, last_ts_ms BIGINT, updated_at BIGINT);
```

`TF_LIMITS` (`backend/charts/constants.py`):
```
1m: 10000,  5m: 8000,  15m: 5000,  1h: 3000,  4h: 3000,  1d: 3000
```

Это **rolling window** — старые свечи удаляются при превышении лимита.

### Кто пишет в БД

**Python web** (не Go). Поток:
1. Go-сервис batched отправляет закрытые свечи в `scr:klines`.
2. web подписан → батчит несколько секунд → одна транзакция.
3. ~200 writes/sec в пике (1m-свечи закрываются раз в минуту).

---

## 5. ПЛАН ФАЗ

### ✅ Phase 1 — CPUAffinity density (done 2026-06-08)

`CPUAffinity=0 1` для всех 5 density Go-сервисов. Освободили ядра 2-3.

### ✅ Phase 2 — Research (мостою сделано)

- ✅ JuCoin — `docs.jucoin.com`, формат `kline@btc_usdt,5m`.
- ⏳ OurBit — публичной доки не найдено, исключён до URL от user.
- Остальные крупные (Kraken/Coinbase/Bitfinex/HTX) — известны.

### ✅ Phase 3 — Скаффолд `goingest-klines` (done 2026-06-08)

Файлы (в репо и на VPS):
- `goingest/klines.go` — типы `klineMsg`, health logger.
- `goingest/klines_bybit.go` — bybit WS handler.
- `goingest/bus.go` — `Bus.QueueKline()` + `Bus.KlineFlushLoop()` (batched dedup 500мс).
- `goingest/main.go` — env `INGEST_MODE=klines` запускает klines-режим.
- `deploy/goingest-klines.service` — pprof :6066, `CPUAffinity=2`, `GOGC=500`.
- `backend/bus.py` — `CH_KLINES = "scr:klines"`.
- `backend/main.py` — `bus.subscribe(CH_KLINES, klines_cache.apply_kline_event)`.
- `backend/charts/service.py` — `apply_kline_event()` + fast-path skip
  (если нет watcher И нет cache — пропускаем).
- `backend/charts/live_klines.py` — `_PY_DISABLED_EXCH` set из env
  `CHARTS_PY_DISABLED_EXCH` (для отключения Python WS на биржу-в-Go).

### ✅ Phase 4 — bybit klines MVP (done 2026-06-08)

bybit klines работает в проде: ~1000 пар × 6 tf, batched flush 500мс,
end-to-end проверено (Go → Redis → Python → Browser). CPU goingest-klines
~12% одного ядра, Python web +14% от klines.

**Найдено узкое место** (важный вывод): Python web fanout — при масштабировании
на 25 бирж не помещается на одно ядро event loop'а. Решение → Phase 5 ниже.

### 🚀 Phase 5 — `goingest-gateway` (СЛЕДУЮЩАЯ — приоритет 1)

**Цель**: вынести WebSocket-сервер для клиентов на Go. Python перестаёт
участвовать в горячем пути.

**См. раздел 6 ниже — там подробный план Gateway.**

### Phase 6 — Порт остальных существующих klines-коннекторов

**Прогресс 2026-06-08 (текущая сессия):**
- ✅ **bybit** (`klines_bybit.go`) — был сделан в Phase 4
- ✅ **binance** (`klines_binance.go`) — combined-stream, **HARD LIMIT 200 streams/conn** (33 syms × 6 tf). Снижение → "bad handshake" rejects.
- ✅ **okx** (`klines_okx.go`) — subscribe-based, 50 syms × 6 tf = 300 subs/conn, text "ping" keepalive.
- ✅ **gate** (`klines_gate.go`) — per-symbol-tf subscribes, `futures.candlesticks`/`spot.candlesticks`, ts in SECONDS (×1000). gate spot=2036 pairs (большой объём).
- ⏳ **bitget** — TODO. Похож на bitget.go (density): `candle1m` channel, instType=USDT-FUTURES/SPOT.
- ⏳ **mexc** — TODO. Не было density для mexc. Python `_MexcHandler` есть в `backend/charts/live_klines.py`.
- ⏳ **bingx** — TODO. Не было density. Python `_BingXHandler` есть.
- ⏳ **kucoin** — TODO. Не было density. Python `_KuCoinHandler` есть.

**4 биржи на Go (bybit+binance+okx+gate, ~5400 пар) = 10.9% CPU, 286MB RSS.**
Прогноз для полных 8 бирж: ~20-25% CPU.

После каждой новой биржи:
1. Обновить drop-in `/etc/systemd/system/goingest-klines.service.d/exchanges.conf`:
   `Environment=INGEST_EXCHANGES=bybit,binance,okx,gate,<new>`
2. Обновить drop-in `/etc/systemd/system/screener.service.d/disabled-klines.conf`:
   `Environment=CHARTS_PY_DISABLED_EXCH=bybit,binance,okx,gate,<new>`
3. `systemctl daemon-reload && systemctl restart goingest-klines screener`

По одному, тестируем каждый. Каждый раз:
1. Реализовать `goingest/klines_<exch>.go`.
2. Добавить в `startKlinesExchange()` в `main.go`.
3. Включить в `INGEST_EXCHANGES=...` в `goingest-klines.service`.
4. Установить env `CHARTS_PY_DISABLED_EXCH=...<exch>` в drop-in
   `/etc/systemd/system/screener.service.d/disabled-klines.conf` чтобы
   Python не открывал WS для этой биржи.
5. Verify в браузере — графики живые и не зависают.

### Phase 7 — Новые klines-коннекторы (17-18 штук)

Группами по сходству протокола:
- **Bybit-style** (subscribe + known): WEEX, OrangeX, KCEX
- **Binance-style** (combined-stream URL): Toobit, BitMart, Phemex, Bitunix
- **OKX/Bitget-style** (subscribe args): HTX, AscendEX, LBank, CoinW, XT
- **Уникальные**: Coinbase, Kraken, Bitfinex, BitMEX, Upbit, Backpack,
  JuCoin, Lighter, edgeX, Aster DEX, Pionex

Каждый коннектор:
1. WS-handler в `goingest/klines_<exch>.go`.
2. REST-fetcher в `backend/charts/fetcher.py` (для warmer-а).
3. Добавление в `WARM_SOURCES` и `KNOWN_EXCHANGES` в `constants.py`.
4. Добавление в `CHART_EXCH_MAP` (frontend ID).
5. Маппинг таймфреймов `TF_<EXCH>` если формат отличается.

### Phase 8 — Удалить Python `live_klines.py` целиком

Когда все 25+ бирж работают через Go, Python WS-handlers больше не нужны.
REST-fetcher (`fetcher.py`, `warm.py`) **остаётся** в Python.

### ✅ Phase 9 — Background-warmer на SQLite (done 2026-06-08)

Раньше был отключён на SQLite (см. раздел 7). Сейчас **включён**:
- `WARM_CONCURRENCY = 1`, `WARM_REQUEST_DELAY = 1.0` — очень consrvative.
- В `warm.py:run_seeder()` убрана проверка backend == "postgres".
- Density больше не пишет в SQLite из web (она на Go) — главная причина
  старого ограничения исчезла.
- За первый час работы накопилось ~700k+ свечей; растёт постепенно.

### Phase 10 — Тесты нагрузки + tuning

Когда все Phase 5-8 готовы — снять CPU/RAM на всех 25 биржах под пиковой
нагрузкой. Целевые цифры см. раздел 0 (4 ядра / ~150-200% совокупно).

---

## 6. АРХИТЕКТУРА GATEWAY (Phase 5 — детальный план)

### Зачем

При полной миграции klines на Go без Gateway:
- 25 бирж × ~1300 msg/sec = ~30k msg/sec через шину
- Python web event loop задыхается (33-50%+ CPU steady, до 100% в пиках)
- Клиенты отключаются по таймауту → "графики виснут" (мы это видели в Phase 4)

С Gateway:
- Go-сервис открывает WS-сервер для браузеров
- Подписан на все каналы шины (`scr:events`, `scr:klines`, `scr:trades` и т.д.)
- Прямо шлёт клиентам без участия Python
- Python остаётся только для холодных задач (REST API, warmer, splash, arb)

**Прогноз нагрузки после Gateway:**
- Python web: ~5-10% steady (вместо ~30%)
- goingest-gateway: ~10-15% (fanout 25 бирж к ~10-100 клиентам)
- core 2: klines+gateway = ~25-35% total
- core 3: Python web — почти свободен

### Архитектура

```
браузер (https://cryptoscreener.live)
   ↓
nginx (TLS termination + upgrade headers)
   ↓
   /ws  → goingest-gateway :7000 (Go, новый сервис)  ← НОВЫЙ ПУТЬ
   /api → uvicorn :8000 (Python, REST only)
   ↓
goingest-gateway:
  - Принимает WSocket-коннекты от клиентов
  - Парсит client-команды (chart_sub, chart_unsub и т.д.)
  - Подписан на Redis шину (scr:events, scr:klines, scr:trades, scr:splash...)
  - Управляет chart_subs / ws_chart мапами в RAM
  - Fanout пакетов клиентам с throttle 50мс per chart_key
```

### Файлы для реализации

1. **`goingest/gateway.go`** — основной WS-сервер:
   - HTTP listener на `:7000`
   - WSocket upgrader (gorilla/websocket)
   - Per-client read/write goroutines
   - Маршрутизация команд от клиента: `chart_sub`, `chart_unsub`, `density_sub`,
     `splash_sub` и т.д. (повторить wire format из Python)

2. **`goingest/gateway_state.go`** — состояние:
   - `chart_subs map[string]map[*Client]bool` — kline-watcher rooms
   - `density_clients map[*Client]bool` — кто подписан на density events
   - `trade_clients ...`, и т.д.
   - Mutex для safety (или per-room channels)

3. **`goingest/gateway_busloop.go`** — приём из шины:
   - subscribe на `scr:events`, `scr:klines`, `scr:trades`
   - per-msg: lookup get watchers → fanout

4. **`deploy/goingest-gateway.service`** — новый systemd-юнит:
   - `Environment=REDIS_ADDR=127.0.0.1:6379`
   - `Environment=LISTEN_ADDR=:7000`
   - `CPUAffinity=2` (вместе с goingest-klines)
   - `MemoryMax=600M`

5. **nginx конфиг** — изменить `/ws` upstream:
   - `proxy_pass http://localhost:7000;` (вместо 8000)
   - WS upgrade headers (Connection: upgrade, Upgrade: websocket)
   - `proxy_read_timeout 3600s` (для long-lived WS)

6. **`backend/static/index.html`** — frontend (если есть жёсткий URL):
   - Если URL `/ws` фиксированный — менять не нужно (nginx переключит).
   - Если URL переменный (ENV или config) — проверить.

7. **Python web — что НЕ убирать**:
   - Оставить `bus.subscribe(CH_EVENTS, ...)` пока splash/arb там
   - REST API endpoints
   - Warmer
   - НО **можно удалить** apply_kline_event subscribe — Gateway сам шлёт.

### Контракт сообщений (клиент ↔ Gateway)

Клиент шлёт текстовые JSON команды (как сейчас Python WS принимает):
```json
{"action":"chart_sub", "exchange":"bybit_futures", "symbol":"BTCUSDT", "tf":"1m"}
{"action":"chart_unsub", "exchange":"bybit_futures", "symbol":"BTCUSDT", "tf":"1m"}
{"action":"chart_history", "exchange":"...", "symbol":"...", "tf":"...", "before_ts": 17806...}
```

Gateway шлёт клиенту те же типы сообщений что и сегодня Python:
```json
{"type":"kline_update", "exchange":"...", "symbol":"...", "tf":"...", "candle":[...]}
{"type":"klines_data", ...}
{"type":"klines_full", ...}
{"type":"klines_history", ...}
{"type":"density_new_batch", ...}
{"type":"density_remove_batch", ...}
{"type":"density_pct_batch", ...}
{"type":"density_sync", ...}
```

**Это значит протокол клиент-сервер не меняется** — фронт не нужно править,
он не видит разницы.

### Что Gateway НЕ умеет (и должен делегировать Python)

- `chart_history` — клиент просит исторические свечи (для пагинации scroll).
  Это REST → БД lookup. Gateway может:
  - **Вариант 1**: проксировать в Python REST `/api/charts/klines?...`
  - **Вариант 2**: читать БД напрямую в Go (sqlite driver — простая read-операция)
  - Рекомендую вариант 2 — меньше hops.

- `splash`, `arb` — Python шлёт их в `scr:splash`, `scr:arb` каналы.
  Gateway просто пересылает.

### Чек-лист реализации Phase 5

1. ⬜ Создать `goingest/gateway.go` (gorilla/websocket HTTP listener)
2. ⬜ Реализовать `Client` struct + `read_pump` / `write_pump`
3. ⬜ Парсинг команд от клиента (`chart_sub`, ...)
4. ⬜ State manager (`chart_subs`, `density_clients`, ...)
5. ⬜ Bus loop (subscribe на каналы Redis, fanout)
6. ⬜ Throttle 50мс per chart_key (как в Python)
7. ⬜ `chart_history` handler — read из `charts.db` + REST в bybit/etc если нужно
8. ⬜ `deploy/goingest-gateway.service` юнит
9. ⬜ Изменить nginx конфиг — `/ws` → `:7000`
10. ⬜ Тест на dev — 1 клиент, проверить kline_update + density_new_batch
11. ⬜ Тест в проде — заменить nginx upstream, проверить graceful migration
12. ⬜ Удалить `bus.subscribe(CH_KLINES, ...)` из `backend/main.py` (опционально)

### Возможные ловушки

- **TLS termination**: nginx делает TLS, Gateway работает по plain HTTP — это
  норма. Gateway не нужно знать про SSL.
- **CORS**: если фронт обращается с другого origin (test setup) — добавить
  `Access-Control-Allow-Origin` в Gateway.
- **WebSocket ping/pong**: gorilla/websocket делает автоматически — НО надо
  установить read/write deadlines иначе зависшие коннекты накапливаются.
- **Backpressure**: если клиент медленный, write_pump может блокировать. Решение
  — bounded send-channel, при переполнении дропать клиента.
- **Goroutine leak**: на каждого клиента 2 горутины (read+write). При 1000
  клиентов = 2000 горутин — норма. При 100000 — нужна оптимизация (epoll loop).

### Оценка времени

~6-8 часов чистой работы для рабочего MVP с bybit klines + density events.
Покрытие остальных бирж и каналов — дополнительные часы.

---

## 7. БД И ИСТОРИЯ

> Историческое примечание: раньше warmер был отключён на SQLite. Сейчас
> (2026-06-08) он **включён** — см. Phase 9 в плане. Текущие настройки:
> `WARM_CONCURRENCY=1`, `WARM_REQUEST_DELAY=1.0`, без backend-gate.
> Это работает потому что density больше не в Python (она в Go), и SQLite
> single-writer-bottleneck с warmer'ом не пересекается.

В `warm.py` БЫЛО жёсткое условие (теперь убрано):

```python
if chart_db.backend() != "postgres":
    logger.warning("[seeder] bulk pre-seed DISABLED on %s backend — charts fill "
                   "on-open via expand. Set CHART_DATABASE_URL=postgres to enable...")
    return
```

**Что это значит:** на SQLite background warmер **отключён**. Сейчас единственный
путь наполнения БД — **on-open**: когда клиент в браузере открывает график на паре
X, web fetch'нет ~10k свечей для этой пары и кэширует. Других путей нет.

**Почему отключено на SQLite:**
- SQLite — single-writer
- При bulk seeding 13k серий все писатели стоят в очереди
- Detection scan (плотности) тоже пишет → ему 13-16с вместо 3с
- На 1000 параллельных записей SQLite ставится колом

**Варианты решения:**

| | sqlite + on-open (сейчас) | sqlite + slow warm | PostgreSQL |
|---|---|---|---|
| код | без правок | убрать gate в warm.py + concurrency=1 | новая миграция |
| скорость | пары наполняются когда юзер открывает | медленно (недели) | быстро (часы) |
| риск | низкий | детекция может тормозить | прерывание работы для миграции |
| долгосрочно | плохо | сойдёт | правильно |

**Текущая рекомендация (баланс):** оставить on-open сейчас. Добавить REST-fetcher
для всех новых бирж в Python (так они будут работать когда юзер откроет график).
PostgreSQL миграция — отдельный трек, обсудить позже.

---

## 7. КОНКРЕТНЫЕ ПАРАМЕТРЫ

- Throttle web→клиент: **50мс** (20 fps), уже стоит, не меняем.
- `TF_LIMITS` — без изменений (см. раздел 4).
- WARM_CONCURRENCY = **2** (если включаем), WARM_REQUEST_DELAY = **0.25с**.
- В Go klines-коннекторах: один WS-конн на батч из 100-500 подписок
  (зависит от лимита биржи).
- `GOGC=500`, как в density-сервисах.
- pprof :6066 для klines-сервиса.

---

## 8. ИЗВЕСТНЫЕ ОСОБЕННОСТИ ПО БИРЖАМ

- **Upbit** — KRW-пары, не USDT. Только в klines/графиках, не в density/arb.
  В фильтре символов — `KRW_PAIRS_ONLY=true` для этой биржи.
- **Hyperliquid** — DEX, есть WS, но Python сейчас REST poll. При миграции
  лучше сразу WS.
- **Binance** — была история IP-банов из-за REST шторма на снапшоты. Любой
  REST-fetcher (warmer) к binance — с жёстким троттлингом. Сейчас в Go-
  density используется глобальный rate-limit `binanceSnapLimiter` (~1/350мс).
- **OKX spot** — silently drops подписки больше ~30/conn (см. `goingest/okx.go`).
- **bybit** — `orderbook.500` не поддерживается, max 200. Для klines таких
  ограничений нет, но WS-rate-limit handshake такой же — нужен stagger.
- **gate** — futures.order_book уровни как `{p,s}` dict, spot — `[p,q]` массив;
  для klines (`futures.candlesticks` / `spot.candlesticks`) формат единый.

---

## 9. ЧТО НЕЛЬЗЯ ЛОМАТЬ

1. **Density-сервисы** (goingest, -binance, -okx, -gate, -bitget) и их
   `CPUAffinity=0 1`. Контракты в `scr:events` и `scr:trades` — стабильны.
2. **Web bus.py fix** (выделенный pubsub-клиент без `socket_timeout`, см.
   `vps-deployment.md`). Если будешь править `bus.py` — не сломай этот фикс.
3. **`live_klines.py` Python WS-коннекторы** — не выключать пока Go не отлажен.
4. **`charts.db`** — schema, индексы. Только добавлять данные.
5. **`backend/charts/service.py:on_trade`** — обновление close при каждом
   трейде — обязательно для "живых" графиков.

---

## 10. ROADMAP ДАЛЬШЕ (после klines)

После завершения klines:

- **Arb-detector расширение** — сейчас работает по 5 биржам, после klines будет
  возможность по 25.
- **Alerts/нотификации** — пользователь упоминал. Не в текущем scope.
- **Listings-страница** — `index.html` → tab `Listings`. Не в текущем scope.
- **PostgreSQL миграция** — для масштабирования БД (см. раздел 6).
- **Web tier scaling (5k+ юзеров)** — отдельный backlog. Сейчас 1 web-процесс,
  при росте — N процессов за load-balancer.

---

## 11. КОМАНДЫ ДЛЯ VPS

```bash
# Статус всех сервисов
systemctl status goingest goingest-binance goingest-okx goingest-gate goingest-bitget goingest-klines screener redis-server

# Логи klines-сервиса
journalctl -u goingest-klines -f

# Размер БД
ls -lh /opt/screener/charts.db*

# Сколько свечей в БД
sqlite3 /opt/screener/charts.db "SELECT COUNT(*) FROM chart_candles"

# Сколько серий
sqlite3 /opt/screener/charts.db "SELECT COUNT(*) FROM series_meta"

# CPU per service (15s precise)
HZ=$(getconf CLK_TCK)
for s in goingest goingest-binance goingest-okx goingest-gate goingest-bitget goingest-klines; do
  PID=$(systemctl show -p MainPID --value $s)
  S1=$(awk "{print \$14+\$15}" /proc/$PID/stat)
  sleep 15
  S2=$(awk "{print \$14+\$15}" /proc/$PID/stat)
  echo "$s: $(echo "scale=1; ($S2-$S1)/$HZ/15*100" | bc)%"
done
```

---

## 12. ССЫЛКИ НА СМЕЖНЫЕ ФАЙЛЫ

- [`goingest/HANDOFF.md`](HANDOFF.md) — про density (контекст архитектуры,
  fastjson, recv-bound, memory bus).
- `backend/charts/constants.py` — `TF_LIMITS`, `WARM_SOURCES`, `CHART_EXCH_MAP`,
  TF-маппинги per биржа.
- `backend/charts/live_klines.py` — текущие Python WS-коннекторы (8 шт).
- `backend/charts/fetcher.py` — REST-fetchers (12 функций).
- `backend/charts/warm.py` — фоновый seeder (отключён на SQLite, см. секцию 6).
- `backend/charts/service.py` — RAM cache + on_trade + broadcast.
- Память (на dev-машине пользователя):
  - `vps-deployment.md` — про сервер, баги, состояние прода
  - `vps-linux-fixes.md` — про Linux-specific фиксы

---

## 13. 🔧 ИЗВЕСТНЫЕ БАГИ ГРАФИКОВ (приоритет 1 для следующего сеанса)

Эти проблемы существовали ДО Phase 4/5 (миграции klines на Go и запуска
Gateway). Они НЕ связаны с моими изменениями — это **frontend-рендеринг**
или **контракты данных**. Подтверждено пользователем: "и на питоне то же
самое было".

### Симптомы (скриншот пользователя 2026-06-08 ~15:45)

Layout: 4 графика одновременно на сайте, биржа bybit_futures, tf=1m.

1. **BTCUSDT, EPICUSDT — свечи не обновляются**, график "стоит". История
   подгружена, но live-апдейтов нет.
2. **NEARUSDT — резкий вертикальный спайк до ~2.2** в самой последней
   свече, хотя цена там стабильна ~2.13. Артефакт похож на проблему
   с close-перезаписью аномальным значением.
3. **HYPEUSDT — окно пустое целиком**, графика нет.

### Что НЕ может быть причиной (исключено):
- Gateway (только что задеплоен, эти баги были и до него)
- goingest-klines (он шлёт данные в шину, ловится тем же Gateway)
- Python apply_kline_event (он закомментирован после Gateway)

### Гипотезы причин (по приоритету диагностики)

**HYPOTHESIS-1 (наиболее вероятная): Frontend шлёт `chart_sub` только
для одного активного графика, не для всех 4 панелей.**
Проверка: посмотреть в `backend/static/index.html`, как создаются
панели графиков и как они вызывают chart_sub. Открыть DevTools →
Network → WS → посмотреть какие messages шлёт фронт при открытии
макета 2×2.

**HYPOTHESIS-2: bybit WS snapshot vs delta не различаются.**
Проверка: в `goingest/klines_bybit.go` сейчас обрабатывается каждое
сообщение одинаково, не учитывая поле `type` в bybit-frame (там есть
"snapshot" и "delta"). При snapshot биржа может прислать "холодные"
данные которые перезатирают свежие в `klines_cache._store`. Решение:
парсить `type` и для snapshot — либо игнорировать (полагаться на
delta), либо использовать как fresh seed для пустого store.

**HYPOTHESIS-3: chart_sub в Gateway работает по принципу "один ws =
один key" (как в Python).**
Проверка: см. `goingest/gateway.go` функция `chartSub()` — она
moves WS с одного key на другой. То же поведение в Python
`backend/charts/service.py:chart_sub`. Если frontend хочет 4 графика
одновременно — нужно ИЛИ 4 WS-коннекта ИЛИ multi-key подписки на
один WS. Эта семантика **не моя**, она унаследована.

**HYPOTHESIS-4: REST `/api/charts/klines` возвращает пусто для HYPE.**
Проверка: `curl https://cryptoscreener.live/api/charts/klines?symbol=HYPEUSDT&interval=1m&exchange=bybit_futures&limit=300`.
Возможно charts.db не имеет HYPE данных (warmer ещё не дошёл).

### План диагностики (для следующего Клода)

1. **Открыть DevTools в Chrome/Yandex Browser** на cryptoscreener.live, перейти
   на раздел графиков с 2×2 layout. Network → WS → выбрать активную
   WSocket-сессию → Frames → смотреть всё что шлёт фронт.
2. Записать какие chart_sub команды идут — все 4 или только 1?
3. Если 1 → bug в frontend (`backend/static/index.html`), искать как
   панели создаются и должны ли они слать chart_sub каждая отдельно.
4. Если 4 → bug в Gateway/Python: текущая логика "один WS = один
   chart_key" заведомо не поддерживает 4 разных kлюча на одном WS.
   Нужно расширить `gwClient` чтобы держать **множество** chartKeys.
5. Для NEAR спайка: добавить в `goingest/klines_bybit.go` поле `Type`
   в bybitMsg parsing, проверить что именно приходит в момент спайка
   (timestamp когда был спайк → искать в журнале).
6. Для HYPE пустого: проверить REST endpoint, проверить есть ли HYPE
   в WARM_SOURCES/list_symbols, проверить что bybit его отдаёт.

### Статус багов (2026-06-08 20:30):

- ✅ **HYPOTHESIS-3 (multi-key chart_sub)** — fixed в `goingest/gateway.go`.
  `gwClient.chartKeys` теперь множество, не одиночный ключ. Frontend шлёт
  N chart_sub через 1 WS — все N теперь активно живые.
- ✅ **HYPOTHESIS-2 (NEAR spike)** — частично fixed в `goingest/klines_bybit.go`.
  При snapshot bybit с массивом OHLC из N свечей публикуем только с
  МАКСИМАЛЬНЫМ ts (defensive). Старые stale данные в `charts.db` остаются
  до перезаписи нормальным WS-потоком.
- ⚠️ **HYPOTHESIS-4 (HYPE пустой)** — данные ЕСТЬ. REST/instruments API
  возвращают HYPEUSDT правильно. Pipeline не виноват — это **frontend
  rendering issue** при initial load 9-слотового layout. Workaround:
  Ctrl+F5 или переключить layout.
- 🔧 **Новый баг (frontend, отдельная задача)**: при открытии многослотового
  layout (9 panes) не все слоты получают initial OHLC fetch. Логика в
  index.html выглядит корректной, но async callbacks (`_withSlotCtx` +
  fetch + REST race) дают timing issue. Чинится DevTools-debugging на
  стороне браузера, **не блокирует** дальнейшую миграцию klines.

### Phase 6 разрешена для старта (2026-06-08):

После multi-key fix Gateway корректно работает для всех слотов. Python
больше не bottleneck (20-23% CPU). Можно безопасно мигрировать
остальные biрж klines на Go.
- НЕ удалять Python `live_klines.py` — он сейчас служит fallback'ом
  для бирж кроме bybit.
- НЕ трогать `goingest-gateway` без понимания что в нём.

### Где смотреть код

- `backend/static/index.html` — frontend (огромный single-file, ищи
  по "chart_sub", "createChart", "WebSocket")
- `backend/charts/service.py:chart_sub` — Python WS handler (ссылка
  семантики; Gateway повторяет её)
- `goingest/gateway.go:chartSub` — текущая Go-реализация (один key)
- `goingest/klines_bybit.go` — парсинг bybit WS, поле type не учтено
- `backend/charts/service.py:push_live_candle` — где ts comparison

### Когда баги починены — переходить к Phase 6

После починки графиков bybit, продолжить миграцию остальных бирж
klines на Go (см. раздел 5 Phase 6). Каждая биржа добавит ~5-10% в
goingest-klines и 0% в Python (через Gateway).

---

## 14. РЕЗЮМЕ ОДНОЙ ФРАЗОЙ (актуально 2026-06-08)

`goingest-klines` уже работает для bybit (Phase 4 done) — но прод-bottleneck
не Go, а **Python web fanout** к клиентам. **Следующий шаг — Phase 5:
написать `goingest-gateway` (Go WS-сервер для браузеров)**, чтобы убрать
Python из горячего пути. Потом — Phase 6 (порт остальных 24 бирж klines)
и Phase 7 (новые биржи). Throttle 50мс между Gateway и клиентом сохраняется.
Прогноз финальной нагрузки на 25 биржах: core 2 (klines+gateway) ~25-35%,
core 3 (Python REST+warmer+splash+arb) ~10%.
