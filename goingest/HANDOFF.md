# GO INGESTION REWRITE — HANDOFF (для новой сессии Claude)

> Этот файл — единственная точка входа. Прочитай его целиком, потом файлы в `goingest/`.
> Контекст по серверу/архитектуре — в памяти: `vps-deployment.md`, `vps-linux-fixes.md`.

---

## 0. С ЧЕГО НАЧАТЬ (самое важное действие)

**Go-сервис написан ПОЛНОСТЬЮ (включая `main.go`), но ещё НЕ собран на VPS и НЕ задеплоен.**
`main.go` дописан (см. раздел 5, шаг 1 — сделано). `deploy/goingest.service` тоже создан (шаг 4).
Локально Go-тулчейна нет → `go build`/`go vet` не прогонялись; первая реальная сборка — на VPS.

Осталась чисто деплойная последовательность (ничего из этого нельзя сделать из dev-окружения —
там SSH-ключ не работает, деплой идёт через PuTTY plink/pscp с паролем):
`go mod tidy && go build` на VPS → установить `goingest.service` → **убрать bybit из ЖИВОГО
Python-воркера** → сравнить CPU. Детали — раздел 5.

⚠️ **КРИТИЧНО (дрейф конфигов):** файлы `deploy/screener-worker*.service` в РЕПО — устаревшая
2-воркерная схема и НЕ совпадают с живым VPS. На VPS (см. память `vps-deployment.md`) — **4 воркера**:
`gate` / `binance,bybit` / `okx,bitget` / `mexc,bingx,kucoin`. Значит **bybit на VPS живёт в
`screener-worker2` (`binance,bybit`)** — убирать его надо именно оттуда (станет `binance`), правя
ЖИВОЙ юнит на сервере, а НЕ репо-файл. Не делай `scp` репо-воркеров на VPS — затрёшь рабочую раскладку.

---

## 1. ЗАЧЕМ ЭТО (проблема)

Скринер плотностей крутится 24/7 на Vultr VPS (Linux, 4 ядра, IP в `vps-deployment.md`).
Узкое место — **НЕ детекция плотностей (она дешёвая), а ПРИЁМ данных**: парсинг WS-потоков
(WS-фреймы + JSON-декод + поддержание стаканов на каждом сообщении). Это нативный CPU.

Замеры (пик активного рынка, 5 бирж в плотностях): приём ≈ **3.3–3.5 ядра** Python.
Разбивка: gate ~60%, binance/okx ~85%, bitget ~90%, **bybit ~95–98% (самый тяжёлый)**.
Детекция (скан стаканов раз в 10с) — копейки.

**Негативный сценарий (1k юзеров + 15 бирж в графиках):** на чистом Python ≈ **5.5–6.5 ядер**
(нужен бокс за $80–160, и впритык). С Go-приёмом ≈ **2.5–3 ядра** → текущий 4-ядерный бокс ($40)
тянет с запасом. **Поэтому переписываем приём на Go** — это снимает 5–10x с самого жирного куска.

Go выбран вместо Rust (друг-прогер подтвердил: на Go проще, разница в нагрузке Go vs Rust
несущественна — главный выигрыш в уходе от Python). Решение пользователя: строить правильно
СЕЙЧАС, пока есть время, а не латать в панике потом.

---

## 2. АРХИТЕКТУРА (уже работает на Python — это фундамент)

Ключевое решение, принятое раньше: **Redis pub/sub шина** разделяет приём и отдачу. Благодаря
ей Go-сервис — это **drop-in замена** одного воркера, без переписывания остального.

```
[WORKER процессы]  --(Redis: scr:events / scr:trades)-->  [WEB процесс]
 ROLE=worker                                               ROLE=web
 приём WS + детекция плотностей                            подписка на шину +
 публикует события в шину                                  отдача в браузер +
                                                           арбитраж/splash/графики
```

- **ROLE=worker**: подключается к биржам, держит стаканы, детектит плотности, ПУБЛИКУЕТ в Redis.
  Несколько воркеров делят биржи через env `WORKER_EXCHANGES` (по ядрам).
- **ROLE=web**: ПОДПИСАН на Redis, применяет события к своему зеркалу состояния, отдаёт юзерам
  по WS. Тут же арбитраж, splash, графики (klines).
- Воркеры НЕ пишут в БД-состояние клиентам напрямую; всё через шину.

**Go-сервис встаёт на место bybit-воркера**: подключается к bybit, детектит, публикует в ТУ ЖЕ
шину в ТОМ ЖЕ формате. Web даже не знает, что данные пришли из Go. Python-воркеры остальных бирж
работают как работали.

Systemd-юниты воркеров: `deploy/screener-worker*.service` (+ drop-ins
`/etc/systemd/system/screener-worker*.service.d/limits.conf` с `MALLOC_ARENA_MAX=2`,
`MemoryMax=1500M`, `Restart=always`). Какой воркер сейчас обслуживает bybit — смотри
`WORKER_EXCHANGES` в юнитах на VPS.

---

## 3. ЧТО УЖЕ НАПИСАНО (`goingest/` — пофайлово)

Go-модуль `goingest`. Зависимости: `gorilla/websocket`, `redis/go-redis/v9`, JSON — stdlib.

| Файл | Назначение | Статус |
|---|---|---|
| `go.mod` | модуль + зависимости | ✅ готов (нужен `go mod tidy` → создаст `go.sum`) |
| `config.go` | порт порогов/RWA/исключений из `backend/config.py` 1:1 | ✅ готов |
| `bus.go` | Redis-публикация: `scr:events` + `scr:trades`, троттлинг трейдов | ✅ готов |
| `store.go` | потокобезопасное хранилище стаканов (price→volume_usd) | ✅ готов |
| `bybit.go` | WS-коннектор bybit (perp+spot): connect/subscribe/parse + сборка стакана | ✅ готов |
| `detect.go` | порт `detect_densities` + ВЕСЬ жизненный цикл плотностей + публикация событий | ✅ готов (+ `ActiveCount()` для диагностики) |
| `main.go` | точка входа: fetch символов, сборка всего, запуск, health-лог раз в 60с | ✅ готов (не собран на VPS) |

Детали:

- **`config.go`**: константы (`densityRangePct=10`, `densityMultiplier=50`, `rwaMultiplier=400`,
  `minDensityUSD=50000`, `nearLevels=20`, `densityMinAgeSec=30`, `detectionInterval=10s`,
  `staleSec=60`, `staleDensityTTL=90`, `recentlyRemovedTTL=60`, `matchTolPct=0.3`,
  `maxPromotionsPerCycle=300`, `missCountRemove=3`, `syncEveryCycles=6`). Карты `symbolMinUSD`,
  `rwaBases`, `excludedSymbols`. Функции `isRWA`, `minDensityFor`, `multiplierFor`.
  ⚠️ Если меняешь пороги в Python — меняй и тут (две копии правды).

- **`bus.go`**: `Bus.PublishEvent(v)` → `PUBLISH scr:events json(v)`. `QueueTrade(exch,sym,market,price)`
  копит последнюю цену по ключу; `TradeFlushLoop()` каждые 500мс шлёт батч в `scr:trades`.
  Адрес Redis передаётся в `NewBus(addr)`.

- **`store.go`**: `Store` = map `"exch:sym:market"`→`*Book`. `Apply(key, snapshot, b, a)` применяет
  bybit snapshot/delta (qty=0 удаляет уровень; иначе `book[price]=qty*price`). `Snapshot()` копирует
  все стаканы для скана детекции (по ~50 уровней, дёшево). Лок на карту + лок на каждый стакан.

- **`bybit.go`**: `runBybit(store,bus,wsURL,market,symbols,withTrades)` — батчит символы
  (100 топиков/конн; perp = 2 топика/символ → 50 символов, spot = 1 → 100), на каждый батч своя
  горутина с reconnect-циклом. `bybitConnect` — подписка чанками по 10 с retry по `req_id`
  (bybit отклоняет весь subscribe, если хоть один топик битый — повторяем по одному), keepalive
  `{"op":"ping"}` каждые 20с, парсинг `publicTrade` (→ `QueueTrade`, только цена) и `orderbook.50`
  (→ `store.Apply`). perp market="perp" (+trades), spot market="spot" (без trades).

- **`detect.go`**: `Density` struct с json-тегами = `Density.to_dict()` питона. `Detector.Loop()` —
  тик каждые 10с → `cycle()`. `cycle()` реплицирует `manager._run_detection`:
  - скан всех стаканов; пропуск устаревших (>`staleSec`) и `excludedSymbols`;
  - `detectDensities` на стакан (порт алгоритма: range±10%, near-levels медиана×mult vs minUsd);
  - матчинг raw против active/pending по цене ±0.3% (та же сторона); restore из `recentlyRemoved`
    по точной цене;
  - промоушен pending→active после 30с (кап 300/цикл); удаление active после 3 промахов → в
    `recentlyRemoved` (TTL 60с); sweep active с пропавшим стаканом (>90с);
  - **публикация**: `density_new_batch` (промоушены+restore), `density_remove_batch`, каждый цикл
    `density_pct_batch`, каждые 6 циклов `density_sync` (полный список — анти-десинк).

---

## 4. КОНТРАКТЫ (Go обязан повторять ТОЧЬ-В-ТОЧЬ)

Это то, что web ждёт из шины. Несоответствие = плотности не отрисуются / задвоятся.

**Канал `scr:events`** (JSON-объект на сообщение):
```
{"type":"density_new_batch",    "data":[<density.to_dict>, ...]}
{"type":"density_remove_batch", "data":["<id>", ...]}
{"type":"density_pct_batch",    "data":[{"id":"<id>","pct":<float3>,"vol":<int>}, ...]}
{"type":"density_sync",         "data":[<density.to_dict>, ...]}   // полный список активных
```
**`density.to_dict`** (поля строго эти): `id, symbol, exchange, market, side, price, volume_usd,
pct_from_price, three_min_vol, first_seen, last_seen, binance_f, miss_count`.
`id` = `"{exch}:{sym}:{market}:{side}:{uuid8hex}"`. `side` ∈ {`bid`,`ask`}. `market` ∈ {`perp`,`spot`}.

**Канал `scr:trades`**: `{"bybit:BTCUSDT:perp": 65000.0, ...}` (ключ→последняя цена), батч ~2/с.

Источники правды в питоне: `backend/bus.py`, `backend/screener/manager.py`
(`_broadcast`/`_run_detection`/`web_apply_event`), `backend/screener/state.py` (Density),
`backend/screener/density_detector.py` (`detect_densities`), `backend/screener/exchanges/bybit.py`
(протокол). Go-порт сверять с ними.

---

## 5. ЧТО НЕ СДЕЛАНО — БЛИЖАЙШИЕ ШАГИ

### Шаг 1 — написать `goingest/main.go` ✅ СДЕЛАНО
Написан. Что внутри (сверь, если что): читает `REDIS_ADDR` (плейн host:port) или `REDIS_URL`
(`redis://...`, как в питоне), дефолт `127.0.0.1:6379`; REST-фетч perp+spot символов с фильтрами
1:1 как `fetch_bybit_symbols`/`fetch_bybit_spot_symbols` (+ отсев `excludedSymbols` уже на фетче,
чтобы не подписываться зря); создаёт store/bus/detector; `go bus.TradeFlushLoop()`,
`runBybit(...,"perp",...,true)`, `runBybit(...,"spot",...,false)`, `go det.Loop()`; health-лог
раз в 60с (`books=… active_densities=…`, через `store.Len()` и атомарный `det.ActiveCount()`);
`select{}`. Исходные требования (для сверки):
1. Прочитать адрес Redis из env (`REDIS_URL`/`REDIS_ADDR`, дефолт `127.0.0.1:6379`).
2. Получить списки символов bybit через REST (порт `fetch_bybit_symbols` / `fetch_bybit_spot_symbols`):
   - perp: `GET https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000`,
     фильтр: `status=="Trading"`, `contractType=="LinearPerpetual"`, символ оканчивается на `USDT`,
     НЕ в `excludedSymbols`.
   - spot: `?category=spot&limit=1000`, фильтр: `status=="Trading"`, оканчивается на `USDT`,
     не в `excludedSymbols`.
3. Создать `store := NewStore()`, `bus := NewBus(addr)`, `det := NewDetector(store, bus)`.
4. Запустить: `go bus.TradeFlushLoop()`, `runBybit(store,bus,bybitPerpURL,"perp",perp,true)`,
   `runBybit(store,bus,bybitSpotURL,"spot",spot,false)`, `go det.Loop()`.
5. Заблокироваться (`select{}`).
   Желательно: лог раз в 30–60с (кол-во стаканов / active плотностей) для диагностики.

### Шаг 2 — собрать на VPS
Проще собрать НА сервере (Linux), чем кросс-компилить с Windows.
```
# на VPS: поставить Go (apt install golang  ИЛИ официальный tarball — версия >= 1.22)
# скопировать папку goingest/ на VPS (scp)
cd goingest && go mod tidy && go build -o goingest .
```
(Кросс-компиляция с Windows тоже ок: `GOOS=linux GOARCH=amd64 go build`.)

### Шаг 3 — отключить bybit в ЖИВОМ Python-воркере
**КРИТИЧНО.** Иначе bybit-плотности будут публиковать ОБА (Python + Go) → дубли/конфликты.
⚠️ Репо-файлы `deploy/` устарели (см. раздел 0). На ЖИВОМ VPS bybit — в `screener-worker2`
(`WORKER_EXCHANGES=binance,bybit`). Правь юнит ПРЯМО на сервере:
`binance,bybit` → `binance`, затем `systemctl daemon-reload && systemctl restart screener-worker2`.
(Сначала проверь раскладку на месте: `systemctl cat screener-worker*` — вдруг что-то сдвинулось.)

### Шаг 4 — systemd-юнит для Go ✅ ФАЙЛ СОЗДАН (`deploy/goingest.service`)
Готов: `ExecStart=/opt/screener/goingest/goingest`, `WorkingDirectory=/opt/screener/goingest`,
`Environment=REDIS_ADDR=127.0.0.1:6379`, `Restart=always`, `MemoryMax=512M`, `Requires=redis-server`.
На VPS: `scp` юнит в `/etc/systemd/system/`, поправь пути если папка иная,
`systemctl daemon-reload && systemctl enable --now goingest`.

### Шаг 5 — ЗАМЕРИТЬ выигрыш
- До: CPU Python-bybit-воркера на пике (`top`/`pidstat`, было ~95–98% ядра).
- После: CPU процесса `goingest` (ожидание ~10–20% ядра, т.е. 5–10x).
- Проверить в браузере: bybit-плотности появляются/обновляются/исчезают как раньше; графики живы.
- Сверить число bybit-плотностей Go vs то, что было на Python (должно совпадать ±немного).

---

## 6. ПРОБЛЕМНЫЕ МЕСТА / ГРАБЛИ

1. **Сборка не проверена** — `main.go` написан, но локально нет Go-тулчейна, `go build` не гонялся.
   Первая сборка на VPS (`go mod tidy` доберёт indirect-deps go-redis и создаст `go.sum`). Если
   что-то не так — почти наверняка тривиальное (импорт/опечатка), не логика.
2. **Двойная публикация bybit** если не убрать его из ЖИВОГО Python-воркера `screener-worker2` (Шаг 3).
3. **`binance_f` захардкожен `false`** в `makeDensity` (Go не знает список фьюч-символов binance).
   Для bybit-плотностей фильтр «только на binance-futures» во фронте будет их прятать. Для пилота ок.
   Потом: один REST-запрос к binance fapi `exchangeInfo` на старте → множество символов → проставлять
   `BinanceF`. (В питоне это `state.binance_f_symbols`.)
4. **Две копии порогов** (Python `config.py` ↔ Go `config.go`). При правках синхронизировать.
   Долгосрочно — вынести в общий JSON-конфиг, читаемый обоими.
5. **Поведение жизненного цикла**: матчинг/промоушен/удаление портированы по памяти из
   `_run_detection`. Сверить пограничные случаи с питоном (особенно restore из `recentlyRemoved` и
   sweep по `staleDensityTTL`). Если bybit-плотности «мигают» или живут не так — копать сюда.
6. **bybit пагинация**: `limit=1000` одним запросом (как в питоне). Если у bybit станет >1000
   инструментов — обрежется. Сейчас не проблема.
7. **gorilla concurrent write**: в `bybit.go` все записи в сокет идут через `wsConn.writeJSON` под
   мьютексом (ping + subscribe). Не писать в сокет мимо этого.
8. **Память Go**: должна быть низкой и стабильной, но проследить за `MemoryMax` в юните как у Python.

---

## 7. ПОЛНЫЙ ROADMAP (после успешного пилота bybit)

**Тир 1 — приём (то, что делаем сейчас):**
- [x] Архитектура шины (готова, Python).
- [~] **bybit на Go (пилот)** ← мы здесь.
- [ ] Замерить, подтвердить 5–10x.
- [ ] Портировать остальные биржи приёма по одной: bitget → okx → binance → gate.
  Каждая = свой `<exch>.go` коннектор (свой WS-протокол!), та же `store`/`detect`/`bus`.
  **Протоколы разные** — binance `@depth20` partial-book (НЕ snapshot/diff — был бан REST!),
  у каждого свой формат подписки/сообщений. Брать из `backend/screener/exchanges/<exch>.py`.
- [ ] Когда все 5 на Go — Python-воркеры приёма выключить совсем.

**Тир 1.5 — графики (klines), будущее расширение до 15 бирж:**
- Сейчас klines принимает web (Python). При 15 биржах + 1k юзеров приём klines тоже лучше на Go
  (отдельный сервис или часть goingest), публикация klines-тиков в шину. Объём сообщений у klines
  на порядок меньше стаканов — не горит, но в общий план входит.

**Тир 2 — отдача (web fan-out), для 5k+ юзеров (ОТДЕЛЬНО, не сейчас):**
- При 1k юзеров один Python-web-процесс ещё тянет (~1–1.5 ядра на рассылку+арбитраж).
- При 5k+ — упрёмся в веб-тир. Решение уже заложено архитектурой: **несколько web-инстансов**
  (каждый подписан на ту же шину) за load-balancer’ом. Это размножение процессов, а не переписывание.
- Арбитраж/splash тоже можно вынести из web в отдельный сервис на шине.

**Известный баг к проверке (из прошлого):** на 1м-свече график BTC еле обновлялся. Вероятно
наследие фриза (уже структурно починен через `ws_util.fanout`) ИЛИ троттлинг трейдов в шине
ограничивает обновление close-цены до ~2/с. Проверить после стабилизации Go-пилота.

---

## 8. СПРАВКА

- **VPS-доступ, деплой, пути** → память `vps-deployment.md`.
- **Linux-фиксы (asyncio loop, orjson, shared TLS, фриз-фикс через `ws_util.fanout`, фикс десинка
  через `density_sync`, вывод «нагрузка — это приём, а не детекция»)** → память `vps-linux-fixes.md`.
- **5 бирж в плотностях**: bybit, gate, binance, okx, bitget. Только USDT-пары (фильтр в
  `backend/screener/exchanges/base.py`: `s.endswith("USDT")`). USDC/USD/USDE убраны.
- **bingx/mexc/kucoin** — только в графиках, НЕ в плотностях.
- **Деплой фронта** = копирование одного `index.html` (см. `frontend-mobile-adaptation.md`).
- **Запуск Python**: `run.py` (web, `loop="asyncio"`), `run_worker.py` (worker). `runtime_patches.py`
  импортируется первым (shared TLS context, `json.loads`→`orjson.loads`, `compression=None`).

---

### Резюме одной фразой
Go-сервис-замена bybit-воркера написан и лежит в `goingest/` — **допиши `main.go`, собери на VPS,
убери bybit из Python-воркера, заведи systemd, замерь CPU**. Дальше по тому же шаблону — остальные
биржи. Контракты шины (раздел 4) — святое.
