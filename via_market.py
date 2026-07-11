"""via_market.py — живой рынок игры VIA (загружается из Биржа.csv).

Спека Рината:
  - рынок живёт сам (авто-тики на сервере), 1 круг = 1 год
  - акции стабильнее: ~1-4%/год, мягко
  - крипта дикая: то буст, то -99%
  - 58 активов из Downloads/Биржа.csv: BTC/ETH/AAPL/Gold/APXM(MEME)/...
  - у каждого: мин / средняя / макс цена + Volatility (шкала 1..70) + тип

МОДЕЛЬ v2 (11июл) — убран читаемый цикл «жди дно → купи → жди пик → продай»:
  ЯКОРЬ (справедливая цена) больше НЕ фиксирован — он сам БЛУЖДАЕТ (random walk + типовой рост).
  → нет известного пола/потолка, у которого можно ждать гарантированный откат.
  возврат = K * (якорь - цена)   # цена тянется к якорю (стабильность), но якорь уже плавает
  шум     = цена * (vol/200) * N(0,1)
  RUG (крипта): крах двигает и цену, И ЯКОРЬ вниз навсегда → скам может умереть, дно = падающий нож.
  МЁРТВАЯ ЗОНА после события: возврат слабый, новых событий нет → памп/крах не фармится по кругу.
  БУМ: чаще временный спайк (якорь на месте → откат, опоздавший теряет), редко — реальный пробой.
  Итог: биржа = риск. Единицы срывают куш, большинство в минус. Дорога к победе — пассивные активы.
"""
import csv, math, os, random, time

# 2026-07-04: ищем биржу и рядом со скриптом (для облака/Render, где нет Downloads).
_HERE = os.path.dirname(os.path.abspath(__file__))
_CSV_CANDIDATES = [
    os.path.join(os.path.expanduser("~"), "Downloads", "Биржа.csv"),  # локально
    os.path.join(_HERE, "birzha.csv"),                                # облако (в репо)
    os.path.join(_HERE, "Биржа.csv"),
]
CSV_PATH = next((p for p in _CSV_CANDIDATES if os.path.exists(p)), _CSV_CANDIDATES[0])
SECONDS_PER_YEAR = 600     # 1 «год» = 10 мин (настраивается)
TICK_SECONDS = 30     # темп рынка (было 15 — слишком быстро, люди не успевали). Клиент показывает отсчёт.
TICKS_PER_YEAR = SECONDS_PER_YEAR / TICK_SECONDS
REVERT_K = 0.10            # сила возврата к якорю за тик (чтоб рост акций доезжал)
# ── РЫНОК v2 (11июл): убираем читаемый цикл «жди дно → купи → жди пик → продай».
# Якорь (справедливая цена) больше НЕ фиксирован — он сам блуждает. Нет известного пола.
# Скам может умереть навсегда (rug двигает якорь вниз). После события — «мёртвая зона».
ANCHOR_DRIFT_VOL = {"stock": 0.004, "metal": 0.005, "commodity": 0.015, "crypto": 0.045}  # блуждание якоря/тик по типу
COOLDOWN_TICKS = 6         # мёртвая зона после крупного события: памп/крах не фармится по кругу
BOOM_ANCHOR_CHANCE = 0.30  # шанс, что памп — РЕАЛЬНЫЙ пробой (двигает якорь вверх); иначе временный спайк → откат
MAX_GAME_YEARS = 300       # длинная живая партия: рынок не замирает в течение сессии (потолок высокий, но конечный).
                           # 1 год = 10 мин → ~5 часов сессии. Настраивается.

# годовой рост якоря по типу (акции мягко вверх; крипта/металл/сырьё ~флэт)
TYPE_GROWTH = {"stock": 0.03, "crypto": 0.00, "metal": 0.02, "commodity": 0.00}

# ЛИКВИДНОСТЬ СТАКАНА в $ по типу (Ринат 5июл): акции и золото/металл — ОГРОМНЫЕ стаканы,
# цена еле идёт даже на крупной сделке; крипта — МЕЛКИЕ стаканы, летит в космос.
# Сдвиг цены при сделке = (объём$ / liq) × IMPACT_MULT (жёстко: $5k в $10k → +100%).
TYPE_LIQ = {"stock": 2_000_000, "metal": 3_000_000, "commodity": 500_000, "crypto": 40_000}

def asset_liq(typ, vol):
    liq = TYPE_LIQ.get(typ, 500_000)
    if typ == "crypto":
        liq *= 12.0 / max(float(vol or 0) or 12.0, 12.0)   # чем волатильнее (мемкоин) — тем тоньше стакан
    return round(liq)


def _num(s, default=0.0):
    if s is None: return default
    s = str(s).strip().replace(" ", "").replace(" ", "").replace(",", ".")
    try: return float(s)
    except Exception: return default


def _norm_type(t):
    t = (t or "").strip().lower()
    if "crypto" in t: return "crypto"
    if "metal" in t: return "metal"
    if t.startswith(("comm", "сomm")) or "modit" in t or "motit" in t: return "commodity"
    if "stock" in t: return "stock"
    return "stock"   # пусто -> акция по умолчанию


def load_assets(path=CSV_PATH):
    assets = []
    if not os.path.exists(path):
        return assets
    for r in csv.DictReader(open(path, encoding="utf-8-sig")):
        name = (r.get("Название") or "").strip()
        if not name: continue
        typ = _norm_type(r.get("Тип Актива"))
        cur = _num(r.get("Текущая цена ") or r.get("Текущая цена"), 0)
        pmin = _num(r.get("Минимальная цена ") or r.get("Минимальная цена"), 0)
        pavg = _num(r.get("Средняя цена ") or r.get("Средняя цена"), cur)
        pmax = _num(r.get("Максимальная Цена ") or r.get("Максимальная Цена"), cur * 10 or 1)
        vol = _num(r.get("Volatility"), 0)
        if cur <= 0: cur = pavg or pmin or 1
        if vol <= 0: vol = {"crypto": 20, "stock": 4, "metal": 2, "commodity": 3}[typ]
        # санитизация коридора: иногда в CSV мин выше текущей или макс ниже —
        # тогда цена клинит у границы. Чиним: мин ВСЕГДА ниже текущей, макс выше.
        if pmin <= 0 or pmin >= cur: pmin = round(cur * 0.1, 4)
        if typ == "crypto": pmin = min(pmin, round(cur * 0.03, 6))   # скам может провалиться глубоко (rug) и залипнуть
        if pmax <= cur: pmax = cur * 5
        if not (pmin < pavg < pmax): pavg = cur
        assets.append({
            "id": (r.get("AssetID") or name).strip(), "name": name, "type": typ,
            "price": round(cur, 2), "price0": round(cur, 2), "min": pmin,
            "avg0": pavg, "avg": round(cur, 2), "anchor": round(cur, 2), "cool": 0, "max": pmax,
            "vol": vol, "liq": asset_liq(typ, vol), "history": [round(cur, 2)],
        })
    return assets


def init_market(path=CSV_PATH):
    return {"assets": load_assets(path), "started_at": time.time(),
            "game_seconds": 0, "game_year": 1, "game_over": False,
            "last_tick": time.time(), "csv": path,
            "nextTickAt": time.time() + TICK_SECONDS}   # старт отсчёта до первого обновления


def tick_market(market):
    events = []
    # ИГРА КОНЕЧНА: дошли до MAX_GAME_YEARS → рынок замирает, время не копится дальше
    # (устраняет «бесконечный замкнутый цикл» и разгон до Года 279). Новая игра = init_market().
    if market.get("game_year", 1) > MAX_GAME_YEARS:
        market["game_over"] = True
        return events
    # игровое время = по тикам (надёжно к рестарту, тестируемо), не по wall-clock
    market["game_seconds"] = market.get("game_seconds", 0) + TICK_SECONDS
    elapsed_years = market["game_seconds"] / SECONDS_PER_YEAR
    for a in market["assets"]:
        # PUMP-режим (монета-приманка): принудительный рост каждый тик до потолка.
        # Ведущий заряжает жадность — цена растёт на глазах, пока не обнулят.
        if a.get("pump"):
            new = a["price"] * a["pump"]
            new = min(a["max"], max(a["min"], new))
            a["price"] = round(new, 4 if new < 1 else 2)
            a["history"].append(a["price"])
            if len(a["history"]) > 120:
                a["history"] = a["history"][-120:]
            continue
        # РЫНОК v2: якорь (справедливая цена) сам БЛУЖДАЕТ — нет фиксированного «пола/потолка»,
        # у которого можно ждать гарантированный откат. «Низко» может значить «умирает», а не «отскочит».
        typ = a["type"]
        g = TYPE_GROWTH.get(typ, 0.0)
        price = a["price"]
        anchor = a.get("anchor") or a.get("avg0") or a["price0"]
        cool = int(a.get("cool") or 0)
        sick = int(a.get("sick") or 0)   # «болеет» после краха: якорь дрейфует вниз, скам умирает

        # 1) якорь = случайное блуждание + типовой рост. Больной актив (после rug) тянет ВНИЗ — не оживает.
        adv = ANCHOR_DRIFT_VOL.get(typ, 0.01)
        bias = (g / TICKS_PER_YEAR) + (-0.03 if sick > 0 else 0.0)
        anchor = anchor + anchor * (bias + adv * random.gauss(0, 1))
        anchor = max(a["min"], min(a["max"], anchor))

        # 2) цена тянется к якорю (стабильность) + шум. В мёртвой зоне тянет СЛАБО — не отскакивает.
        k = REVERT_K * (0.3 if cool > 0 else 1.0)
        revert = k * (anchor - price)
        noise = price * (a["vol"] / 200.0) * random.gauss(0, 1)
        drift = price * (g / TICKS_PER_YEAR)
        new = price + revert + noise + drift

        # 3) события крипты — только ВНЕ мёртвой зоны; частота/сила растут с volatility
        if typ == "crypto" and a["vol"] >= 8 and cool <= 0:
            crash_p = 0.004 + a["vol"] / 4000.0     # vol70 -> ~2.2%/тик
            boom_p = 0.006 + a["vol"] / 3000.0
            r = random.random()
            if r < crash_p:
                # RUG: цена И ЯКОРЬ рушатся навсегда (−80..−95%) — скам может умереть, откупить дно = поймать нож
                f = random.uniform(0.05, 0.20)
                new = price * f
                anchor = max(a["min"], anchor * f)
                a["cool"] = COOLDOWN_TICKS
                a["sick"] = 25              # монета «болеет»: якорь ползёт вниз ~25 тиков — обычно умирает, редко выкарабкивается
                events.append({"id": a["id"], "name": a["name"], "kind": "КРАХ", "pct": round((f - 1) * 100)})
            elif r < crash_p + boom_p:
                f = random.uniform(1.5, 3.0)
                new = price * f
                if random.random() < BOOM_ANCHOR_CHANCE:
                    anchor = min(a["max"], anchor * random.uniform(1.2, 1.8))   # редкий РЕАЛЬНЫЙ пробой (якорь вверх)
                # иначе временный спайк: якорь на месте → памп откатится, опоздавший на пике теряет
                a["cool"] = COOLDOWN_TICKS
                events.append({"id": a["id"], "name": a["name"], "kind": "БУМ", "pct": round((f - 1) * 100)})

        new = max(a["min"], min(a["max"], new))
        a["anchor"] = round(anchor, 4 if anchor < 1 else 2)
        a["avg"] = a["anchor"]
        a["price"] = round(new, 4 if new < 1 else 2)
        if cool > 0:
            a["cool"] = cool - 1
        if sick > 0:
            a["sick"] = sick - 1
        a["history"].append(a["price"])
        if len(a["history"]) > 120: a["history"] = a["history"][-120:]
    market["game_year"] = 1 + int(elapsed_years)
    market["last_tick"] = time.time()
    return events


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    m = init_market()
    print(f"загружено активов: {len(m['assets'])}")
    from collections import Counter
    print("по типам:", dict(Counter(a["type"] for a in m["assets"])))
    start = {a["id"]: a["price"] for a in m["assets"]}
    # симуляция 1 года
    ev = []
    for _ in range(int(TICKS_PER_YEAR)): ev += tick_market(m)
    print(f"\n=== за 1 ГОД (выборка) ===")
    show = [a for a in m["assets"] if a["name"] in ("BTC","ETH","AAPL","Gold","APXM (MEME)","ZEN","TSLA","TechCorp")]
    for a in show:
        chg = (a["price"]/start[a["id"]]-1)*100
        print(f"  {a['name']:<14} {a['type']:<9} {start[a['id']]:>9.2f} -> {a['price']:>10.2f} ({chg:+.0f}%)  [{a['min']:.0f}..{a['max']:.0f}]")
    print(f"  событий: {len(ev)}")
    for e in ev[:6]: print(f"     {e['name']} {e['kind']} {e['pct']}%")
