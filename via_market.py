"""via_market.py — живой рынок игры VIA (загружается из Биржа.csv).

Спека Рината:
  - рынок живёт сам (авто-тики на сервере), 1 круг = 1 год
  - акции стабильнее: ~1-4%/год, мягко
  - крипта дикая: то буст, то -99%
  - 58 активов из Downloads/Биржа.csv: BTC/ETH/AAPL/Gold/APXM(MEME)/...
  - у каждого: мин / средняя / макс цена + Volatility (шкала 1..70) + тип

МОДЕЛЬ (за один тик), использует ВСЕ его данные:
  возврат_к_средней = K * (средняя - цена)         # держит цену в коридоре
  шум               = цена * (vol/200) * N(0,1)    # дрожь, масштаб по Volatility
  рост              = цена * годовой_рост/тиков     # только акции, лёгкий тренд вверх
  новая = clamp(цена + возврат + шум + рост, мин, макс)
  + у крипты с высокой vol — события КРАХ(к мин)/БУМ(к макс)
Средняя (avg) — якорь: цена болтается вокруг неё, vol задаёт амплитуду,
коридор [мин,макс] не пускает в абсурд. Долгий тренд = плавный подъём якоря у акций.
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
TICK_SECONDS = 15
TICKS_PER_YEAR = SECONDS_PER_YEAR / TICK_SECONDS
REVERT_K = 0.10            # сила возврата к якорю за тик (чтоб рост акций доезжал)
MAX_GAME_YEARS = 300       # длинная живая партия: рынок не замирает в течение сессии (потолок высокий, но конечный).
                           # 1 год = 10 мин → ~5 часов сессии. Настраивается.

# годовой рост якоря по типу (акции мягко вверх; крипта/металл/сырьё ~флэт)
TYPE_GROWTH = {"stock": 0.03, "crypto": 0.00, "metal": 0.02, "commodity": 0.00}


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
        if pmax <= cur: pmax = cur * 5
        if not (pmin < pavg < pmax): pavg = cur
        assets.append({
            "id": (r.get("AssetID") or name).strip(), "name": name, "type": typ,
            "price": round(cur, 2), "price0": round(cur, 2), "min": pmin,
            "avg0": pavg, "avg": pavg, "max": pmax,
            "vol": vol, "history": [round(cur, 2)],
        })
    return assets


def init_market(path=CSV_PATH):
    return {"assets": load_assets(path), "started_at": time.time(),
            "game_seconds": 0, "game_year": 1, "game_over": False,
            "last_tick": time.time(), "csv": path}


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
        # якорь растёт ОТ ТЕКУЩЕЙ цены по типу актива (а не прыгает к средней) ->
        # акция мягко +1-4%/год, крипта ~флэт с дикой амплитудой вокруг старта.
        g = TYPE_GROWTH.get(a["type"], 0.0)
        anchor = a["price0"] * ((1 + g) ** elapsed_years)
        # ПОТОЛОК ЯКОРЮ (фикс 1июл): не выше 85% коридора — иначе за долгую игру
        # рост акций компаундится сквозь потолок и цену намертво прибивает к max
        # («рынок умирает наверху»). Оставляем место шуму снизу от границы.
        ceil = a["min"] + 0.85 * (a["max"] - a["min"])
        if anchor > ceil:
            anchor = ceil
        a["avg"] = round(anchor, 2)
        price = a["price"]
        revert = REVERT_K * (anchor - price)
        noise = price * (a["vol"] / 200.0) * random.gauss(0, 1)
        drift = price * (g / TICKS_PER_YEAR)
        new = price + revert + noise + drift
        # события — только крипта; частота/сила растут с volatility
        if a["type"] == "crypto" and a["vol"] >= 8:
            crash_p = 0.004 + a["vol"] / 4000.0     # vol70 -> ~2.2%/тик
            boom_p = 0.006 + a["vol"] / 3000.0
            r = random.random()
            if r < crash_p:
                f = random.uniform(0.01, 0.5)
                new = price * f
                events.append({"id": a["id"], "name": a["name"], "kind": "КРАХ", "pct": round((f - 1) * 100)})
            elif r < crash_p + boom_p:
                f = random.uniform(1.5, 3.5)
                new = price * f
                events.append({"id": a["id"], "name": a["name"], "kind": "БУМ", "pct": round((f - 1) * 100)})
        new = max(a["min"], min(a["max"], new))
        a["price"] = round(new, 4 if new < 1 else 2)
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
