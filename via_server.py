"""
VIA — локальный сервер синхронизации.

Запуск:
    python via_server.py
или двойной клик по start_via.bat

Откроется http://localhost:8080/via.html в браузере по умолчанию.
Открывай ту же ссылку в любом другом браузере (Edge, Firefox, Opera) —
все будут видеть одни и те же данные.

Данные хранятся в файле via_data.json рядом со скриптом — переживают
рестарт сервера.
"""
import http.server
import json
import os
import queue
import random
import socketserver
import threading
import time
import webbrowser
from pathlib import Path

try:
    import via_market   # живой рынок (тикер цен)
except Exception:
    via_market = None

# ── Мировые события: идут ПО КРУГУ ХОДОВ (раз за круг у «якоря»), НЕ авто ──
# Каждое мировое событие РЕАЛЬНО двигает биржу И БИЗНЕСЫ у ВСЕХ (4-й элемент = эффект).
# Ключи-типы биржи: stock (акции), crypto (крипта), metal (металлы), commodity (сырьё).
# Ключ 'biz' = сдвиг денежного потока бизнесов/активов игроков (пассивный доход). Значение = доля.
WORLD_EVENTS = [
    ('🌍', 'Мировой кризис', 'Экономика замедлилась — рынки и бизнесы падают у всех.', {'stock': -0.10, 'crypto': -0.12, 'metal': 0.04, 'commodity': -0.05, 'biz': -0.08}),
    ('🛢️', 'Скачок цен на нефть', 'Топливо дорожает — сырьё вверх, бизнес под давлением.', {'commodity': 0.14, 'stock': -0.04, 'biz': -0.04}),
    ('🏦', 'Банки снизили ставки', 'Дешёвые деньги — рынки и бизнесы растут.', {'stock': 0.06, 'crypto': 0.05, 'biz': 0.05}),
    ('🔔', 'ЦБ поднял ставку', 'Деньги дорогие — рисковые активы и бизнесы просели.', {'stock': -0.05, 'crypto': -0.07, 'biz': -0.05}),
    ('📈', 'Экономический бум', 'Рынки и бизнесы растут по всему фронту.', {'stock': 0.09, 'crypto': 0.08, 'metal': 0.04, 'commodity': 0.05, 'biz': 0.09}),
    ('📉', 'Рецессия', 'Спад в экономике — активы и бизнесы просели у всех.', {'stock': -0.09, 'crypto': -0.08, 'metal': 0.03, 'commodity': -0.04, 'biz': -0.09}),
    ('🏗️', 'Строительный бум', 'Стройка тянет металлы, сырьё и бизнесы вверх.', {'metal': 0.10, 'commodity': 0.08, 'stock': 0.03, 'biz': 0.06}),
    ('🦠', 'Новая эпидемия', 'Мир на паузе — акции и бизнесы вниз, металлы в убежище.', {'stock': -0.07, 'crypto': -0.04, 'metal': 0.06, 'biz': -0.06}),
    ('💻', 'Технопрорыв', 'Крипта, IT и бизнесы на подъёме.', {'crypto': 0.14, 'stock': 0.06, 'biz': 0.06}),
    ('🪙', 'Крипто-лихорадка', 'Все скупают монеты — крипта взлетела.', {'crypto': 0.22, 'stock': 0.02, 'biz': 0.02}),
    ('🌾', 'Урожайный год', 'Продукты дешевеют — расходы ниже, бизнесы легче дышат.', {'commodity': -0.08, 'stock': 0.02, 'biz': 0.04}),
    ('⚡', 'Энергокризис', 'Энергия дорожает — сырьё вверх, бизнесы вниз.', {'commodity': 0.12, 'stock': -0.05, 'biz': -0.06}),
    ('🛒', 'Потребительский бум', 'Люди тратят — бизнесы в жирном плюсе.', {'stock': 0.07, 'commodity': 0.03, 'biz': 0.10}),
    ('💱', 'Валютный шторм', 'Курсы штормит — металлы в цене, бизнес слегка вниз.', {'metal': 0.08, 'crypto': -0.05, 'stock': -0.02, 'biz': -0.02}),
    ('🌐', 'Открылись новые рынки', 'Новые возможности — рынок и бизнесы в плюсе.', {'stock': 0.06, 'crypto': 0.05, 'commodity': 0.04, 'biz': 0.06}),
    ('🏭', 'Промышленный подъём', 'Заводы и логистика тянут рынок и бизнесы вверх.', {'stock': 0.07, 'metal': 0.06, 'commodity': 0.05, 'biz': 0.07}),
]
_TYPE_RU = {'stock': 'Акции', 'crypto': 'Крипта', 'metal': 'Металлы', 'commodity': 'Сырьё', 'biz': 'Бизнесы'}
def _fx_text(fx):
    parts = ['%s %s%d%%' % (_TYPE_RU.get(t, t), '+' if v > 0 else '', round(v * 100)) for t, v in fx.items() if v]
    return ' · '.join(parts)
def apply_market_shock(fx):
    """Разово двигает цены на бирже по типам активов (эффект мирового события). Вызывать ВНУТРИ LOCK."""
    mkt = DATA.get('market') or {}
    for a in mkt.get('assets', []):
        pct = fx.get(a.get('type'), 0)
        if not pct:
            continue
        lo = float(a.get('min') or 0)
        hi = float(a.get('max') or (float(a['price']) * 10))
        new = float(a['price']) * (1 + pct)
        new = max(lo, min(hi, new))
        a['price'] = round(new, 4 if new < 1 else 2)
        a.setdefault('history', []).append(a['price'])
        if len(a['history']) > 120:
            a['history'] = a['history'][-120:]
def apply_business_shock(pct):
    """Разово двигает денежный поток (cf) бизнесов/активов ВСЕХ игроков. Вызывать ВНУТРИ LOCK."""
    if not pct:
        return
    for p in DATA.get('players', []):
        for a in p.get('assets', []):
            try:
                cf = float(a.get('cf') or 0)
            except (TypeError, ValueError):
                continue
            if cf:
                a['cf'] = round(cf * (1 + pct), 2)

def _num(v):
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0

def calc_flow(p):
    """Серверное зеркало клиентского calc(): месячный ЧИСТЫЙ поток = доход − расходы.
    Используется для АВТО-начисления зарплаты при проходе клетки «Зарплата»."""
    prof = p.get('prof') or {}
    skill_bonus = min(0.20, sum(_num(s.get('pct')) for s in (p.get('skills') or [])))
    salary = round(_num(prof.get('salary')) * (1 + skill_bonus))
    passive = sum(_num(a.get('cf')) for a in (p.get('assets') or []))
    income = salary + passive
    tax_disc = min(90.0, max(0.0, _num(p.get('taxDiscPct'))))   # 🔒 тюрьма: «продавил силой» → скидка к налогам
    taxes = (max(0.0, _num(prof.get('taxes')) + _num(p.get('taxMod'))) + round(income * _num(p.get('taxPct')) / 100)) * (1 - tax_disc / 100)
    mortgage = 0 if p.get('isM') else _num(prof.get('mortgagePay'))
    edu = 0 if p.get('isE') else _num(prof.get('eduPay'))
    auto = 0 if p.get('isA') else _num(prof.get('autoPay'))
    cc = 0 if p.get('isCC') else _num(prof.get('ccPay'))
    retail = 0 if p.get('isR') else _num(prof.get('retailPay'))
    exp_up = _num(p.get('otherExpPct')) + _num(p.get('jailPress'))   # 🔒 тюрьма: штраф «сломлен» + давление сидки
    other = (_num(prof.get('otherExp')) + _num(p.get('expMod'))) * (1 + exp_up / 100)
    child = _num(p.get('children')) * _num(prof.get('childCost'))
    bank = round(_num(p.get('bankLoan')) * 0.10)
    total_exp = taxes + mortgage + edu + auto + cc + retail + other + child + bank
    return round(income - total_exp, 2)
def pick_world_event():
    e = random.choice(WORLD_EVENTS)
    fx = e[3] if len(e) > 3 else {}
    return {'id': int(time.time() * 1000), 'emoji': e[0], 'title': e[1], 'text': e[2],
            'fx': fx, 'fxtext': _fx_text(fx)}


# ═══════════ 🌍 ЖИВОЙ МИР: ЭПОХИ И ЦЕПОЧКИ ═══════════
# Спека: Desktop\VIA_WORLD_RUMORS_SPEC_2026_07_20.md
# Ринат: «в мире же постоянно что-то происходит, а у нас мир вроде идёт — и ничего не происходит».
# Было: плоский список, раз в круг берётся СЛУЧАЙНОЕ. Событие мигнуло и исчезло, из него ничего
# не выросло. Стало: события идут ЦЕПОЧКАМИ (война → нефть → инфляция → ставка), а над ними стоит
# ЭПОХА, которая решает, какие цепочки возможны и насколько сильно они бьют.
# Механику доставки (apply_market_shock / apply_business_shock / broadcast) не трогаем.

# Эпоха: (имя для экрана, множитель хороших новостей, множитель плохих).
# В перегрев хорошее бьёт сильнее, а плохое приглушено — потому и кажется, что риска нет.
ERAS = {
    'growth':   ('Рост',             1.0, 1.0),
    'overheat': ('Перегрев',         1.3, 0.7),
    'crisis':   ('Кризис',           0.7, 1.4),
    'recovery': ('Восстановление',   1.1, 0.9),
}
# Куда эпоха переходит дальше. Повторы в списке = вес, чтобы не выучили наизусть.
ERA_NEXT = {
    'growth':   ['overheat', 'overheat', 'crisis'],
    'overheat': ['crisis', 'crisis', 'growth'],
    'crisis':   ['recovery', 'recovery', 'crisis'],
    'recovery': ['growth', 'growth', 'overheat'],
}
ERA_LEN = (3, 5)   # сколько кругов живёт эпоха

# Цепочки. rumor — чем шепчут о СЛЕДУЮЩЕМ звене (карта «Слухи», см. спеку).
CHAINS = [
    {'k': 'rate', 'eras': ['overheat', 'crisis'],
     'rumor': 'Поговаривают, ставку могут поднять — деньги подорожают.',
     'links': [
         ('🔔', 'ЦБ поднял ставку', 'Деньги дорогие — рисковые активы просели.',
          {'stock': -0.05, 'crypto': -0.07, 'biz': -0.05}),
         ('🏢', 'Компании режут расходы', 'Сокращения пошли по рынку.',
          {'stock': -0.04, 'biz': -0.07}),
         ('🏚️', 'Долги дорожают', 'Кто сидел в кредитах — тому тяжелее всех.',
          {'biz': -0.04, 'stock': -0.02, 'metal': 0.03}),
     ]},
    {'k': 'oil', 'eras': ['growth', 'overheat', 'crisis', 'recovery'],
     'rumor': 'На Ближнем Востоке неспокойно — топливо может рвануть.',
     'links': [
         ('🛢️', 'Скачок цен на нефть', 'Топливо дорожает — сырьё вверх, бизнес под давлением.',
          {'commodity': 0.14, 'stock': -0.04, 'biz': -0.04}),
         ('⚡', 'Энергия дорожает следом', 'Счета выросли у всех, кто что-то производит.',
          {'commodity': 0.08, 'stock': -0.03, 'biz': -0.06}),
         ('🌾', 'Продукты подорожали', 'Расходы выросли у всех — цепочка дошла до полки.',
          {'commodity': 0.05, 'biz': -0.03}),
     ]},
    {'k': 'tech', 'eras': ['growth', 'overheat'],
     'rumor': 'Ходят разговоры про новую технологию — деньги побегут туда.',
     'links': [
         ('💻', 'Технопрорыв', 'Крипта, IT и бизнесы на подъёме.',
          {'crypto': 0.14, 'stock': 0.06, 'biz': 0.06}),
         ('🪙', 'Крипто-лихорадка', 'Все скупают монеты — цена оторвалась от земли.',
          {'crypto': 0.22, 'stock': 0.02, 'biz': 0.02}),
         ('💥', 'Пузырь лопнул', 'Кто зашёл последним — тот и заплатил за всех.',
          {'crypto': -0.25, 'stock': -0.05, 'biz': -0.02}),
     ]},
    {'k': 'crash', 'eras': ['crisis'],
     'rumor': 'Говорят, у крупного банка дыра в балансе.',
     'links': [
         ('🌍', 'Мировой кризис', 'Экономика замедлилась — рынки и бизнесы падают у всех.',
          {'stock': -0.10, 'crypto': -0.12, 'metal': 0.04, 'commodity': -0.05, 'biz': -0.08}),
         ('🦠', 'Паника на рынках', 'Продают всё подряд, не разбирая.',
          {'stock': -0.07, 'crypto': -0.09, 'metal': 0.06, 'biz': -0.05}),
         ('🏦', 'Банки снизили ставки', 'Дешёвые деньги — экономику спасают.',
          {'stock': 0.06, 'crypto': 0.05, 'biz': 0.05}),
         ('📈', 'Восстановление пошло', 'Худшее позади — рынки поднимают голову.',
          {'stock': 0.09, 'crypto': 0.08, 'biz': 0.09}),
     ]},
    {'k': 'build', 'eras': ['recovery', 'growth'],
     'rumor': 'Слышно, государство заходит большими стройками.',
     'links': [
         ('🏗️', 'Строительный бум', 'Стройка тянет металлы, сырьё и бизнесы вверх.',
          {'metal': 0.10, 'commodity': 0.08, 'stock': 0.03, 'biz': 0.06}),
         ('🛒', 'Потребительский бум', 'Люди тратят — бизнесы в жирном плюсе.',
          {'stock': 0.07, 'commodity': 0.03, 'biz': 0.10}),
     ]},
]


# Чем шепчут ПЕРЕД следующим звеном. WHISPERS[k][i] — слух о звене номер (i+2).
# Первое звено цепочки приходит БЕЗ предупреждения: первый удар всегда неожиданный.
# А вот дальше люди уже начинают гадать, что будет — и вот тут появляется слух.
WHISPERS = {
    'rate':  ['Слышно, крупные компании готовят сокращения.',
              'Говорят, обслуживать долги стало нечем — посыплются те, кто сидит в кредитах.'],
    'oil':   ['Поговаривают, следом полетят счета за энергию.',
              'Говорят, дорогая логистика вот-вот дойдёт до полки — продукты подорожают.'],
    'tech':  ['Ходят разговоры, что в монеты заходят очень большие деньги.',
              'Шепчут, что это пузырь и он вот-вот лопнет.'],
    'crash': ['Говорят, начинается паника — скоро будут продавать всё подряд.',
              'Слышно, банки готовят дешёвые деньги, чтобы спасти экономику.',
              'Поговаривают, дно пройдено и рынки поднимут голову.'],
    'build': ['Слышно, у людей появились деньги и они начали тратить.'],
}
RUMOR_TRUE_P   = 0.67   # примерно каждый третий слух пустой — иначе это не слух, а расписание
RUMOR_CHECK_FEE = 0.05  # доля кассы за проверку («аналитика», свой человек)
RUMOR_SPREAD_AMP = 1.25 # насколько сильнее бьёт событие, если слух разогнали


def _portfolio_value(p):
    """Сколько у игрока стоит портфель на бирже — от него считается сила решения по слуху."""
    mkt = DATA.get('market') or {}
    prices = {a.get('id'): float(a.get('price') or 0) for a in mkt.get('assets', [])}
    total = 0.0
    for aid, h in (p.get('holdings') or {}).items():
        try:
            total += float(h.get('qty') or 0) * prices.get(aid, 0)
        except (TypeError, ValueError):
            continue
    return total


def publish_rumor(cdef, next_link):
    """Слух о звене next_link (нумерация с 1). Треть слухов — пустые: текст берём у ЧУЖОЙ
    цепочки, так что поверивший действует по неверной картине."""
    idx = next_link - 2
    whispers = WHISPERS.get(cdef['k']) or []
    if idx < 0 or idx >= len(whispers):
        DATA['rumor'] = None
        return
    is_true = random.random() < RUMOR_TRUE_P
    if is_true:
        text = whispers[idx]
    else:
        others = [w for k, ws in WHISPERS.items() if k != cdef['k'] for w in ws]
        text = random.choice(others) if others else whispers[idx]
    DATA['rumor'] = {'id': int(time.time() * 1000), 'text': text, 'true': is_true,
                     'chain': cdef['k'], 'link': next_link, 'choices': {}, 'resolved': False}


def resolve_rumor(r, fx):
    """Звено наступило — разбираем, кто что выбрал. Вызывать ВНУТРИ LOCK, ДО применения шока."""
    mag = max([abs(float(v)) for k, v in (fx or {}).items() if k != 'biz'] or [0.05])
    is_true = bool(r.get('true'))
    for p in DATA.get('players', []):
        ch = (r.get('choices') or {}).get(str(p.get('id')))
        if not ch or ch == 'wait':
            continue                      # переждал — ничего не теряет и не получает
        port = _portfolio_value(p)
        if port <= 0:
            port = max(0.0, float(p.get('savings') or 0)) * 0.5   # без портфеля — по кассе, мягче
        delta = 0.0
        if ch == 'believe':
            delta = port * mag if is_true else -port * mag * 0.5
        elif ch == 'check':
            delta = port * mag * 0.5 if is_true else 0.0          # знал, что пусто — не полез
        elif ch == 'spread':
            delta = port * mag * RUMOR_SPREAD_AMP if is_true else -port * mag * 0.5
            if not is_true:
                # разогнал пустой слух, люди потеряли деньги, поверив тебе → в тьму (закон души)
                p['betrayals'] = int(p.get('betrayals') or 0) + 1
        delta = round(delta, 2)
        if delta:
            p['savings'] = round(float(p.get('savings') or 0) + delta, 2)
        p['rumorResult'] = {'choice': ch, 'true': is_true, 'delta': delta}
    r['resolved'] = True


def era_now():
    e = DATA.get('era')
    if not isinstance(e, dict) or e.get('k') not in ERAS:
        e = DATA['era'] = {'k': 'growth', 'left': random.randint(*ERA_LEN)}
    e['name'] = ERAS[e['k']][0]
    return e


def era_tick():
    """Эпоха прожила круг. Кончилась — переходим к следующей."""
    e = era_now()
    e['left'] = int(e.get('left') or 0) - 1
    if e['left'] <= 0:
        e['k'] = random.choice(ERA_NEXT.get(e['k'], ['growth']))
        e['left'] = random.randint(*ERA_LEN)
        e['name'] = ERAS[e['k']][0]


def scale_fx(fx):
    """Эпоха множит силу удара. Игрок этих множителей не видит никогда."""
    _, pos, neg = ERAS.get(era_now()['k'], ERAS['growth'])
    return {k: round(float(v) * (pos if float(v) > 0 else neg), 4) for k, v in (fx or {}).items()}


def _chain_by_k(k):
    for c in CHAINS:
        if c['k'] == k:
            return c
    return None


def next_world_event():
    """Следующее ЗВЕНО цепочки вместо случайного события.
    Цепочка кончилась (или её нет) — эпоха проживает круг и начинается новая цепочка,
    разрешённая нынешней эпохой."""
    ch = DATA.get('chain') if isinstance(DATA.get('chain'), dict) else None
    cdef = _chain_by_k(ch['k']) if ch else None
    if not cdef or int(ch.get('idx') or 0) >= len(cdef['links']):
        era_tick()
        k = era_now()['k']
        pool = [c for c in CHAINS if k in c['eras']] or CHAINS
        cdef = random.choice(pool)
        ch = DATA['chain'] = {'k': cdef['k'], 'idx': 0}
    i = int(ch.get('idx') or 0)
    link = cdef['links'][i]
    ch['idx'] = i + 1
    fx = scale_fx(link[3] if len(link) > 3 else {})

    # ─── СЛУХ ───
    # 1) Был ли слух ПРО ЭТО звено — разбираем выборы (до применения шока).
    r = DATA.get('rumor')
    if (isinstance(r, dict) and not r.get('resolved')
            and r.get('chain') == cdef['k'] and int(r.get('link') or 0) == i + 1):
        if any(c == 'spread' for c in (r.get('choices') or {}).values()):
            fx = {k: round(v * RUMOR_SPREAD_AMP, 4) for k, v in fx.items()}   # толпа усилила удар
        resolve_rumor(r, fx)
    # 2) Рождается слух о СЛЕДУЮЩЕМ звене. Кончилась цепочка — шептать больше не о чем.
    if i + 1 < len(cdef['links']):
        publish_rumor(cdef, i + 2)
    else:
        DATA['rumor'] = None

    e = era_now()
    return {'id': int(time.time() * 1000), 'emoji': link[0], 'title': link[1], 'text': link[2],
            'fx': fx, 'fxtext': _fx_text(fx),
            'era': e['k'], 'eraName': e['name'],
            'chain': cdef['k'], 'link': i + 1, 'links': len(cdef['links'])}

# 2026-05-31: PORT from env so it runs on any free cloud host (Render/Railway/etc.).
# Locally still defaults to 8080. Cloud hosts inject $PORT.
PORT = int(os.environ.get("PORT", 8080))
IS_CLOUD = bool(os.environ.get("PORT"))   # cloud sets PORT; locally we open a browser
ROOT = Path(__file__).resolve().parent
DATA_FILE = ROOT / 'via_data.json'
LOCK = threading.Lock()

# 2026-07-04: SSE push (реальное время). Держим открытые провода к каждому браузеру
# и мгновенно рассылаем изменения — вместо опроса раз в секунду. Опрос остаётся
# у клиента как страховка, если провод оборвётся.
SUBS_LOCK = threading.Lock()
SUBSCRIBERS = set()   # набор queue.Queue — по одному на подключённый браузер
APPLIED_TX = set()    # txid уже начисленных зарплат — защита от двойного клика/ретрая сети
TRACK_LEN = 29        # клеток на поле (позиции 0..28)
SALARY_CELLS = {0, 16}  # клетки «Зарплата» — при проходе/приземлении АВТО-начисляем месячный поток
# позиция клетки (0..28) → что авто-открывается игроку при приземлении.
# '__BUY__' = открыть окно покупки акций ДЛЯ ВСЕХ; None = ничего (ведущий решает: зарплата/тюрьма/дети и т.п.).
CELL_ALLOW = [
    None, 'career', '__BUY__', 'skill', '__CHILD__', None, 'expense', '__BUY__', 'sdelka', 'gov', None, '__JAIL__',
    'work', 'expense', 'tax', None,
    None, 'sdelka', '__BUY__', 'skill', 'svyaz', 'expense', None, '__BUY__', 'sdelka', None,
    'sdelka', 'gov', 'med'
]
TURN_SECONDS = 60     # ход длится 1 минуту (мягкий таймер: предупреждает, ведущий передаёт сам)
BUY_SECONDS = 60      # окно покупки акций — 1 минута для всех (жёсткое авто-закрытие)
JAIL_TURNS = 3        # клетка Тюрьма сажает на 3 хода (ведущий может поправить в пульте)
IMPACT_MULT = 2.0     # жёсткость ликвидности: (объём$/liq)×MULT = сдвиг цены ($5k в $10k liq → +100%)
BUY_MOVE_CAP = 2.0    # один трейд двигает цену вверх не больше +200%
SELL_MOVE_CAP = 0.7   # и вниз не больше −70% за трейд (иначе цена в ноль)


def sync_board_roster():
    """Держим board.order в согласии с игроками: добавляем новых (клетка 0), убираем ушедших,
    позиции существующих сохраняем. Вызывать ВНУТРИ LOCK."""
    b = DATA.setdefault('board', {'order': [], 'turnIdx': 0, 'positions': {}, 'levels': {}, 'lastRoll': None})
    ids = [p['id'] for p in DATA.get('players', []) if 'id' in p]
    idset = set(str(i) for i in ids)
    for pid in ids:
        if pid not in b['order']:
            b['order'].append(pid)
        b['positions'].setdefault(str(pid), 0)
        b['levels'].setdefault(str(pid), 0)
    b['order'] = [pid for pid in b['order'] if str(pid) in idset]
    b['positions'] = {k: v for k, v in b['positions'].items() if k in idset}
    b['levels'] = {k: v for k, v in b['levels'].items() if k in idset}
    b['turnIdx'] = (b['turnIdx'] % len(b['order'])) if b['order'] else 0


def broadcast():
    """Толкнуть текущий стейт всем подключённым браузерам мгновенно.
    Вызывается ВНУТРИ LOCK после каждой мутации DATA."""
    try:
        body = json.dumps(DATA, ensure_ascii=False)
    except Exception:
        return
    with SUBS_LOCK:
        subs = list(SUBSCRIBERS)
    for q in subs:
        try:
            q.put_nowait(body)
        except Exception:
            pass   # очередь переполнена/мертва — не блокируем, страховка догонит опросом


def load_data():
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text(encoding='utf-8'))
        except Exception:
            pass
    return {'players': [], 'deals': [], 'market': None, 'world': None,
            'board': {'order': [], 'turnIdx': 0, 'positions': {}, 'levels': {}, 'lastRoll': None}}


_dirty = False


def save_data(d):
    # ОТЛОЖЕННАЯ запись: помечаем стейт «грязным», фоновый флашер пишет на диск пачкой ~1с.
    # Убирает лаг от записи на КАЖДЫЙ POST — мутация в памяти и broadcast остаются мгновенными.
    global _dirty
    _dirty = True


def _flush_now():
    global _dirty
    with LOCK:
        body = json.dumps(DATA, ensure_ascii=False, indent=2)
        _dirty = False
    DATA_FILE.write_text(body, encoding='utf-8')


def data_flusher():
    """Фоновый поток: раз в секунду сбрасывает стейт на диск, если менялся."""
    while True:
        time.sleep(1.0)
        if _dirty:
            try:
                _flush_now()
            except Exception as e:
                print('flusher error:', e)


# ═══ ВЛОЖЕНИЕ В СЕБЯ И СОЗДАНИЕ (спека VIA_SELF_CREATION_SPEC_2026_07_21.md) ═══
# Тренинг НЕ даёт денег и НЕ трогает зарплату — он ключ к созданию и к колоде.
# Держим на сервере, потому что здесь считаются деньги; клиент рисует те же цифры.
SELF_TRAININGS = {
    'ai':     {'name': 'ИИ и автоматизация 🤖',     'cost': 4000},
    'sales':  {'name': 'Продажи 💼',                'cost': 3000},
    'nego':   {'name': 'Переговоры 🤝',             'cost': 3500},
    'brand':  {'name': 'Личный бренд ⭐',           'cost': 5000},
    'stage':  {'name': 'Публичные выступления 🎤',  'cost': 2500},
    'fin':    {'name': 'Финансовая грамотность 📊', 'cost': 2000},
    'psy':    {'name': 'Психология и эмоции 🧠',    'cost': 3000},
    'health': {'name': 'Здоровье и энергия 💪',     'cost': 2000},
    'mkt':    {'name': 'Маркетинг и трафик 📣',     'cost': 3500},
    'eng':    {'name': 'Английский 🗣️',             'cost': 2500},
    'voice':  {'name': 'Курсы голоса 🎙️',           'cost': 2500},   # Ринат 22июл: открывает «Стать звездой»
}
# req — нужны ВСЕ; reqAny — достаточно одного; boost — двигают колоду 7/3 в сторону успеха.
CREATIONS = {
    'book':       {'name': 'Книга 📖',                 'cost': 10000, 'cf': 800,
                   'req': [], 'reqAny': ['brand', 'stage'], 'boost': ['brand', 'sales', 'mkt', 'eng']},
    'prog':       {'name': 'Программа 💻',             'cost': 10000, 'cf': 1000,
                   'req': ['ai'], 'boost': ['ai', 'mkt', 'eng', 'sales']},
    'course':     {'name': 'Онлайн-курс 🎓',           'cost': 8000,  'cf': 900,
                   'req': ['sales', 'stage'], 'boost': ['stage', 'sales', 'brand', 'psy']},
    'brandprod':  {'name': 'Свой бренд товара 🏷️',     'cost': 15000, 'cf': 2000,
                   'req': ['mkt', 'sales'], 'boost': ['mkt', 'sales', 'nego', 'brand']},
    'blog':       {'name': 'Блог и контент 📱',        'cost': 1500,  'cf': 300, 'grow': 150,
                   'req': ['brand'], 'boost': ['brand', 'mkt', 'stage', 'psy']},
    'mentor':     {'name': 'Наставничество 🧑‍🏫',       'cost': 2500,  'cf': 150,
                   'req': [], 'boost': [], 'minSkills': 4, 'needFail': True},
    # Ринат 22июл: путь артиста — кафе (100$/мес) → маленькие концерты (×2) → звезда города (×2)
    'star':       {'name': 'Стать звездой 🌟',         'cost': 1500,  'cf': 100,
                   'req': ['voice'], 'boost': ['stage', 'brand', 'mkt', 'psy']},
}
# Потолок колоды: шансы поднимают максимум 3 навыка. Купил десять — они откроют тебе все
# созданиs, но гарантии не купишь. Выигрывает тот, кто заранее решил, КЕМ он становится,
# и вложился прицельно. «Раскрыть СВОИ таланты» — значит выбрать, а не собрать всё.
BOOST_CAP = 3

# ─── РАЗВИТИЕ СОЗДАННОГО (Ринат 21 июл) ───
# Три ступени. Первая — само творение. Каждая следующая УДВАИВАЕТ поток и стоит от своей же
# цены: вторая столько же, третья втрое. Так книга за 10к вырастает с 800 до 3200 в месяц,
# вложив всего 50к — вровень с хорошим бизнесом.
# Проценты от маленького зерна не работали: 500к ради +480 в месяц окупались бы 86 лет,
# а партия идёт 30. Поэтому удвоение, а не прибавка процента.
# Развитие НЕ тянет колоду: лотерея была один раз, на входе. Дальше ты уже автор.
DEV_COST_MULT = {2: 1.0, 3: 3.0}
CREATION_STEPS = {
    'book':      {2: 'Допечатка тиража',    3: 'Перевод и аудиоверсия'},
    'prog':      {2: 'Платная подписка',    3: 'Мобильная версия'},
    'course':    {2: 'Живые потоки',        3: 'Своя школа'},
    'brandprod': {2: 'Своя точка продаж',   3: 'Франшиза'},
    'blog':      {2: 'Реклама у тебя',      3: 'Своё медиа'},
    'mentor':    {2: 'Групповые разборы',   3: 'Школа наставников'},
    'star':      {2: 'Маленькие концерты',  3: 'Звезда своего города'},
}

# Цена растёт ступеньками (Ринат 21 июл): первый тренинг по базовой цене, 2-4 дороже на 20%,
# с 5-го на 40%, с 9-го на 80%. Смысл: вход дёшев для бедного — лестница жива, — а собрать
# всё подряд дорого, и человеку приходится ВЫБИРАТЬ, кем становиться.
def train_mult(owned):
    if owned <= 0:
        return 1.0
    if owned <= 3:
        return 1.2
    if owned <= 7:
        return 1.4
    return 1.8


def train_price(key, owned):
    return round(SELF_TRAININGS[key]['cost'] * train_mult(owned))


# ═══ 💎 КОЛЛЕКЦИЯ И АУКЦИОН (Ринат 23июл, спека VIA_COLLECTION_AUCTION_SPEC_2026_07_23.md) ═══
# Дух: бизнес кормит, биржа играет, коллекция БОГАТЕЕТ МОЛЧА. Вещь не капает в поток —
# она дорожает на бархате. Зачем собирать: сет темы = +200$/мес, легендарка = козырь двери,
# ПОЛНАЯ коллекция (4 темы) = на большой круг пускают с 4 картами души из 5 (слово Рината).
# Все деньги и вся судьба лотов — ЗДЕСЬ, под LOCK. Клиент только рисует и шлёт намерение.
# Пять кругов редкости — как в больших играх (Ринат 23июл): серый (туфта) → голубой →
# золотой → фиолетовый мифик → ТЁМНО-КРАСНЫЙ легендарный, при виде которого стол замолкает.
# gray/blue — без лимита копий; gold — 2 копии на партию; mythic/legend — ПО ОДНОЙ, вышла и всё.
COLLECTION_ITEMS = {
    # Ринат 23июл: «мусора много, голда тоже много, легендарок побольше — их много в мире. Как в жизни».
    # тема ВРЕМЯ ⌛
    'watch':     {'name': 'Карманные часы',           'theme': 'time',   'rar': 'gray',   'base': 3000},
    'compass':   {'name': 'Старый компас',            'theme': 'time',   'rar': 'gray',   'base': 3500},
    'hourglass': {'name': 'Песочные часы капитана',   'theme': 'time',   'rar': 'blue',   'base': 6000},
    'clock':     {'name': 'Каминные часы с боем',     'theme': 'time',   'rar': 'gold',   'base': 20000},
    'astrolabe': {'name': 'Астролябия звездочёта',    'theme': 'time',   'rar': 'gold',   'base': 35000},
    # тема ИСКУССТВО 🎻
    'cup':       {'name': 'Фарфоровая чашка',         'theme': 'art',    'rar': 'gray',   'base': 2000},
    'statue':    {'name': 'Статуэтка танцовщицы',     'theme': 'art',    'rar': 'blue',   'base': 5500},
    'shkatulka': {'name': 'Музыкальная шкатулка',     'theme': 'art',    'rar': 'blue',   'base': 9000},
    'violin':    {'name': 'Скрипка старого мастера',  'theme': 'art',    'rar': 'gold',   'base': 28000},
    'painting':  {'name': 'Картина «Буря»',           'theme': 'art',    'rar': 'gold',   'base': 38000},
    'goblet':    {'name': 'Кубок венецианского стекла', 'theme': 'art',  'rar': 'gold',   'base': 22000},
    # тема СЛОВО 📜
    'newspaper': {'name': 'Столетняя газета',         'theme': 'word',   'rar': 'gray',   'base': 1500},
    'quill':     {'name': 'Перо писателя',            'theme': 'word',   'rar': 'gray',   'base': 2500},
    'map':       {'name': 'Старая карта мира',        'theme': 'word',   'rar': 'blue',   'base': 7000},
    'letter':    {'name': 'Письмо с сургучом',        'theme': 'word',   'rar': 'blue',   'base': 6500},
    'firsted':   {'name': 'Первое издание',           'theme': 'word',   'rar': 'gold',   'base': 30000},
    # тема СИЛА ⚔️
    'horseshoe': {'name': 'Ржавая подкова',           'theme': 'power',  'rar': 'gray',   'base': 1000},
    'coin':      {'name': 'Монета империи',           'theme': 'power',  'rar': 'gray',   'base': 4000},
    'king':      {'name': 'Шахматный король',         'theme': 'power',  'rar': 'blue',   'base': 8000},
    'helmet':    {'name': 'Шлем крестоносца',         'theme': 'power',  'rar': 'gold',   'base': 36000},
    'blade':     {'name': 'Клинок дамасской стали',   'theme': 'power',  'rar': 'gold',   'base': 32000},
    'kamcha':    {'name': 'Ханская камча',            'theme': 'power',  'rar': 'gold',   'base': 40000},
    # 👑 ФИОЛЕТОВЫЕ МИФИКИ — по одному на партию. desc читает аукционист (Ринат 23июл).
    'lion':      {'name': 'Чёрный лев',                'theme': 'legend', 'rar': 'mythic', 'base': 150000,
                  'desc': 'Вырезан из цельного обсидиана мастером, чьё имя стёрло время. Говорят, он приносит хозяину хладнокровие в сделках.'},
    'shogun':    {'name': 'Меч сёгуна',                'theme': 'legend', 'rar': 'mythic', 'base': 200000,
                  'desc': 'Клинок последнего сёгуна. Волна закалки видна до сих пор — оружие, которое ни разу не проиграло.'},
    'khan':      {'name': 'Сабля великого хана',       'theme': 'legend', 'rar': 'mythic', 'base': 250000,
                  'desc': 'Золото и бирюза Великой степи. Такой саблей не рубили — ею правили.'},
    'crown':     {'name': 'Корона забытого царства',   'theme': 'legend', 'rar': 'mythic', 'base': 300000,
                  'desc': 'Царство исчезло с карт, а корона пережила всех, кто её носил. Рубины тёмные, как его история.'},
    'necklace':  {'name': 'Ожерелье царицы',           'theme': 'legend', 'rar': 'mythic', 'base': 400000,
                  'desc': 'Изумруды и жемчуг, помнящие шею царицы. Каждый камень — чья-то присяга.'},
    # 🔴 ТЁМНО-КРАСНЫЕ ЛЕГЕНДАРКИ — по одной на партию, при виде которых стол замолкает
    'egg':       {'name': 'Золотое яйцо ювелира',      'theme': 'legend', 'rar': 'legend', 'base': 600000,
                  'desc': 'Последняя работа великого ювелира. Внутри — механизм, который никто не решается завести.'},
    'mask':      {'name': 'Золотая маска фараона',     'theme': 'legend', 'rar': 'legend', 'base': 700000,
                  'desc': 'Три тысячи лет молчания в чистом золоте и лазурите. Гробница, из которой её вынесли, засекречена.'},
    'scepter':   {'name': 'Скипетр императора',        'theme': 'legend', 'rar': 'legend', 'base': 800000,
                  'desc': 'Власть, отлитая в золоте, с рубином размером с сердце. Империя пала — скипетр остался.'},
    'ring':      {'name': 'Перстень Клеопатры',        'theme': 'legend', 'rar': 'legend', 'base': 1000000,
                  'desc': 'Изумрудный скарабей на золоте трёх империй. Эту руку целовали цари.'},
    'diamond':   {'name': 'Чёрный бриллиант «Око ночи»', 'theme': 'legend', 'rar': 'legend', 'base': 900000,
                  'desc': 'Редчайший чёрный бриллиант в мире. В его гранях горит фиолетовый огонь, который никто не смог объяснить.'},
    # Живые сокровища и казахский пантеон (Ринат 23июл): цены как у арабов — миллионы.
    'ram':       {'name': 'Дикий архар',               'theme': 'legend', 'rar': 'legend', 'base': 200000,
                  'desc': 'Редчайшая порода горных архаров — чистейшие гены на земле. Для разведения и создания лучшего потомства в мире. Дикого не приручить — в этом и статус.'},
    'eagle':     {'name': 'Золотой беркут',            'theme': 'legend', 'rar': 'legend', 'base': 500000,
                  'desc': 'Царь неба Великой степи, отлитый в чистом золоте, с глазами из бирюзы. Символ тех, кто смотрит сверху.'},
    'horse':     {'name': 'Золотой тулпар',            'theme': 'legend', 'rar': 'legend', 'base': 1500000,
                  'desc': 'Крылатый конь степных легенд в чистом золоте. Говорят, кто владеет тулпаром — того не догнать ни в деле, ни в судьбе.'},
    'stallion':  {'name': 'Арабский скакун',           'theme': 'legend', 'rar': 'legend', 'base': 3000000,
                  'desc': 'Легендарный вороной производитель чистейшей арабской линии. Родословная длиннее, чем у иных королей. Для разведения и потомства, которое войдёт в историю. Торги — начало!'},
    'altyn':     {'name': 'Золотой человек',           'theme': 'legend', 'rar': 'legend', 'base': 5000000,
                  'desc': 'Алтын Адам. Воин Великой степи в доспехе из тысяч золотых пластин. Душа народа, отлитая в золоте. Национальное достояние — и сегодня оно на торгах.'},
}
COL_THEMES = ('time', 'art', 'word', 'power')       # сет = 3 вещи одной темы
COL_GROWTH = (0.02, 0.04)   # антиквариат дорожает: +2..4% за игровой год, сам, молча
AUC_START_PCT = 0.5         # старт лота = половина текущей цены
AUC_LOT_SECONDS = 30        # таймер лота (Ринат 23июл: «60с долго, игра всего 3 часа» — было 60)
AUC_ANTISNIPE = 10          # ставка в последние 10с продлевает на 10с (перебивание на флажке)
AUC_MIN_STEP = 0.05         # шаг ставки — от +5%
AUC_LOTS = 3                # лотов от игры за аукцион
AUC_DARK_LOTS = 2           # тёмных лотов за аукцион
# ── Анти-сухость (Ринат дал добро 23июл): резерв + Незнакомец + аукцион-праздник ──
AUC_RESERVE_PCT = 0.75      # скрытый резерв лота игры: ниже 75% красной цены вещь не отдаём
AUC_PERIOD = 18 * 60        # праздник сам, раз в ~2 круга стола (18 мин), ведущему не вспоминать
AUC_WARN = 120              # объявление за 2 минуты: «готовьте деньги»
WORLD_AUTO_GAP = 12 * 60    # мир не молчит дольше 12 мин: событие придёт само, даже если ходы стоят
# НЕЗНАКОМЕЦ — таинственный коллекционер. За мусором приходит не всегда и торгуется вяло,
# за легендарками — всегда и бешено. Может выиграть: мифик/легендарка уходит из мира навсегда.
NPC_NAME = 'Незнакомец'
NPC_RANGES = {'gray': (0.55, 0.85), 'blue': (0.60, 0.90), 'gold': (0.70, 1.00),
              'mythic': (0.85, 1.15), 'legend': (0.85, 1.20)}   # потолок его торга, доля красной цены
NPC_INTEREST = {'gray': 0.5, 'blue': 0.65, 'gold': 0.85}        # шанс, что вообще пришёл (остальные = 1.0)
NPC_BID_CHANCE = 0.30       # шанс ставки на тик (2с) + всегда пытается на флажке
DARK_PRICE = 4000           # цена запечатанного ящика
DARK_WIN = 0.60             # правило Рината: 60% вещь дороже цены ящика, 40% — дешевле
# Редкость лота с молотка: обычная 70% / редкая 25% / легендарка 5%.
# Копии: обычных вещей в мире несколько, редких — по 2 на партию, легендарка — одна.
RARE_COPIES = 2


def col_prices():
    ps = DATA.setdefault('colPrices', {})
    for k, it in COLLECTION_ITEMS.items():
        ps.setdefault(k, it['base'])
    return ps


def col_grow_prices():
    ps = col_prices()
    for k in ps:
        ps[k] = round(ps[k] * (1 + random.uniform(*COL_GROWTH)))


def _col_out_count(key):
    """Сколько копий вещи уже живёт в коллекциях/эскроу — для лимитов редких и легендарок."""
    n = 0
    for p in DATA.get('players', []):
        n += sum(1 for c in (p.get('collection') or []) if c.get('key') == key)
    n += sum(1 for s in DATA.get('colSellQueue', []) if s.get('key') == key)
    a = DATA.get('auction') or {}
    cur = a.get('cur') or {}
    if cur.get('item') == key and not cur.get('seller'):
        n += 1
    n += sum(1 for l in (a.get('queue') or []) if l.get('item') == key and not l.get('seller'))
    return n


def col_available(rar):
    out = []
    for k, it in COLLECTION_ITEMS.items():
        if it['rar'] != rar:
            continue
        # мифик и легендарка — единственные в мире: вышла один раз и всё
        if rar in ('mythic', 'legend') and (k in DATA.get('colLegendOut', []) or _col_out_count(k)):
            continue
        if rar == 'gold' and _col_out_count(k) >= RARE_COPIES:
            continue
        out.append(k)
    return out


def col_pick_lot(allow_legend=True):
    """Вытянуть вещь для лота с молотка. Веса пяти кругов: серый 40 / голубой 30 /
    золотой 22 / мифик 5 / легендарный 3. Редчайшее — редчайший гость молотка."""
    r = random.random()
    if allow_legend and r < 0.03:
        order = ['legend', 'mythic', 'gold', 'blue', 'gray']
    elif allow_legend and r < 0.08:
        order = ['mythic', 'gold', 'blue', 'gray']
    elif r < 0.30:
        order = ['gold', 'blue', 'gray']
    elif r < 0.60:
        order = ['blue', 'gray', 'gold']
    else:
        order = ['gray', 'blue', 'gold']
    for rar in order:
        pool = col_available(rar)
        if pool:
            return random.choice(pool)
    return None


def _npc_ceiling(key):
    """До скольки Незнакомец готов торговаться за эту вещь. 0 = не пришёл."""
    it = COLLECTION_ITEMS.get(key) or {}
    rar = it.get('rar', 'gray')
    if random.random() > NPC_INTEREST.get(rar, 1.0):
        return 0
    lo, hi = NPC_RANGES.get(rar, (0.5, 0.8))
    worth = col_prices().get(key, it.get('base') or 0)
    return round(worth * random.uniform(lo, hi))


def auction_settle_lot(a):
    """Закрыть текущий лот: вещь победителю, деньги продавцу или в банк. Вызывать под LOCK."""
    cur = a.get('cur')
    if not cur:
        return
    key, price, bidder = cur.get('item'), float(cur.get('price') or 0), cur.get('bidder')
    it = COLLECTION_ITEMS.get(key) or {}
    log = a.setdefault('log', [])
    # 🕴️ Незнакомец выиграл: вещь уходит из мира, продавцу-игроку — живые деньги
    if bidder == 'npc':
        if it.get('rar') in ('mythic', 'legend'):
            DATA.setdefault('colLegendOut', []).append(key)   # ушла навсегда
        if cur.get('seller') is not None:
            seller = next((p for p in DATA['players'] if p.get('id') == cur['seller']), None)
            if seller is not None:
                seller['savings'] = round(float(seller.get('savings') or 0) + price, 2)
                seller['p2pSeq'] = int(seller.get('p2pSeq') or 0) + 1
                seller['notify'] = '🔨 «%s» забрал %s за %s$. Деньги в кассе.' % (it.get('name', key), NPC_NAME, int(price))
        log.append({'item': key, 'price': price, 'name': NPC_NAME, 'sold': True, 'npc': True})
        a['cur'] = None
        return
    winner = next((p for p in DATA['players'] if p.get('id') == bidder), None) if bidder is not None else None
    # 🤫 Скрытый резерв лота игры: дешевле 75% красной цены мир вещь не отдаёт
    if winner is not None and cur.get('res') and cur.get('seller') is None and price < float(cur['res']):
        winner['notify'] = '🔨 «%s» снята с торгов — резерв не взят. Вещь вернулась в мир.' % it.get('name', key)
        log.append({'item': key, 'price': price, 'name': None, 'sold': False, 'res': True})
        a['cur'] = None
        return
    if winner is not None and float(winner.get('savings') or 0) >= price:
        winner['savings'] = round(float(winner.get('savings') or 0) - price, 2)
        # касса изменена сервером → двигаем p2pSeq, чтобы устаревший снимок бланка её не затёр
        winner['p2pSeq'] = int(winner.get('p2pSeq') or 0) + 1
        winner.setdefault('collection', []).append({'key': key, 'paid': price, 'at': int(time.time())})
        winner['notify'] = '🔨 Твоя! «%s» за %s$ — в коллекции.' % (it.get('name', key), int(price))
        if it.get('rar') in ('mythic', 'legend'):
            DATA.setdefault('colLegendOut', []).append(key)
        seller = None
        if cur.get('seller') is not None:
            seller = next((p for p in DATA['players'] if p.get('id') == cur['seller']), None)
        if seller is not None:
            seller['savings'] = round(float(seller.get('savings') or 0) + price, 2)
            seller['p2pSeq'] = int(seller.get('p2pSeq') or 0) + 1
            seller['notify'] = '🔨 «%s» продана с молотка за %s$.' % (it.get('name', key), int(price))
        # лот от игры → деньги сгорают в банк: стол не заливаем
        log.append({'item': key, 'price': price, 'name': winner.get('name'), 'sold': True})
    else:
        # никто не взял (или у победителя к закрытию не хватило) — вещь возвращается
        if cur.get('seller') is not None:
            owner = next((p for p in DATA['players'] if p.get('id') == cur['seller']), None)
            if owner is not None:
                owner.setdefault('collection', []).append(
                    {'key': key, 'paid': float(cur.get('min') or 0), 'at': int(time.time())})
                owner['notify'] = '🔨 «%s» не нашла покупателя — вернулась в коллекцию.' % it.get('name', key)
        log.append({'item': key, 'price': 0, 'name': None, 'sold': False})
    a['cur'] = None


def auction_next_lot(a):
    """Поставить следующий лот на молоток. Вызывать под LOCK."""
    q = a.setdefault('queue', [])
    if not q:
        a['status'] = 'done'
        a['doneAt'] = time.time()
        return
    lot = q.pop(0)
    key = lot['item']
    worth = col_prices().get(key, COLLECTION_ITEMS[key]['base'])
    start = lot.get('min') or max(1, round(worth * AUC_START_PCT))
    a['cur'] = {'item': key, 'price': start, 'start': start, 'bidder': None, 'bidderName': None,
                'seller': lot.get('seller'), 'min': lot.get('min'),
                # резерв только у лотов игры; у лота игрока его минималка и есть старт
                'res': (round(worth * AUC_RESERVE_PCT) if lot.get('seller') is None else None),
                'npcMax': _npc_ceiling(key),   # 🕴️ до скольки сегодня торгуется Незнакомец
                'endsAt': time.time() + AUC_LOT_SECONDS}


def auction_open(by):
    """Собрать лоты и открыть торги. Вызывать под LOCK. Возвращает (ok, reason|лотов)."""
    a = DATA.get('auction')
    if a and a.get('status') == 'live':
        return False, 'busy'
    queue = []
    allow_legend = True
    for _ in range(AUC_LOTS):
        k = col_pick_lot(allow_legend)
        if k is None:
            break
        if COLLECTION_ITEMS[k]['rar'] in ('mythic', 'legend'):
            allow_legend = False   # мифик/легендарка — не больше одной за аукцион
        queue.append({'item': k, 'seller': None})
    # вещи игроков, выставленные на продажу, идут после лотов игры
    sq = DATA.get('colSellQueue', [])
    queue.extend({'item': s['key'], 'seller': s['pid'], 'min': s.get('min')} for s in sq)
    DATA['colSellQueue'] = []
    if not queue:
        return False, 'empty'
    DATA['auction'] = {'status': 'live', 'openedBy': str(by or 'ведущий'),
                       'queue': queue, 'cur': None, 'log': [],
                       'darkLeft': AUC_DARK_LOTS, 'startedAt': time.time()}
    auction_next_lot(DATA['auction'])
    # расписание праздника: следующий сам через AUC_PERIOD, объявление ещё не делали
    DATA['aucNextAt'] = time.time() + AUC_PERIOD
    DATA['aucWarned'] = False
    return True, len(queue) + 1


def _notify_all(msg):
    """Шепнуть всему столу. Вызывать под LOCK."""
    for p in DATA.get('players', []):
        p['notify'] = msg


def auction_ticker():
    """Фоновый поток: закрывает истёкшие лоты, торгуется за Незнакомца, открывает
    аукцион-праздник по расписанию, страхует мировые события, растит цены антиквариата."""
    while True:
        time.sleep(2.0)
        try:
            with LOCK:
                changed = False
                now = time.time()
                # рост цен: цепляемся к game_year живого рынка
                gy = int(((DATA.get('market') or {}).get('game_year')) or 0)
                if gy and gy != int(DATA.get('colYear') or 0):
                    DATA['colYear'] = gy
                    col_grow_prices()
                    changed = True
                a = DATA.get('auction')
                if a and a.get('status') == 'live':
                    cur = a.get('cur')
                    if cur and now >= float(cur.get('endsAt') or 0):
                        auction_settle_lot(a)
                        auction_next_lot(a)
                        changed = True
                    elif not cur:
                        auction_next_lot(a)
                        changed = True
                    elif cur:
                        # 🕴️ НЕЗНАКОМЕЦ: вяло на мусор, бешено на легендарки, любит флажок
                        left = float(cur.get('endsAt') or 0) - now
                        price = float(cur.get('price') or 0)
                        need = price if cur.get('bidder') is None else round(price * (1 + AUC_MIN_STEP))
                        if (cur.get('npcMax') and cur.get('bidder') != 'npc'
                                and need <= float(cur['npcMax'])
                                and (random.random() < NPC_BID_CHANCE or left < 8)):
                            cur['price'] = need
                            cur['bidder'] = 'npc'
                            cur['bidderName'] = NPC_NAME
                            if left < AUC_ANTISNIPE:
                                cur['endsAt'] = now + AUC_ANTISNIPE
                            changed = True
                else:
                    # 🎪 ПРАЗДНИК ПО РАСПИСАНИЮ (Ринат: «забываю про аукцион — пусть автоматом»)
                    nxt = float(DATA.get('aucNextAt') or 0)
                    if not nxt:
                        DATA['aucNextAt'] = now + AUC_PERIOD
                    elif DATA.get('players'):
                        if now >= nxt - AUC_WARN and not DATA.get('aucWarned'):
                            DATA['aucWarned'] = True
                            _notify_all('🔨 Через 2 минуты — АУКЦИОН! Готовьте деньги: молоток ждать не будет.')
                            changed = True
                        if now >= nxt:
                            ok, _n = auction_open('мир')
                            if ok:
                                _notify_all('🔨 АУКЦИОН ОТКРЫТ — молоток стучит! Смотри плитку «Аукцион».')
                            else:
                                DATA['aucNextAt'] = now + AUC_PERIOD   # выставить нечего — перенесли
                                DATA['aucWarned'] = False
                            changed = True
                # 🌍 СТРАХОВКА МИРА: событие раз в круг идёт от ходов, но если ходы стоят —
                # мир всё равно живёт: не молчим дольше WORLD_AUTO_GAP
                wl = float(DATA.get('worldLastAt') or 0)
                if not wl:
                    DATA['worldLastAt'] = now
                elif DATA.get('players') and now - wl > WORLD_AUTO_GAP:
                    try:
                        we = next_world_event()
                        DATA['world'] = we
                        _fx = we.get('fx') or {}
                        apply_market_shock(_fx)
                        apply_business_shock(_fx.get('biz', 0))
                        DATA['worldLastAt'] = now
                        changed = True
                    except Exception as _e:
                        print('world auto error:', _e)
                        DATA['worldLastAt'] = now   # не долбим каждые 2с
                if changed:
                    save_data(DATA)
                    broadcast()
        except Exception as e:
            print('auction ticker error:', e)


DATA = load_data()
# инициализируем живой рынок, если его ещё нет (первый запуск)
if via_market and not DATA.get('market'):
    DATA['market'] = via_market.init_market()
    save_data(DATA)
# board может отсутствовать в старом via_data.json — заводим
if not DATA.get('board'):
    DATA['board'] = {'order': [], 'turnIdx': 0, 'positions': {}, 'levels': {}, 'lastRoll': None}
    save_data(DATA)
# сразу посадить уже загруженных игроков на поле
sync_board_roster()
save_data(DATA)
# ликвидность стакана могла отсутствовать у активов из старого via_data.json — проставляем по типу
if via_market and DATA.get('market') and DATA['market'].get('assets'):
    _liq_changed = False
    for _a in DATA['market']['assets']:
        if not _a.get('liq'):
            _a['liq'] = via_market.asset_liq(_a.get('type'), _a.get('vol'))
            _liq_changed = True
    if _liq_changed:
        save_data(DATA)
# НОВЫЕ активы из CSV, которых нет в сохранённом рынке (напр. добавленные мем-токены) — ДОМЕРЖИМ,
# не трогая цены уже живущих активов. Так на деплое появятся ASS/TASAK и др. без сброса игры.
# Плюс обновляем ЛИКВИДНОСТЬ существующих под новую логику стаканов (крупные — глубокие, мемы — тонкие).
if via_market and DATA.get('market') and DATA['market'].get('assets'):
    _fresh = via_market.load_assets()
    _by_id = {str(a.get('id')): a for a in _fresh}
    _have = {str(a.get('id')) for a in DATA['market']['assets']}
    _changed = False
    for _a in DATA['market']['assets']:                     # пересчёт стаканов существующих
        _src = _by_id.get(str(_a.get('id')))
        _newliq = via_market.asset_liq(_a.get('type'), _a.get('vol'))
        if _a.get('liq') != _newliq:
            _a['liq'] = _newliq; _changed = True
    for _fa in _fresh:                                      # домерж новых активов
        if str(_fa.get('id')) not in _have:
            DATA['market']['assets'].append(_fa); _changed = True
    if _changed:
        save_data(DATA)


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=str(ROOT), **kw)

    def log_message(self, *a, **kw):
        pass  # тихий режим

    def end_headers(self):
        # 2026-05-31: запрещаем кэш — иначе телефон показывает старую версию игры
        # после правок (PIN/биржа не появлялись из-за кэша браузера).
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        self.send_header('Pragma', 'no-cache')
        super().end_headers()

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def _send_json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self._cors()
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if self.path == '/events':
            # SSE — открытый провод: сервер сам толкает стейт браузеру мгновенно.
            q = queue.Queue(maxsize=20)
            with SUBS_LOCK:
                SUBSCRIBERS.add(q)
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream; charset=utf-8')
            self.send_header('Cache-Control', 'no-cache, no-store')
            self.send_header('Connection', 'keep-alive')
            self.send_header('X-Accel-Buffering', 'no')  # не буферить у прокси (nginx/Render)
            self._cors()
            self.end_headers()
            try:
                with LOCK:
                    init = json.dumps(DATA, ensure_ascii=False)
                self.wfile.write(f'data: {init}\n\n'.encode('utf-8'))
                self.wfile.flush()
                while True:
                    try:
                        body = q.get(timeout=20)
                    except queue.Empty:
                        self.wfile.write(b': ping\n\n')  # держим провод живым
                        self.wfile.flush()
                        continue
                    self.wfile.write(f'data: {body}\n\n'.encode('utf-8'))
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass   # браузер закрыл вкладку — нормально
            finally:
                with SUBS_LOCK:
                    SUBSCRIBERS.discard(q)
            return
        if self.path == '/data':
            with LOCK:
                body = json.dumps(DATA, ensure_ascii=False).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Cache-Control', 'no-store')
            self._cors()
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path == '/market':
            with LOCK:
                body = json.dumps(DATA.get('market') or {}, ensure_ascii=False).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Cache-Control', 'no-store')
            self._cors()
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path == '/newgame':
            # НОВАЯ ИГРА — свежая биржа с нуля (Год 1), игроки/сделки очищены.
            with LOCK:
                DATA['players'] = []
                DATA['deals'] = []
                DATA['market'] = via_market.init_market() if via_market else None
                DATA['world'] = None
                # 💎 коллекция: новая партия — новые легендарки, свежие цены, пустой молоток
                DATA['auction'] = None
                DATA['colPrices'] = {}
                DATA['colLegendOut'] = []
                DATA['colSellQueue'] = []
                DATA['colYear'] = 0
                DATA['aucNextAt'] = time.time() + AUC_PERIOD   # праздник придёт сам, часы с нуля
                DATA['aucWarned'] = False
                DATA['worldLastAt'] = time.time()
                save_data(DATA)
                broadcast()   # мгновенно: новая игра у всех
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Cache-Control', 'no-store')
            self._cors()
            self.end_headers()
            self.wfile.write(('<meta charset="utf-8"><body style="font-family:sans-serif;'
                'background:#0b1020;color:#e6edf6;padding:40px;text-align:center">'
                '<h2>🔄 Новая игра — биржа сброшена с нуля (Год 1)</h2>'
                '<p style="color:#8aa0c0">Игроки и сделки очищены, рынок свежий.</p>'
                '<p><a style="color:#2b6fff;font-size:18px" href="/via.html">▶ Открыть Via</a></p>'
                ).encode('utf-8'))
            return
        if self.path == '/' or self.path == '':
            self.send_response(302)
            self.send_header('Location', '/via.html')
            self.end_headers()
            return
        return super().do_GET()

    def do_POST(self):
        if self.path == '/data':
            n = int(self.headers.get('Content-Length', 0) or 0)
            raw = self.rfile.read(n).decode('utf-8') if n else '{}'
            try:
                new = json.loads(raw)
                if not isinstance(new, dict):
                    raise ValueError('payload must be object')
                with LOCK:
                    global DATA
                    me = new.get('me')  # id игрока → слить ТОЛЬКО его; None → ведущий/полная запись
                    if me is not None:
                        # СЛИЯНИЕ (анти-гонка): обновляем только запись игрока `me`,
                        # остальных берём с сервера — чужие покупки/зарплаты не затираются.
                        incoming = {p['id']: p for p in new.get('players', []) if 'id' in p}
                        cur = {p['id']: p for p in DATA['players'] if 'id' in p}
                        if me in incoming:
                            srv_prev = cur.get(me)
                            rec = incoming[me]
                            if srv_prev is not None:
                                # 🔒 СЕРВЕР-АВТОРИТЕТ по тюрьме (Ринат 19июл «тюрьма сама не уходит»):
                                # jail/jailPress меняет ТОЛЬКО сервер (посадка на __JAIL__, декремент в /turn/next,
                                # откуп через /jail/buyout). Клиентский save НЕ должен воскрешать старый срок —
                                # раньше `cur[me]=incoming[me]` затирал серверный декремент старым снимком.
                                rec['jail'] = srv_prev.get('jail', rec.get('jail'))
                                rec['jailPress'] = srv_prev.get('jailPress', rec.get('jailPress'))
                                # 🔒 СЕРВЕР-АВТОРИТЕТ по стартовым ДОЛГАМ (Ринат 19июл «долг воскресает, гасил 4 раза;
                                # закладная не списывается»). Погашение ставит ведущий (одобрение заявки, авторитетная
                                # запись). Игрок сам эти поля НЕ меняет — но его старый бланк затирал «оплачено» назад.
                                # Держим серверные значения флагов и остатков. (Банковский кредит bankLoan НЕ трогаем —
                                # его игрок берёт сам на клиенте.)
                                for _fp in ('isM','isE','isA','isCC','isR'):
                                    if _fp in srv_prev: rec[_fp] = srv_prev[_fp]
                                for _fl in ('mLeft','eLeft','aLeft','ccLeft','rLeft'):
                                    if _fl in srv_prev: rec[_fl] = srv_prev[_fl]
                                # 🔒 СЕРВЕР-АВТОРИТЕТ по сделкам игрок↔игрок (#23/#24). Реестр займов игрок
                                # никогда не правит сам — он меняется только эндпоинтами /p2p/*.
                                rec['p2pDebts'] = srv_prev.get('p2pDebts', rec.get('p2pDebts') or [])
                                rec['p2pLoans'] = srv_prev.get('p2pLoans', rec.get('p2pLoans') or [])
                                # 🔒 СЕРВЕР-АВТОРИТЕТ по коллекции (23июл): вещи двигают только /auction/*.
                                # Иначе старый бланк победителя затирал вещь, положенную молотком.
                                rec['collection'] = srv_prev.get('collection', rec.get('collection') or [])
                                rec['darkLots'] = srv_prev.get('darkLots', rec.get('darkLots') or 0)
                                # ⚠️ ГОНКА КАССЫ: перевод денег между игроками происходит на сервере, а бланк
                                # получателя мог сняться ДО перевода — его save затёр бы пришедшие деньги.
                                # p2pSeq растёт при каждом движении денег по p2p; снимок со старым seq
                                # признаём устаревшим и кассу берём серверную.
                                _srv_seq = int(srv_prev.get('p2pSeq') or 0)
                                if int(rec.get('p2pSeq') or 0) < _srv_seq:
                                    rec['savings'] = srv_prev.get('savings', rec.get('savings'))
                                    rec['assets'] = srv_prev.get('assets', rec.get('assets'))
                                rec['p2pSeq'] = _srv_seq
                            cur[me] = rec                   # апсерт только своего бланка
                        DATA['players'] = list(cur.values())
                        # заявки — аддитивно по id (добавить новые / обновить свои), не удаляем
                        dcur = {d['id']: d for d in DATA['deals'] if 'id' in d}
                        for d in new.get('deals', []):
                            if 'id' in d:
                                dcur[d['id']] = d
                        DATA['deals'] = list(dcur.values())
                        # world/market у игрока не трогаем
                    else:
                        # ВЕДУЩИЙ / лобби — полная авторитетная запись.
                        # 🐞 БАГ, пойманный живым прогоном ботов (21 июл): раньше здесь DATA
                        # ПЕРЕСОБИРАЛСЯ с нуля из шести известных ключей — и любой ключ, добавленный
                        # позже, молча пропадал при каждом сохранении ведущего. Так умирал живой мир:
                        # era сбрасывалась, chain рвалась и начиналась заново с первого звена, rumor
                        # исчезал раньше, чем кто-то успевал ответить. Цепочка никогда не доходила
                        # до второго звена, и слухов не было видно вообще.
                        # Теперь правим ПО МЕСТУ: серверные ключи переживают запись ведущего
                        # автоматически, и следующий новый ключ уже не потеряется.
                        DATA['players'] = new.get('players', [])
                        DATA['deals'] = new.get('deals', [])
                        if new.get('world') is not None:
                            DATA['world'] = new.get('world')   # мировое событие — broadcast всем
                        # market, board, p2p, era, chain, rumor — серверная правда, клиент их не шлёт
                    sync_board_roster()   # держим поле в согласии с ростером игроков
                    save_data(DATA)
                    broadcast()   # мгновенно рассылаем изменение всем браузерам
                self.send_response(200)
                self._cors()
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{"ok":true}')
            except Exception as e:
                self.send_response(400)
                self._cors()
                self.end_headers()
                self.wfile.write(str(e).encode())
            return
        if self.path == '/salary':
            n = int(self.headers.get('Content-Length', 0) or 0)
            raw = self.rfile.read(n).decode('utf-8') if n else '{}'
            try:
                req = json.loads(raw)
                pid = req.get('pid')
                flow = round(float(req.get('flow', 0)), 2)
                txid = req.get('txid')
                applied = False
                with LOCK:
                    if not (txid and txid in APPLIED_TX):
                        pl = next((p for p in DATA['players'] if p.get('id') == pid), None)
                        if pl is None:
                            raise ValueError('player not found')
                        pl['savings'] = round(float(pl.get('savings', 0) or 0) + flow, 2)
                        pl['notify'] = (
                            '💵 ЗАРПЛАТА! Ведущий начислил +%s$ (месячный поток). Касса пополнена! 🎉' % flow
                            if flow >= 0 else
                            '💸 Месяц в минус: %s$ (расходы выше дохода). Управляй потоком!' % flow)
                        if txid:
                            APPLIED_TX.add(txid)
                            if len(APPLIED_TX) > 500:
                                APPLIED_TX.clear()   # простая защита от роста набора
                        applied = True
                        save_data(DATA)
                        broadcast()
                self.send_response(200)
                self._cors()
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'ok': True, 'applied': applied}).encode('utf-8'))
            except Exception as e:
                self.send_response(400)
                self._cors()
                self.end_headers()
                self.wfile.write(str(e).encode())
            return
        if self.path == '/turn/roll':
            n = int(self.headers.get('Content-Length', 0) or 0)
            raw = self.rfile.read(n).decode('utf-8') if n else '{}'
            try:
                req = json.loads(raw)
                pid = req.get('pid')
                val = req.get('value')
                with LOCK:
                    sync_board_roster()
                    b = DATA['board']
                    if pid is None and b['order']:
                        pid = b['order'][b['turnIdx']]
                    order_str = [str(x) for x in b['order']]
                    if str(pid) not in order_str:
                        raise ValueError('player not on board')
                    # ведущий выбрал этого игрока — подсветим его как «ходит»
                    b['turnIdx'] = order_str.index(str(pid))
                    value = int(val) if val else random.randint(1, 6)
                    value = max(1, min(6, value))
                    cur = int(b['positions'].get(str(pid), 0))
                    newpos = (cur + value) % TRACK_LEN
                    if newpos < cur:                      # прошли старт (клетку 0)
                        b['levels'][str(pid)] = min(4, int(b['levels'].get(str(pid), 0)) + 1)
                    # АВТО-ЗАРПЛАТА: сколько клеток «Зарплата» пройдено/задето за этот ход
                    salary_due = 0
                    _p = cur
                    for _ in range(value):
                        _p = (_p + 1) % TRACK_LEN
                        if _p in SALARY_CELLS:
                            salary_due += 1
                    b['positions'][str(pid)] = newpos
                    b['lastRoll'] = {'pid': pid, 'value': value}
                    # АВТО-ЗАРПЛАТА НА СЕРВЕРЕ (Ринат 10июл): прошёл клетку «Зарплата» → поток СРАЗУ в кассу,
                    # без пульта ведущего и без клиента. Начисляем flow за каждую пройденную клетку зарплаты.
                    if salary_due > 0:
                        for pl in DATA['players']:
                            if pl.get('id') == pid:
                                flow = calc_flow(pl)
                                total = round(flow * salary_due, 2)
                                pl['savings'] = round(_num(pl.get('savings')) + total, 2)
                                st = pl.setdefault('stats', {})
                                st['salaryCount'] = int(st.get('salaryCount') or 0) + salary_due
                                st['salaryTotal'] = round(_num(st.get('salaryTotal')) + total, 2)
                                pl['notify'] = (
                                    '💵 ЗАРПЛАТА! Прошёл клетку зарплаты — начислено +%s$ (месячный поток). Касса пополнена! 🎉' % total
                                    if total >= 0 else
                                    '💸 Месяц в минус: %s$ (расходы выше дохода). Управляй потоком!' % total)
                                break
                    # Ф2/Ф3: авто-открытие карточки по клетке + окно покупки + таймер хода
                    key = CELL_ALLOW[newpos] if 0 <= newpos < len(CELL_ALLOW) else None
                    if key == '__BUY__':
                        b['buyWindowEndsAt'] = time.time() + BUY_SECONDS   # покупка открыта ВСЕМ 1 мин
                    elif key == '__JAIL__':
                        for pl in DATA['players']:
                            if pl.get('id') == pid:
                                if int(pl.get('jail') or 0) == 0:           # был на свободе → считаем посадку
                                    pl['jailCount'] = int(pl.get('jailCount') or 0) + 1
                                    pl['jailPending'] = True               # запуск выбора под давлением (2 окна) у игрока
                                pl['jail'] = 2                             # клетка Тюрьма — ровно 2 хода (Ринат 19июл), гарантированное освобождение через /turn/next
                                pl['allow'] = None
                                break
                    elif key == '__CHILD__':
                        for pl in DATA['players']:                          # клетка Дети — АВТО: +1 ребёнок, расходы растут (childCost в calc)
                            if pl.get('id') == pid:
                                pl['children'] = int(pl.get('children') or 0) + 1
                                cc = int((pl.get('prof') or {}).get('childCost') or 0)
                                pl['notify'] = '👶 Пополнение в семье! Теперь детей: %d. Расходы на детей растут (+%d$/мес).' % (pl['children'], cc)
                                pl['allow'] = None
                                break
                    elif key:
                        for pl in DATA['players']:
                            if pl.get('id') == pid:
                                pl['allow'] = key                          # авто-открыть карточку ходящему
                                break
                    b['turnEndsAt'] = time.time() + TURN_SECONDS            # 1 минута на ход
                    save_data(DATA)
                    broadcast()
                self._send_json({'ok': True, 'pid': pid, 'value': value, 'pos': newpos, 'salaryDue': salary_due})
            except Exception as e:
                self._send_json({'error': str(e)}, 400)
            return
        if self.path == '/turn/next':
            try:
                with LOCK:
                    sync_board_roster()
                    b = DATA['board']
                    # уходящий ходящий, если в тюрьме, отбыл этот ход — уменьшаем срок
                    if b['order']:
                        cur_pid = b['order'][b['turnIdx']]
                        for pl in DATA['players']:
                            if pl.get('id') == cur_pid and int(pl.get('jail') or 0) > 0:
                                pl['jail'] = int(pl['jail']) - 1
                                # давление сидки: +1% к прочим расходам за каждый пройденный ход (кап +10%), «здоровье падает»
                                pl['jailPress'] = min(10, int(pl.get('jailPress') or 0) + 1)
                                break
                    if b['order']:
                        b['turnIdx'] = (b['turnIdx'] + 1) % len(b['order'])
                        # МИРОВОЕ СОБЫТИЕ строго РАЗ В КРУГ: считаем ходы, каждые n (число игроков) — событие.
                        # (Раньше «путешествующий якорь» давал сбой n+1 → события реже круга. Теперь ровно раз в круг.)
                        n = len(b['order'])
                        tsa = int(b.get('turnsSinceAnchor', 0)) + 1
                        if tsa >= n:
                            we = next_world_event()
                            DATA['world'] = we
                            _fx = we.get('fx') or {}
                            apply_market_shock(_fx)                    # событие двигает биржу у всех
                            apply_business_shock(_fx.get('biz', 0))    # и денежный поток бизнесов игроков
                            DATA['worldLastAt'] = time.time()          # страховка мира в тикере знает: мир говорил
                            b['turnsSinceAnchor'] = 0
                        else:
                            b['turnsSinceAnchor'] = tsa
                    b['lastRoll'] = None
                    b['turnEndsAt'] = time.time() + TURN_SECONDS   # 1 минута новому ходящему
                    save_data(DATA)
                    broadcast()
                    idx = b['turnIdx']
                self._send_json({'ok': True, 'turnIdx': idx})
            except Exception as e:
                self._send_json({'error': str(e)}, 400)
            return
        if self.path == '/world/rumor':
            # Игрок выбрал, что делать со слухом: believe | check | wait | spread.
            # Выбор ТИХИЙ — чужие решения не видны до наступления звена.
            # «Проверить» стоит денег и СРАЗУ возвращает правду только этому игроку.
            try:
                req = json.loads(self.rfile.read(int(self.headers.get('Content-Length') or 0)) or b'{}')
                pid = str(req.get('pid') or '')
                choice = str(req.get('choice') or '')
                if choice not in ('believe', 'check', 'wait', 'spread'):
                    raise ValueError('неизвестный выбор')
                with LOCK:
                    r = DATA.get('rumor')
                    if not isinstance(r, dict) or r.get('resolved'):
                        self._send_json({'ok': False, 'reason': 'closed'}, 200)
                        return
                    if pid in (r.get('choices') or {}):
                        self._send_json({'ok': False, 'reason': 'already'}, 200)
                        return
                    p = next((x for x in DATA.get('players', []) if str(x.get('id')) == pid), None)
                    if p is None:
                        self._send_json({'ok': False, 'reason': 'no_player'}, 200)
                        return
                    fee = 0
                    if choice == 'check':
                        fee = round(max(0.0, float(p.get('savings') or 0)) * RUMOR_CHECK_FEE, 2)
                        p['savings'] = round(float(p.get('savings') or 0) - fee, 2)
                    r.setdefault('choices', {})[pid] = choice
                    save_data(DATA); broadcast()
                # правду отдаём ТОЛЬКО тому, кто заплатил за проверку
                self._send_json({'ok': True, 'fee': fee,
                                 'truth': bool(r.get('true')) if choice == 'check' else None})
            except Exception as e:
                self._send_json({'error': str(e)}, 400)
            return
        if self.path == '/world/draw':
            # Ведущий ВРУЧНУЮ тянет мировое событие с пульта («Событие круга»).
            # Раньше это делал клиент и двигал только p.assets — реальная биржа не двигалась
            # и эффект не записывался. Теперь событие тянет и применяет СЕРВЕР (авторитетно):
            # apply_market_shock двигает живые цены биржи, apply_business_shock — cf бизнесов всех,
            # затем broadcast рассылает карту на все экраны (оверлей у игроков через SSE).
            try:
                with LOCK:
                    we = next_world_event()
                    DATA['world'] = we
                    _fx = we.get('fx') or {}
                    apply_market_shock(_fx)                    # событие двигает биржу у всех
                    apply_business_shock(_fx.get('biz', 0))    # и денежный поток бизнесов игроков
                    save_data(DATA)
                    broadcast()
                self._send_json({'ok': True, 'world': we})
            except Exception as e:
                self._send_json({'error': str(e)}, 400)
            return
        if self.path == '/jail/buyout':
            # ОТКУП «дать на лапу» (Ринат 19июл): выйти из тюрьмы сразу за 10% от СУММЫ КАСС ВСЕХ игроков
            # (общий котёл). Серверно-авторитетно (не клиентом) — иначе гонка воскрешала бы срок.
            n = int(self.headers.get('Content-Length', 0) or 0)
            raw = self.rfile.read(n).decode('utf-8') if n else '{}'
            try:
                req = json.loads(raw)
                pid = req.get('pid')
                with LOCK:
                    pl = next((p for p in DATA['players'] if p.get('id') == pid), None)
                    if pl is None:
                        raise ValueError('player not found')
                    if int(pl.get('jail') or 0) <= 0:
                        raise ValueError('not in jail')
                    pot = round(sum(float(p.get('savings') or 0) for p in DATA['players']), 2)
                    cost = round(pot * 0.10, 2)
                    if float(pl.get('savings') or 0) < cost:
                        self._send_json({'ok': False, 'reason': 'not_enough', 'cost': cost, 'pot': pot}, 200)
                        return
                    pl['savings'] = round(float(pl.get('savings') or 0) - cost, 2)
                    pl['jail'] = 0
                    pl['shadow'] = int(pl.get('shadow') or 0) + 1   # сделка с собой → копится на «Ложь»
                    save_data(DATA)
                    broadcast()
                self._send_json({'ok': True, 'cost': cost, 'pot': pot})
            except Exception as e:
                self._send_json({'error': str(e)}, 400)
            return
        if self.path.startswith('/self/'):
            # ═══ ВЛОЖЕНИЕ В СЕБЯ И СОЗДАНИЕ (Ринат 21 июл, спека VIA_SELF_CREATION_SPEC_2026_07_21.md)
            # Хребет: тренинг САМ ПО СЕБЕ не даёт ни тенге — он молчит, пока не применён.
            # Деньги — в создании, но создать можно только то, подо что есть навык, и колода
            # 7 провалов / 3 успеха сдвигается ровно на то, сколько ты в себя вложил.
            # Вся тяга колоды и все списания — ЗДЕСЬ, под LOCK. Клиент только рисует: иначе
            # повторится баг «деньги ушли и не пришли» (18 июл).
            n = int(self.headers.get('Content-Length', 0) or 0)
            raw = self.rfile.read(n).decode('utf-8') if n else '{}'
            try:
                req = json.loads(raw)
                pid = req.get('pid')
                with LOCK:
                    pl = next((p for p in DATA['players'] if p.get('id') == pid), None)
                    if pl is None:
                        raise ValueError('player not found')
                    pl.setdefault('selfSkills', [])
                    pl.setdefault('creations', [])
                    pl.setdefault('failExp', 0)

                    if self.path == '/self/train':
                        key = req.get('key')
                        tr = SELF_TRAININGS.get(key)
                        if tr is None:
                            raise ValueError('unknown training')
                        if key in pl['selfSkills']:
                            self._send_json({'ok': False, 'reason': 'already'}, 200)
                            return
                        price = train_price(key, len(pl['selfSkills']))
                        if float(pl.get('savings') or 0) < price:
                            self._send_json({'ok': False, 'reason': 'not_enough', 'cost': price}, 200)
                            return
                        pl['savings'] = round(float(pl.get('savings') or 0) - price, 2)
                        pl['selfSkills'].append(key)
                        save_data(DATA)
                        broadcast()
                        self._send_json({'ok': True, 'key': key, 'cost': price})
                        return

                    if self.path == '/self/create':
                        key = req.get('key')
                        cr = CREATIONS.get(key)
                        if cr is None:
                            raise ValueError('unknown creation')
                        have = set(pl['selfSkills'])
                        missing = [r for r in cr['req'] if r not in have]
                        if cr.get('reqAny'):
                            missing = [] if (have & set(cr['reqAny'])) else [cr['reqAny'][0]]
                        if cr.get('minSkills') and len(have) < cr['minSkills']:
                            missing = ['_minSkills']
                        if cr.get('needFail') and int(pl['failExp']) < 1:
                            missing = ['_needFail']
                        if missing:
                            self._send_json({'ok': False, 'reason': 'locked', 'missing': missing}, 200)
                            return
                        if any(c.get('key') == key for c in pl['creations']):
                            self._send_json({'ok': False, 'reason': 'already'}, 200)
                            return
                        if float(pl.get('savings') or 0) < cr['cost']:
                            self._send_json({'ok': False, 'reason': 'not_enough', 'cost': cr['cost']}, 200)
                            return

                        # ── КОЛОДА 7 на 3. Каждый навык-усилитель меняет провал на успех.
                        #    Плюс «опыт»: каждое прошлое падение снимает ещё одну карту провала.
                        #    Пол — 2 провала: чуда без риска не бывает никогда.
                        boost = min(BOOST_CAP, len([b for b in cr['boost'] if b in have])) + int(pl['failExp'])
                        fails = max(2, 7 - boost)
                        wins = 10 - fails
                        deck = ['fail'] * fails + ['win'] * wins
                        drawn = random.choice(deck)

                        pl['savings'] = round(float(pl.get('savings') or 0) - cr['cost'], 2)
                        won = (drawn == 'win')
                        if won:
                            pl.setdefault('assets', []).append({
                                'id': int(time.time() * 1000) % 10**9,
                                'kind': 'CREATION',        # ← неотчуждаемо: не кризис, не закон души, не продажа
                                'ckey': key,               # чтобы развитие нашло свой актив
                                'type': 'BUSINESS',
                                'title': cr['name'],
                                't': cr['name'],
                                'price': cr['cost'],
                                'cf': cr['cf'],
                                'grow': cr.get('grow', 0),  # блог: +150 каждый круг
                            })
                            pl['creations'].append({'key': key, 'cf': cr['cf'], 'level': 1})
                        else:
                            pl['failExp'] = int(pl['failExp']) + 1
                        save_data(DATA)
                        broadcast()
                        self._send_json({'ok': True, 'won': won, 'fails': fails, 'wins': wins,
                                         'cost': cr['cost'], 'cf': cr['cf'] if won else 0,
                                         'name': cr['name'], 'key': key})
                        return

                    if self.path == '/self/develop':
                        key = req.get('key')
                        cr = CREATIONS.get(key)
                        if cr is None:
                            raise ValueError('unknown creation')
                        item = next((c for c in pl['creations'] if c.get('key') == key), None)
                        if item is None:
                            self._send_json({'ok': False, 'reason': 'not_created'}, 200)
                            return
                        lvl = int(item.get('level') or 1)
                        if lvl >= 3:
                            self._send_json({'ok': False, 'reason': 'max_level'}, 200)
                            return
                        nxt = lvl + 1
                        price = round(cr['cost'] * DEV_COST_MULT[nxt])
                        if float(pl.get('savings') or 0) < price:
                            self._send_json({'ok': False, 'reason': 'not_enough', 'cost': price}, 200)
                            return
                        # 🩹 Ищем по КЛЮЧУ, а не по названию: после первой ступени актив
                        # переименовывается («… · ур.2»), и поиск по имени переставал его находить —
                        # запись росла, а поток в активе оставался старым. Ключ проставляем и старым.
                        # Ищем ДО списания: иначе при сбое деньги уходят в никуда (баг 18 июл).
                        hit = None
                        for a in pl.get('assets', []):
                            if a.get('kind') != 'CREATION':
                                continue
                            if a.get('ckey') == key or str(a.get('title') or '').startswith(cr['name']):
                                hit = a
                                break
                        if hit is None:
                            self._send_json({'ok': False, 'reason': 'asset_missing'}, 200)
                            return
                        newcf = int(item.get('cf') or cr['cf']) * 2   # ступень УДВАИВАЕТ поток
                        pl['savings'] = round(float(pl.get('savings') or 0) - price, 2)
                        item['level'] = nxt
                        item['cf'] = newcf
                        hit['ckey'] = key
                        hit['cf'] = newcf
                        hit['title'] = hit['t'] = f"{cr['name']} · ур.{nxt}"
                        save_data(DATA)
                        broadcast()
                        self._send_json({'ok': True, 'key': key, 'level': nxt, 'cf': newcf,
                                         'cost': price, 'step': CREATION_STEPS[key][nxt]})
                        return

                    raise ValueError('unknown self path')
            except Exception as e:
                self._send_json({'error': str(e)}, 400)
            return
        if self.path.startswith('/auction/'):
            # ═══ 💎 АУКЦИОН (Ринат 23июл). Живой торг: лот, таймер, антиснайпер.
            # Тёмный лот 60/40 — вещь приходит ВСЕГДА, просто иногда переплатил (это рынок, не казино).
            # Деньги за лоты игры сгорают в банк; за лоты игроков — продавцу.
            n = int(self.headers.get('Content-Length', 0) or 0)
            raw = self.rfile.read(n).decode('utf-8') if n else '{}'
            try:
                req = json.loads(raw)
                action = self.path[9:]
                with LOCK:
                    players = DATA['players']

                    def find(pid):
                        return next((p for p in players if p.get('id') == pid), None)

                    if action == 'open':
                        # Ведущий открывает торги всему столу (обычно они приходят сами, раз в ~2 круга).
                        ok, res = auction_open(req.get('by'))
                        if not ok:
                            self._send_json({'ok': False, 'reason': res}, 200)
                            return
                        _notify_all('🔨 АУКЦИОН ОТКРЫТ — молоток стучит! Смотри плитку «Аукцион».')
                        save_data(DATA)
                        broadcast()
                        self._send_json({'ok': True, 'lots': res})
                        return

                    a = DATA.get('auction')

                    if action == 'bid':
                        pl = find(req.get('pid'))
                        if pl is None:
                            raise ValueError('player not found')
                        if not a or a.get('status') != 'live' or not a.get('cur'):
                            self._send_json({'ok': False, 'reason': 'no_lot'}, 200)
                            return
                        cur = a['cur']
                        if time.time() >= float(cur.get('endsAt') or 0):
                            self._send_json({'ok': False, 'reason': 'closed'}, 200)
                            return
                        if cur.get('bidder') == pl.get('id'):
                            self._send_json({'ok': False, 'reason': 'yours'}, 200)
                            return
                        if cur.get('seller') is not None and cur.get('seller') == pl.get('id'):
                            # свою вещь не перебивают: разгон собственного лота — пустой цирк
                            self._send_json({'ok': False, 'reason': 'own'}, 200)
                            return
                        price = float(cur.get('price') or 0)
                        # первая ставка = старт; дальше шаг от +5%
                        need = price if cur.get('bidder') is None else round(price * (1 + AUC_MIN_STEP))
                        amount = round(float(req.get('amount') or need), 2)
                        if amount < need:
                            self._send_json({'ok': False, 'reason': 'low', 'need': need}, 200)
                            return
                        if float(pl.get('savings') or 0) < amount:
                            self._send_json({'ok': False, 'reason': 'not_enough', 'need': amount}, 200)
                            return
                        cur['price'] = amount
                        cur['bidder'] = pl.get('id')
                        cur['bidderName'] = pl.get('name')
                        left = float(cur.get('endsAt') or 0) - time.time()
                        if left < AUC_ANTISNIPE:            # перебивание на флажке
                            cur['endsAt'] = time.time() + AUC_ANTISNIPE
                        save_data(DATA)
                        broadcast()
                        self._send_json({'ok': True, 'price': amount})
                        return

                    if action == 'dark':
                        # Тёмный лот: фикс-цена, внутри вещь — 60% дороже ящика, 40% дешевле.
                        pl = find(req.get('pid'))
                        if pl is None:
                            raise ValueError('player not found')
                        if not a or a.get('status') != 'live':
                            self._send_json({'ok': False, 'reason': 'no_auction'}, 200)
                            return
                        if int(a.get('darkLeft') or 0) <= 0:
                            self._send_json({'ok': False, 'reason': 'sold_out'}, 200)
                            return
                        if float(pl.get('savings') or 0) < DARK_PRICE:
                            self._send_json({'ok': False, 'reason': 'not_enough', 'need': DARK_PRICE}, 200)
                            return
                        # в тёмном ящике живут серые/голубые/золотые — мифик и легендарку
                        # в ящик не заворачивают, они выходят только с молотка при всех
                        ps = col_prices()
                        ordinary = col_available('gray') + col_available('blue') + col_available('gold')
                        rich = [k for k in ordinary if ps[k] > DARK_PRICE]
                        poor = [k for k in ordinary if ps[k] <= DARK_PRICE]
                        pool = rich if (random.random() < DARK_WIN and rich) else (poor or rich)
                        if not pool:
                            self._send_json({'ok': False, 'reason': 'sold_out'}, 200)
                            return
                        key = random.choice(pool)
                        pl['savings'] = round(float(pl.get('savings') or 0) - DARK_PRICE, 2)
                        pl['p2pSeq'] = int(pl.get('p2pSeq') or 0) + 1   # касса изменена сервером
                        pl.setdefault('collection', []).append({'key': key, 'paid': DARK_PRICE, 'at': int(time.time())})
                        pl['darkLots'] = int(pl.get('darkLots') or 0) + 1   # >5 за партию — жадность (закон души)
                        a['darkLeft'] = int(a.get('darkLeft') or 0) - 1
                        worth = ps.get(key, COLLECTION_ITEMS[key]['base'])
                        a.setdefault('log', []).append({'item': key, 'price': DARK_PRICE,
                                                        'name': pl.get('name'), 'sold': True, 'dark': True})
                        save_data(DATA)
                        broadcast()
                        self._send_json({'ok': True, 'item': key, 'name': COLLECTION_ITEMS[key]['name'],
                                         'paid': DARK_PRICE, 'worth': worth, 'lucky': worth > DARK_PRICE})
                        return

                    if action == 'sell':
                        # Выставить свою вещь на следующие торги. Вещь уходит в эскроу сразу.
                        pl = find(req.get('pid'))
                        if pl is None:
                            raise ValueError('player not found')
                        key = req.get('key')
                        col = pl.setdefault('collection', [])
                        item = next((c for c in col if c.get('key') == key), None)
                        if item is None:
                            self._send_json({'ok': False, 'reason': 'not_yours'}, 200)
                            return
                        col.remove(item)
                        minp = max(1, round(float(req.get('min') or 0))) or None
                        DATA.setdefault('colSellQueue', []).append(
                            {'pid': pl.get('id'), 'key': key, 'min': minp})
                        pl['notify'] = '🔨 «%s» уйдёт с молотка на следующих торгах.' % COLLECTION_ITEMS.get(key, {}).get('name', key)
                        save_data(DATA)
                        broadcast()
                        self._send_json({'ok': True})
                        return

                    raise ValueError('unknown auction action')
            except Exception as e:
                self._send_json({'error': str(e)}, 400)
            return
        if self.path.startswith('/p2p/'):
            # ═══ ИГРОК ↔ ИГРОК (Ринат 19июл, задачи #23/#24): займы под % и партнёрство «скинуться на бизнес».
            # ВСЯ логика денег — здесь, на сервере, под LOCK. Клиент только рисует и шлёт намерение:
            # иначе две касс[ы] правятся на двух телефонах одновременно и деньги множатся из воздуха
            # (ровно этот класс багов чинили 18 июл сервер-авторитетом по кредитам/долгам).
            n = int(self.headers.get('Content-Length', 0) or 0)
            raw = self.rfile.read(n).decode('utf-8') if n else '{}'
            try:
                req = json.loads(raw)
                action = self.path[5:]
                with LOCK:
                    deals = DATA.setdefault('p2p', [])
                    players = DATA['players']
                    def find(pid):
                        return next((p for p in players if p.get('id') == pid), None)
                    def bump(*ps):
                        """Пометить, что касса игрока изменена сервером — устаревший снимок бланка её не затрёт."""
                        for _p in ps:
                            if _p is not None:
                                _p['p2pSeq'] = int(_p.get('p2pSeq') or 0) + 1

                    if action == 'create':
                        kind = req.get('kind')            # 'loan' | 'partner'
                        frm = find(req.get('from'))
                        if frm is None:
                            raise ValueError('автор не найден')
                        amount = round(float(req.get('amount') or 0), 2)
                        if amount <= 0:
                            raise ValueError('сумма должна быть больше нуля')
                        targets = [t for t in (req.get('to') or []) if find(t) is not None]
                        if not targets:
                            raise ValueError('не выбран ни один игрок')
                        if kind == 'loan':
                            # Заём: деньги даёт АВТОР. Проверяем его кассу уже сейчас, чтобы не обещать пустое.
                            pct = max(0, min(20, int(req.get('pct') or 0)))
                            if float(frm.get('savings') or 0) < amount:
                                self._send_json({'ok': False, 'reason': 'not_enough'}, 200)
                                return
                            deal = {'id': int(time.time() * 1000) % 10 ** 12, 'kind': 'loan', 'from': frm['id'],
                                    'fromName': frm.get('name', ''), 'to': targets[:1], 'accepted': [],
                                    'amount': amount, 'pct': pct, 'status': 'pending', 'ts': time.time()}
                        elif kind == 'partner':
                            # Партнёрство: складываемся на дело. amount = ПОЛНАЯ цена, доля = поровну на всех.
                            deal = {'id': int(time.time() * 1000) % 10 ** 12, 'kind': 'partner', 'from': frm['id'],
                                    'fromName': frm.get('name', ''), 'to': targets, 'accepted': [],
                                    'amount': amount, 'cf': round(float(req.get('cf') or 0), 2),
                                    'price': round(float(req.get('price') or amount), 2),
                                    'liab': round(float(req.get('liab') or 0), 2),
                                    'title': str(req.get('title') or 'Общее дело')[:60],
                                    'status': 'pending', 'ts': time.time()}
                        else:
                            raise ValueError('неизвестный тип сделки')
                        deals.append(deal)
                        # держим список коротким — старьё игре не нужно
                        if len(deals) > 60:
                            del deals[:len(deals) - 60]
                        save_data(DATA); broadcast()
                        self._send_json({'ok': True, 'id': deal['id']})
                        return

                    if action == 'repay':
                        # ВОЗВРАТ ЗАЙМА (частичный или полный). Считает сервер — по своему реестру,
                        # чтобы должник не мог «погасить» долг правкой своего бланка.
                        borrower = find(req.get('pid'))
                        if borrower is None:
                            raise ValueError('игрок не найден')
                        debt = next((x for x in borrower.get('p2pDebts', []) if x.get('id') == req.get('id')), None)
                        if debt is None:
                            raise ValueError('такого долга нет')
                        lender = find(debt['toId'])
                        if lender is None:
                            raise ValueError('кредитор вышел из игры')
                        left = round(float(debt.get('due') or 0), 2)
                        pay = round(float(req.get('amount') or left), 2)
                        pay = max(0.0, min(pay, left))
                        if pay <= 0:
                            raise ValueError('нечего возвращать')
                        if float(borrower.get('savings') or 0) < pay:
                            self._send_json({'ok': False, 'reason': 'not_enough', 'left': left}, 200)
                            return
                        borrower['savings'] = round(float(borrower.get('savings') or 0) - pay, 2)
                        lender['savings'] = round(float(lender.get('savings') or 0) + pay, 2)
                        left = round(left - pay, 2)
                        debt['due'] = left
                        loan = next((x for x in lender.get('p2pLoans', []) if x.get('id') == req.get('id')), None)
                        if loan is not None:
                            loan['due'] = left
                        if left <= 0.01:
                            borrower['p2pDebts'] = [x for x in borrower.get('p2pDebts', []) if x.get('id') != req.get('id')]
                            lender['p2pLoans'] = [x for x in lender.get('p2pLoans', []) if x.get('id') != req.get('id')]
                            # ⚖️ репутация: вернул долг живому человеку полностью — свет (клиент читает helpedBack)
                            borrower['helpedBack'] = int(borrower.get('helpedBack') or 0) + 1
                            borrower['notify'] = '✅ Долг перед %s закрыт полностью.' % lender.get('name', '')
                            lender['notify'] = '✅ %s вернул долг полностью: +%s$.' % (borrower.get('name', ''), int(pay))
                        else:
                            borrower['notify'] = '💸 Вернул %s$ игроку %s. Осталось: %s$.' % (int(pay), lender.get('name', ''), int(left))
                            lender['notify'] = '💰 %s вернул %s$. Осталось за ним: %s$.' % (borrower.get('name', ''), int(pay), int(left))
                        bump(lender, borrower)
                        save_data(DATA); broadcast()
                        self._send_json({'ok': True, 'left': left})
                        return

                    d = next((x for x in deals if x.get('id') == req.get('id')), None)
                    if d is None:
                        raise ValueError('сделка не найдена')

                    if action == 'cancel':
                        if d.get('status') == 'pending':
                            d['status'] = 'cancelled'
                            save_data(DATA); broadcast()
                        self._send_json({'ok': True})
                        return

                    if action == 'respond':
                        pid = req.get('pid')
                        if d.get('status') != 'pending':
                            self._send_json({'ok': False, 'reason': 'closed'}, 200)
                            return
                        if pid not in d.get('to', []):
                            raise ValueError('эта сделка не тебе')
                        if not req.get('accept'):
                            d['status'] = 'declined'
                            save_data(DATA); broadcast()
                            self._send_json({'ok': True, 'declined': True})
                            return

                        lender = find(d['from'])
                        if lender is None:
                            raise ValueError('автор сделки вышел из игры')

                        if d['kind'] == 'loan':
                            borrower = find(pid)
                            amount, pct = d['amount'], d['pct']
                            if float(lender.get('savings') or 0) < amount:
                                d['status'] = 'failed'
                                save_data(DATA); broadcast()
                                self._send_json({'ok': False, 'reason': 'lender_broke'}, 200)
                                return
                            lender['savings'] = round(float(lender.get('savings') or 0) - amount, 2)
                            borrower['savings'] = round(float(borrower.get('savings') or 0) + amount, 2)
                            due = round(amount * (1 + pct / 100.0), 2)
                            borrower.setdefault('p2pDebts', []).append(
                                {'id': d['id'], 'toId': lender['id'], 'toName': lender.get('name', ''),
                                 'amount': amount, 'pct': pct, 'due': due})
                            lender.setdefault('p2pLoans', []).append(
                                {'id': d['id'], 'fromId': borrower['id'], 'fromName': borrower.get('name', ''),
                                 'amount': amount, 'pct': pct, 'due': due})
                            d['status'] = 'done'; d['accepted'] = [pid]
                            bump(lender, borrower)
                            borrower['notify'] = '🤝 %s дал тебе в долг %s$ под %d%%. Вернуть: %s$.' % (
                                lender.get('name', ''), int(amount), pct, int(due))
                            lender['notify'] = '🤝 %s взял твой заём %s$ под %d%%.' % (borrower.get('name', ''), int(amount), pct)
                            save_data(DATA); broadcast()
                            self._send_json({'ok': True})
                            return

                        # ПАРТНЁРСТВО: копим согласия. Как только согласились все — списываем доли и заводим общее дело.
                        if pid not in d['accepted']:
                            d['accepted'].append(pid)
                        if len(d['accepted']) < len(d['to']):
                            save_data(DATA); broadcast()
                            self._send_json({'ok': True, 'waiting': len(d['to']) - len(d['accepted'])})
                            return
                        crowd = [lender] + [find(x) for x in d['accepted']]
                        crowd = [c for c in crowd if c is not None]
                        share = round(d['amount'] / len(crowd), 2)
                        broke = [c.get('name', '') for c in crowd if float(c.get('savings') or 0) < share]
                        if broke:
                            d['status'] = 'failed'
                            save_data(DATA); broadcast()
                            self._send_json({'ok': False, 'reason': 'partner_broke', 'who': broke}, 200)
                            return
                        # ═══ ПАРТНЁРСТВО (правило Рината, 20 июл) ═══
                        # Бизнес ОСТАЁТСЯ У ТОГО, КТО ОТСКАНИРОВАЛ карту и попросил помощи.
                        # Партнёр НЕ владеет делом — он получает ТОЛЬКО долю пассива.
                        # Развивать дело может лишь владелец; выросший поток идёт и партнёрам.
                        cf_total = round(float(d.get('cf') or 0), 2)
                        cf_share = round(cf_total / len(crowd), 2)
                        owner = crowd[0]                      # инициатор = тот, кто тянул карту
                        aid = int(time.time() * 1000) % 10 ** 12
                        mates = [{'id': c['id'], 'name': c.get('name', ''), 'cf': cf_share}
                                 for c in crowd[1:]]
                        for c in crowd:
                            c['savings'] = round(float(c.get('savings') or 0) - share, 2)
                        # владельцу — само дело: полная цена, обязательства, право развивать.
                        # Его пассив = только его доля; чужие доли висят в 'mates'.
                        owner.setdefault('assets', []).append(
                            {'id': aid, 'type': 'BUSINESS', 'title': d['title'],
                             'cf': cf_share, 'cfTotal': cf_total, 'paid': share,
                             'price': round(float(d.get('price') or d['amount']), 2),
                             'liab': round(float(d.get('liab') or 0), 2), 'mates': mates})
                        owner['notify'] = '🤝 «%s» — дело ТВОЁ. Вложил %s$, партнёров %d. Твой поток +%s$/мес, развивать можешь только ты.' % (
                            d['title'], int(share), len(mates), int(cf_share))
                        # партнёрам — зеркало: только поток, ни цены, ни права развивать
                        for c in crowd[1:]:
                            c.setdefault('assets', []).append(
                                {'id': int(time.time() * 1000) % 10 ** 12 + len(c.get('assets', [])),
                                 'type': 'BUSINESS', 'title': '🤝 доля: ' + d['title'], 'cf': cf_share,
                                 'price': 0, 'liab': 0, 'paid': share,
                                 'partnerOf': owner['id'], 'srcAsset': aid, 'noDev': True})
                            c['notify'] = '🤝 Ты в доле «%s»: вложил %s$, получаешь +%s$/мес. Дело у %s — развивает он, поток растёт и тебе.' % (
                                d['title'], int(share), int(cf_share), owner.get('name', ''))
                        bump(*crowd)
                        d['status'] = 'done'
                        save_data(DATA); broadcast()
                        self._send_json({'ok': True, 'share': share, 'partners': len(crowd)})
                        return

                    raise ValueError('неизвестное действие')
            except Exception as e:
                self._send_json({'error': str(e)}, 400)
            return
        if self.path == '/turn/timer':
            # ведущий управляет таймером хода: secs>0 = запустить/перезапустить, secs<=0 = СТОП (убрать таймер)
            n = int(self.headers.get('Content-Length', 0) or 0)
            raw = self.rfile.read(n).decode('utf-8') if n else '{}'
            try:
                req = json.loads(raw)
                secs = int(req.get('secs', TURN_SECONDS))
                with LOCK:
                    b = DATA.setdefault('board', {})
                    b['turnEndsAt'] = None if secs <= 0 else (time.time() + max(5, secs))
                    save_data(DATA)
                    broadcast()
                self._send_json({'ok': True})
            except Exception as e:
                self._send_json({'error': str(e)}, 400)
            return
        if self.path == '/turn/buy':
            # ведущий управляет окном ПОКУПКИ (акции/сделки): secs>0 = открыть ВСЕМ, secs<=0 = закрыть
            n = int(self.headers.get('Content-Length', 0) or 0)
            raw = self.rfile.read(n).decode('utf-8') if n else '{}'
            try:
                req = json.loads(raw)
                secs = int(req.get('secs', BUY_SECONDS))
                with LOCK:
                    b = DATA.setdefault('board', {})
                    b['buyWindowEndsAt'] = None if secs <= 0 else (time.time() + max(5, secs))
                    save_data(DATA)
                    broadcast()
                self._send_json({'ok': True})
            except Exception as e:
                self._send_json({'error': str(e)}, 400)
            return
        if self.path == '/market/impact':
            # Ликвидность/стакан: крупная сделка двигает цену ДЛЯ ВСЕХ. Возвращает цену исполнения (avgFill).
            n = int(self.headers.get('Content-Length', 0) or 0)
            raw = self.rfile.read(n).decode('utf-8') if n else '{}'
            try:
                req = json.loads(raw)
                aid = str(req.get('assetId'))
                side = req.get('side')
                qty = float(req.get('qty') or 0)
                commit = req.get('commit', True)   # False = только КОТИРОВКА (dry-run), рынок НЕ двигаем
                with LOCK:
                    mkt = DATA.get('market') or {}
                    a = next((x for x in mkt.get('assets', []) if str(x.get('id')) == aid), None)
                    if a is None or qty <= 0:
                        raise ValueError('asset not found or qty<=0')
                    P = float(a['price'])
                    liq = float(a.get('liq') or 500000) or 500000
                    move = (qty * P) / liq * IMPACT_MULT      # сдвиг цены от объёма относительно стакана
                    if side == 'sell':
                        move = min(move, SELL_MOVE_CAP)
                        avg_fill = P * (1 - move / 2)          # выходишь ХУЖЕ витрины (провал стакана)
                        new_price = P * (1 - move)
                    else:
                        move = min(move, BUY_MOVE_CAP)
                        avg_fill = P * (1 + move / 2)          # платишь ДОРОЖЕ витрины (ешь стакан)
                        new_price = P * (1 + move)
                    lo = float(a.get('min') or 0)
                    hi = float(a.get('max') or (new_price * 10))
                    new_price = max(lo, min(hi, new_price))
                    new_price = round(new_price, 4 if new_price < 1 else 2)
                    avg_fill = round(max(lo, avg_fill), 4 if avg_fill < 1 else 2)
                    if commit:
                        # реальная сделка — двигаем цену для ВСЕХ и сохраняем
                        a['price'] = new_price
                        a.setdefault('history', []).append(a['price'])
                        if len(a['history']) > 120:
                            a['history'] = a['history'][-120:]
                        save_data(DATA)
                        broadcast()
                        price_out = a['price']
                    else:
                        # КОТИРОВКА: прогноз цены, но рынок НЕ тронут (ни save, ни broadcast)
                        price_out = new_price
                self._send_json({'ok': True, 'avgFill': avg_fill, 'newPrice': price_out, 'move': round(move, 4), 'committed': bool(commit)})
            except Exception as e:
                self._send_json({'error': str(e)}, 400)
            return
        self.send_response(404)
        self._cors()
        self.end_headers()


class ThreadedServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def market_ticker():
    """Фоновый поток: двигает рынок каждые TICK секунд, пишет в общие данные."""
    if not via_market:
        return
    while True:
        time.sleep(via_market.TICK_SECONDS)
        try:
            with LOCK:
                if not DATA.get('market'):
                    DATA['market'] = via_market.init_market()
                events = via_market.tick_market(DATA['market'])
                DATA['market']['events_last'] = events
                DATA['market']['nextTickAt'] = time.time() + via_market.TICK_SECONDS   # для видимого отсчёта у игроков
                # Рыночные крах/бум БОЛЬШЕ НЕ кидают авто-попап на весь экран.
                # Мировые события идут строго по кругу ходов (см. /turn/next).
                # Цены двигаются как обычно, крах/бум видно в самой бирже.
                save_data(DATA)
                broadcast()   # мгновенно: живые цены рынка у всех
        except Exception as e:
            print('ticker error:', e)


def main():
    threading.Thread(target=data_flusher, daemon=True).start()   # фоновая запись на диск пачкой
    threading.Thread(target=auction_ticker, daemon=True).start()  # 💎 молоток: лоты, цены антиквариата
    if via_market:
        threading.Thread(target=market_ticker, daemon=True).start()
        n = len((DATA.get('market') or {}).get('assets', []))
        print(f'  [рынок] тикер запущен: {n} активов, тик {via_market.TICK_SECONDS}с')
    server = ThreadedServer(('0.0.0.0', PORT), Handler)
    url = f'http://localhost:{PORT}/via.html'
    print()
    print('=' * 50)
    print('  VIA SERVER ЗАПУЩЕН')
    print('=' * 50)
    print(f'  Открой в ЛЮБОМ браузере:')
    print(f'  {url}')
    print()
    print(f'  Данные: {DATA_FILE}')
    print(f'  Сейчас в базе: {len(DATA["players"])} игроков, {len(DATA["deals"])} заявок')
    print()
    print('  Ctrl+C — остановить сервер')
    print('=' * 50)
    print()

    if not IS_CLOUD:   # don't try to open a browser on a headless cloud server
        threading.Timer(0.4, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nVIA server остановлен.')
        try:
            _flush_now()   # дописать последние изменения перед выходом
        except Exception:
            pass
        server.shutdown()


if __name__ == '__main__':
    main()
