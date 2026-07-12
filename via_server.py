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
TURN_SECONDS = 120    # ход длится 2 минуты (мягкий таймер: предупреждает, ведущий передаёт сам)
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
                            cur[me] = incoming[me]          # апсерт только своего бланка
                        DATA['players'] = list(cur.values())
                        # заявки — аддитивно по id (добавить новые / обновить свои), не удаляем
                        dcur = {d['id']: d for d in DATA['deals'] if 'id' in d}
                        for d in new.get('deals', []):
                            if 'id' in d:
                                dcur[d['id']] = d
                        DATA['deals'] = list(dcur.values())
                        # world/market у игрока не трогаем
                    else:
                        # ВЕДУЩИЙ / лобби — полная авторитетная запись
                        DATA = {
                            'players': new.get('players', []),
                            'deals': new.get('deals', []),
                            'market': DATA.get('market'),   # рынок не трогаем при апдейте бланков
                            'world': new.get('world', DATA.get('world')),  # мировое событие — broadcast всем
                            'board': DATA.get('board'),     # ⚠️ СОХРАНЯЕМ поле! иначе sync создаст пустое и все шашки прыгнут на старт
                        }
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
                                pl['jail'] = random.randint(1, 3)          # клетка Тюрьма — сажаем на 1-3 хода (по-разному)
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
                    b['turnEndsAt'] = time.time() + TURN_SECONDS            # 2 минуты на ход
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
                        # МИРОВОЕ СОБЫТИЕ раз в круг: когда ход вернулся к «якорю» после полного круга.
                        # Якорь сдвигается на следующего игрока → событие «путешествует» по столу.
                        n = len(b['order'])
                        anchor = b.get('anchorIdx', 0) % n
                        tsa = int(b.get('turnsSinceAnchor', 0)) + 1
                        b['turnsSinceAnchor'] = tsa
                        if b['turnIdx'] == anchor and tsa >= n:
                            we = pick_world_event()
                            DATA['world'] = we
                            _fx = we.get('fx') or {}
                            apply_market_shock(_fx)                    # событие двигает биржу у всех
                            apply_business_shock(_fx.get('biz', 0))    # и денежный поток бизнесов игроков
                            b['anchorIdx'] = (anchor + 1) % n
                            b['turnsSinceAnchor'] = 0
                    b['lastRoll'] = None
                    b['turnEndsAt'] = time.time() + TURN_SECONDS   # 2 минуты новому ходящему
                    save_data(DATA)
                    broadcast()
                    idx = b['turnIdx']
                self._send_json({'ok': True, 'turnIdx': idx})
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
