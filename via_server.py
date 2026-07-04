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
import socketserver
import threading
import time
import webbrowser
from pathlib import Path

try:
    import via_market   # живой рынок (тикер цен)
except Exception:
    via_market = None

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
    return {'players': [], 'deals': [], 'market': None, 'world': None}


def save_data(d):
    DATA_FILE.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding='utf-8')


DATA = load_data()
# инициализируем живой рынок, если его ещё нет (первый запуск)
if via_market and not DATA.get('market'):
    DATA['market'] = via_market.init_market()
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
                        }
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
                save_data(DATA)
                broadcast()   # мгновенно: живые цены рынка у всех
        except Exception as e:
            print('ticker error:', e)


def main():
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
        server.shutdown()


if __name__ == '__main__':
    main()
