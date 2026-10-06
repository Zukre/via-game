"""
VIA — durable key-value store (прогресс игрока переживает рестарт сервера).

Зачем: прод на Render держит состояние в файле на ЭФЕМЕРНОМ диске — при каждом
рестарте/деплое он стирается, прогресс игрока теряется. Этот модуль даёт
постоянное хранилище, привязанное к Telegram-ID.

Два бэкенда, выбор по переменным окружения (env-gated):
  • UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN  → Upstash Redis REST
    (бесплатный тариф, постоянное хранилище, живёт между рестартами и инстансами).
  • иначе → локальный файл via_kv_store.json рядом со скриптом
    (работает всегда для разработки/теста; на Render эфемерен, но функционален).

Только stdlib (urllib). Потокобезопасно. Значения — любые JSON-сериализуемые.
"""
import json
import os
import ssl
import threading
import urllib.request
import urllib.parse
from pathlib import Path

# HTTPS-контекст со свежими корневыми сертификатами (certifi), если он есть.
# На некоторых машинах системный набор CA устаревает → «certificate has expired».
# Проверку подлинности НЕ отключаем — только берём актуальный список доверенных.
try:
    import certifi
    _SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except Exception:
    _SSL_CTX = None   # прод (Linux) и так имеет свежие CA — хватит дефолта

_LOCK = threading.Lock()
_ROOT = Path(__file__).resolve().parent
_LOCAL_FILE = _ROOT / 'via_kv_store.json'

_UP_URL = (os.environ.get('UPSTASH_REDIS_REST_URL') or '').rstrip('/')
_UP_TOKEN = os.environ.get('UPSTASH_REDIS_REST_TOKEN') or ''
USING_UPSTASH = bool(_UP_URL and _UP_TOKEN)


def backend_name():
    return 'upstash' if USING_UPSTASH else 'local-file'


# ─────────────────────────── Upstash REST ───────────────────────────
def _up_call(path):
    """GET к Upstash REST, возвращает поле result (или None)."""
    req = urllib.request.Request(
        _UP_URL + path,
        headers={'Authorization': 'Bearer ' + _UP_TOKEN},
    )
    with urllib.request.urlopen(req, timeout=10, context=_SSL_CTX) as r:
        obj = json.loads(r.read().decode('utf-8'))
    return obj.get('result')


def _up_set(key, value_str):
    """SET key = value_str (значение в теле запроса — безопасно для длинных JSON)."""
    data = value_str.encode('utf-8')
    req = urllib.request.Request(
        _UP_URL + '/set/' + urllib.parse.quote(key, safe=''),
        data=data,
        headers={'Authorization': 'Bearer ' + _UP_TOKEN,
                 'Content-Type': 'text/plain; charset=utf-8'},
        method='POST',
    )
    with urllib.request.urlopen(req, timeout=10, context=_SSL_CTX) as r:
        json.loads(r.read().decode('utf-8'))  # {"result":"OK"}
    return True


# ─────────────────────────── local file ─────────────────────────────
def _local_all():
    if _LOCAL_FILE.exists():
        try:
            return json.loads(_LOCAL_FILE.read_text(encoding='utf-8'))
        except Exception:
            return {}
    return {}


def _local_set(key, value_str):
    store = _local_all()
    store[key] = value_str
    tmp = _LOCAL_FILE.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(store, ensure_ascii=False), encoding='utf-8')
    tmp.replace(_LOCAL_FILE)   # атомарная подмена — файл не рвётся при сбое
    return True


# ─────────────────────────── public API ─────────────────────────────
def kv_get(key):
    """Вернуть распарсенное значение по ключу, либо None."""
    with _LOCK:
        try:
            raw = _up_call('/get/' + urllib.parse.quote(key, safe='')) if USING_UPSTASH \
                else _local_all().get(key)
        except Exception as e:
            print('[kv] get fail (%s): %s' % (key, e))
            return None
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return raw


def kv_set(key, value):
    """Сохранить значение (любое JSON-сериализуемое). True при успехе."""
    try:
        value_str = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    except Exception as e:
        print('[kv] encode fail (%s): %s' % (key, e))
        return False
    with _LOCK:
        try:
            return _up_set(key, value_str) if USING_UPSTASH else _local_set(key, value_str)
        except Exception as e:
            print('[kv] set fail (%s): %s' % (key, e))
            return False
