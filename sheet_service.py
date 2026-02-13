"""
Сервис работы с Google Таблицей ДДС.
Читает справочники и добавляет строки в реестр операций (лист «ДДС: месяц»).
Заполняются только колонки C–I; A, B, J, K — по формулам в таблице.
"""

import json
import re
import sys
import time
from typing import Optional
from urllib.parse import quote

import gspread
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request as AuthRequest
from gspread.utils import ValueRenderOption

# Листы таблицы
SHEET_REGISTER = "ДДС: месяц"  # реестр операций
SHEET_WALLETS = "ДДС: настройки (для ввода сальдо)"
SHEET_DIRECTIONS = "Справочники"
SHEET_ARTICLES = "ДДС: статьи"
SHEET_SUMMARY = "ДДС: Сводный"  # сводка по месяцам для /stats

# Время жизни кэша справочников (секунды) — меньше обращений к API
CACHE_TTL = 300

# Для сортировки статей по частоте читаем только последние N строк реестра (колонка I).
# Ускоряет отклик при большом числе операций в таблице.
MAX_ROWS_ARTICLE_USAGE = 3000

# Таймаут и повторы для запросов к Google Sheets API (сетевые сбои)
SHEETS_REQUEST_TIMEOUT = 30

# Повторы при загрузке справочников (сеть, новая строка в таблице — иногда API сбрасывает соединение)
SHEETS_FETCH_RETRIES = 3
SHEETS_FETCH_RETRY_DELAY = 2.0


def _retry_sheets_fetch(fetch_fn, max_attempts: int = SHEETS_FETCH_RETRIES):
    """
    Выполняет fetch_fn до max_attempts раз при временных сбоях (Connection reset, таймаут).
    Позволяет стабильно загружать статьи после добавления новых строк в таблицу.
    """
    last_error = None
    for attempt in range(max_attempts):
        try:
            return fetch_fn()
        except (ConnectionError, OSError, TimeoutError) as e:
            last_error = e
            if attempt < max_attempts - 1:
                time.sleep(SHEETS_FETCH_RETRY_DELAY * (attempt + 1))
            else:
                raise
    if last_error is not None:
        raise last_error


def _sheets_session() -> requests.Session:
    """Сессия с повторными попытками при временных сбоях (сеть, 5xx)."""
    session = requests.Session()
    retries = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=["GET", "POST"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

# Колонки в реестре: C=3, D=4, E=5, F=6, G=7, H=8, I=9
COL_DATE = 3      # C
COL_AMOUNT = 4    # D
COL_WALLET = 5    # E
COL_DIRECTION = 6 # F
COL_COUNTERPARTY = 7  # G
COL_PURPOSE = 8   # H
COL_ARTICLE = 9  # I


def _parse_amount(text: str) -> Optional[float]:
    """Парсит сумму из строки (допускает запятую как десятичный разделитель)."""
    if not text or not text.strip():
        return None
    s = text.strip().replace(" ", "").replace(",", ".")
    s = re.sub(r"[^\d.\-]", "", s)
    try:
        return float(s)
    except ValueError:
        return None


def _is_transfer_article(name: str) -> bool:
    """Проверяет, что статья про перевод между счетами (по ключевым словам)."""
    n = (name or "").strip().lower()
    return "перевод" in n and "счетами" in n


def _sanitize_json_for_parse(raw: str) -> str:
    """
    Заменяет все управляющие символы в ответе на пробел,
    чтобы json.loads() не падал с «Invalid control character».
    Включая U+2028 (LINE SEPARATOR) и U+2029 (PARAGRAPH SEPARATOR).
    """
    return re.sub(r"[\x00-\x1f\x7f\u2028\u2029]", " ", raw)


def _parse_number_balance(s: str):
    """Парсит число из строки для баланса (пробел — тысячи, запятая — десятичная)."""
    s = (s or "").strip().replace(" ", "").replace(",", ".")
    s = re.sub(r"[^\d.\-]", "", s)
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _load_credentials(credentials_path: str, scopes: list) -> Credentials:
    """
    Загружает учётные данные из JSON, допуская управляющие символы в файле
    (обход «Invalid control character» в credentials.json).
    """
    with open(credentials_path, encoding="utf-8") as f:
        text = f.read()
    sanitized = _sanitize_json_for_parse(text)
    for _ in range(500):
        try:
            info = json.loads(sanitized, strict=False)
            break
        except json.JSONDecodeError as e:
            msg = str(e)
            if "Expecting property name enclosed in double quotes" in msg or "property name" in msg.lower():
                raise RuntimeError(
                    "Неверный формат credentials (ожидается JSON в двойных кавычках). "
                    "На Render проверьте переменную CREDENTIALS_JSON: вставьте целиком содержимое файла credentials.json "
                    "из Google Cloud (от первой { до последней }), без одинарных кавычек. "
                    "Оригинал: " + msg
                ) from e
            pos = getattr(e, "pos", None)
            if pos is None or pos < 0 or pos >= len(sanitized):
                raise
            doc = getattr(e, "doc", sanitized)
            if "Invalid control character" in msg:
                sanitized = doc[:pos] + " " + doc[pos + 1 :]
            elif "Expecting ':' delimiter" in msg:
                sanitized = doc[:pos] + ":" + doc[pos + 1 :]
            elif "Expecting value" in msg:
                sanitized = doc[:pos] + doc[pos + 1 :]
            elif "Expecting ',' delimiter" in msg:
                sanitized = doc[:pos] + "," + doc[pos + 1 :]
            else:
                raise
    else:
        raise RuntimeError("В credentials.json слишком много управляющих символов")
    return Credentials.from_service_account_info(info, scopes=scopes)


def get_balances_standalone(credentials_path: str, sheet_id: str) -> dict[str, float]:
    """
    Получает балансы только через REST API, без gspread.
    Не вызывает open_by_key — обходит «Invalid control character» в ответе метаданных.
    Парсит ответ с json.loads(..., strict=False).
    """
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = _load_credentials(credentials_path, scopes)
    creds.refresh(AuthRequest())
    token = creds.token
    if not token:
        raise RuntimeError("Не удалось получить токен доступа")
    range_a1 = f"'{SHEET_REGISTER}'!A1:I3"
    url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/"
        + quote(range_a1, safe="!")
    )
    try:
        session = _sheets_session()
        resp = session.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params={"valueRenderOption": "UNFORMATTED_VALUE"},
            timeout=SHEETS_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        raise RuntimeError(
            "Таймаут при обращении к Google Sheets. Проверьте интернет и попробуйте позже."
        )
    except requests.exceptions.ConnectionError as e:
        raise RuntimeError(
            f"Нет связи с Google Sheets (сеть или DNS). Попробуйте позже. Детали: {e!r}"
        )
    text = resp.text
    sanitized = _sanitize_json_for_parse(text)
    # Цикл: при каждом «Invalid control character» заменяем символ в e.pos и повторяем
    for _ in range(500):  # не более 500 замен
        try:
            data = json.loads(sanitized, strict=False)
            break
        except json.JSONDecodeError as e:
            if "Invalid control character" not in str(e):
                raise
            pos = getattr(e, "pos", None)
            if pos is None or pos < 0 or pos >= len(sanitized):
                raise
            # В консоль — какой символ мешает (для отладки)
            ch = sanitized[pos]
            snippet = sanitized[max(0, pos - 20) : pos + 21].replace("\n", " ").replace("\r", " ")
            print(f"[balance] Заменяю символ в позиции {pos}: repr={repr(ch)} ord={ord(ch)} фрагмент=...{snippet}...", file=sys.stderr)
            doc = getattr(e, "doc", sanitized)
            sanitized = doc[:pos] + " " + doc[pos + 1 :]
    else:
        raise RuntimeError("Не удалось разобрать ответ API (слишком много управляющих символов)")
    values = data.get("values") or []
    total = None
    balances = {}
    if len(values) > 2 and len(values[2]) > 0:
        total = _parse_number_balance(str(values[2][0]).strip())
    for row in values:
        if not isinstance(row, list) or len(row) < 3:
            continue
        for name_idx, balance_idx in [(1, 2), (3, 4), (5, 6), (7, 8)]:
            if len(row) <= balance_idx:
                continue
            # Ячейки могут приходить как число (результат формулы) — всегда приводим к строке
            name = str(row[name_idx] or "").strip() if name_idx < len(row) else ""
            b_str = str(row[balance_idx] or "").strip() if balance_idx < len(row) else ""
            b = _parse_number_balance(b_str)
            if name and b is not None and name.lower() != "итого":
                balances[name] = b
    if total is None and balances:
        total = sum(balances.values())
    if total is not None:
        balances["Итого"] = total
    return balances


class DDSSheetService:
    def __init__(self, credentials_path: str, sheet_id: str):
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        creds = _load_credentials(credentials_path, scopes)
        self._creds = creds
        self._sheet_id = sheet_id
        self._gc = gspread.authorize(creds)
        self._sheet = self._gc.open_by_key(sheet_id)
        self._cache: dict[str, tuple[object, float]] = {}

    def _cached(self, key: str, fetch_fn):
        now = time.monotonic()
        if key in self._cache:
            val, ts = self._cache[key]
            if now - ts < CACHE_TTL:
                return val
        val = _retry_sheets_fetch(fetch_fn)
        self._cache[key] = (val, now)
        return val

    def _worksheet(self, name: str):
        return self._sheet.worksheet(name)

    def get_wallets(self) -> list[str]:
        """Список кошельков из листа «ДДС: настройки (для ввода сальдо)», колонка A с 3-й строки.
        Учитываются только строки с названием (не цифры): пустые ячейки и ячейки вроде «7», «8» пропускаются.
        """
        def _fetch():
            ws = self._worksheet(SHEET_WALLETS)
            col = ws.col_values(1)
            result = []
            for v in col[2:]:
                s = (v or "").strip()
                if not s:
                    continue
                if s.isdigit():
                    continue
                result.append(s)
            return result
        return self._cached("wallets", _fetch)

    def get_business_directions(self) -> list[str]:
        """Направления бизнеса из листа «Справочники», колонка A со 2-й строки."""
        def _fetch():
            ws = self._worksheet(SHEET_DIRECTIONS)
            col = ws.col_values(1)
            return [v.strip() for v in col[1:] if v and str(v).strip()]
        return self._cached("directions", _fetch)

    def get_articles_by_type(self, group: str, exclude_technical: bool = False) -> list[str]:
        """
        Статьи из листа «ДДС: статьи».
        group: «Поступление» или «Выбытие» (колонка B — Группа).
        exclude_technical: если True, исключает статьи с «Вид деятельности» = «Техническая операция» (колонка C).
        Возвращает названия статей (колонка A) — точные строки из таблицы.
        """
        cache_key = f"articles_{group.strip()}_excl{exclude_technical}"
        def _fetch():
            ws = self._worksheet(SHEET_ARTICLES)
            col_a = ws.col_values(1)
            col_b = ws.col_values(2)
            col_c = ws.col_values(3)
            group_norm = group.strip().lower()
            result = []
            for i in range(1, min(len(col_a), len(col_b), len(col_c))):
                name = (col_a[i] or "").strip()
                gr = (col_b[i] or "").strip().lower()
                activity = (col_c[i] or "").strip().lower()
                if name and gr == group_norm:
                    if exclude_technical and activity == "техническая операция":
                        continue
                    result.append(name)
            return result
        return self._cached(cache_key, _fetch)

    def _get_article_usage_counts(self) -> dict[str, int]:
        """
        Подсчёт использований каждой статьи в реестре «ДДС: месяц» (колонка I).
        Читаем только последние MAX_ROWS_ARTICLE_USAGE строк — иначе при большом реестре запрос очень медленный.
        Возвращает словарь: название статьи → количество операций с этой статьёй.
        """
        def _fetch():
            ws = self._worksheet(SHEET_REGISTER)
            rc = getattr(ws, "row_count", 1000)
            start_row = max(2, rc - MAX_ROWS_ARTICLE_USAGE + 1)
            end_row = max(2, rc)
            range_str = f"I{start_row}:I{end_row}"
            raw = ws.get(range_str)
            counts: dict[str, int] = {}
            for row in raw:
                name = (row[0] if row else "").strip()
                if not name:
                    continue
                counts[name] = counts.get(name, 0) + 1
            return counts
        return self._cached("article_usage", _fetch)

    def get_articles_by_type_sorted_by_usage(
        self, group: str, exclude_technical: bool = False
    ) -> list[str]:
        """
        Статьи из «ДДС: статьи» для группы (Поступление/Выбытие), отсортированные по частоте использования:
        сначала те, что чаще встречаются в реестре операций, затем остальные в порядке таблицы.
        """
        articles = self.get_articles_by_type(group, exclude_technical)
        if not articles:
            return articles
        usage = self._get_article_usage_counts()
        # Сортировка: по убыванию количества использований, при равенстве — порядок в таблице
        order = {name: idx for idx, name in enumerate(articles)}
        def sort_key(name: str) -> tuple:
            return (-(usage.get(name) or 0), order.get(name, 9999))
        return sorted(articles, key=sort_key)

    def get_transfer_articles(self) -> tuple[str, str]:
        """
        Статьи для перевода между счетами из листа «ДДС: статьи».
        Ищет по ключевым словам «перевод» и «счетами» в названии.
        Возвращает (статья_выбытие, статья_поступление) — точные строки из таблицы.
        """
        def _fetch():
            out_articles = self.get_articles_by_type("Выбытие")
            in_articles = self.get_articles_by_type("Поступление")
            out_name = next((a for a in out_articles if _is_transfer_article(a)), None)
            in_name = next((a for a in in_articles if _is_transfer_article(a)), None)
            if not out_name or not in_name:
                raise ValueError(
                    "В листе «ДДС: статьи» не найдены статьи перевода между счетами "
                    "(нужны по одной в группе «Выбытие» и «Поступление» с текстом «перевод» и «счетами»)."
                )
            return (out_name, in_name)
        return self._cached("transfer_articles", _fetch)

    def get_default_business_direction(self) -> Optional[str]:
        """Если направление одно — возвращаем его, иначе None (нужен выбор)."""
        directions = self.get_business_directions()
        if len(directions) == 1:
            return directions[0]
        return None

    def _sanitize_cell(self, s) -> str:
        """Убирает управляющие символы из значения ячейки (чтобы не ломать JSON при передаче)."""
        if s is None:
            return ""
        return re.sub(r"[\x00-\x1f\x7f]", "", str(s).strip())

    def _parse_number(self, s: str):
        """Парсит число из строки (пробел — тысячи, запятая — десятичная)."""
        s = (s or "").strip().replace(" ", "").replace(",", ".")
        s = re.sub(r"[^\d.\-]", "", s)
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None

    def _cell_value(self, ws, cell_name: str) -> str:
        """Читает одну ячейку (UNFORMATTED — без лишних символов форматирования); при ошибке — пустая строка."""
        try:
            data = ws.get(
                cell_name,
                value_render_option=ValueRenderOption.unformatted,
            )
            if data and len(data) > 0 and len(data[0]) > 0:
                val = data[0][0]
                if val is None:
                    return ""
                return self._sanitize_cell(str(val))
        except Exception:
            pass
        return ""

    def _fetch_balances_raw(self, ws) -> dict[str, float]:
        """
        Читает балансы из листа «ДДС: месяц».
        A3 — итог; 12 слотов: пары (название, баланс) в B/C, D/E, F/G, H/I по строкам 1–3.
        Читаем каждую ячейку отдельным запросом, чтобы при Invalid control character в одной
        ячейке остальные всё равно получили.
        """
        total = None
        balances = {}

        a3_val = self._cell_value(ws, "A3")
        if a3_val:
            total = self._parse_number(a3_val)

        cells_b1_i3 = [
            ("B1", "C1"), ("D1", "E1"), ("F1", "G1"), ("H1", "I1"),
            ("B2", "C2"), ("D2", "E2"), ("F2", "G2"), ("H2", "I2"),
            ("B3", "C3"), ("D3", "E3"), ("F3", "G3"), ("H3", "I3"),
        ]
        for name_cell, balance_cell in cells_b1_i3:
            w = self._cell_value(ws, name_cell)
            b_str = self._cell_value(ws, balance_cell)
            b = self._parse_number(b_str)
            if w and b is not None and w.lower() != "итого":
                balances[w] = b

        if total is None and balances:
            total = sum(balances.values())
        if total is not None:
            balances["Итого"] = total
        return balances

    def _fetch_balances_via_rest(self) -> dict[str, float]:
        """
        Читает балансы через REST API с санитизацией JSON.
        Обходит ошибку «Invalid control character», если в ячейках есть неэкранированные символы.
        """
        self._creds.refresh(AuthRequest())
        token = self._creds.token
        if not token:
            raise RuntimeError("Не удалось получить токен доступа")
        range_a1 = f"'{SHEET_REGISTER}'!A1:I3"
        url = (
            f"https://sheets.googleapis.com/v4/spreadsheets/{self._sheet_id}/values/"
            + quote(range_a1, safe="!")
        )
        try:
            session = _sheets_session()
            resp = session.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                params={"valueRenderOption": "UNFORMATTED_VALUE"},
                timeout=SHEETS_REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
        except requests.exceptions.Timeout:
            raise RuntimeError(
                "Таймаут при обращении к Google Sheets. Проверьте интернет и попробуйте позже."
            )
        except requests.exceptions.ConnectionError as e:
            raise RuntimeError(
                f"Нет связи с Google Sheets (сеть или DNS). Попробуйте позже. Детали: {e!r}"
            )
        text = resp.text
        sanitized = _sanitize_json_for_parse(text)
        data = json.loads(sanitized)
        values = data.get("values") or []
        total = None
        balances = {}
        # A3 = итог; колонки B/C, D/E, F/G, H/I — 4 пары «кошелёк / баланс» по строкам 0,1,2 (12 слотов)
        # Ячейки с формулами приходят как число (результат) — всегда приводим к строке перед .strip()
        if len(values) > 2 and len(values[2]) > 0:
            total = self._parse_number(str(values[2][0]).strip())
        for row_idx, row in enumerate(values):
            if not isinstance(row, list) or len(row) < 3:
                continue
            for name_idx, balance_idx in [(1, 2), (3, 4), (5, 6), (7, 8)]:
                if len(row) <= balance_idx:
                    continue
                name = str(row[name_idx] or "").strip() if name_idx < len(row) else ""
                b_str = str(row[balance_idx] or "").strip() if balance_idx < len(row) else ""
                b = self._parse_number(b_str)
                if name and b is not None and name.lower() != "итого":
                    balances[name] = b
        if total is None and balances:
            total = sum(balances.values())
        if total is not None:
            balances["Итого"] = total
        return balances

    def _worksheet_for_balances(self):
        """Лист «ДДС: месяц»: по имени; при ошибке (Invalid control character в метаданных) — по индексу 0."""
        try:
            return self._worksheet(SHEET_REGISTER)
        except Exception:
            pass
        try:
            return self._sheet.get_worksheet(0)
        except Exception:
            raise

    def get_balances(self, use_cache: bool = True) -> dict[str, float]:
        """
        Балансы кошельков из листа «ДДС: месяц» (строки 1-3).
        Только через REST с санитизацией JSON (обход Invalid control character).
        """
        def _fetch():
            return self._fetch_balances_via_rest()
        if use_cache:
            return self._cached("balances", _fetch)
        self._cache.pop("balances", None)
        return _fetch()

    def invalidate_balances_cache(self) -> None:
        """Сбросить кэш балансов и счётчика использований статей (вызывать после добавления операции)."""
        self._cache.pop("balances", None)
        self._cache.pop("article_usage", None)

    # Слоты кошельков: в «ДДС: настройки» строки 3–14 = слоты 1–12; в «ДДС: Сводный» строки 3–14 = кошельки 1–12
    # В «ДДС: месяц» вверху 12 слотов: 4 пары (название, баланс) в строках 1–3: B/C, D/E, F/G, H/I
    WALLET_SLOTS_FIRST_ROW = 3  # 1-based: строка 3 = слот 1
    WALLET_SLOTS_COUNT = 12
    SUMMARY_WALLET_FIRST_ROW = 3  # 1-based: строка 3 = первый кошелёк в Сводном

    @staticmethod
    def _month_sheet_wallet_cells(position: int) -> tuple[str, str]:
        """
        Ячейки в листе «ДДС: месяц» для слота position (1–12): название и баланс.
        Сетка: 4 пары колонок (B/C, D/E, F/G, H/I) в строках 1, 2, 3.
        Возвращает (name_cell, balance_cell) в A1-нотации, например ("B1", "C1") для position=1.
        """
        if not 1 <= position <= 12:
            raise ValueError("position должен быть от 1 до 12")
        row_1 = (position - 1) // 4 + 1
        pair = (position - 1) % 4
        col_name_1 = 2 + pair * 2   # 2,4,6,8 -> B,D,F,H
        col_balance_1 = col_name_1 + 1
        def col_letter(c):
            return chr(64 + c) if c <= 26 else ""
        name_cell = f"{col_letter(col_name_1)}{row_1}"
        balance_cell = f"{col_letter(col_balance_1)}{row_1}"
        return name_cell, balance_cell

    # Порядок слотов в «ДДС: месяц»: сверху вниз по каждой паре колонок (F1→F2→F3, затем H1→H2→H3 и т.д.)
    _MONTH_SHEET_SLOT_ORDER = [
        ("B1", "C1"), ("B2", "C2"), ("B3", "C3"),
        ("D1", "E1"), ("D2", "E2"), ("D3", "E3"),
        ("F1", "G1"), ("F2", "G2"), ("F3", "G3"),
        ("H1", "I1"), ("H2", "I2"), ("H3", "I3"),
    ]

    def get_first_free_month_slot(self) -> tuple[str, str]:
        """
        Первый свободный слот в листе «ДДС: месяц» (вверху): пара (ячейка названия, ячейка баланса).
        Свободный = ячейка названия пустая или содержит только цифру 1–12.
        Порядок: B1,C1 → D1,E1 → F1,G1 → H1,I1 → B2,C2 → … → H3,I3.
        """
        ws = self._worksheet(SHEET_REGISTER)
        for name_cell, balance_cell in self._MONTH_SHEET_SLOT_ORDER:
            # Значение ячейки может быть int/float (формула или число) — приводим к строке
            val = str(ws.acell(name_cell).value or "").strip()
            if not val or (val.isdigit() and 1 <= int(val) <= self.WALLET_SLOTS_COUNT):
                return name_cell, balance_cell
        raise RuntimeError("В листе «ДДС: месяц» нет свободных слотов для кошелька (все 12 заняты)")

    def get_free_wallet_slots(self) -> list[dict]:
        """
        Свободные слоты кошельков из листа «ДДС: настройки (для ввода сальдо)».
        Слот свободен, если в колонке A в строке (позиция 1–12 → строка 3–14) стоит только цифра от 1 до 12.
        Возвращает список словарей: {"position": 1–12, "sheet_number": 1–12}.
        position — номер строки-слота; sheet_number — число из ячейки (имя скрытого листа для этой таблицы).
        В разных таблицах свободными могут быть любые слоты: у вас 7–12, в другой таблице могут быть 1–3 и т.д.
        """
        def _fetch():
            ws = self._worksheet(SHEET_WALLETS)
            col_a = ws.col_values(1)
            free = []
            for position in range(1, self.WALLET_SLOTS_COUNT + 1):
                row_idx_0 = (position + 2) - 1  # строка 1-based = position+2
                if row_idx_0 >= len(col_a):
                    continue
                val = (col_a[row_idx_0] or "").strip()
                if not val.isdigit():
                    continue
                num = int(val)
                if 1 <= num <= self.WALLET_SLOTS_COUNT:
                    free.append({"position": position, "sheet_number": num})
            return free
        return _retry_sheets_fetch(_fetch)

    def _sheets_batch_update(self, requests: list) -> dict:
        """Вызов spreadsheets.batchUpdate через REST (нужен для скрытия/переименования листов и строк)."""
        self._creds.refresh(AuthRequest())
        token = self._creds.token
        if not token:
            raise RuntimeError("Не удалось получить токен доступа")
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{self._sheet_id}:batchUpdate"
        body = {"requests": requests}
        session = _sheets_session()
        resp = session.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=SHEETS_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def _get_spreadsheet_sheets(self) -> list[dict]:
        """Метаданные листов: sheetId, title, hidden (для поиска скрытого листа по номеру)."""
        self._creds.refresh(AuthRequest())
        token = self._creds.token
        if not token:
            raise RuntimeError("Не удалось получить токен доступа")
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{self._sheet_id}?fields=sheets(properties(sheetId,title,hidden))"
        session = _sheets_session()
        resp = session.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=SHEETS_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        return [
            {
                "sheetId": p["sheetId"],
                "title": p.get("title", ""),
                "hidden": p.get("hidden", False),
            }
            for s in data.get("sheets", [])
            for p in [s.get("properties", {})]
        ]

    def add_wallet(
        self,
        position: int,
        sheet_number: int,
        wallet_name: str,
        start_balance: float = 0.0,
    ) -> None:
        """
        Добавить новый кошелёк в слот.
        position (1–12) — номер строки-слота в «ДДС: настройки» и «ДДС: Сводный».
        sheet_number (1–12) — число из ячейки; по нему ищем скрытый лист с названием str(sheet_number).
        1) Записать в «ДДС: настройки» имя и сумму на начало (0).
        2) Найти скрытый лист с названием = str(sheet_number), показать и переименовать в имя кошелька.
        3) В листе кошелька установить A1 = имя (для выпадающего списка/формул).
        4) В «ДДС: Сводный» показать строку position и записать имя в колонку A.
        """
        if not 1 <= position <= self.WALLET_SLOTS_COUNT:
            raise ValueError(f"Позиция слота должна быть от 1 до {self.WALLET_SLOTS_COUNT}")
        if not 1 <= sheet_number <= self.WALLET_SLOTS_COUNT:
            raise ValueError(f"Номер листа должен быть от 1 до {self.WALLET_SLOTS_COUNT}")
        name = (wallet_name or "").strip()
        if not name:
            raise ValueError("Название кошелька не может быть пустым")

        # 1) Лист «ДДС: настройки»: строка = position + 2, A = имя, B = сумма на начало
        row_settings = position + 2
        ws_settings = self._worksheet(SHEET_WALLETS)
        ws_settings.update_acell(f"A{row_settings}", name)
        ws_settings.update_acell(f"B{row_settings}", start_balance if start_balance != 0 else "0")

        # 2) Найти лист по названию = str(sheet_number), показать и переименовать
        sheets = self._get_spreadsheet_sheets()
        target_sheet_id = None
        sheet_title = str(sheet_number)
        for s in sheets:
            if (s["title"] == sheet_title) or (s["title"].strip() == sheet_title):
                target_sheet_id = s["sheetId"]
                break
        if target_sheet_id is None:
            raise RuntimeError(f"Не найден лист с названием «{sheet_title}» (скрытый лист для этого слота)")

        self._sheets_batch_update([
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": target_sheet_id, "hidden": False, "title": name},
                    "fields": "hidden,title",
                }
            }
        ])

        # 3) В листе кошелька (теперь он с именем name) установить A1 = имя
        try:
            ws_wallet = self._worksheet(name)
            ws_wallet.update_acell("A1", name)
        except Exception:
            pass  # лист уже переименован; A1 может быть формулой — не критично

        # 4) «ДДС: Сводный»: sheetId, строка = position + 2, показать строку и записать имя в A
        summary_sheet_id = None
        for s in sheets:
            if s["title"] == SHEET_SUMMARY:
                summary_sheet_id = s["sheetId"]
                break
        if summary_sheet_id is not None:
            row_summary_1 = position + 2  # 1-based
            row_index_0 = row_summary_1 - 1
            self._sheets_batch_update([
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": summary_sheet_id,
                            "dimension": "ROWS",
                            "startIndex": row_index_0,
                            "endIndex": row_index_0 + 1,
                        },
                        "properties": {"hiddenByUser": False},
                        "fields": "hiddenByUser",
                    }
                }
            ])
            ws_summary = self._worksheet(SHEET_SUMMARY)
            ws_summary.update_acell(f"A{row_summary_1}", name)

        # 5) «ДДС: месяц»: записать только название кошелька в первый свободный слот (name_cell).
        # Ячейку баланса (G1, G2, …) не трогаем — там формулы, которые считают баланс по операциям.
        name_cell, _ = self.get_first_free_month_slot()
        ws_month = self._worksheet(SHEET_REGISTER)
        ws_month.update_acell(name_cell, name)

        self._cache.pop("wallets", None)
        self.invalidate_balances_cache()

    def get_daily_income(self, date_str: str) -> float:
        """
        Сумма поступлений за день: сумма положительных сумм в реестре за указанную дату.
        Исключаются «технические» поступления от перевода между счетами (отчисления в фонды и т.п.),
        иначе выручка завышается и повторный /funds считает отчисления дважды.
        date_str в формате ДД.ММ.ГГГГ.
        """
        ws = self._worksheet(SHEET_REGISTER)
        col_c = ws.col_values(COL_DATE)
        col_d = ws.col_values(COL_AMOUNT)
        col_i = ws.col_values(COL_ARTICLE)
        try:
            _, article_in_transfer = self.get_transfer_articles()
        except Exception:
            article_in_transfer = None
        total = 0.0
        date_norm = (date_str or "").strip()
        for i in range(max(0, 1), min(len(col_c), len(col_d))):
            d = (col_c[i] or "").strip()
            if d != date_norm:
                continue
            amt = self._parse_number(str(col_d[i] or "").strip())
            if amt is None or amt <= 0:
                continue
            article = (col_i[i] or "").strip() if i < len(col_i) else ""
            if article_in_transfer and article == article_in_transfer:
                continue
            total += amt
        return round(total, 2)

    def get_fund_transfers_done_today(self, date_str: str) -> dict[str, float]:
        """
        Суммы, уже переведённые в каждый фонд за указанную дату (назначение «Отчисление в фонд»).
        Возвращает словарь: кошелёк-назначение → сумма поступлений на него за день.
        Нужно для «дотягивания»: при повторном /funds переводим только разницу до целевого % от выручки.
        """
        ws = self._worksheet(SHEET_REGISTER)
        col_c = ws.col_values(COL_DATE)
        col_d = ws.col_values(COL_AMOUNT)
        col_e = ws.col_values(COL_WALLET)
        col_h = ws.col_values(COL_PURPOSE)
        date_norm = (date_str or "").strip()
        marker_out = "Отчисление в фонд"
        marker_in = "Поступление в Фонд"
        by_destination = {}
        for i in range(max(0, 1), min(len(col_c), len(col_d), len(col_e), len(col_h))):
            d = (col_c[i] or "").strip()
            if d != date_norm:
                continue
            purpose = (col_h[i] or "").strip() if i < len(col_h) else ""
            if marker_out not in purpose and marker_in not in purpose:
                continue
            amt = self._parse_number(str(col_d[i] or "").strip())
            if amt is None or amt <= 0:
                continue
            wallet = (col_e[i] or "").strip() if i < len(col_e) else ""
            if not wallet:
                continue
            by_destination[wallet] = by_destination.get(wallet, 0.0) + amt
        return by_destination

    def _next_row_register(self) -> int:
        """Номер следующей свободной строки в реестре (первая строка после данных)."""
        ws = self._worksheet(SHEET_REGISTER)
        col_date = ws.col_values(COL_DATE)
        last = 0
        for i, v in enumerate(col_date):
            if v and str(v).strip():
                last = i + 1
        return last + 1

    def _append_row(self, row: list):
        """Добавляет одну строку в реестр: значения только для C–I (индексы 2–8 в 0-based)."""
        ws = self._worksheet(SHEET_REGISTER)
        next_row = self._next_row_register()
        range_str = f"C{next_row}:I{next_row}"
        ws.update(range_str, [row], value_input_option="USER_ENTERED")

    def get_operations_by_date(self, date_str: str) -> list[dict]:
        """
        Список операций за дату (формат ДД.ММ.ГГГГ).
        Каждый элемент: row (номер строки в листе 1-based), date, amount, wallet, direction,
        counterparty, purpose, article, op_type ("поступление" | "выбытие").
        Колонки в листе: A,B — служебные; C=дата, D=сумма, E=кошелёк, F=направление, G=контрагент, H=назначение, I=статья.
        В get_all_values() индексы: 0=A, 1=B, 2=C, 3=D, 4=E, 5=F, 6=G, 7=H, 8=I.
        """
        ws = self._worksheet(SHEET_REGISTER)
        rows = ws.get_all_values()
        if not rows or len(rows) < 2:
            return []
        date_norm = (date_str or "").strip()
        result = []
        for i in range(1, len(rows)):
            row = rows[i]
            if len(row) < 6:
                continue
            d = (row[2] if len(row) > 2 else "").strip()
            if d != date_norm:
                continue
            amt = self._parse_number(str(row[3] if len(row) > 3 else "").strip())
            wallet = (row[4] if len(row) > 4 else "").strip()
            direction = (row[5] if len(row) > 5 else "").strip()
            counterparty = (row[6] if len(row) > 6 else "").strip()
            purpose = (row[7] if len(row) > 7 else "").strip()
            article = (row[8] if len(row) > 8 else "").strip()
            op_type = "поступление" if (amt is not None and amt > 0) else "выбытие"
            result.append({
                "row": i + 1,
                "date": d,
                "amount": amt or 0,
                "wallet": wallet,
                "direction": direction,
                "counterparty": counterparty,
                "purpose": purpose,
                "article": article,
                "op_type": op_type,
            })
        return result

    def get_last_operation(self) -> Optional[dict]:
        """Последняя операция в реестре (по строке). Формат как в get_operations_by_date. Колонки C–I = индексы 2–8."""
        ws = self._worksheet(SHEET_REGISTER)
        rows = ws.get_all_values()
        if not rows or len(rows) < 2:
            return None
        i = len(rows) - 1
        row = rows[i]
        if len(row) < 6:
            return None
        d = (row[2] if len(row) > 2 else "").strip()
        if not d:
            return None
        amt = self._parse_number(str(row[3] if len(row) > 3 else "").strip())
        wallet = (row[4] if len(row) > 4 else "").strip()
        direction = (row[5] if len(row) > 5 else "").strip()
        counterparty = (row[6] if len(row) > 6 else "").strip()
        purpose = (row[7] if len(row) > 7 else "").strip()
        article = (row[8] if len(row) > 8 else "").strip()
        op_type = "поступление" if (amt is not None and amt > 0) else "выбытие"
        return {
            "row": i + 1,
            "date": d,
            "amount": amt or 0,
            "wallet": wallet,
            "direction": direction,
            "counterparty": counterparty,
            "purpose": purpose,
            "article": article,
            "op_type": op_type,
        }

    def update_operation(
        self,
        sheet_row: int,
        amount: Optional[float] = None,
        counterparty: Optional[str] = None,
        purpose: Optional[str] = None,
        article: Optional[str] = None,
    ) -> None:
        """Обновляет ячейки операции в строке sheet_row (1-based). Передаются только меняемые поля."""
        ws = self._worksheet(SHEET_REGISTER)
        row_data = ws.row_values(sheet_row)
        if len(row_data) < 7:
            row_data = row_data + [""] * (7 - len(row_data))
        updates = []
        if amount is not None:
            updates.append(("D", amount))
        if counterparty is not None:
            updates.append(("G", counterparty))
        if purpose is not None:
            updates.append(("H", purpose))
        if article is not None:
            updates.append(("I", article))
        for col, val in updates:
            cell = f"{col}{sheet_row}"
            ws.update_acell(cell, val)

    def delete_operation(self, sheet_row: int) -> None:
        """Удаляет строку sheet_row (1-based) из реестра."""
        ws = self._worksheet(SHEET_REGISTER)
        ws.delete_rows(sheet_row, 1)

    def append_operation(
        self,
        date_str: str,
        amount: float,
        wallet: str,
        business_direction: str,
        counterparty: str,
        purpose: str,
        article: str,
    ) -> None:
        """Добавляет одну операцию (поступление или выбытие) в реестр."""
        row = [date_str, amount, wallet, business_direction, counterparty, purpose, article]
        self._append_row(row)

    def append_transfer(
        self,
        date_str: str,
        amount: float,
        wallet_from: str,
        wallet_to: str,
        purpose: str,
        business_direction: str,
        purpose_inflow: Optional[str] = None,
    ) -> None:
        """Добавляет две строки: выбытие с wallet_from, поступление на wallet_to. Статьи берутся из таблицы.
        purpose — назначение для выбытия; для поступления используется purpose_inflow, если задано, иначе purpose."""
        article_out, article_in = self.get_transfer_articles()
        purpose_in = (purpose_inflow if purpose_inflow is not None else purpose).strip()
        row_out = [
            date_str,
            -abs(amount),
            wallet_from,
            business_direction,
            "",
            purpose.strip(),
            article_out,
        ]
        row_in = [
            date_str,
            abs(amount),
            wallet_to,
            business_direction,
            "",
            purpose_in,
            article_in,
        ]
        self._append_row(row_out)
        self._append_row(row_in)

    def get_summary_for_month(self, month: int) -> Optional[dict]:
        """
        Сводка по месяцу из листа «ДДС: Сводный».
        month: 1–12. Колонки B..M = месяцы 1..12.
        Ищет в колонке A строки: «Денег на начало месяца», «Изменение денег за месяц», «Денег на конец месяца»;
        при наличии — «Выручка»/«Доходы» и «Расходы».
        Возвращает dict: start_balance, end_balance, change, revenue (или None), expenses (или None).
        """
        if not 1 <= month <= 12:
            return None
        try:
            ws = self._worksheet(SHEET_SUMMARY)
        except Exception:
            return None
        rows = _retry_sheets_fetch(lambda: ws.get_all_values())
        if not rows or len(rows) < 2:
            return None
        col_idx = month  # B=1, C=2, ..., M=12 (0-based: col B = index 1)
        if col_idx >= len(rows[0]):
            return None
        labels = [((row[0] or "").strip().lower()) for row in rows]
        start_balance = None
        end_balance = None
        change = None
        revenue = None
        expenses = None
        for i, row in enumerate(rows):
            if col_idx >= len(row):
                continue
            val = self._parse_number(str(row[col_idx] or "").strip())
            if val is None:
                continue
            lab = labels[i] if i < len(labels) else ""
            if "денег на начало" in lab or "начало месяца" in lab:
                start_balance = val
            elif "денег на конец" in lab or "конец месяца" in lab:
                end_balance = val
            elif "изменение денег" in lab or "изменение за месяц" in lab:
                change = val
            elif "выручка" in lab or "доходы" in lab:
                if revenue is None and val >= 0:
                    revenue = val
            elif "расходы" in lab:
                expenses = abs(val) if val < 0 else val
        if start_balance is None and end_balance is not None and change is not None:
            start_balance = end_balance - change
        if end_balance is None and start_balance is not None and change is not None:
            end_balance = start_balance + change
        if change is None and start_balance is not None and end_balance is not None:
            change = end_balance - start_balance
        if start_balance is None and end_balance is None:
            return None
        return {
            "start_balance": start_balance or 0,
            "end_balance": end_balance or 0,
            "change": change or 0,
            "revenue": revenue,
            "expenses": expenses,
        }

    def get_summary_for_date_range(self, date_from: str, date_to: str) -> Optional[dict]:
        """
        Сводка за диапазон дат по реестру «ДДС: месяц».
        date_from, date_to в формате ДД.ММ.ГГГГ.
        Считает доходы (сумма положительных), расходы (абсолютная сумма отрицательных), изменение.
        Текущий баланс = get_balances()["Итого"], начальный = текущий − изменение.
        """
        from datetime import datetime

        def parse_dt(s: str):
            try:
                return datetime.strptime(s.strip(), "%d.%m.%Y")
            except ValueError:
                return None

        d1 = parse_dt(date_from)
        d2 = parse_dt(date_to)
        if d1 is None or d2 is None or d1 > d2:
            return None
        ws = self._worksheet(SHEET_REGISTER)
        col_c = ws.col_values(COL_DATE)
        col_d = ws.col_values(COL_AMOUNT)
        total_income = 0.0
        total_expense = 0.0
        for i in range(1, min(len(col_c), len(col_d))):
            d = (col_c[i] or "").strip()
            dt = parse_dt(d)
            if dt is None:
                continue
            if dt < d1 or dt > d2:
                continue
            amt = self._parse_number(str(col_d[i] or "").strip())
            if amt is None:
                continue
            if amt > 0:
                total_income += amt
            else:
                total_expense += abs(amt)
        change = round(total_income - total_expense, 2)
        try:
            balances = self.get_balances(use_cache=False)
            end_balance = balances.get("Итого")
        except Exception:
            end_balance = None
        if end_balance is not None:
            start_balance = round(end_balance - change, 2)
        else:
            start_balance = None
        return {
            "start_balance": start_balance,
            "end_balance": end_balance,
            "change": change,
            "revenue": round(total_income, 2),
            "expenses": round(total_expense, 2),
        }

    @staticmethod
    def parse_amount(text: str) -> Optional[float]:
        return _parse_amount(text)
