"""
Telegram-бот для ввода операций ДДС в Google Таблицу.
Сценарий: дата → тип (Поступление/Выбытие/Перевод) → поля по типу → сохранение.
"""

import asyncio
import calendar
import json
import os
import re
import sys
import time
import traceback
from datetime import date, timedelta
from typing import Optional

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import NetworkError, TimedOut
from telegram.ext import (
    Application,
    ApplicationHandlerStop,
    BaseHandler,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
    ConversationHandler,
)


def _parse_allowed_user_ids() -> set:
    """Список разрешённых Telegram user ID из TELEGRAM_ALLOWED_IDS (через запятую). Пусто = доступ у всех."""
    raw = os.getenv("TELEGRAM_ALLOWED_IDS", "").strip()
    if not raw:
        return set()
    return {int(x.strip()) for x in raw.split(",") if x.strip()}


class _BlockedUserFilter(filters.UpdateFilter):
    """Фильтр: True для пользователей, которых нет в allowlist (нужно отклонить доступ)."""

    def __init__(self, allowed_ids: set):
        self.allowed_ids = allowed_ids

    def filter(self, update: Update) -> bool:
        if not self.allowed_ids:
            return False  # ограничение выключено — никого не блокируем
        user = update.effective_user if update else None
        if not user:
            return False
        return user.id not in self.allowed_ids


class _BlockedUserHandler(BaseHandler[Update, ContextTypes.DEFAULT_TYPE, None]):
    """Обработчик, срабатывающий только для пользователей не из allowlist (любые сообщения и callback)."""

    def __init__(self, allowed_ids: set, callback):
        super().__init__(callback)
        self.allowed_ids = allowed_ids

    def check_update(self, update: object) -> bool:
        if not self.allowed_ids:
            return False
        if not isinstance(update, Update):
            return False
        user = update.effective_user
        if not user:
            return False
        return user.id not in self.allowed_ids


async def _deny_access_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Для пользователей не из allowlist — никакого ответа (будто бот не работает). Останавливаем обработку."""
    try:
        if update.callback_query:
            await update.callback_query.answer()  # снимаем «загрузку» у кнопки, без текста
        # Сообщение не отправляем — полная тишина для посторонних
    except Exception:
        pass
    raise ApplicationHandlerStop


# Пользователи, ожидающие ввод при «Изменить» (сумма/контрагент/назначение) — чтобы фильтр не требовал application
_text_edit_waiting_user_ids = set()
# Пользователи, ожидающие ввод в настройках отчислений в фонды (источник / фонд / %)
_settings_waiting_user_ids = set()
_stats_waiting_user_ids = set()  # ожидание ввода диапазона дат для /stats

# Защита от дублей: не слать меню /settings повторно, если уже отправили недавно (сетевой сбой → пользователь жмёт несколько раз)
_settings_cmd_last_sent: dict[int, float] = {}
SETTINGS_DEBOUNCE_SEC = 3.0

# Повторы отправки при временных сетевых сбоях (NetworkError, TimedOut)
_SEND_RETRY_ATTEMPTS = 3
_SEND_RETRY_DELAY_SEC = 1.5


async def _retry_on_network(awaitable_factory):
    """Выполняет действие с повторами при NetworkError/TimedOut. awaitable_factory() каждый раз создаёт новый awaitable (напр. bot.send_message(...))."""
    last_exc = None
    for attempt in range(_SEND_RETRY_ATTEMPTS):
        try:
            return await awaitable_factory()
        except (NetworkError, TimedOut) as e:
            last_exc = e
            if attempt < _SEND_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(_SEND_RETRY_DELAY_SEC * (attempt + 1))
            else:
                raise
    if last_exc is not None:
        raise last_exc


async def _reply_text_with_retry(
    update: Update, text: str, max_attempts: int = _SEND_RETRY_ATTEMPTS, **kwargs
):
    """Отправляет reply_text с повторами при NetworkError/TimedOut."""
    last_exc = None
    for attempt in range(max_attempts):
        try:
            return await update.message.reply_text(text, **kwargs)
        except (NetworkError, TimedOut) as e:
            last_exc = e
            if attempt < max_attempts - 1:
                await asyncio.sleep(_SEND_RETRY_DELAY_SEC * (attempt + 1))
            else:
                raise
        except Exception:
            raise
    if last_exc is not None:
        raise last_exc


def _chat_id_from_update(update: object) -> Optional[int]:
    """Извлечь chat_id из update (сообщение или callback), иначе None."""
    if not isinstance(update, Update):
        return None
    if update.effective_chat:
        return update.effective_chat.id
    if update.callback_query and update.callback_query.message:
        return update.callback_query.message.chat_id
    return None


async def _global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка ошибок: сетевые сбои — короткий лог и уведомление пользователю, остальное — в консоль."""
    err = context.error
    if err is None:
        return
    if isinstance(err, (NetworkError, TimedOut)):
        print(f"[Бот] Сетевой сбой: {type(err).__name__}: {err}", file=sys.stderr)
        chat_id = _chat_id_from_update(update)
        if chat_id is not None:
            try:
                await _retry_on_network(
                    lambda: context.bot.send_message(
                        chat_id,
                        "⚠️ Сеть временно недоступна. Повторите запрос через несколько секунд.",
                    )
                )
            except Exception:
                pass
        return
    # Остальные ошибки — полный traceback в консоль
    print("\n--- Ошибка в боте ---", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    print("---\n", file=sys.stderr)


def _text_form_should_handle(update: Update) -> bool:
    """Обрабатывать сообщение в handle_form: текст в формате операции ИЛИ ожидание ввода редактирования/настроек."""
    if not update.message or not update.message.text or update.message.text.strip().startswith("/"):
        return False
    user_id = update.effective_user.id if update.effective_user else None
    if user_id is not None and user_id in _text_edit_waiting_user_ids:
        return True
    if user_id is not None and user_id in _settings_waiting_user_ids:
        return True
    if user_id is not None and user_id in _stats_waiting_user_ids:
        return True
    return _is_one_window_message(update.message.text)


from sheet_service import DDSSheetService, get_balances_standalone

# Папка, где лежит bot.py — для путей к .env и credentials.json (не зависим от текущей директории запуска).
_bot_dir = os.path.dirname(os.path.abspath(__file__))


def _resolve_credentials_path(path: str) -> str:
    """Путь к credentials: если относительный — считаем от папки с bot.py."""
    p = (path or "").strip()
    if not p:
        return os.path.join(_bot_dir, "credentials.json")
    if os.path.isabs(p):
        return p
    return os.path.join(_bot_dir, p)


# .env ищем рядом с bot.py; если файла нет — пробуем текущую рабочую директорию.
# override=True — значения из .env перезаписывают уже установленные (чтобы не подхватить плейсхолдер из другого места).
_load_env_path = os.path.join(_bot_dir, ".env")
if os.path.isfile(_load_env_path):
    load_dotenv(_load_env_path, override=True)
else:
    load_dotenv(override=True)  # загрузка .env из текущей рабочей директории

# Состояния диалога
(
    DATE, TYPE_OP, ARTICLE, WALLET, AMOUNT, COUNTERPARTY, PURPOSE, CONFIRM,
    CONFIRM_EDIT_MENU, CONFIRM_EDIT_INPUT, CONFIRM_EDIT_ARTICLE, CONFIRM_EDIT_WALLET,
    TRANSFER_FROM, TRANSFER_TO, TRANSFER_AMOUNT, TRANSFER_PURPOSE, TRANSFER_CONFIRM,
) = range(17)

# Ключи callback
CB_TODAY = "date_today"
CB_DATE_PREFIX = "date:"  # date:DD.MM.YYYY — выбор даты «вчера», «2 дня назад», «3 дня назад»
CB_TYPE_IN = "type_in"
CB_TYPE_OUT = "type_out"
CB_TYPE_TR = "type_tr"
CB_ARTICLE_PREFIX = "art:"
CB_WALLET_PREFIX = "wal:"
CB_ARTICLE_BACK = "art_back"
CB_WALLET_BACK = "wal_back"
CB_TRANSFER_FROM_BACK = "tr_from_back"
CB_TRANSFER_TO_BACK = "tr_to_back"
CB_SKIP = "skip"
CB_CANCEL = "cancel"
CB_BACK = "conv_back"  # 🔙 Назад в пошаговом потоке (тип операции → дата; контрагент/назначение → сумма)
CB_CONFIRM_YES = "confirm_yes"
CB_CONFIRM_NO = "confirm_no"
CB_ADD_OPERATION = "add_op"
CB_SHOW_BALANCE = "show_balance"
CB_BALANCE_BACK = "balance_back"  # Назад из окна «Показать баланс» (вернуться к «Операция внесена»)
CB_RUN_FUNDS = "run_funds"
CB_TEXT_ART_PREFIX = "text_art:"
CB_TEXT_ART_BACK = "text_art_back"
CB_TEXT_ART_PAGE_NEXT = "text_art_next"
CB_TEXT_ART_PAGE_PREV = "text_art_prev"
CB_TEXT_CONFIRM_YES = "text_confirm_yes"
CB_TEXT_CONFIRM_NO = "text_confirm_no"
CB_TEXT_EDIT = "text_edit"
CB_TEXT_EDIT_AMOUNT = "text_edit_amount"
CB_TEXT_EDIT_CT = "text_edit_ct"
CB_TEXT_EDIT_PURPOSE = "text_edit_purpose"
CB_TEXT_EDIT_BACK = "text_edit_back"
CB_EDIT = "edit"
CB_EDIT_AMOUNT = "edit_amount"
CB_EDIT_CT = "edit_ct"
CB_EDIT_PURPOSE = "edit_purpose"
CB_EDIT_ARTICLE = "edit_article"
CB_EDIT_BACK = "edit_back"
CB_EDIT_ARTICLE_PREFIX = "edit_art:"
CB_EDIT_ARTICLE_PAGE_NEXT = "edit_art_next"
CB_EDIT_ARTICLE_PAGE_PREV = "edit_art_prev"
CB_EDIT_WALLET = "edit_wallet"
CB_EDIT_WALLET_PREFIX = "edit_wallet:"
CB_EDIT_WALLET_PAGE_NEXT = "edit_wallet_next"
CB_EDIT_WALLET_PAGE_PREV = "edit_wallet_prev"
CB_TEXT_EDIT_ARTICLE = "text_edit_article"
CB_TEXT_EDIT_ART_PREFIX = "text_edit_art:"
CB_TEXT_EDIT_ART_BACK = "text_edit_art_back"
CB_TEXT_EDIT_ART_PAGE_NEXT = "text_edit_art_next"
CB_TEXT_EDIT_ART_PAGE_PREV = "text_edit_art_prev"
# Настройки / отчисления в фонды
CB_SETTINGS = "settings"
CB_SETTINGS_FUNDS = "settings_funds"
CB_SETTINGS_FUNDS_EDIT_PREFIX = "sf_edit:"
CB_SETTINGS_FUNDS_ADD = "sf_add"
CB_SETTINGS_FUNDS_DEL_PREFIX = "sf_del:"
CB_SETTINGS_FUNDS_SRC_PREFIX = "sf_src:"
CB_SETTINGS_FUNDS_DST_PREFIX = "sf_dst:"
CB_SETTINGS_FUNDS_PCT_PREFIX = "sf_pct:"
CB_SETTINGS_FUNDS_PCT_CUSTOM = "sf_pct_custom"
CB_SETTINGS_FUNDS_BACK = "sf_back"
CB_SETTINGS_FUNDS_BACK_TO_LIST = "sf_back_list"
CB_SETTINGS_BACK = "settings_back"
# Отчёт /stats
CB_STATS_TODAY = "stats_today"
CB_STATS_WEEK = "stats_week"
CB_STATS_MONTH = "stats_month"
CB_STATS_CANCEL = "stats_cancel"
CB_STATS_BACK = "stats_back"
CB_STATS_RANGE = "stats_range"
CB_STATS_OPEN = "stats_open"  # открыть выбор периода отчёта (кнопка под балансом)
CB_SETTINGS_ADD_WALLET = "settings_add_wallet"
CB_SETTINGS_ADD_WALLET_SLOT_PREFIX = "settings_wallet_slot:"
CB_SETTINGS_ADD_WALLET_BACK = "settings_add_wallet_back"

# Списки статей/кошельков: если пунктов больше LIST_PAGE_SIZE — постранично (кнопка «Стр. 1/2»)
LIST_PAGE_SIZE = 9
CB_ARTICLE_PAGE_NEXT = "art_page_next"
CB_ARTICLE_PAGE_PREV = "art_page_prev"
CB_WALLET_PAGE_NEXT = "wal_page_next"
CB_WALLET_PAGE_PREV = "wal_page_prev"
CB_TRANSFER_FROM_PAGE_NEXT = "tr_from_page_next"
CB_TRANSFER_FROM_PAGE_PREV = "tr_from_page_prev"
CB_TRANSFER_TO_PAGE_NEXT = "tr_to_page_next"
CB_TRANSFER_TO_PAGE_PREV = "tr_to_page_prev"

# Правила отчислений по умолчанию: источник → фонд, %
DEFAULT_FUND_RULES = [
    {"source": "Точка Банк", "destination": "Фонд операционных расходов", "percent": 40},
    {"source": "Точка Банк", "destination": "Фонд Прибыли", "percent": 15},
    {"source": "Точка Банк", "destination": "Фонд Безопасности", "percent": 7},
    {"source": "Точка Банк", "destination": "Фонд Развития", "percent": 18},
]

DATE_PATTERN = re.compile(r"^\s*(\d{1,2})\.(\d{1,2})\.(\d{4})\s*$")


def _today_str() -> str:
    d = date.today()
    return f"{d.day:02d}.{d.month:02d}.{d.year}"


def _date_n_days_ago(n: int) -> str:
    """Дата n дней назад в формате ДД.ММ.ГГГГ."""
    d = date.today() - timedelta(days=n)
    return f"{d.day:02d}.{d.month:02d}.{d.year}"


def _format_amount(value: float) -> str:
    """Форматирует сумму: пробел — разделитель тысяч, запятая — десятичная (63 722,00)."""
    s = f"{value:,.2f}".replace(",", " ").replace(".", ",")
    return s


def _escape_md(s: str) -> str:
    """Экранирует символы для Telegram Markdown (чтобы _ и * не ломали разметку)."""
    if not s:
        return ""
    return str(s).replace("\\", "\\\\").replace("_", "\\_").replace("*", "\\*")


def _escape_html(s: str) -> str:
    """Экранирует символы для Telegram HTML (чтобы <, >, & не ломали разметку)."""
    if not s:
        return ""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _split_wallets(balances: dict[str, float]) -> tuple[dict[str, float], dict[str, float]]:
    """Делит кошельки на основные (выше) и фонды (блок «ФОНДЫ»): название начинается с «Фонд» — в фонды, остальное — в кошельки."""
    main, funds = {}, {}
    for name, amount in balances.items():
        if (name or "").strip().startswith("Фонд"):
            funds[name] = amount
        else:
            main[name] = amount
    return main, funds


def _validate_date(s: str) -> Optional[str]:
    m = DATE_PATTERN.match(s)
    if not m:
        return None
    day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        date(year, month, day)
        return f"{day:02d}.{month:02d}.{year}"
    except ValueError:
        return None


def _parse_date_range(text: str) -> Optional[tuple[str, str]]:
    """Парсит «ДД.ММ.ГГГГ – ДД.ММ.ГГГГ» или «ДД.ММ.ГГГГ ДД.ММ.ГГГГ». Возвращает (date_from, date_to) или None."""
    if not text or not text.strip():
        return None
    parts = re.split(r"\s*[–\-]\s*|\s+", text.strip(), maxsplit=1)
    if len(parts) < 2:
        return None
    d1 = _validate_date(parts[0].strip())
    d2 = _validate_date(parts[1].strip())
    if d1 is None or d2 is None:
        return None
    try:
        day1, month1, year1 = int(d1[:2]), int(d1[3:5]), int(d1[6:10])
        day2, month2, year2 = int(d2[:2]), int(d2[3:5]), int(d2[6:10])
        if date(year1, month1, day1) > date(year2, month2, day2):
            d1, d2 = d2, d1
    except (ValueError, IndexError):
        pass
    return (d1, d2)


def _fund_rules_path() -> str:
    """Путь к файлу правил отчислений в фонды. Если FUND_RULES_PATH не задан, ищем рядом с bot.py."""
    custom_path = os.getenv("FUND_RULES_PATH", "").strip()
    if custom_path:
        return custom_path if os.path.isabs(custom_path) else os.path.join(_bot_dir, custom_path)
    return os.path.join(_bot_dir, "fund_rules.json")


def _get_fund_rules(context: ContextTypes.DEFAULT_TYPE) -> list[dict]:
    """
    Правила отчислений в фонды: список {source, destination, percent}.
    Загружает из bot_data (если уже загружено), затем из JSON, иначе используется DEFAULT_FUND_RULES.
    """
    # Если уже загружено в контекст бота, используем его
    if "fund_rules" in context.bot_data:
        return list(context.bot_data["fund_rules"])
    
    # Пытаемся загрузить из JSON файла
    path = _fund_rules_path()
    if os.path.isfile(path):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list) and len(data) > 0:
                rules = [
                    {
                        "source": str(r.get("source", "")).strip(),
                        "destination": str(r.get("destination", "")).strip(),
                        "percent": float(r.get("percent", 0))
                    }
                    for r in data
                ]
                context.bot_data["fund_rules"] = rules
                return rules
        except json.JSONDecodeError as e:
            print(f"[Бот] Ошибка парсинга {path}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"[Бот] Ошибка чтения {path}: {e}", file=sys.stderr)
    
    # Используем правила по умолчанию
    context.bot_data["fund_rules"] = list(DEFAULT_FUND_RULES)
    return list(DEFAULT_FUND_RULES)


def _save_fund_rules(context: ContextTypes.DEFAULT_TYPE, rules: list[dict]) -> None:
    """
    Сохраняет правила отчислений в bot_data и в JSON файл.
    При ошибке логирует её в stderr.
    """
    context.bot_data["fund_rules"] = list(rules)
    path = _fund_rules_path()
    
    try:
        # Создаем директорию, если её нет
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rules, f, ensure_ascii=False, indent=2)
        
        print(f"[Бот] Правила отчислений сохранены в {path}", file=sys.stderr)
    except Exception as e:
        print(f"[Бот] ОШИБКА сохранения правил в {path}: {e}", file=sys.stderr)


# Текстовый ввод:
# Поступление: Поступление/Доход/Плюс 5000 Сбербанк (ИП Моргун) за ноги; или 5000 Сбербанк (ИП Моргун) за ноги (сумма в начале = доход).
# Выбытие: Минус/-/Выбытие/Расход 5000 Касса (ИП Моргун) за ноги. В скобках — контрагент, дальше — назначение.
# Перевод: Перевод 5000 Сбербанк Касса
TYPE_OUT = ("минус", "выбытие", "расход")
TYPE_IN = ("плюс", "поступление", "доход")
TYPE_TRANSFER = ("перевод",)
ONE_WINDOW_FIRST_WORDS = TYPE_OUT + TYPE_IN + TYPE_TRANSFER

# Контрагент в скобках: (ИП Моргун) → counterparty
_COUNTERPARTY_RE = re.compile(r"\s*\(([^)]*)\)\s*")


def _parse_short_form(text: str, wallets: list[str]) -> Optional[dict]:
    """
    Парсит текстовый формат:
    - Поступление: «Поступление/Доход/Плюс 5000 Сбербанк (ИП Моргун) за ноги» или «5000 Сбербанк (ИП Моргун) за ноги».
      В скобках — контрагент, после скобок — назначение. Без скобок: «5000 Сбербанк» — контрагент и назначение пустые.
    - Выбытие: «Минус/-/Выбытие/Расход 5000 Касса (ИП Моргун) за ноги» — то же.
    - Перевод: «Перевод 5000 Сбербанк Касса» — сумма, откуда, куда.
    Не возвращает article — статью пользователь выбирает кнопками после ввода.
    """
    line = (text or "").strip()
    if not line:
        return None
    parts = line.split()
    if len(parts) < 2:
        return None
    first = parts[0].lower()
    # Определяем тип и позицию суммы
    if first == "перевод":
        op_type = "transfer"
        amount_raw = parts[1].replace(",", ".")
        rest = " ".join(parts[2:]).strip()
    elif first in TYPE_OUT or first == "-":
        op_type = "out"
        if first == "-":
            if len(parts) < 2:
                return None
            amount_raw = parts[1].replace(",", ".")
            rest = " ".join(parts[2:]).strip()
        else:
            amount_raw = parts[1].replace(",", ".")
            rest = " ".join(parts[2:]).strip()
    elif first in TYPE_IN:
        op_type = "in"
        amount_raw = parts[1].replace(",", ".")
        rest = " ".join(parts[2:]).strip()
    elif DDSSheetService.parse_amount(parts[0].replace(",", ".")) is not None and DDSSheetService.parse_amount(parts[0].replace(",", ".")) > 0:
        # Сумма в начале — всегда поступление
        op_type = "in"
        amount_raw = parts[0].replace(",", ".")
        rest = " ".join(parts[1:]).strip()
    else:
        return None
    amount = DDSSheetService.parse_amount(amount_raw)
    if amount is None or amount <= 0:
        return None
    wallets_sorted = sorted(wallets, key=len, reverse=True)

    def match_wallet(s: str):
        s = s.strip()
        for w in wallets_sorted:
            if s.lower().startswith(w.lower()):
                after = s[len(w):].strip()
                return w, after
        return None, s

    def extract_counterparty_purpose(s: str):
        m = _COUNTERPARTY_RE.search(s)
        if m:
            counterparty = m.group(1).strip()
            before, after = s[: m.start()], s[m.end() :]
            purpose = after.strip()
            return counterparty, purpose
        return "", s.strip()

    if op_type == "transfer":
        if not rest:
            return None
        wallet_from, after_first = match_wallet(rest)
        if not wallet_from:
            return None
        wallet_to, after_second = match_wallet(after_first)
        if not wallet_to:
            return None
        purpose = after_second.strip() or ""
        return {
            "type": "transfer",
            "date": _today_str(),
            "amount": amount,
            "wallet_from": wallet_from,
            "wallet_to": wallet_to,
            "purpose": purpose,
        }
    else:
        if not rest:
            return None
        wallet, after = match_wallet(rest)
        if not wallet:
            return None
        counterparty, purpose = extract_counterparty_purpose(after)
        return {
            "type": op_type,
            "date": _today_str(),
            "amount": amount,
            "wallet": wallet,
            "counterparty": counterparty,
            "purpose": purpose,
        }


def _is_one_window_message(text: str) -> bool:
    """Сообщение в формате текстового ввода: ключевое слово или сумма в начале. Одно число не считаем — это ввод суммы в пошаговом сценарии."""
    if not text or not text.strip():
        return False
    parts = text.strip().split()
    if not parts:
        return False
    first = parts[0].lower()
    if first in ONE_WINDOW_FIRST_WORDS:
        return True
    if first == "-" and len(parts) >= 2:
        return True
    # Сумма в начале = поступление, но одно слово "5000" — это ввод суммы в шаге, не перехватываем
    if len(parts) == 1 and DDSSheetService.parse_amount(parts[0].replace(",", ".")) is not None:
        return False
    if DDSSheetService.parse_amount(parts[0].replace(",", ".")) is not None and DDSSheetService.parse_amount(parts[0].replace(",", ".")) > 0:
        return True
    return False


class OneWindowFilter(filters.MessageFilter):
    """Пропускает только сообщения в формате «в одном окне» (начинаются с типа операции)."""
    def filter(self, message):
        if not message.text:
            return False
        return _is_one_window_message(message.text)


one_window_filter = OneWindowFilter()


async def handle_form(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Текстовый ввод: парсим → выбор статьи (для дохода/расхода) или подтверждение (для перевода) → сохранение по подтверждению."""
    text = (update.message.text or "").strip()
    user_id = update.effective_user.id if update.effective_user else None
    # Ожидание ввода диапазона дат для /stats
    if user_id is not None and user_id in _stats_waiting_user_ids:
        await stats_range_input_handler(update, context)
        return
    # Ожидание ввода в настройках (отчисления в фонды или название нового кошелька)
    if user_id is not None and user_id in _settings_waiting_user_ids:
        if context.user_data.get("_settings_add_wallet_position") is not None:
            await _handle_settings_add_wallet_name_input(update, context)
            return
        await _handle_settings_fund_input(update, context)
        return
    # Ожидание ввода при «Изменить» (сумма / контрагент / назначение) — обрабатываем любой текст
    waiting = context.user_data.pop("_waiting_for", None)
    if waiting is not None and context.user_data.get("_conv_chat_id") is not None and context.user_data.get("_conv_message_id") is not None:
        user_id = update.effective_user.id if update.effective_user else None
        if user_id is not None:
            _text_edit_waiting_user_ids.discard(user_id)
        chat_id = context.user_data["_conv_chat_id"]
        message_id = context.user_data["_conv_message_id"]
        if waiting == "amount":
            amount = DDSSheetService.parse_amount(text)
            if amount is not None and amount > 0:
                context.user_data["amount"] = amount
        elif waiting == "counterparty":
            context.user_data["counterparty"] = text if text != "—" else ""
        elif waiting == "purpose":
            context.user_data["purpose"] = text if text != "—" else ""
        try:
            confirm_text = _format_confirm_income_expense(context.user_data)
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=confirm_text,
                reply_markup=_keyboard_confirm_text(),
            )
        except Exception:
            pass
        return
    if not text or not _is_one_window_message(text):
        await update.message.reply_text(
            "Чтобы ввести операцию одним сообщением, начните с: Поступление / Доход / Плюс, Выбытие / Расход / Минус или Перевод.\nПодробнее: /text"
        )
        return

    try:
        svc = _get_sheet_service(context)
    except Exception as e:
        await update.message.reply_text(f"Ошибка подключения к таблице: {e}")
        return

    wallets = await asyncio.to_thread(svc.get_wallets)
    data = _parse_short_form(text, wallets)
    if not data:
        await update.message.reply_text(
            "Не удалось разобрать сообщение. Формат:\n"
            "• Поступление: Поступление/Доход/Плюс 5000 Сбербанк (ИП Моргун) за ноги или 5000 Сбербанк\n"
            "• Выбытие: Минус/-/Выбытие/Расход 5000 Касса (ИП Моргун) за ноги\n"
            "• Перевод: Перевод 5000 Сбербанк Касса\n"
            "В скобках — контрагент, дальше — назначение. /text — справка."
        )
        return

    context.user_data["_from_text"] = True
    context.user_data["date"] = data["date"]
    context.user_data["type"] = data["type"]
    context.user_data["amount"] = data["amount"]
    context.user_data["counterparty"] = data.get("counterparty", "")
    context.user_data["purpose"] = data.get("purpose", "")
    if data["type"] == "transfer":
        context.user_data["wallet_from"] = data["wallet_from"]
        context.user_data["wallet_to"] = data["wallet_to"]
    else:
        context.user_data["wallet"] = data["wallet"]

    if data["type"] == "transfer":
        text_confirm = _format_confirm_transfer(context.user_data)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить", callback_data=CB_TEXT_CONFIRM_YES)],
            [InlineKeyboardButton("❌ Отмена", callback_data=CB_TEXT_CONFIRM_NO)],
        ])
        msg = await update.message.reply_text(text_confirm, reply_markup=kb)
        context.user_data["_conv_chat_id"] = msg.chat_id
        context.user_data["_conv_message_id"] = msg.message_id
        return

    if data["type"] == "in":
        articles = await asyncio.to_thread(svc.get_articles_by_type_sorted_by_usage, "Поступление", True)
    else:
        articles = await asyncio.to_thread(svc.get_articles_by_type_sorted_by_usage, "Выбытие", True)
    if not articles:
        await update.message.reply_text("В таблице нет подходящих статей. Проверьте лист «ДДС: статьи».")
        context.user_data.pop("_from_text", None)
        return
    context.user_data["_text_articles"] = articles
    context.user_data["_text_articles_page"] = 0
    kb = _build_list_kb_with_pagination(
        articles, 0, lambda i, _: CB_TEXT_ART_PREFIX + str(i),
        CB_TEXT_ART_BACK, CB_TEXT_CONFIRM_NO, CB_TEXT_ART_PAGE_NEXT, CB_TEXT_ART_PAGE_PREV,
        show_back=False,
    )
    msg = await update.message.reply_text("Выберите статью:", reply_markup=kb)
    context.user_data["_conv_chat_id"] = msg.chat_id
    context.user_data["_conv_message_id"] = msg.message_id


async def handle_text_form_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка кнопок после текстового ввода: выбор статьи → подтверждение → сохранение."""
    query = update.callback_query
    data = query.data
    try:
        await query.answer()
    except Exception:
        pass
    if not context.user_data.get("_from_text"):
        try:
            await query.edit_message_text("Сессия истекла. Введите операцию заново или /start.")
        except Exception:
            pass
        return
    if data == CB_TEXT_CONFIRM_NO:
        uid = update.effective_user.id if update.effective_user else None
        if uid is not None:
            _text_edit_waiting_user_ids.discard(uid)
        await query.edit_message_text("Операция отменена.")
        context.user_data.pop("_from_text", None)
        context.user_data.pop("_text_articles", None)
        context.user_data.pop("_text_articles_page", None)
        context.user_data.pop("_conv_chat_id", None)
        context.user_data.pop("_conv_message_id", None)
        return
    if data == CB_TEXT_ART_BACK:
        context.user_data.pop("_from_text", None)
        context.user_data.pop("_text_articles", None)
        context.user_data.pop("_text_articles_page", None)
        context.user_data.pop("_conv_chat_id", None)
        context.user_data.pop("_conv_message_id", None)
        await query.edit_message_text("Операция отменена.")
        return
    if data == CB_TEXT_ART_PAGE_NEXT or data == CB_TEXT_ART_PAGE_PREV:
        articles = context.user_data.get("_text_articles", [])
        page = context.user_data.get("_text_articles_page", 0)
        if data == CB_TEXT_ART_PAGE_NEXT and (page + 1) * LIST_PAGE_SIZE < len(articles):
            page += 1
            context.user_data["_text_articles_page"] = page
        elif data == CB_TEXT_ART_PAGE_PREV and page > 0:
            page -= 1
            context.user_data["_text_articles_page"] = page
        else:
            return
        kb = _build_list_kb_with_pagination(
            articles, page, lambda i, _: CB_TEXT_ART_PREFIX + str(i),
            CB_TEXT_ART_BACK, CB_TEXT_CONFIRM_NO, CB_TEXT_ART_PAGE_NEXT, CB_TEXT_ART_PAGE_PREV,
            show_back=False,
        )
        await query.edit_message_text("Выберите статью:", reply_markup=kb)
        return
    if data == CB_TEXT_CONFIRM_YES:
        uid = update.effective_user.id if update.effective_user else None
        if uid is not None:
            _text_edit_waiting_user_ids.discard(uid)
        ud = dict(context.user_data)
        context.user_data.clear()
        try:
            svc = _get_sheet_service(context)
            direction = await asyncio.to_thread(svc.get_default_business_direction) or (svc.get_business_directions()[0] if svc.get_business_directions() else "")
        except Exception as e:
            try:
                await query.edit_message_text(f"Ошибка: {e}")
            except Exception:
                await context.bot.send_message(chat_id=query.message.chat_id, text=f"Ошибка: {e}")
            return
        if ud.get("type") == "transfer":
            try:
                await asyncio.to_thread(
                    svc.append_transfer,
                    date_str=ud["date"],
                    amount=ud["amount"],
                    wallet_from=ud["wallet_from"],
                    wallet_to=ud["wallet_to"],
                    purpose=ud.get("purpose", ""),
                    business_direction=direction,
                )
                await _send_balance_after(query, context, [ud["wallet_from"], ud["wallet_to"]], prefix="✅ Операция в ДДС внесена.\n\n")
            except Exception as e:
                try:
                    await query.edit_message_text(f"Ошибка записи: {e}")
                except Exception:
                    await context.bot.send_message(chat_id=query.message.chat_id, text=f"Ошибка записи: {e}")
        else:
            amount = ud.get("amount", 0)
            if ud.get("type") == "out":
                amount = -abs(amount)
            article = ud.get("article")
            if not article:
                try:
                    await query.edit_message_text("Ошибка: не выбрана статья. Введите операцию заново или /start.")
                except Exception:
                    await context.bot.send_message(chat_id=query.message.chat_id, text="Ошибка: не выбрана статья. Введите операцию заново или /start.")
                return
            try:
                await asyncio.to_thread(
                    svc.append_operation,
                    date_str=ud["date"],
                    amount=amount,
                    wallet=ud["wallet"],
                    business_direction=direction,
                    counterparty=ud.get("counterparty", ""),
                    purpose=ud.get("purpose", ""),
                    article=article,
                )
                await _send_balance_after(query, context, [ud["wallet"]], prefix="✅ Операция в ДДС внесена.\n\n")
            except Exception as e:
                try:
                    await query.edit_message_text(f"Ошибка записи: {e}")
                except Exception:
                    await context.bot.send_message(chat_id=query.message.chat_id, text=f"Ошибка записи: {e}")
        return
    if data.startswith(CB_TEXT_ART_PREFIX):
        try:
            idx = int(data[len(CB_TEXT_ART_PREFIX) :])
            articles = context.user_data.get("_text_articles", [])
            article = articles[idx]
        except (ValueError, IndexError):
            await query.edit_message_text("Ошибка выбора статьи. Начните заново.")
            context.user_data.pop("_from_text", None)
            return
        context.user_data["article"] = article
        context.user_data.pop("_text_articles", None)
        context.user_data.pop("_text_articles_page", None)
        text_confirm = _format_confirm_income_expense(context.user_data)
        await query.edit_message_text(text_confirm, reply_markup=_keyboard_confirm_text())
        return
    if data == CB_TEXT_EDIT:
        text_confirm = _format_confirm_income_expense(context.user_data)
        await query.edit_message_text(text_confirm, reply_markup=_keyboard_edit_menu_text())
        return
    if data == CB_TEXT_EDIT_AMOUNT:
        context.user_data["_waiting_for"] = "amount"
        uid = update.effective_user.id if update.effective_user else None
        if uid is not None:
            _text_edit_waiting_user_ids.add(uid)
        await query.edit_message_text("Введите новую сумму (числом):")
        return
    if data == CB_TEXT_EDIT_CT:
        context.user_data["_waiting_for"] = "counterparty"
        uid = update.effective_user.id if update.effective_user else None
        if uid is not None:
            _text_edit_waiting_user_ids.add(uid)
        await query.edit_message_text("Введите контрагента (или — для пустого):")
        return
    if data == CB_TEXT_EDIT_PURPOSE:
        context.user_data["_waiting_for"] = "purpose"
        uid = update.effective_user.id if update.effective_user else None
        if uid is not None:
            _text_edit_waiting_user_ids.add(uid)
        await query.edit_message_text("Введите назначение платежа (или — для пустого):")
        return
    if data == CB_TEXT_EDIT_ARTICLE:
        try:
            svc = _get_sheet_service(context)
        except Exception as e:
            await query.edit_message_text(f"Ошибка подключения к таблице: {e}")
            return
        ud = context.user_data
        group = "Поступление" if ud.get("type") == "in" else "Выбытие"
        try:
            articles = await asyncio.to_thread(svc.get_articles_by_type_sorted_by_usage, group, True)
        except Exception as e:
            await query.edit_message_text(_format_sheet_error(e))
            return
        if not articles:
            await query.edit_message_text("В таблице нет статей для этого типа.")
            return
        ud["_text_edit_articles"] = articles
        ud["_text_edit_articles_page"] = 0
        kb = _build_list_kb_with_pagination(
            articles, 0, lambda i, _: CB_TEXT_EDIT_ART_PREFIX + str(i),
            CB_TEXT_EDIT_ART_BACK, CB_TEXT_CONFIRM_NO, CB_TEXT_EDIT_ART_PAGE_NEXT, CB_TEXT_EDIT_ART_PAGE_PREV,
        )
        await query.edit_message_text("Выберите новую статью:", reply_markup=kb)
        return
    if data == CB_TEXT_EDIT_ART_BACK:
        text_confirm = _format_confirm_income_expense(context.user_data)
        await query.edit_message_text(text_confirm, reply_markup=_keyboard_edit_menu_text())
        return
    if data.startswith(CB_TEXT_EDIT_ART_PREFIX):
        try:
            idx = int(data[len(CB_TEXT_EDIT_ART_PREFIX):])
            articles = context.user_data.get("_text_edit_articles", [])
            article = articles[idx]
        except (ValueError, IndexError):
            await query.edit_message_text("Ошибка выбора статьи.")
            return
        context.user_data["article"] = article
        context.user_data.pop("_text_edit_articles", None)
        context.user_data.pop("_text_edit_articles_page", None)
        text_confirm = _format_confirm_income_expense(context.user_data)
        await query.edit_message_text(text_confirm, reply_markup=_keyboard_confirm_text())
        return
    if data == CB_TEXT_EDIT_ART_PAGE_NEXT or data == CB_TEXT_EDIT_ART_PAGE_PREV:
        articles = context.user_data.get("_text_edit_articles", [])
        page = context.user_data.get("_text_edit_articles_page", 0)
        if data == CB_TEXT_EDIT_ART_PAGE_NEXT and (page + 1) * LIST_PAGE_SIZE < len(articles):
            page += 1
            context.user_data["_text_edit_articles_page"] = page
        elif data == CB_TEXT_EDIT_ART_PAGE_PREV and page > 0:
            page -= 1
            context.user_data["_text_edit_articles_page"] = page
        else:
            return
        kb = _build_list_kb_with_pagination(
            articles, page, lambda i, _: CB_TEXT_EDIT_ART_PREFIX + str(i),
            CB_TEXT_EDIT_ART_BACK, CB_TEXT_CONFIRM_NO, CB_TEXT_EDIT_ART_PAGE_NEXT, CB_TEXT_EDIT_ART_PAGE_PREV,
        )
        await query.edit_message_text("Выберите новую статью:", reply_markup=kb)
        return
    if data == CB_TEXT_EDIT_BACK:
        text_confirm = _format_confirm_income_expense(context.user_data)
        await query.edit_message_text(text_confirm, reply_markup=_keyboard_confirm_text())
        return


def _format_balance_after(wallet_names: list[str], balances: dict, total: Optional[float]) -> str:
    """Форматирует блок «Баланс счета после операции» и «ОБЩИЙ БАЛАНС» (Markdown, суммы жирным)."""
    lines = ["📊 Баланс счета после операции:"]
    for w in wallet_names:
        if w in balances:
            amt = _format_amount(balances[w])
            lines.append(f"{w}: *{amt} ₽*")
    if total is not None:
        lines.append(f"\nОБЩИЙ БАЛАНС: *{_format_amount(total)} ₽*")
    return "\n".join(lines)


async def _send_balance_after(
    update_or_query,
    context: ContextTypes.DEFAULT_TYPE,
    wallet_names: list[str],
    prefix: Optional[str] = None,
):
    """После операции: сбросить кэш балансов, отправить «✅ Операция в ДДС внесена.» + баланс затронутых счетов и общий баланс (Markdown)."""
    try:
        svc = _get_sheet_service(context)
        svc.invalidate_balances_cache()
        balances = await asyncio.to_thread(svc.get_balances, False)
    except Exception:
        return
    total = balances.pop("Итого", None)
    if not wallet_names and total is None and not prefix:
        return
    text = _format_balance_after(wallet_names, balances, total)
    if prefix:
        text = prefix + text
    reply_markup = None
    if prefix:
        context.user_data["_last_balance_wallets"] = list(wallet_names)
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("Добавить операцию в ДДС ✅", callback_data=CB_ADD_OPERATION)],
            [InlineKeyboardButton("Показать баланс", callback_data=CB_SHOW_BALANCE)],
            [InlineKeyboardButton("Рассчитать фонды", callback_data=CB_RUN_FUNDS)],
        ])
    if hasattr(update_or_query, "edit_message_text"):
        try:
            await update_or_query.edit_message_text(text, reply_markup=reply_markup, parse_mode="Markdown")
        except Exception:
            pass
    else:
        msg = getattr(update_or_query, "message", None)
        if msg and hasattr(msg, "reply_text"):
            try:
                await msg.reply_text(text, reply_markup=reply_markup, parse_mode="Markdown")
            except Exception:
                pass


def _format_sheet_error(e: Exception) -> str:
    """Сообщение для пользователя при ошибке загрузки из таблицы (сеть, таймаут)."""
    err = str(e).lower()
    if isinstance(e, (ConnectionError, OSError)) or "connection" in err or "reset" in err or "timeout" in err:
        return (
            "Не удалось загрузить данные из таблицы (сеть или Google Таблицы). "
            "Попробуйте через минуту или нажмите /start ещё раз."
        )
    return f"Ошибка загрузки: {e}"


def _get_sheet_service(context: ContextTypes.DEFAULT_TYPE) -> DDSSheetService:
    """Единый экземпляр сервиса (кэш справочников внутри) — быстрее отклик."""
    if "sheet_service" not in context.bot_data:
        path = _resolve_credentials_path(os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json"))
        sheet_id = os.getenv("GOOGLE_SHEET_ID")
        if not sheet_id:
            raise RuntimeError("GOOGLE_SHEET_ID не задан в .env")
        context.bot_data["sheet_service"] = DDSSheetService(path, sheet_id)
    return context.bot_data["sheet_service"]


async def _build_full_balance_message():
    """Формирует текст и клавиатуру полного баланса (как /balance). Возвращает (text, reply_markup) или (None, None) при ошибке."""
    path = _resolve_credentials_path(os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json"))
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        return None, None
    try:
        balances = await asyncio.to_thread(get_balances_standalone, path, sheet_id)
    except Exception as e:
        print("\n--- Ошибка при загрузке балансов ---", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        print("---\n", file=sys.stderr)
        return f"Ошибка загрузки балансов: {str(e).split(chr(10))[0][:200]}", None
    if not balances:
        return "Балансы не найдены.", None
    total = balances.pop("Итого", None)
    if total is None and balances:
        total = sum(balances.values())
    main_wallets, fund_wallets = _split_wallets(balances)
    date_str = _today_str()
    total_val = total or 0
    main_sorted = sorted(main_wallets.items(), key=lambda x: x[1], reverse=True)
    fund_sorted = sorted(fund_wallets.items(), key=lambda x: x[1], reverse=True)
    total_fmt = _format_amount(total_val)
    lines = [
        f"💰📊 <b>ОБЩИЙ БАЛАНС на {date_str}:</b>",
        f"<b>></b> <b><u>{total_fmt} ₽</u></b>",
        "",
    ]
    if main_sorted:
        main_total = sum(m for _, m in main_sorted)
        main_total_fmt = _format_amount(main_total)
        lines.append(f"💸 <b>КОШЕЛЬКИ: {main_total_fmt} ₽</b>")
        for wallet, amount in main_sorted:
            w_esc = _escape_html(wallet)
            lines.append(f"• {w_esc}: <b>{_format_amount(amount)} ₽</b>")
    if fund_sorted:
        if main_sorted:
            lines.append("")
        fund_total = sum(m for _, m in fund_sorted)
        fund_total_fmt = _format_amount(fund_total)
        lines.append(f"🏦 <b>ФОНДЫ: {fund_total_fmt} ₽</b>")
        for wallet, amount in fund_sorted:
            w_esc = _escape_html(wallet)
            lines.append(f"• {w_esc}: <b>{_format_amount(amount)} ₽</b>")
    reply_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("Добавить операцию ✅", callback_data=CB_ADD_OPERATION)],
        [InlineKeyboardButton("Сформировать отчёт 📝", callback_data=CB_STATS_OPEN)],
    ])
    return "\n".join(lines), reply_markup


async def balance_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает балансы кошельков и общий итог из листа «ДДС: месяц» (без gspread — обход Invalid control character)."""
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        try:
            await update.message.reply_text("GOOGLE_SHEET_ID не задан в .env")
        except Exception:
            pass
        return
    text, reply_markup = await _build_full_balance_message()
    if text is None:
        return
    try:
        kwargs = {"reply_markup": reply_markup} if reply_markup else {}
        await update.message.reply_text(text, parse_mode="HTML" if reply_markup else None, **kwargs)
    except Exception:
        pass


async def show_balance_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """По нажатию кнопки «Показать баланс» после внесения операции — показываем баланс в том же окне и кнопку Назад."""
    query = update.callback_query
    try:
        await _retry_on_network(lambda: query.answer("Загрузка…"))
    except Exception:
        try:
            await query.answer()
        except Exception:
            pass
    text, _ = await _build_full_balance_message()
    if not text:
        return
    keyboard_balance_back = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Назад", callback_data=CB_BALANCE_BACK)],
    ])
    try:
        await query.edit_message_text(
            text,
            parse_mode="HTML",
            reply_markup=keyboard_balance_back,
        )
    except Exception:
        pass


async def balance_back_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """По нажатию «🔙 Назад» в окне баланса (после «Показать баланс») — вернуть экран «Операция внесена»."""
    query = update.callback_query
    try:
        await _retry_on_network(lambda: query.answer("Загрузка…"))
    except Exception:
        try:
            await query.answer()
        except Exception:
            pass
    wallet_names = context.user_data.get("_last_balance_wallets")
    if not wallet_names:
        try:
            await query.edit_message_text("Сессия сброшена. Внесите операцию заново.")
        except Exception:
            pass
        return
    try:
        svc = _get_sheet_service(context)
        balances = await asyncio.to_thread(svc.get_balances, False)
    except Exception:
        try:
            await query.edit_message_text("Ошибка загрузки балансов.")
        except Exception:
            pass
        return
    total = balances.pop("Итого", None)
    text = _format_balance_after(wallet_names, balances, total)
    text = "✅ Операция в ДДС внесена.\n\n" + text
    reply_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("Добавить операцию в ДДС ✅", callback_data=CB_ADD_OPERATION)],
        [InlineKeyboardButton("Показать баланс", callback_data=CB_SHOW_BALANCE)],
        [InlineKeyboardButton("Рассчитать фонды", callback_data=CB_RUN_FUNDS)],
    ])
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode="Markdown")
    except Exception:
        pass


def _format_stats_report(
    period_label: str,
    period_str: str,
    start_balance: Optional[float],
    end_balance: Optional[float],
    change: float,
    revenue: Optional[float],
    expenses: Optional[float],
) -> str:
    """Формирует текст отчёта для /stats."""
    lines = [f"📊 *{period_label}*", f"Период: {period_str}", ""]
    if start_balance is not None:
        lines.append(f"Начальный остаток: *{_format_amount(start_balance)} ₽*")
    lines.append("")
    if revenue is not None:
        lines.append(f"Поступления: +{_format_amount(revenue)} ₽")
    if expenses is not None:
        lines.append(f"Выбытия: -{_format_amount(expenses)} ₽")
    change_str = _format_amount(abs(change))
    sign = "+" if change >= 0 else "−"
    lines.append(f"Изменение: {sign}{change_str} ₽")
    lines.append("")
    if end_balance is not None:
        lines.append(f"Текущий баланс: *{_format_amount(end_balance)} ₽*")
    return "\n".join(lines)


def _keyboard_stats_period() -> InlineKeyboardMarkup:
    """Клавиатура выбора периода для отчёта /stats: один ряд Сегодня/Неделя/Месяц, ниже Ввести диапазон и Отмена."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Сегодня", callback_data=CB_STATS_TODAY),
            InlineKeyboardButton("Неделя", callback_data=CB_STATS_WEEK),
            InlineKeyboardButton("Месяц", callback_data=CB_STATS_MONTH),
        ],
        [InlineKeyboardButton("Ввести диапазон", callback_data=CB_STATS_RANGE)],
        [InlineKeyboardButton("Отмена ❌", callback_data=CB_STATS_CANCEL)],
    ])


def _keyboard_stats_waiting_range() -> InlineKeyboardMarkup:
    """Клавиатура при вводе диапазона: только Назад."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Назад", callback_data=CB_STATS_BACK)],
    ])


def _keyboard_stats_after_report() -> InlineKeyboardMarkup:
    """Клавиатура под отчётом: Добавить операцию, Назад к выбору периода."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Добавить операцию в ДДС ✅", callback_data=CB_ADD_OPERATION)],
        [InlineKeyboardButton("🔙 Назад", callback_data=CB_STATS_BACK)],
    ])


async def stats_range_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка пошагового ввода диапазона дат: сначала «с какой даты», потом «до какой даты»."""
    uid = update.effective_user.id if update.effective_user else None
    text = (update.message.text or "").strip()
    date_from = context.user_data.get("_stats_date_from")

    if date_from is None:
        # Шаг 1: ввод начальной даты
        parsed = _validate_date(text)
        if not parsed:
            try:
                await update.message.reply_text("Неверный формат. Введите дату в формате ДД.ММ.ГГГГ (например 01.01.2026):")
            except Exception:
                pass
            if uid is not None:
                _stats_waiting_user_ids.add(uid)
            return
        context.user_data["_stats_date_from"] = parsed
        try:
            await update.message.reply_text("Введите конечную дату (ДД.ММ.ГГГГ):")
        except Exception:
            pass
        if uid is not None:
            _stats_waiting_user_ids.add(uid)
        return

    # Шаг 2: ввод конечной даты
    date_to = _validate_date(text)
    if not date_to:
        try:
            await update.message.reply_text("Неверный формат. Введите дату в формате ДД.ММ.ГГГГ (например 14.01.2026):")
        except Exception:
            pass
        if uid is not None:
            _stats_waiting_user_ids.add(uid)
        return
    # Упорядочиваем: если ввели «до» раньше «с», меняем местами
    try:
        d1 = date(int(date_from[6:10]), int(date_from[3:5]), int(date_from[:2]))
        d2 = date(int(date_to[6:10]), int(date_to[3:5]), int(date_to[:2]))
        if d1 > d2:
            date_from, date_to = date_to, date_from
    except (ValueError, IndexError):
        pass
    context.user_data.pop("_stats_waiting_range", None)
    context.user_data.pop("_stats_date_from", None)
    if uid is not None:
        _stats_waiting_user_ids.discard(uid)
    try:
        svc = _get_sheet_service(context)
    except Exception as e:
        try:
            await update.message.reply_text(_format_sheet_error(e))
        except Exception:
            pass
        return
    try:
        report = await asyncio.to_thread(svc.get_summary_for_date_range, date_from, date_to)
    except Exception as e:
        try:
            await update.message.reply_text(_format_sheet_error(e))
        except Exception:
            pass
        return
    if report is None:
        try:
            await update.message.reply_text("Нет данных за выбранный период.")
        except Exception:
            pass
        return
    period_str = f"{date_from} – {date_to}"
    text_report = _format_stats_report(
        period_label="Диапазон",
        period_str=period_str,
        start_balance=report.get("start_balance"),
        end_balance=report.get("end_balance"),
        change=report.get("change", 0),
        revenue=report.get("revenue"),
        expenses=report.get("expenses"),
    )
    try:
        await update.message.reply_text(text_report, parse_mode="Markdown", reply_markup=_keyboard_stats_after_report())
    except Exception:
        try:
            await update.message.reply_text(text_report, reply_markup=_keyboard_stats_after_report())
        except Exception:
            pass


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /stats: выбор периода и отчёт по данным из «ДДС: Сводный» или реестра."""
    try:
        await update.message.reply_text("Выберите период для отчёта:", reply_markup=_keyboard_stats_period())
    except Exception:
        pass


async def stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора периода в /stats и кнопки Назад под отчётом."""
    query = update.callback_query
    try:
        await _retry_on_network(lambda: query.answer())
    except Exception:
        pass
    data = query.data
    if data == CB_STATS_OPEN:
        # Кнопка «Сформировать отчёт» под балансом — открыть выбор периода (как /stats)
        try:
            await _retry_on_network(
                lambda: context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text="Выберите период для отчёта:",
                    reply_markup=_keyboard_stats_period(),
                )
            )
        except Exception:
            pass
        return
    if data == CB_STATS_BACK:
        context.user_data.pop("_stats_waiting_range", None)
        context.user_data.pop("_stats_date_from", None)
        uid = update.effective_user.id if update.effective_user else None
        if uid is not None:
            _stats_waiting_user_ids.discard(uid)
        try:
            await _retry_on_network(
                lambda: query.edit_message_text("Выберите период для отчёта:", reply_markup=_keyboard_stats_period())
            )
        except Exception:
            pass
        return
    if data == CB_STATS_RANGE:
        context.user_data["_stats_waiting_range"] = True
        context.user_data.pop("_stats_date_from", None)
        uid = update.effective_user.id if update.effective_user else None
        if uid is not None:
            _stats_waiting_user_ids.add(uid)
        prompt = "Введите начальную дату (ДД.ММ.ГГГГ):"
        try:
            await _retry_on_network(
                lambda: query.edit_message_text(prompt, reply_markup=_keyboard_stats_waiting_range())
            )
        except Exception:
            pass
        return
    if data == CB_STATS_CANCEL:
        try:
            await _retry_on_network(lambda: query.edit_message_text("Отчёт отменён."))
        except Exception:
            pass
        return
    try:
        svc = _get_sheet_service(context)
    except Exception as e:
        try:
            await _retry_on_network(lambda: query.edit_message_text(_format_sheet_error(e)))
        except Exception:
            pass
        return
    today = date.today()
    today_str = _today_str()
    report = None
    period_label = ""
    period_str = ""
    if data == CB_STATS_TODAY:
        period_label = "Сегодня"
        period_str = today_str
        report = await asyncio.to_thread(svc.get_summary_for_date_range, today_str, today_str)
    elif data == CB_STATS_WEEK:
        period_label = "Неделя"
        from_d = today - timedelta(days=6)
        from_str = f"{from_d.day:02d}.{from_d.month:02d}.{from_d.year}"
        period_str = f"{from_str} – {today_str}"
        report = await asyncio.to_thread(svc.get_summary_for_date_range, from_str, today_str)
    elif data == CB_STATS_MONTH:
        period_label = "Месяц"
        month = today.month
        year = today.year
        from_d = date(year, month, 1)
        last_day = calendar.monthrange(year, month)[1]
        to_d = date(year, month, last_day)
        period_str = f"{from_d.day:02d}.{from_d.month:02d}.{from_d.year} – {to_d.day:02d}.{to_d.month:02d}.{to_d.year}"
        report = await asyncio.to_thread(svc.get_summary_for_month, month)
    if report is None:
        try:
            await _retry_on_network(
                lambda: query.edit_message_text(
                    "Нет данных за выбранный период или ошибка чтения таблицы («ДДС: месяц» или «ДДС: Сводный»)."
                )
            )
        except Exception:
            pass
        return
    text = _format_stats_report(
        period_label=period_label,
        period_str=period_str,
        start_balance=report.get("start_balance"),
        end_balance=report.get("end_balance"),
        change=report.get("change", 0),
        revenue=report.get("revenue"),
        expenses=report.get("expenses"),
    )
    kb = _keyboard_stats_after_report()
    try:
        await _retry_on_network(lambda: query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb))
    except Exception:
        try:
            await _retry_on_network(lambda: query.edit_message_text(text, reply_markup=kb))
        except Exception:
            pass


async def _run_funds_logic(context: ContextTypes.DEFAULT_TYPE) -> str:
    """
    Общая логика «Рассчитать фонды»: возвращает текст ответа (для /funds и кнопки).
    
    Отчисления производятся ТОЛЬКО для правил, где источник = "Точка Банк".
    Доход считается только из поступлений на "Точка Банк".
    """
    svc = _get_sheet_service(context)
    date_str = _today_str()
    rules = _get_fund_rules(context)
    if not rules:
        return "Правила отчислений не настроены. Используйте /settings → Настройка отчислений в фонды."
    
    # Фильтруем только правила с источником "Точка Банк"
    punto_bank_rules = [
        r for r in rules
        if (r.get("source") or "").strip().lower() == "точка банк"
        and (r.get("destination") or "").strip()
        and float(r.get("percent", 0)) > 0
    ]
    
    if not punto_bank_rules:
        return (
            "⚠️ Нет активных правил отчисления для 'Точка Банк'.\n\n"
            "Отчисления в фонды производятся только из 'Точка Банк'. "
            "Доход из других источников (например, Карта Т-банк) не облагается отчислениями."
        )
    
    # Получаем доход ТОЛЬКО из "Точка Банк"
    daily_income = await asyncio.to_thread(svc.get_daily_income, date_str, wallet_filter="Точка Банк")
    if daily_income <= 0:
        return (
            f"За *{date_str}* не было поступлений на Точка Банк.\n\n"
            "Отчисления в фонды делаются только с поступлений на Точка Банк."
        )
    
    try:
        already_done = await asyncio.to_thread(svc.get_fund_transfers_done_today, date_str)
    except Exception:
        already_done = {}
    
    total_percent = sum(float(r.get("percent", 0)) for r in punto_bank_rules)
    total_already_today = sum(already_done.values())
    
    if total_percent > 0 and total_already_today > 0:
        revenue_already_used = round(total_already_today * 100 / total_percent, 2)
        new_revenue = round(daily_income - revenue_already_used, 2)
    else:
        new_revenue = daily_income
    
    if new_revenue <= 0:
        return (
            f"Отчисления в Фонды за *{date_str}* уже произведены или дохода нет.\n\n"
            "Отчисления в фонды делаются только с новых поступлений на Точка Банк."
        )
    
    try:
        direction = await asyncio.to_thread(svc.get_default_business_direction) or (svc.get_business_directions()[0] if svc.get_business_directions() else "")
    except Exception:
        direction = ""
    
    wallets_affected = set()
    transfers_made = []
    this_run_total = 0.0
    
    for r in punto_bank_rules:
        source = "Точка Банк"
        destination = (r.get("destination") or "").strip()
        percent = float(r.get("percent", 0))
        
        if not destination or percent <= 0:
            continue
        
        # Округляем до рубля
        raw = new_revenue * percent / 100
        to_transfer = int(raw + 0.5) if raw >= 0 else int(raw - 0.5)
        if to_transfer <= 0:
            continue
        
        try:
            await asyncio.to_thread(
                svc.append_transfer,
                date_str=date_str,
                amount=to_transfer,
                wallet_from=source,
                wallet_to=destination,
                purpose="Отчисление в Фонд",
                business_direction=direction,
                purpose_inflow=f"Поступление в Фонд за {date_str}",
            )
            wallets_affected.add(source)
            wallets_affected.add(destination)
            this_run_total += to_transfer
            transfers_made.append((destination, to_transfer, None))
        except Exception as e:
            transfers_made.append((destination, -1, str(e)))
    
    svc.invalidate_balances_cache()
    
    if not transfers_made:
        return "Отчисления не выполнены."
    
    transfers_made.sort(key=lambda x: -x[1] if x[1] >= 0 else -1)
    transfer_lines = []
    for dest, to_transfer, err in transfers_made:
        if to_transfer >= 0:
            amt = _format_amount(to_transfer)
            transfer_lines.append(f"{dest} → *{amt} ₽*")
        else:
            transfer_lines.append(f"{dest}: ошибка — {err or '?'}")
    
    revenue_str = _format_amount(daily_income)
    lines = [
        f"🏦 Расчет отчислений в Фонды за *{date_str}* произведен.",
        "",
        f"Выручка из Точка Банк за *{date_str}*: *{revenue_str} ₽*",
        "",
    ]
    lines.extend(transfer_lines)
    lines.append("")
    total_today = total_already_today + this_run_total
    if total_already_today > 0:
        if this_run_total == int(this_run_total):
            this_run_str = f"{int(this_run_total):,}".replace(",", " ")
        else:
            this_run_str = _format_amount(this_run_total)
        lines.append(f"Сумма, отправленная в фонды с учетом последних операций: *{this_run_str} ₽*")
        lines.append("")
    if total_today == int(total_today):
        total_str = f"{int(total_today):,}".replace(",", " ")
    else:
        total_str = _format_amount(total_today)
    lines.append(f"✅ Общая сумма, отправленная в Фонды за сегодня: *{total_str} ₽*")
    return "\n".join(lines)


async def funds_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Рассчитать и провести отчисления в фонды: сумма поступлений за день × % по правилам."""
    try:
        text = await _run_funds_logic(context)
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")
        return
    await update.message.reply_text(text, parse_mode="Markdown")


async def funds_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """По нажатию кнопки «Рассчитать фонды» после внесения операции."""
    query = update.callback_query
    try:
        await _retry_on_network(lambda: query.answer("Загрузка…"))
    except Exception:
        try:
            await query.answer()
        except Exception:
            pass
    try:
        text = await _run_funds_logic(context)
    except Exception as e:
        text = str(e)
    try:
        await query.edit_message_text(text, parse_mode="Markdown")
    except Exception:
        try:
            await query.message.reply_text(text, parse_mode="Markdown")
        except Exception:
            pass


def _format_fund_rules_text(rules: list[dict]) -> str:
    """Текст списка правил отчислений."""
    if not rules:
        return "Правила не заданы."
    lines = ["**Отчисления в фонды:**", ""]
    for i, r in enumerate(rules, 1):
        src = r.get("source", "") or "—"
        dst = r.get("destination", "") or "—"
        pct = r.get("percent", 0)
        lines.append(f"{i}. {src} → {dst}: {pct}%")
    return "\n".join(lines)


def _keyboard_settings() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Настройка отчислений в фонды", callback_data=CB_SETTINGS_FUNDS)],
        [InlineKeyboardButton("Добавить новый кошелёк", callback_data=CB_SETTINGS_ADD_WALLET)],
        [InlineKeyboardButton("🔙 Назад", callback_data=CB_SETTINGS_BACK)],
    ])


def _keyboard_fund_rules(rules: list[dict]) -> InlineKeyboardMarkup:
    """Кнопки: Изменить 1, ..., Удалить 1, ..., Добавить, Назад."""
    rows = []
    for i in range(len(rules)):
        rows.append([
            InlineKeyboardButton(f"Изменить {i + 1}", callback_data=CB_SETTINGS_FUNDS_EDIT_PREFIX + str(i)),
            InlineKeyboardButton(f"Удалить {i + 1}", callback_data=CB_SETTINGS_FUNDS_DEL_PREFIX + str(i)),
        ])
    rows.append([InlineKeyboardButton("Добавить отчисление", callback_data=CB_SETTINGS_FUNDS_ADD)])
    rows.append([InlineKeyboardButton("Назад", callback_data=CB_SETTINGS_FUNDS_BACK)])
    return InlineKeyboardMarkup(rows)


def _keyboard_fund_source(wallets: list[str]) -> InlineKeyboardMarkup:
    """Кнопки выбора кошелька-источника (с какого списывать)."""
    return _build_full_list_kb(
        wallets,
        lambda i, _: CB_SETTINGS_FUNDS_SRC_PREFIX + str(i),
        back_btn=CB_SETTINGS_FUNDS_BACK_TO_LIST,
    )


def _keyboard_fund_destination(wallets: list[str]) -> InlineKeyboardMarkup:
    """Кнопки выбора кошелька-назначения (в какой фонд переводить)."""
    return _build_full_list_kb(
        wallets,
        lambda i, _: CB_SETTINGS_FUNDS_DST_PREFIX + str(i),
        back_btn=CB_SETTINGS_FUNDS_BACK_TO_LIST,
    )


def _keyboard_fund_percent() -> InlineKeyboardMarkup:
    """Кнопки выбора процента: 5%, 10%, 15%, 20% или ввести свой."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("5%", callback_data=CB_SETTINGS_FUNDS_PCT_PREFIX + "5"),
            InlineKeyboardButton("10%", callback_data=CB_SETTINGS_FUNDS_PCT_PREFIX + "10"),
            InlineKeyboardButton("15%", callback_data=CB_SETTINGS_FUNDS_PCT_PREFIX + "15"),
            InlineKeyboardButton("20%", callback_data=CB_SETTINGS_FUNDS_PCT_PREFIX + "20"),
        ],
        [InlineKeyboardButton("Ввести свой %", callback_data=CB_SETTINGS_FUNDS_PCT_CUSTOM)],
        [InlineKeyboardButton("🔙 Назад", callback_data=CB_SETTINGS_FUNDS_BACK_TO_LIST)],
    ])


def _format_op_short(op: dict) -> str:
    """Формат одной операции для списка: сумма и тип (поступление/выбытие)."""
    amt = op.get("amount") or 0
    op_type = op.get("op_type") or ("поступление" if amt > 0 else "выбытие")
    return f"{amt:,.0f} ₽ {op_type}".replace(",", " ")


def _format_op_list_line(op: dict, index: int) -> str:
    """Одна строка в нумерованном списке операций: номер, сумма, тип, контрагент/назначение (удобно читать)."""
    amt = op.get("amount") or 0
    op_type = op.get("op_type") or ("поступление" if amt > 0 else "выбытие")
    cp = (op.get("counterparty") or "").strip()
    purpose = (op.get("purpose") or "").strip()
    desc = cp if cp else (purpose if purpose else "—")
    if len(desc) > 45:
        desc = desc[:42] + "..."
    return f"{index}. {amt:,.0f} ₽ {op_type} — {desc}".replace(",", " ")


def _format_operations_table(ops: list[dict], date_label: str, start_num: int = 1) -> str:
    """Форматирует операции в виде таблицы: заголовки столбцов, выровненные данные в моноширинном блоке. start_num — с какого номера нумеровать (для подсказки «стр. 2»)."""
    w_num = 3
    w_sum = 12
    w_type = 12
    w_desc = 32
    header = f"{'№':<{w_num}} {'Сумма':<{w_sum}} {'Тип':<{w_type}} Контрагент / Назначение"
    sep = "─" * min(60, w_num + w_sum + w_type + w_desc + 3)
    lines = [f"Операции за {date_label}", "", header, sep]
    for i, o in enumerate(ops, start_num):
        amt = o.get("amount") or 0
        op_type = o.get("op_type") or ("поступление" if amt > 0 else "выбытие")
        sum_str = f"{amt:,.0f} ₽".replace(",", " ")
        cp = (o.get("counterparty") or "").strip()
        purpose = (o.get("purpose") or "").strip()
        desc = cp if cp else (purpose if purpose else "—")
        if len(desc) > w_desc:
            desc = desc[: w_desc - 3] + "..."
        row = f"{i:<{w_num}} {sum_str:<{w_sum}} {op_type:<{w_type}} {desc:<{w_desc}}"
        lines.append(row)
    return "\n".join(lines)


async def settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Настройки: меню с пунктом «Отчисления в фонды». Повтор отправки при сбоях сети; защита от дублей при многократном нажатии."""
    user_id = update.effective_user.id if update.effective_user else None
    if user_id is not None:
        now = time.monotonic()
        last = _settings_cmd_last_sent.get(user_id, 0)
        if now - last < SETTINGS_DEBOUNCE_SEC:
            return
        _settings_cmd_last_sent[user_id] = now
    await _reply_text_with_retry(
        update,
        "⚙️ **Настройки**\n\nВыберите раздел:",
        reply_markup=_keyboard_settings(),
        parse_mode="Markdown",
    )


async def _handle_settings_fund_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода своего % (когда нажали «Ввести свой %») при настройке отчислений в фонды."""
    text = (update.message.text or "").strip()
    user_id = update.effective_user.id if update.effective_user else None
    phase = context.user_data.pop("_settings_fund_phase", None)
    rule_idx = context.user_data.get("_settings_fund_rule_idx", 0)
    if phase is None or phase != "percent":
        _settings_waiting_user_ids.discard(user_id)
        return
    is_new = rule_idx < 0
    pct = DDSSheetService.parse_amount(text.replace(",", "."))
    if pct is None or pct < 0:
        pct = 0.0
    pct = round(float(pct), 2)
    if is_new:
        draft = context.user_data.get("_settings_fund_new_rule", {})
        draft["percent"] = pct
        rules = list(_get_fund_rules(context))
        rules.append(draft)
        context.user_data.pop("_settings_fund_new_rule", None)
    else:
        rules = list(context.user_data.get("_settings_fund_rules_draft") or _get_fund_rules(context))
        if 0 <= rule_idx < len(rules):
            rules[rule_idx]["percent"] = pct
        context.user_data.pop("_settings_fund_rules_draft", None)
    _save_fund_rules(context, rules)
    _settings_waiting_user_ids.discard(user_id)
    context.user_data.pop("_settings_fund_rule_idx", None)
    context.user_data.pop("_settings_fund_wallets", None)
    num = len(rules) if is_new else rule_idx + 1
    await update.message.reply_text(f"Готово. Правило {num} {'добавлено' if is_new else 'обновлено'}.")
    await update.message.reply_text(
        _format_fund_rules_text(_get_fund_rules(context)),
        reply_markup=_keyboard_fund_rules(_get_fund_rules(context)),
        parse_mode="Markdown",
    )


async def _handle_settings_add_wallet_name_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода названия кошелька при «Добавить новый кошелёк»."""
    text = (update.message.text or "").strip()
    user_id = update.effective_user.id if update.effective_user else None
    position = context.user_data.pop("_settings_add_wallet_position", None)
    sheet_number = context.user_data.pop("_settings_add_wallet_sheet_number", None)
    _settings_waiting_user_ids.discard(user_id)
    if position is None or sheet_number is None or not text:
        await update.message.reply_text("Действие отменено или название пусто. Можно снова выбрать «Добавить новый кошелёк» в настройках.")
        return
    try:
        svc = _get_sheet_service(context)
        await asyncio.to_thread(svc.add_wallet, position, sheet_number, text, 0.0)
    except Exception as e:
        await update.message.reply_text(f"Ошибка при добавлении кошелька: {e}")
        return
    await update.message.reply_text(
        f"✅ Кошелёк «{_escape_md(text)}» добавлен в слот {sheet_number}.\n"
        "Сумма на начало: 0. Лист переименован, строка в «ДДС: Сводный» отображена.",
        parse_mode="Markdown",
    )


async def handle_settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка кнопок настроек: Отчисления в фонды, Изменить правило, Добавить, Назад."""
    query = update.callback_query
    try:
        await _retry_on_network(lambda: query.answer())
    except Exception:
        try:
            await query.answer()
        except Exception:
            pass
    data = query.data
    if data == CB_SETTINGS_BACK:
        await query.edit_message_text("Настройки закрыты.")
        return
    if data == CB_SETTINGS_FUNDS_BACK:
        for key in ("_settings_fund_phase", "_settings_fund_rule_idx", "_settings_fund_new_rule", "_settings_fund_rules_draft", "_settings_fund_wallets"):
            context.user_data.pop(key, None)
        uid = update.effective_user.id if update.effective_user else None
        if uid is not None:
            _settings_waiting_user_ids.discard(uid)
        await query.edit_message_text(
            "⚙️ **Настройки**\n\nВыберите раздел:",
            reply_markup=_keyboard_settings(),
            parse_mode="Markdown",
        )
        return
    if data == CB_SETTINGS_FUNDS_BACK_TO_LIST:
        for key in ("_settings_fund_phase", "_settings_fund_rule_idx", "_settings_fund_new_rule", "_settings_fund_rules_draft", "_settings_fund_wallets"):
            context.user_data.pop(key, None)
        uid = update.effective_user.id if update.effective_user else None
        if uid is not None:
            _settings_waiting_user_ids.discard(uid)
        rules = _get_fund_rules(context)
        await query.edit_message_text(
            _format_fund_rules_text(rules),
            reply_markup=_keyboard_fund_rules(rules),
            parse_mode="Markdown",
        )
        return
    if data == CB_SETTINGS_FUNDS:
        rules = _get_fund_rules(context)
        await query.edit_message_text(
            _format_fund_rules_text(rules),
            reply_markup=_keyboard_fund_rules(rules),
            parse_mode="Markdown",
        )
        return
    if data == CB_SETTINGS_ADD_WALLET:
        try:
            svc = _get_sheet_service(context)
            free_slots = await asyncio.to_thread(svc.get_free_wallet_slots)
        except Exception as e:
            await query.edit_message_text(f"Ошибка при чтении таблицы: {e}")
            return
        if not free_slots:
            await query.edit_message_text(
                "Свободных слотов для добавления кошелька нет. Перейдите в таблицу через Google Sheets и отредактируйте кошельки вручную.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=CB_SETTINGS_ADD_WALLET_BACK)]]),
            )
            return
        # Берём первый свободный слот, сразу просим ввести название
        first = free_slots[0]
        position, sheet_number = first["position"], first["sheet_number"]
        context.user_data["_settings_add_wallet_position"] = position
        context.user_data["_settings_add_wallet_sheet_number"] = sheet_number
        uid = update.effective_user.id if update.effective_user else None
        if uid is not None:
            _settings_waiting_user_ids.add(uid)
        await query.edit_message_text(
            f"💰 **Добавить новый кошелёк**\n\nПервый свободный слот — {sheet_number}. Введите название кошелька (например: Фонд Резерв).\nСумма на начало будет 0.",
            parse_mode="Markdown",
        )
        return
    if data == CB_SETTINGS_ADD_WALLET_BACK:
        await query.edit_message_text(
            "⚙️ **Настройки**\n\nВыберите раздел:",
            reply_markup=_keyboard_settings(),
            parse_mode="Markdown",
        )
        return
    if data == CB_SETTINGS_FUNDS_ADD:
        context.user_data["_settings_fund_rule_idx"] = -1
        context.user_data["_settings_fund_new_rule"] = {"source": "", "destination": "", "percent": 0.0}
        try:
            svc = _get_sheet_service(context)
            wallets = await asyncio.to_thread(svc.get_wallets)
        except Exception as e:
            await query.edit_message_text(f"Ошибка загрузки кошельков: {e}")
            return
        if not wallets:
            await query.edit_message_text("В таблице нет кошельков. Проверьте настройки.")
            return
        context.user_data["_settings_fund_wallets"] = wallets
        await query.edit_message_text(
            "С какого кошелька списывать отчисления?",
            reply_markup=_keyboard_fund_source(wallets),
        )
        return
    if data.startswith(CB_SETTINGS_FUNDS_EDIT_PREFIX):
        try:
            idx = int(data[len(CB_SETTINGS_FUNDS_EDIT_PREFIX) :])
        except ValueError:
            return
        rules = _get_fund_rules(context)
        if idx < 0 or idx >= len(rules):
            return
        context.user_data["_settings_fund_rule_idx"] = idx
        context.user_data["_settings_fund_rules_draft"] = list(rules)
        try:
            svc = _get_sheet_service(context)
            wallets = await asyncio.to_thread(svc.get_wallets)
        except Exception as e:
            await query.edit_message_text(f"Ошибка загрузки кошельков: {e}")
            return
        if not wallets:
            await query.edit_message_text("В таблице нет кошельков. Проверьте настройки.")
            return
        context.user_data["_settings_fund_wallets"] = wallets
        context.user_data["_settings_fund_src_page"] = 0
        await query.edit_message_text(
            "С какого кошелька списывать отчисления?",
            reply_markup=_keyboard_fund_source(wallets),
        )
        return
    if data.startswith(CB_SETTINGS_FUNDS_SRC_PREFIX):
        try:
            i = int(data[len(CB_SETTINGS_FUNDS_SRC_PREFIX) :])
        except ValueError:
            return
        wallets = context.user_data.get("_settings_fund_wallets", [])
        if i < 0 or i >= len(wallets):
            return
        source = wallets[i]
        rule_idx = context.user_data.get("_settings_fund_rule_idx", 0)
        is_new = rule_idx < 0
        if is_new:
            context.user_data["_settings_fund_new_rule"] = {"source": source, "destination": "", "percent": 0.0}
        else:
            rules = list(context.user_data.get("_settings_fund_rules_draft") or _get_fund_rules(context))
            if 0 <= rule_idx < len(rules):
                rules[rule_idx]["source"] = source
            context.user_data["_settings_fund_rules_draft"] = rules
        await query.edit_message_text(
            "В какой фонд переводить?",
            reply_markup=_keyboard_fund_destination(wallets),
        )
        return
    if data.startswith(CB_SETTINGS_FUNDS_DST_PREFIX):
        try:
            i = int(data[len(CB_SETTINGS_FUNDS_DST_PREFIX) :])
        except ValueError:
            return
        wallets = context.user_data.get("_settings_fund_wallets", [])
        if i < 0 or i >= len(wallets):
            return
        destination = wallets[i]
        rule_idx = context.user_data.get("_settings_fund_rule_idx", 0)
        is_new = rule_idx < 0
        if is_new:
            draft = context.user_data.get("_settings_fund_new_rule", {})
            draft["destination"] = destination
            context.user_data["_settings_fund_new_rule"] = draft
        else:
            rules = list(context.user_data.get("_settings_fund_rules_draft") or _get_fund_rules(context))
            if 0 <= rule_idx < len(rules):
                rules[rule_idx]["destination"] = destination
            context.user_data["_settings_fund_rules_draft"] = rules
        await query.edit_message_text(
            "Какой % от выручки отчислять в этот фонд?",
            reply_markup=_keyboard_fund_percent(),
        )
        return
    if data == CB_SETTINGS_FUNDS_PCT_CUSTOM:
        context.user_data["_settings_fund_phase"] = "percent"
        uid = update.effective_user.id if update.effective_user else None
        if uid is not None:
            _settings_waiting_user_ids.add(uid)
        await query.edit_message_text("Введите процент (число, например 50 или 100):")
        return
    if data.startswith(CB_SETTINGS_FUNDS_PCT_PREFIX):
        try:
            pct_str = data[len(CB_SETTINGS_FUNDS_PCT_PREFIX) :]
            pct = float(pct_str)
        except ValueError:
            return
        if pct < 0 or pct > 100:
            return
        rule_idx = context.user_data.get("_settings_fund_rule_idx", 0)
        is_new = rule_idx < 0
        if is_new:
            draft = context.user_data.get("_settings_fund_new_rule", {})
            draft["percent"] = round(pct, 2)
            rules = list(_get_fund_rules(context))
            rules.append(draft)
            context.user_data.pop("_settings_fund_new_rule", None)
        else:
            rules = list(context.user_data.get("_settings_fund_rules_draft") or _get_fund_rules(context))
            if 0 <= rule_idx < len(rules):
                rules[rule_idx]["percent"] = round(pct, 2)
            context.user_data.pop("_settings_fund_rules_draft", None)
        _save_fund_rules(context, rules)
        context.user_data.pop("_settings_fund_rule_idx", None)
        context.user_data.pop("_settings_fund_wallets", None)
        num = len(rules) if is_new else rule_idx + 1
        await query.edit_message_text(
            f"Готово. Правило {num} {'добавлено' if is_new else 'обновлено'}.\n\n"
            + _format_fund_rules_text(_get_fund_rules(context)),
            reply_markup=_keyboard_fund_rules(_get_fund_rules(context)),
            parse_mode="Markdown",
        )
        return
    if data.startswith(CB_SETTINGS_FUNDS_DEL_PREFIX):
        try:
            idx = int(data[len(CB_SETTINGS_FUNDS_DEL_PREFIX) :])
        except ValueError:
            return
        rules = _get_fund_rules(context)
        rules = list(rules)
        if 0 <= idx < len(rules):
            rules.pop(idx)
            _save_fund_rules(context, rules)
        await query.edit_message_text(
            _format_fund_rules_text(_get_fund_rules(context)),
            reply_markup=_keyboard_fund_rules(_get_fund_rules(context)),
            parse_mode="Markdown",
        )
        return


async def text_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Текстовый ввод: подсказка формата."""
    help_text = (
        "📝 *Напишите операцию одним сообщением.*\n\n"
        "Поступление / Выбытие:\n\n"
        "Тип Сумма Счёт (Контрагент) Назначение\n\n"
        "_Пример: Расход 5000 Сбербанк (Озон) Расходники для процедур_\n\n"
        "*Контрагента и назначение вводить необязательно.* После ввода сообщения бот попросит выбрать статью.\n\n"
        "Перевод:\n\n"
        "Сумма Счёт1 Счёт2\n\n"
        "Пример: Перевод 5000 Сбербанк Наличные\n\n"
        "При поступлениях в типе операций можно указать:\n"
        "Поступление / Доход / Плюс или начать сразу с суммы\n\n"
        "При выбытиях в типе операций можно указывать:\n"
        "Минус / Выбытие / Расход"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")


def _conv_msg_id(context: ContextTypes.DEFAULT_TYPE):
    """Возвращает (chat_id, message_id) сохранённого «окна» диалога или None."""
    ud = context.user_data
    c = ud.get("_conv_chat_id")
    m = ud.get("_conv_message_id")
    return (c, m) if c is not None and m is not None else None


async def _edit_conv_message(context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
    """Обновляет одно сообщение диалога («всё в одном окне»)."""
    ids = _conv_msg_id(context)
    if not ids:
        return
    chat_id, message_id = ids
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=reply_markup,
        )
    except Exception:
        pass


def _conv_one_window_text(ud: dict, prompt: str) -> str:
    """Текст для «одного окна»: заголовок + уже выбранные поля + текущий шаг."""
    lines = ["📝 Добавить операцию в ДДС", "────────────────────"]
    if ud.get("date"):
        lines.append(f"📅 Дата: {ud['date']}")
    if ud.get("type"):
        lines.append(f"📋 Тип: {'Поступление' if ud['type'] == 'in' else 'Выбытие' if ud['type'] == 'out' else 'Перевод'}")
    if ud.get("article"):
        lines.append(f"📌 Статья: {ud['article']}")
    if ud.get("wallet"):
        lines.append(f"💳 Кошелёк: {ud['wallet']}")
    if ud.get("wallet_from"):
        lines.append(f"💸 С: {ud['wallet_from']}")
    if ud.get("wallet_to"):
        lines.append(f"💸 В: {ud['wallet_to']}")
    if ud.get("amount") is not None:
        lines.append(f"💰 Сумма: {_format_amount(ud['amount'])} ₽")
    if "counterparty" in ud:
        lines.append(f"👤 Контрагент: {ud.get('counterparty') or '—'}")
    if "purpose" in ud:
        lines.append(f"📝 Назначение: {ud.get('purpose') or '—'}")
    lines.append("")
    lines.append(prompt)
    return "\n".join(lines)


def _keyboard_date() -> InlineKeyboardMarkup:
    """Кнопки выбора даты: Сегодня (одна кнопка), ниже 4 даты ДД.ММ (1–4 дня назад), Отмена."""
    day1 = _date_n_days_ago(1)
    day2 = _date_n_days_ago(2)
    day3 = _date_n_days_ago(3)
    day4 = _date_n_days_ago(4)
    def short(d: str) -> str:
        return d[:5] if len(d) >= 5 else d
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Сегодня", callback_data=CB_TODAY)],
        [
            InlineKeyboardButton(short(day1), callback_data=CB_DATE_PREFIX + day1),
            InlineKeyboardButton(short(day2), callback_data=CB_DATE_PREFIX + day2),
        ],
        [
            InlineKeyboardButton(short(day3), callback_data=CB_DATE_PREFIX + day3),
            InlineKeyboardButton(short(day4), callback_data=CB_DATE_PREFIX + day4),
        ],
        [InlineKeyboardButton("Отмена ❌", callback_data=CB_CANCEL)],
    ])


async def add_operation_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """По нажатию «Добавить операцию в ДДС ✅» — отправляем новое сообщение с шагом выбора даты (не редактируем, чтобы работало и под отчётом /stats)."""
    query = update.callback_query
    await query.answer()
    ud = context.user_data
    text = _conv_one_window_text(ud, "Введите дату операции в формате ДД.ММ.ГГГГ или выберите один из вариантов ниже.")
    # Всегда отправляем новое сообщение: под отчётом edit_message_text может не сработать (Markdown/размер), плюс отчёт остаётся на экране
    new_msg = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=text,
        reply_markup=_keyboard_date(),
    )
    ud["_conv_chat_id"] = new_msg.chat_id
    ud["_conv_message_id"] = new_msg.message_id
    return DATE


async def start_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запуск (пошаговый ввод): одно сообщение обновляется на каждом шаге."""
    ud = context.user_data
    text = _conv_one_window_text(ud, "Введите дату операции в формате ДД.ММ.ГГГГ или выберите один из вариантов ниже.")
    msg = await update.message.reply_text(text, reply_markup=_keyboard_date())
    ud["_conv_chat_id"] = msg.chat_id
    ud["_conv_message_id"] = msg.message_id
    return DATE


async def date_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    context.user_data["date"] = _today_str()
    await _ask_type(update, context)
    return TYPE_OP


async def date_preset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Выбор даты «Вчера», «2 дня назад», «3 дня назад» (callback_data = date:DD.MM.YYYY)."""
    query = update.callback_query
    await query.answer()
    if not query.data or not query.data.startswith(CB_DATE_PREFIX):
        return DATE
    date_str = query.data[len(CB_DATE_PREFIX) :].strip()
    context.user_data["date"] = date_str
    await _ask_type(update, context)
    return TYPE_OP


async def date_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    ud = context.user_data
    parsed = _validate_date(text)
    if not parsed:
        await _edit_conv_message(
            context,
            _conv_one_window_text(ud, "Неверный формат. Введите дату ДД.ММ.ГГГГ или выберите кнопку (Сегодня / Вчера / …)."),
            _keyboard_date(),
        )
        return DATE
    ud["date"] = parsed
    await _edit_conv_message(context, _conv_one_window_text(ud, "Выберите тип операции:"), _keyboard_type())
    return TYPE_OP


def _keyboard_type() -> InlineKeyboardMarkup:
    """Тип операции: три кнопки типа, внизу ряд 🔙 Назад (слева) и Отмена (справа)."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Поступление 💰", callback_data=CB_TYPE_IN)],
        [InlineKeyboardButton("Выбытие 💸", callback_data=CB_TYPE_OUT)],
        [InlineKeyboardButton("Перевод 🔄", callback_data=CB_TYPE_TR)],
        [InlineKeyboardButton("🔙 Назад", callback_data=CB_BACK), InlineKeyboardButton("Отмена ❌", callback_data=CB_CANCEL)],
    ])


def _keyboard_skip_back_cancel() -> InlineKeyboardMarkup:
    """Пропустить; ниже ряд 🔙 Назад и Отмена (шаг контрагента/назначения после суммы)."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Пропустить", callback_data=CB_SKIP)],
        [InlineKeyboardButton("🔙 Назад", callback_data=CB_BACK), InlineKeyboardButton("Отмена ❌", callback_data=CB_CANCEL)],
    ])


def _keyboard_back_cancel() -> InlineKeyboardMarkup:
    """Один ряд: 🔙 Назад и Отмена (шаг ввода суммы)."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Назад", callback_data=CB_BACK), InlineKeyboardButton("Отмена ❌", callback_data=CB_CANCEL)],
    ])


def _build_full_list_kb(
    items: list,
    callback_fn,  # (index: int, item) -> str
    cancel_btn: Optional[str] = None,
    back_btn: Optional[str] = None,
    bottom_row: Optional[list[tuple[str, str]]] = None,
) -> InlineKeyboardMarkup:
    """Клавиатура: полный список в один столбец. Внизу — одна кнопка или bottom_row (например Назад 🔙, Отмена ❌)."""
    rows = [[InlineKeyboardButton(item, callback_data=callback_fn(i, item))] for i, item in enumerate(items)]
    if bottom_row:
        rows.append([InlineKeyboardButton(t, callback_data=c) for t, c in bottom_row])
    elif cancel_btn:
        rows.append([InlineKeyboardButton("Отмена ❌", callback_data=cancel_btn)])
    elif back_btn:
        rows.append([InlineKeyboardButton("Назад 🔙", callback_data=back_btn)])
    return InlineKeyboardMarkup(rows)


def _build_list_kb_with_pagination(
    items: list,
    page: int,
    callback_fn,
    back_cb: str,
    cancel_cb: str,
    page_next_cb: str,
    page_prev_cb: str,
    show_back: bool = True,
) -> InlineKeyboardMarkup:
    """Список в один столбец. Если пунктов ≤9 — все на одном «листе»; если >9 — по 9 на страницу. Внизу: при show_back — Назад 🔙, [Стр. X/Y], Отмена ❌; иначе только [Стр. X/Y] и Отмена ❌."""
    if len(items) <= LIST_PAGE_SIZE:
        if show_back:
            bottom_row = [("Назад 🔙", back_cb), ("Отмена ❌", cancel_cb)]
        else:
            bottom_row = [("Отмена ❌", cancel_cb)]
        return _build_full_list_kb(items, callback_fn, bottom_row=bottom_row)
    start = page * LIST_PAGE_SIZE
    end = min(start + LIST_PAGE_SIZE, len(items))
    total_pages = (len(items) + LIST_PAGE_SIZE - 1) // LIST_PAGE_SIZE
    rows = [[InlineKeyboardButton(items[i], callback_data=callback_fn(i, items[i]))] for i in range(start, end)]
    nav_btns = []
    if show_back:
        nav_btns.append(InlineKeyboardButton("Назад 🔙", callback_data=back_cb))
    if page > 0:
        nav_btns.append(InlineKeyboardButton(f"◀ {page + 1}/{total_pages}", callback_data=page_prev_cb))
    if page < total_pages - 1:
        nav_btns.append(InlineKeyboardButton(f"{page + 1}/{total_pages} ▶", callback_data=page_next_cb))
    nav_btns.append(InlineKeyboardButton("Отмена ❌", callback_data=cancel_cb))
    rows.append(nav_btns)
    return InlineKeyboardMarkup(rows)


async def _ask_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ud = context.user_data
    text = _conv_one_window_text(ud, "Выберите тип операции:")
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=_keyboard_type())
    else:
        await update.message.reply_text(text, reply_markup=_keyboard_type())


async def type_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    # Сразу отвечаем на callback (и показываем «Загрузка…»), чтобы не сработал таймаут Telegram (~25 с)
    try:
        await _retry_on_network(lambda: query.answer("Загрузка…"))
    except Exception:
        try:
            await query.answer()
        except Exception:
            pass
    data = query.data
    if data == CB_CANCEL:
        await query.edit_message_text("Операция отменена.")
        context.user_data.clear()
        return ConversationHandler.END
    if data == CB_BACK:
        ud = context.user_data
        await query.edit_message_text(
            _conv_one_window_text(ud, "Введите дату операции в формате ДД.ММ.ГГГГ или выберите один из вариантов ниже."),
            reply_markup=_keyboard_date(),
        )
        return DATE

    try:
        svc = _get_sheet_service(context)
    except Exception as e:
        await query.edit_message_text(f"Ошибка подключения к таблице: {e}")
        context.user_data.clear()
        return ConversationHandler.END

    ud = context.user_data
    if data == CB_TYPE_IN:
        ud["type"] = "in"
        try:
            await query.edit_message_text(_conv_one_window_text(ud, "Загрузка списка статей…"))
        except Exception:
            pass
        try:
            articles = await asyncio.to_thread(svc.get_articles_by_type_sorted_by_usage, "Поступление", True)
        except Exception as e:
            await query.edit_message_text(_format_sheet_error(e))
            context.user_data.clear()
            return ConversationHandler.END
        if not articles:
            await query.edit_message_text("В таблице нет статей типа «Поступление». Проверьте лист «ДДС: статьи».")
            return ConversationHandler.END
        ud["_articles"] = articles
        ud["_articles_page"] = 0
        kb = _build_list_kb_with_pagination(
            articles, 0, lambda i, _: CB_ARTICLE_PREFIX + str(i),
            CB_ARTICLE_BACK, CB_CANCEL, CB_ARTICLE_PAGE_NEXT, CB_ARTICLE_PAGE_PREV,
        )
        await query.edit_message_text(_conv_one_window_text(ud, "Выберите статью:"), reply_markup=kb)
        return ARTICLE

    if data == CB_TYPE_OUT:
        ud["type"] = "out"
        try:
            await query.edit_message_text(_conv_one_window_text(ud, "Загрузка списка статей…"))
        except Exception:
            pass
        try:
            articles = await asyncio.to_thread(svc.get_articles_by_type_sorted_by_usage, "Выбытие", True)
        except Exception as e:
            await query.edit_message_text(_format_sheet_error(e))
            context.user_data.clear()
            return ConversationHandler.END
        if not articles:
            await query.edit_message_text("В таблице нет статей типа «Выбытие». Проверьте лист «ДДС: статьи».")
            return ConversationHandler.END
        ud["_articles"] = articles
        ud["_articles_page"] = 0
        kb = _build_list_kb_with_pagination(
            articles, 0, lambda i, _: CB_ARTICLE_PREFIX + str(i),
            CB_ARTICLE_BACK, CB_CANCEL, CB_ARTICLE_PAGE_NEXT, CB_ARTICLE_PAGE_PREV,
        )
        await query.edit_message_text(_conv_one_window_text(ud, "Выберите статью:"), reply_markup=kb)
        return ARTICLE

    if data == CB_TYPE_TR:
        ud["type"] = "transfer"
        try:
            await query.edit_message_text(_conv_one_window_text(ud, "Загрузка списка кошельков…"))
        except Exception:
            pass
        try:
            await asyncio.to_thread(svc.get_transfer_articles)
        except ValueError as e:
            await query.edit_message_text(str(e))
            return ConversationHandler.END
        except Exception as e:
            await query.edit_message_text(_format_sheet_error(e))
            context.user_data.clear()
            return ConversationHandler.END
        try:
            wallets = await asyncio.to_thread(svc.get_wallets)
        except Exception as e:
            await query.edit_message_text(_format_sheet_error(e))
            context.user_data.clear()
            return ConversationHandler.END
        if len(wallets) < 2:
            await query.edit_message_text("Нужно минимум 2 кошелька для перевода. Проверьте настройки.")
            return ConversationHandler.END
        ud["_wallets"] = wallets
        ud["_wallets_page"] = 0
        kb = _build_list_kb_with_pagination(
            wallets, 0, lambda _, w: CB_WALLET_PREFIX + w,
            CB_TRANSFER_FROM_BACK, CB_CANCEL, CB_TRANSFER_FROM_PAGE_NEXT, CB_TRANSFER_FROM_PAGE_PREV,
        )
        await query.edit_message_text(_conv_one_window_text(ud, "С какого кошелька переводим?"), reply_markup=kb)
        return TRANSFER_FROM

    return TYPE_OP


async def article_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    ud = context.user_data
    data = query.data
    if data == CB_CANCEL:
        await query.edit_message_text("Операция отменена.")
        context.user_data.clear()
        return ConversationHandler.END
    if data == CB_ARTICLE_BACK:
        ud.pop("article", None)
        text = _conv_one_window_text(ud, "Выберите тип операции:")
        await query.edit_message_text(text, reply_markup=_keyboard_type())
        return TYPE_OP
    if data == CB_ARTICLE_PAGE_NEXT or data == CB_ARTICLE_PAGE_PREV:
        articles = ud.get("_articles", [])
        page = ud.get("_articles_page", 0)
        if data == CB_ARTICLE_PAGE_NEXT and (page + 1) * LIST_PAGE_SIZE < len(articles):
            page += 1
        elif data == CB_ARTICLE_PAGE_PREV and page > 0:
            page -= 1
        else:
            return ARTICLE
        ud["_articles_page"] = page
        kb = _build_list_kb_with_pagination(
            articles, page, lambda i, _: CB_ARTICLE_PREFIX + str(i),
            CB_ARTICLE_BACK, CB_CANCEL, CB_ARTICLE_PAGE_NEXT, CB_ARTICLE_PAGE_PREV,
        )
        await query.edit_message_text(_conv_one_window_text(ud, "Выберите статью:"), reply_markup=kb)
        return ARTICLE
    articles = ud.get("_articles", [])
    try:
        idx = int(data[len(CB_ARTICLE_PREFIX):])
        ud["article"] = articles[idx]
    except (ValueError, IndexError):
        await query.edit_message_text("Ошибка выбора статьи. Начните заново: /start")
        context.user_data.clear()
        return ConversationHandler.END
    try:
        svc = _get_sheet_service(context)
        wallets = await asyncio.to_thread(svc.get_wallets)
    except Exception as e:
        await query.edit_message_text(_format_sheet_error(e))
        context.user_data.clear()
        return ConversationHandler.END
    ud["_wallets"] = wallets
    ud["_wallets_page"] = 0
    kb = _build_list_kb_with_pagination(
        wallets, 0, lambda _, w: CB_WALLET_PREFIX + w,
        CB_WALLET_BACK, CB_CANCEL, CB_WALLET_PAGE_NEXT, CB_WALLET_PAGE_PREV,
    )
    await query.edit_message_text(_conv_one_window_text(ud, "Выберите кошелёк:"), reply_markup=kb)
    return WALLET


async def wallet_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    ud = context.user_data
    data = query.data
    if data == CB_CANCEL:
        await query.edit_message_text("Операция отменена.")
        context.user_data.clear()
        return ConversationHandler.END
    if data == CB_WALLET_BACK:
        ud.pop("wallet", None)
        articles = ud.get("_articles", [])
        page = ud.get("_articles_page", 0)
        kb = _build_list_kb_with_pagination(
            articles, page, lambda i, _: CB_ARTICLE_PREFIX + str(i),
            CB_ARTICLE_BACK, CB_CANCEL, CB_ARTICLE_PAGE_NEXT, CB_ARTICLE_PAGE_PREV,
        )
        await query.edit_message_text(_conv_one_window_text(ud, "Выберите статью:"), reply_markup=kb)
        return ARTICLE
    if data == CB_WALLET_PAGE_NEXT or data == CB_WALLET_PAGE_PREV:
        wallets = ud.get("_wallets", [])
        page = ud.get("_wallets_page", 0)
        if data == CB_WALLET_PAGE_NEXT and (page + 1) * LIST_PAGE_SIZE < len(wallets):
            page += 1
        elif data == CB_WALLET_PAGE_PREV and page > 0:
            page -= 1
        else:
            return WALLET
        ud["_wallets_page"] = page
        kb = _build_list_kb_with_pagination(
            wallets, page, lambda _, w: CB_WALLET_PREFIX + w,
            CB_WALLET_BACK, CB_CANCEL, CB_WALLET_PAGE_NEXT, CB_WALLET_PAGE_PREV,
        )
        await query.edit_message_text(_conv_one_window_text(ud, "Выберите кошелёк:"), reply_markup=kb)
        return WALLET
    if not data.startswith(CB_WALLET_PREFIX):
        return WALLET
    wallet_name = data[len(CB_WALLET_PREFIX):]
    if wallet_name not in ud.get("_wallets", []):
        return WALLET
    ud["wallet"] = wallet_name
    await query.edit_message_text(
        _conv_one_window_text(ud, "Введите сумму (числом, например 5000 или 1250,50):"),
        reply_markup=_keyboard_back_cancel(),
    )
    return AMOUNT


async def amount_entered(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    ud = context.user_data
    amount = DDSSheetService.parse_amount(text)
    if amount is None or amount <= 0:
        await _edit_conv_message(
            context,
            _conv_one_window_text(ud, "Введите положительное число (сумму)."),
            _keyboard_back_cancel(),
        )
        return AMOUNT
    ud["amount"] = amount
    await _edit_conv_message(
        context,
        _conv_one_window_text(ud, "Введите контрагента или нажмите «Пропустить»."),
        _keyboard_skip_back_cancel(),
    )
    return COUNTERPARTY


async def counterparty_entered(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    ud = context.user_data
    ud["counterparty"] = (update.message.text or "").strip()
    await _edit_conv_message(
        context,
        _conv_one_window_text(ud, "Введите назначение платежа или нажмите «Пропустить»."),
        _keyboard_skip_back_cancel(),
    )
    return PURPOSE


async def counterparty_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    ud = context.user_data
    ud["counterparty"] = ""
    text = _conv_one_window_text(ud, "Введите назначение платежа или нажмите «Пропустить».")
    await _edit_conv_message(
        context,
        text,
        _keyboard_skip_back_cancel(),
    )
    return PURPOSE


async def amount_back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """🔙 Назад с шага ввода суммы → возврат к выбору кошелька."""
    query = update.callback_query
    await query.answer()
    ud = context.user_data
    wallets = ud.get("_wallets", [])
    page = ud.get("_wallets_page", 0)
    kb = _build_list_kb_with_pagination(
        wallets, page, lambda _, w: CB_WALLET_PREFIX + w,
        CB_WALLET_BACK, CB_CANCEL, CB_WALLET_PAGE_NEXT, CB_WALLET_PAGE_PREV,
    )
    await query.edit_message_text(_conv_one_window_text(ud, "Выберите кошелёк:"), reply_markup=kb)
    return WALLET


async def counterparty_back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """🔙 Назад с шага контрагента → возврат на ввод суммы."""
    query = update.callback_query
    await query.answer()
    ud = context.user_data
    await _edit_conv_message(
        context,
        _conv_one_window_text(ud, "Введите сумму (числом, например 5000 или 1250,50):"),
        _keyboard_back_cancel(),
    )
    return AMOUNT


async def purpose_back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """🔙 Назад с шага назначения → возврат на ввод контрагента."""
    query = update.callback_query
    await query.answer()
    ud = context.user_data
    await _edit_conv_message(
        context,
        _conv_one_window_text(ud, "Введите контрагента или нажмите «Пропустить»."),
        _keyboard_skip_back_cancel(),
    )
    return COUNTERPARTY


def _format_confirm_income_expense(ud: dict) -> str:
    """Один блок текста «Подтвердите операцию» для поступления/выбытия (без дублирования)."""
    op_label = "Поступление" if ud.get("type") == "in" else "Выбытие"
    lines = [
        "📝 Подтвердите операцию",
        "────────────────────",
        f"📅 Дата: {ud.get('date', '')}",
        f"📋 Тип: {op_label}",
        f"📌 Статья: {ud.get('article', '')}",
        f"💳 Кошелёк: {ud.get('wallet', '')}",
        f"💰 Сумма: {_format_amount(ud.get('amount', 0))} ₽",
        f"👤 Контрагент: {ud.get('counterparty', '') or '—'}",
        f"📝 Назначение: {ud.get('purpose', '') or '—'}",
    ]
    return "\n".join(lines)


def _keyboard_confirm() -> InlineKeyboardMarkup:
    """Подтверждение: Подтвердить, Изменить, Отмена (пошаговый поток)."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить", callback_data=CB_CONFIRM_YES)],
        [InlineKeyboardButton("📝 Изменить", callback_data=CB_EDIT)],
        [InlineKeyboardButton("❌ Отмена", callback_data=CB_CONFIRM_NO)],
    ])


def _keyboard_confirm_text() -> InlineKeyboardMarkup:
    """Подтверждение после текстового ввода: Подтвердить, Изменить, Отмена."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить", callback_data=CB_TEXT_CONFIRM_YES)],
        [InlineKeyboardButton("📝 Изменить", callback_data=CB_TEXT_EDIT)],
        [InlineKeyboardButton("❌ Отмена", callback_data=CB_TEXT_CONFIRM_NO)],
    ])


def _keyboard_edit_menu_text() -> InlineKeyboardMarkup:
    """Меню «Что изменить?» для текстового потока."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Изменить сумму", callback_data=CB_TEXT_EDIT_AMOUNT)],
        [InlineKeyboardButton("Изменить контрагента", callback_data=CB_TEXT_EDIT_CT)],
        [InlineKeyboardButton("Изменить назначение платежа", callback_data=CB_TEXT_EDIT_PURPOSE)],
        [InlineKeyboardButton("Изменить статью ДДС", callback_data=CB_TEXT_EDIT_ARTICLE)],
        [InlineKeyboardButton("🔙 Назад", callback_data=CB_TEXT_EDIT_BACK)],
    ])


def _keyboard_edit_menu() -> InlineKeyboardMarkup:
    """Меню «Что изменить?» для пошагового потока (поступление/выбытие)."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Изменить сумму", callback_data=CB_EDIT_AMOUNT)],
        [InlineKeyboardButton("Изменить контрагента", callback_data=CB_EDIT_CT)],
        [InlineKeyboardButton("Изменить назначение платежа", callback_data=CB_EDIT_PURPOSE)],
        [InlineKeyboardButton("Изменить статью ДДС", callback_data=CB_EDIT_ARTICLE)],
        [InlineKeyboardButton("Изменить кошелёк", callback_data=CB_EDIT_WALLET)],
        [InlineKeyboardButton("🔙 Назад", callback_data=CB_EDIT_BACK)],
    ])


async def purpose_entered(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    ud = context.user_data
    ud["purpose"] = (update.message.text or "").strip()
    await _edit_conv_message(
        context,
        _format_confirm_income_expense(ud),
        _keyboard_confirm(),
    )
    return CONFIRM


async def purpose_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    ud = context.user_data
    ud["purpose"] = ""
    text = _format_confirm_income_expense(ud)
    await _edit_conv_message(context, text, _keyboard_confirm())
    return CONFIRM


async def confirm_income_expense(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка кнопок Подтвердить/Изменить/Отмена на шаге подтверждения (поступление/выбытие)."""
    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        pass
    ud = context.user_data
    if query.data == CB_CONFIRM_NO:
        await query.edit_message_text("Операция отменена.")
        context.user_data.clear()
        return ConversationHandler.END
    if query.data == CB_EDIT:
        text_confirm = _format_confirm_income_expense(ud)
        await query.edit_message_text(text_confirm, reply_markup=_keyboard_edit_menu())
        return CONFIRM_EDIT_MENU
    if query.data != CB_CONFIRM_YES:
        return CONFIRM
    return await _save_income_expense_callback(query, context)


async def confirm_edit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Меню «Что изменить?»: сумма / контрагент / назначение / статья ДДС / назад."""
    query = update.callback_query
    await query.answer()
    ud = context.user_data
    if query.data == CB_EDIT_BACK:
        text = _format_confirm_income_expense(ud)
        await query.edit_message_text(text, reply_markup=_keyboard_confirm())
        return CONFIRM
    if query.data == CB_EDIT_AMOUNT:
        ud["_edit_field"] = "amount"
        await query.edit_message_text(_conv_one_window_text(ud, "Введите новую сумму (числом):"))
        return CONFIRM_EDIT_INPUT
    if query.data == CB_EDIT_CT:
        ud["_edit_field"] = "counterparty"
        await query.edit_message_text(_conv_one_window_text(ud, "Введите контрагента (или — для пустого):"))
        return CONFIRM_EDIT_INPUT
    if query.data == CB_EDIT_PURPOSE:
        ud["_edit_field"] = "purpose"
        await query.edit_message_text(_conv_one_window_text(ud, "Введите назначение платежа (или — для пустого):"))
        return CONFIRM_EDIT_INPUT
    if query.data == CB_EDIT_ARTICLE:
        try:
            svc = _get_sheet_service(context)
        except Exception as e:
            await query.edit_message_text(f"Ошибка подключения к таблице: {e}")
            return CONFIRM_EDIT_MENU
        group = "Поступление" if ud.get("type") == "in" else "Выбытие"
        try:
            articles = await asyncio.to_thread(svc.get_articles_by_type_sorted_by_usage, group, True)
        except Exception as e:
            await query.edit_message_text(_format_sheet_error(e))
            return CONFIRM_EDIT_MENU
        if not articles:
            await query.edit_message_text("В таблице нет статей для этого типа. Проверьте лист «ДДС: статьи».")
            return CONFIRM_EDIT_MENU
        ud["_articles"] = articles
        ud["_articles_page"] = 0
        kb = _build_list_kb_with_pagination(
            articles, 0, lambda i, _: CB_EDIT_ARTICLE_PREFIX + str(i),
            CB_EDIT_BACK, CB_CANCEL, CB_EDIT_ARTICLE_PAGE_NEXT, CB_EDIT_ARTICLE_PAGE_PREV,
        )
        await query.edit_message_text(_format_confirm_income_expense(ud) + "\n\nВыберите новую статью:", reply_markup=kb)
        return CONFIRM_EDIT_ARTICLE
    if query.data == CB_EDIT_WALLET:
        try:
            svc = _get_sheet_service(context)
        except Exception as e:
            await query.edit_message_text(f"Ошибка подключения к таблице: {e}")
            return CONFIRM_EDIT_MENU
        try:
            wallets = await asyncio.to_thread(svc.get_wallets)
        except Exception as e:
            await query.edit_message_text(_format_sheet_error(e))
            return CONFIRM_EDIT_MENU
        if not wallets:
            await query.edit_message_text("Список кошельков пуст. Проверьте настройки таблицы.")
            return CONFIRM_EDIT_MENU
        ud["_edit_wallets"] = wallets
        ud["_edit_wallets_page"] = 0
        kb = _build_list_kb_with_pagination(
            wallets, 0, lambda i, _: CB_EDIT_WALLET_PREFIX + str(i),
            CB_EDIT_BACK, CB_CANCEL, CB_EDIT_WALLET_PAGE_NEXT, CB_EDIT_WALLET_PAGE_PREV,
        )
        await query.edit_message_text(_format_confirm_income_expense(ud) + "\n\nВыберите кошелёк:", reply_markup=kb)
        return CONFIRM_EDIT_WALLET
    return CONFIRM_EDIT_MENU


async def confirm_edit_article(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Выбор новой статьи при «Изменить статью ДДС» в пошаговом потоке."""
    query = update.callback_query
    await query.answer()
    ud = context.user_data
    data = query.data
    if data == CB_EDIT_BACK:
        text = _format_confirm_income_expense(ud)
        await query.edit_message_text(text, reply_markup=_keyboard_edit_menu())
        return CONFIRM_EDIT_MENU
    if data == CB_CANCEL:
        await query.edit_message_text("Операция отменена.")
        context.user_data.clear()
        return ConversationHandler.END
    articles = ud.get("_articles", [])
    page = ud.get("_articles_page", 0)
    if data == CB_EDIT_ARTICLE_PAGE_NEXT and (page + 1) * LIST_PAGE_SIZE < len(articles):
        ud["_articles_page"] = page + 1
        page = page + 1
    elif data == CB_EDIT_ARTICLE_PAGE_PREV and page > 0:
        ud["_articles_page"] = page - 1
        page = page - 1
    else:
        if data.startswith(CB_EDIT_ARTICLE_PREFIX):
            try:
                idx = int(data[len(CB_EDIT_ARTICLE_PREFIX):])
            except ValueError:
                return CONFIRM_EDIT_ARTICLE
            if 0 <= idx < len(articles):
                ud["article"] = articles[idx]
                ud.pop("_articles", None)
                ud.pop("_articles_page", None)
                text = _format_confirm_income_expense(ud)
                await query.edit_message_text(text, reply_markup=_keyboard_confirm())
                return CONFIRM
        return CONFIRM_EDIT_ARTICLE
    kb = _build_list_kb_with_pagination(
        articles, page, lambda i, _: CB_EDIT_ARTICLE_PREFIX + str(i),
        CB_EDIT_BACK, CB_CANCEL, CB_EDIT_ARTICLE_PAGE_NEXT, CB_EDIT_ARTICLE_PAGE_PREV,
    )
    await query.edit_message_text(_format_confirm_income_expense(ud) + "\n\nВыберите новую статью:", reply_markup=kb)
    return CONFIRM_EDIT_ARTICLE


async def confirm_edit_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Выбор кошелька при «Изменить кошелёк» в пошаговом потоке."""
    query = update.callback_query
    await query.answer()
    ud = context.user_data
    data = query.data
    if data == CB_EDIT_BACK:
        text = _format_confirm_income_expense(ud)
        await query.edit_message_text(text, reply_markup=_keyboard_edit_menu())
        return CONFIRM_EDIT_MENU
    if data == CB_CANCEL:
        await query.edit_message_text("Операция отменена.")
        context.user_data.clear()
        return ConversationHandler.END
    wallets = ud.get("_edit_wallets", [])
    page = ud.get("_edit_wallets_page", 0)
    if data == CB_EDIT_WALLET_PAGE_NEXT and (page + 1) * LIST_PAGE_SIZE < len(wallets):
        ud["_edit_wallets_page"] = page + 1
        page = page + 1
    elif data == CB_EDIT_WALLET_PAGE_PREV and page > 0:
        ud["_edit_wallets_page"] = page - 1
        page = page - 1
    else:
        if data.startswith(CB_EDIT_WALLET_PREFIX):
            try:
                idx = int(data[len(CB_EDIT_WALLET_PREFIX):])
            except ValueError:
                return CONFIRM_EDIT_WALLET
            if 0 <= idx < len(wallets):
                ud["wallet"] = wallets[idx]
                ud.pop("_edit_wallets", None)
                ud.pop("_edit_wallets_page", None)
                text = _format_confirm_income_expense(ud)
                await query.edit_message_text(text, reply_markup=_keyboard_confirm())
                return CONFIRM
        return CONFIRM_EDIT_WALLET
    kb = _build_list_kb_with_pagination(
        wallets, page, lambda i, _: CB_EDIT_WALLET_PREFIX + str(i),
        CB_EDIT_BACK, CB_CANCEL, CB_EDIT_WALLET_PAGE_NEXT, CB_EDIT_WALLET_PAGE_PREV,
    )
    await query.edit_message_text(_format_confirm_income_expense(ud) + "\n\nВыберите кошелёк:", reply_markup=kb)
    return CONFIRM_EDIT_WALLET


async def confirm_edit_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Ввод нового значения (сумма / контрагент / назначение) после «Изменить»."""
    text = (update.message.text or "").strip()
    ud = context.user_data
    field = ud.pop("_edit_field", None)
    if not field:
        return CONFIRM_EDIT_INPUT
    if field == "amount":
        amount = DDSSheetService.parse_amount(text)
        if amount is not None and amount > 0:
            ud["amount"] = amount
    elif field == "counterparty":
        ud["counterparty"] = text if text != "—" else ""
    elif field == "purpose":
        ud["purpose"] = text if text != "—" else ""
    await _edit_conv_message(
        context,
        _format_confirm_income_expense(ud),
        _keyboard_confirm(),
    )
    return CONFIRM


async def _save_income_expense_callback(query, context: ContextTypes.DEFAULT_TYPE) -> int:
    ud = dict(context.user_data)
    context.user_data.clear()
    try:
        svc = _get_sheet_service(context)
        direction = svc.get_default_business_direction() or (svc.get_business_directions()[0] if svc.get_business_directions() else "")
        amount = ud["amount"]
        if ud["type"] == "out":
            amount = -abs(amount)
        svc.append_operation(
            date_str=ud["date"],
            amount=amount,
            wallet=ud["wallet"],
            business_direction=direction,
            counterparty=ud.get("counterparty", ""),
            purpose=ud.get("purpose", ""),
            article=ud["article"],
        )
        await _send_balance_after(query, context, [ud["wallet"]], prefix="✅ Операция в ДДС внесена.\n\n")
    except Exception as e:
        try:
            await query.edit_message_text(f"Ошибка записи: {e}")
        except Exception:
            try:
                await context.bot.send_message(chat_id=query.message.chat_id, text=f"Ошибка записи: {e}")
            except Exception:
                pass
    return ConversationHandler.END


# --- Перевод ---
async def transfer_from_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    ud = context.user_data
    data = query.data
    if data == CB_CANCEL:
        await query.edit_message_text("Операция отменена.")
        context.user_data.clear()
        return ConversationHandler.END
    wallets = ud.get("_wallets", [])
    if data == CB_TRANSFER_FROM_BACK:
        ud.pop("wallet_from", None)
        text = _conv_one_window_text(ud, "Выберите тип операции:")
        await query.edit_message_text(text, reply_markup=_keyboard_type())
        return TYPE_OP
    if data == CB_TRANSFER_FROM_PAGE_NEXT or data == CB_TRANSFER_FROM_PAGE_PREV:
        wallets = ud.get("_wallets", [])
        page = ud.get("_wallets_page", 0)
        if data == CB_TRANSFER_FROM_PAGE_NEXT and (page + 1) * LIST_PAGE_SIZE < len(wallets):
            page += 1
        elif data == CB_TRANSFER_FROM_PAGE_PREV and page > 0:
            page -= 1
        else:
            return TRANSFER_FROM
        ud["_wallets_page"] = page
        kb = _build_list_kb_with_pagination(
            wallets, page, lambda _, w: CB_WALLET_PREFIX + w,
            CB_TRANSFER_FROM_BACK, CB_CANCEL, CB_TRANSFER_FROM_PAGE_NEXT, CB_TRANSFER_FROM_PAGE_PREV,
        )
        await query.edit_message_text(_conv_one_window_text(ud, "С какого кошелька переводим?"), reply_markup=kb)
        return TRANSFER_FROM
    if not data.startswith(CB_WALLET_PREFIX):
        return TRANSFER_FROM
    wallet_name = data[len(CB_WALLET_PREFIX):]
    if wallet_name not in wallets:
        return TRANSFER_FROM
    ud["wallet_from"] = wallet_name
    try:
        svc = _get_sheet_service(context)
        all_wallets = await asyncio.to_thread(svc.get_wallets)
        wallets_to = [w for w in all_wallets if w != ud["wallet_from"]]
    except Exception as e:
        await query.edit_message_text(_format_sheet_error(e))
        context.user_data.clear()
        return ConversationHandler.END
    ud["_wallets"] = wallets_to
    ud["_wallets_page"] = 0
    kb = _build_list_kb_with_pagination(
        wallets_to, 0, lambda _, w: CB_WALLET_PREFIX + w,
        CB_TRANSFER_TO_BACK, CB_CANCEL, CB_TRANSFER_TO_PAGE_NEXT, CB_TRANSFER_TO_PAGE_PREV,
    )
    await query.edit_message_text(_conv_one_window_text(ud, "На какой кошелёк переводим?"), reply_markup=kb)
    return TRANSFER_TO


async def transfer_to_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    ud = context.user_data
    data = query.data
    if data == CB_CANCEL:
        await query.edit_message_text("Операция отменена.")
        context.user_data.clear()
        return ConversationHandler.END
    if data == CB_TRANSFER_TO_BACK:
        ud.pop("wallet_to", None)
        try:
            svc = _get_sheet_service(context)
            wallets = await asyncio.to_thread(svc.get_wallets)
        except Exception as e:
            await query.edit_message_text(_format_sheet_error(e))
            return TRANSFER_TO
        ud["_wallets"] = wallets
        ud["_wallets_page"] = 0
        kb = _build_list_kb_with_pagination(
            wallets, 0, lambda _, w: CB_WALLET_PREFIX + w,
            CB_TRANSFER_FROM_BACK, CB_CANCEL, CB_TRANSFER_FROM_PAGE_NEXT, CB_TRANSFER_FROM_PAGE_PREV,
        )
        await query.edit_message_text(_conv_one_window_text(ud, "С какого кошелька переводим?"), reply_markup=kb)
        return TRANSFER_FROM
    if data == CB_TRANSFER_TO_PAGE_NEXT or data == CB_TRANSFER_TO_PAGE_PREV:
        wallets = ud.get("_wallets", [])
        page = ud.get("_wallets_page", 0)
        if data == CB_TRANSFER_TO_PAGE_NEXT and (page + 1) * LIST_PAGE_SIZE < len(wallets):
            page += 1
        elif data == CB_TRANSFER_TO_PAGE_PREV and page > 0:
            page -= 1
        else:
            return TRANSFER_TO
        ud["_wallets_page"] = page
        kb = _build_list_kb_with_pagination(
            wallets, page, lambda _, w: CB_WALLET_PREFIX + w,
            CB_TRANSFER_TO_BACK, CB_CANCEL, CB_TRANSFER_TO_PAGE_NEXT, CB_TRANSFER_TO_PAGE_PREV,
        )
        await query.edit_message_text(_conv_one_window_text(ud, "На какой кошелёк переводим?"), reply_markup=kb)
        return TRANSFER_TO
    if not data.startswith(CB_WALLET_PREFIX):
        return TRANSFER_TO
    wallet_name = data[len(CB_WALLET_PREFIX):]
    if wallet_name not in ud.get("_wallets", []):
        return TRANSFER_TO
    ud["wallet_to"] = wallet_name
    await query.edit_message_text(
        _conv_one_window_text(ud, "Введите сумму перевода (положительное число):"),
        reply_markup=_keyboard_back_cancel(),
    )
    return TRANSFER_AMOUNT


async def transfer_amount_entered(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    ud = context.user_data
    amount = DDSSheetService.parse_amount(text)
    if amount is None or amount <= 0:
        await _edit_conv_message(
            context,
            _conv_one_window_text(ud, "Введите положительное число (сумму перевода)."),
            _keyboard_back_cancel(),
        )
        return TRANSFER_AMOUNT
    ud["amount"] = amount
    await _edit_conv_message(
        context,
        _conv_one_window_text(ud, "Введите назначение платежа или нажмите «Пропустить»."),
        _keyboard_skip_back_cancel(),
    )
    return TRANSFER_PURPOSE


async def transfer_amount_back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """🔙 Назад с шага ввода суммы перевода → возврат к выбору кошелька назначения."""
    query = update.callback_query
    await query.answer()
    ud = context.user_data
    all_wallets = ud.get("_wallets", [])
    wallets_to = [w for w in all_wallets if w != ud.get("wallet_from")]
    kb = _build_list_kb_with_pagination(
        wallets_to, 0, lambda _, w: CB_WALLET_PREFIX + w,
        CB_TRANSFER_TO_BACK, CB_CANCEL, CB_TRANSFER_TO_PAGE_NEXT, CB_TRANSFER_TO_PAGE_PREV,
    )
    await query.edit_message_text(_conv_one_window_text(ud, "На какой кошелёк переводим?"), reply_markup=kb)
    return TRANSFER_TO


async def transfer_purpose_back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """🔙 Назад с шага назначения перевода → возврат на ввод суммы перевода."""
    query = update.callback_query
    await query.answer()
    ud = context.user_data
    await _edit_conv_message(
        context,
        _conv_one_window_text(ud, "Введите сумму перевода (положительное число):"),
        _keyboard_back_cancel(),
    )
    return TRANSFER_AMOUNT


def _format_confirm_transfer(ud: dict) -> str:
    """Один блок текста «Подтвердите операцию» для перевода (без дублирования)."""
    lines = [
        "📝 Подтвердите операцию",
        "────────────────────",
        f"📅 Дата: {ud.get('date', '')}",
        "📋 Тип: Перевод",
        f"💳 С: {ud.get('wallet_from', '')}",
        f"💳 В: {ud.get('wallet_to', '')}",
        f"💰 Сумма: {_format_amount(ud.get('amount', 0))} ₽",
        f"📝 Назначение: {ud.get('purpose', '') or '—'}",
    ]
    return "\n".join(lines)


async def transfer_purpose_entered(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    ud = context.user_data
    ud["purpose"] = (update.message.text or "").strip()
    await _edit_conv_message(
        context,
        _format_confirm_transfer(ud),
        _keyboard_confirm(),
    )
    return TRANSFER_CONFIRM


async def transfer_purpose_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    ud = context.user_data
    ud["purpose"] = ""
    text = _format_confirm_transfer(ud)
    await _edit_conv_message(context, text, _keyboard_confirm())
    return TRANSFER_CONFIRM


async def confirm_transfer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка кнопок Подтвердить/Отмена на шаге подтверждения перевода."""
    query = update.callback_query
    await query.answer()
    if query.data == CB_CONFIRM_NO:
        await query.edit_message_text("Операция отменена.")
        context.user_data.clear()
        return ConversationHandler.END
    if query.data != CB_CONFIRM_YES:
        return TRANSFER_CONFIRM
    return await _save_transfer_callback(query, context)


async def _save_transfer_callback(query, context: ContextTypes.DEFAULT_TYPE) -> int:
    ud = dict(context.user_data)
    context.user_data.clear()
    try:
        svc = _get_sheet_service(context)
        direction = svc.get_default_business_direction() or (svc.get_business_directions()[0] if svc.get_business_directions() else "")
        svc.append_transfer(
            date_str=ud["date"],
            amount=ud["amount"],
            wallet_from=ud["wallet_from"],
            wallet_to=ud["wallet_to"],
            purpose=ud.get("purpose", ""),
            business_direction=direction,
        )
        await _send_balance_after(query, context, [ud["wallet_from"], ud["wallet_to"]], prefix="✅ Операция в ДДС внесена.\n\n")
    except Exception as e:
        try:
            await query.edit_message_text(f"Ошибка записи: {e}")
        except Exception:
            pass
    return ConversationHandler.END


def _run_webhook_with_health(app: Application, port: int, webhook_url: str) -> None:
    """Запуск webhook с маршрутом GET / (200 OK) для cron-job.org и POST /webhook для Telegram."""
    from aiohttp import web

    async def health(_request: web.Request) -> web.Response:
        return web.Response(text="OK", status=200)

    async def webhook_handler(request: web.Request) -> web.Response:
        if request.method != "POST":
            return web.Response(status=405)
        try:
            data = await request.json()
            update = Update.de_json(data, app.bot)
            await app.update_queue.put(update)
        except Exception:
            pass
        return web.Response(status=200)

    async def run() -> None:
        await app.initialize()
        if callable(getattr(app, "post_init", None)):
            await app.post_init()
        await app.bot.set_webhook(url=webhook_url, allowed_updates=Update.ALL_TYPES)
        await app.start()
        app_web = web.Application()
        app_web.router.add_get("/", health)
        app_web.router.add_get("/health", health)
        app_web.router.add_post("/webhook", webhook_handler)
        runner = web.AppRunner(app_web)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            await runner.cleanup()
            await app.stop()
            await app.shutdown()

    asyncio.run(run())


def main() -> None:
    # На Python 3.10+ в MainThread может не быть event loop — PTB падает без этого
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        raise RuntimeError(
            f"TELEGRAM_BOT_TOKEN не задан. Проверьте файл .env в папке с bot.py.\n"
            f"Ожидаемый путь: {_load_env_path}\n"
            f"Текущая рабочая директория: {os.getcwd()}"
        )
    # Проверка на плейсхолдер (реальный токен — цифры, двоеточие, латиница)
    if "ТВОЙ_ТОКЕН" in token or "BOTFATHER" in token or "your_bot_token" in token:
        raise RuntimeError(
            f"В .env указан плейсхолдер вместо реального токена. Замените на токен из @BotFather и сохраните файл .env (Cmd+S).\n"
            f"Файл: {_load_env_path}"
        )

    allowed_ids = _parse_allowed_user_ids()
    if allowed_ids:
        print(f"[Бот] Ограничение доступа: включено, разрешённых ID: {len(allowed_ids)}", file=sys.stderr)
    else:
        print("[Бот] Ограничение доступа: выключено (TELEGRAM_ALLOWED_IDS не задан)", file=sys.stderr)

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", start_step),
            CommandHandler("step", start_step),
            CallbackQueryHandler(add_operation_from_button, pattern=f"^{re.escape(CB_ADD_OPERATION)}$"),
        ],
        states={
            DATE: [
                CallbackQueryHandler(date_today, pattern=f"^{re.escape(CB_TODAY)}$"),
                CallbackQueryHandler(date_preset, pattern=f"^{re.escape(CB_DATE_PREFIX)}"),
                CallbackQueryHandler(cancel_cmd, pattern=f"^{re.escape(CB_CANCEL)}$"),
                # Текст в формате операции (Поступление/Перевод...) не перехватываем — пусть обработает handle_form
                MessageHandler(filters.TEXT & ~filters.COMMAND & ~one_window_filter, date_text),
            ],
            TYPE_OP: [CallbackQueryHandler(type_selected)],
            ARTICLE: [
                CallbackQueryHandler(
                    article_selected,
                    pattern=f"^({re.escape(CB_ARTICLE_BACK)}|{re.escape(CB_CANCEL)}|{re.escape(CB_ARTICLE_PAGE_NEXT)}|{re.escape(CB_ARTICLE_PAGE_PREV)}|{re.escape(CB_ARTICLE_PREFIX)}[0-9]+)$",
                ),
            ],
            WALLET: [CallbackQueryHandler(wallet_selected)],
            AMOUNT: [
                CallbackQueryHandler(cancel_callback, pattern=f"^{re.escape(CB_CANCEL)}$"),
                CallbackQueryHandler(amount_back, pattern=f"^{re.escape(CB_BACK)}$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND & ~one_window_filter, amount_entered),
            ],
            COUNTERPARTY: [
                CallbackQueryHandler(cancel_callback, pattern=f"^{re.escape(CB_CANCEL)}$"),
                CallbackQueryHandler(counterparty_back, pattern=f"^{re.escape(CB_BACK)}$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND & ~one_window_filter, counterparty_entered),
                CallbackQueryHandler(counterparty_skip, pattern=f"^{re.escape(CB_SKIP)}$"),
            ],
            PURPOSE: [
                CallbackQueryHandler(cancel_callback, pattern=f"^{re.escape(CB_CANCEL)}$"),
                CallbackQueryHandler(purpose_back, pattern=f"^{re.escape(CB_BACK)}$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND & ~one_window_filter, purpose_entered),
                CallbackQueryHandler(purpose_skip, pattern=f"^{re.escape(CB_SKIP)}$"),
            ],
            CONFIRM: [
                CallbackQueryHandler(confirm_income_expense, pattern=f"^({re.escape(CB_CONFIRM_YES)}|{re.escape(CB_CONFIRM_NO)}|{re.escape(CB_EDIT)})$"),
            ],
            CONFIRM_EDIT_MENU: [
                CallbackQueryHandler(confirm_edit_menu, pattern=f"^({re.escape(CB_EDIT_AMOUNT)}|{re.escape(CB_EDIT_CT)}|{re.escape(CB_EDIT_PURPOSE)}|{re.escape(CB_EDIT_ARTICLE)}|{re.escape(CB_EDIT_WALLET)}|{re.escape(CB_EDIT_BACK)})$"),
            ],
            CONFIRM_EDIT_ARTICLE: [
                CallbackQueryHandler(
                    confirm_edit_article,
                    pattern=f"^({re.escape(CB_EDIT_BACK)}|{re.escape(CB_CANCEL)}|{re.escape(CB_EDIT_ARTICLE_PAGE_NEXT)}|{re.escape(CB_EDIT_ARTICLE_PAGE_PREV)}|{re.escape(CB_EDIT_ARTICLE_PREFIX)}[0-9]+)$",
                ),
            ],
            CONFIRM_EDIT_WALLET: [
                CallbackQueryHandler(
                    confirm_edit_wallet,
                    pattern=f"^({re.escape(CB_EDIT_BACK)}|{re.escape(CB_CANCEL)}|{re.escape(CB_EDIT_WALLET_PAGE_NEXT)}|{re.escape(CB_EDIT_WALLET_PAGE_PREV)}|{re.escape(CB_EDIT_WALLET_PREFIX)}[0-9]+)$",
                ),
            ],
            CONFIRM_EDIT_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & ~one_window_filter, confirm_edit_input),
            ],
            TRANSFER_FROM: [CallbackQueryHandler(transfer_from_selected)],
            TRANSFER_TO: [CallbackQueryHandler(transfer_to_selected)],
            TRANSFER_AMOUNT: [
                CallbackQueryHandler(cancel_callback, pattern=f"^{re.escape(CB_CANCEL)}$"),
                CallbackQueryHandler(transfer_amount_back, pattern=f"^{re.escape(CB_BACK)}$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, transfer_amount_entered),
            ],
            TRANSFER_PURPOSE: [
                CallbackQueryHandler(cancel_callback, pattern=f"^{re.escape(CB_CANCEL)}$"),
                CallbackQueryHandler(transfer_purpose_back, pattern=f"^{re.escape(CB_BACK)}$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, transfer_purpose_entered),
                CallbackQueryHandler(transfer_purpose_skip, pattern=f"^{re.escape(CB_SKIP)}$"),
            ],
            TRANSFER_CONFIRM: [
                CallbackQueryHandler(confirm_transfer, pattern=f"^({re.escape(CB_CONFIRM_YES)}|{re.escape(CB_CONFIRM_NO)})$"),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_cmd),
            CommandHandler("start", start_step),
            CommandHandler("step", start_step),
        ],
    )

    # Увеличенные таймауты: при медленном Google Sheets ответ бота не должен обрываться (TimedOut)
    webhook_base = os.getenv("WEBHOOK_BASE_URL", "").rstrip("/")
    builder = (
        Application.builder()
        .token(token)
        .connect_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .get_updates_connect_timeout(30.0)
        .get_updates_read_timeout(30.0)
        .get_updates_write_timeout(30.0)
    )
    if webhook_base:
        builder = builder.updater(None)  # свой сервер с GET / для cron и POST /webhook
    app = builder.build()
    # Ограничение доступа: если задан TELEGRAM_ALLOWED_IDS, только эти пользователи могут пользоваться ботом
    if allowed_ids:
        app.add_handler(
            _BlockedUserHandler(allowed_ids, _deny_access_handler),
            group=-1,
        )
    app.add_handler(CommandHandler("balance", balance_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("funds", funds_cmd))
    app.add_handler(CommandHandler("settings", settings_cmd))
    app.add_handler(CommandHandler("text", text_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    # Настройки: Отчисления в фонды, Добавить кошелёк
    app.add_handler(CallbackQueryHandler(handle_settings_callback, pattern="^(settings|sf_)"))
    # Отчёт /stats
    app.add_handler(CallbackQueryHandler(stats_callback, pattern="^stats_"))
    # Кнопка «Показать баланс» после внесения операции (показ в том же окне)
    app.add_handler(CallbackQueryHandler(show_balance_button_callback, pattern=f"^{re.escape(CB_SHOW_BALANCE)}$"))
    # Кнопка «🔙 Назад» в окне баланса (вернуться к «Операция внесена»)
    app.add_handler(CallbackQueryHandler(balance_back_callback, pattern=f"^{re.escape(CB_BALANCE_BACK)}$"))
    # Кнопка «Рассчитать фонды» после внесения операции
    app.add_handler(CallbackQueryHandler(funds_button_callback, pattern=f"^{re.escape(CB_RUN_FUNDS)}$"))
    # Кнопки после текстового ввода (выбор статьи, подтверждение)
    app.add_handler(CallbackQueryHandler(handle_text_form_callback, pattern="^text_"))
    # Текстовый ввод операций — регистрируем ПЕРЕД ConversationHandler, иначе пошаговый сценарий перехватывает сообщения
    class TextFormHandler(MessageHandler):
        def check_update(self, update):
            if not super().check_update(update):
                return False
            return _text_form_should_handle(update)

    app.add_handler(TextFormHandler(filters.TEXT & ~filters.COMMAND, handle_form))
    # Пошаговый ввод (дата → тип → статья → ...) — после текстовой формы
    app.add_handler(conv)
    # Fallback: если нажали «Подтвердить», но диалог не обработал (сессия потеряна) — хотя бы снять загрузку и ответить
    app.add_handler(CallbackQueryHandler(
        lambda u, c: _confirm_fallback(u, c),
        pattern=f"^({re.escape(CB_CONFIRM_YES)}|{re.escape(CB_TEXT_CONFIRM_YES)})$",
    ))
    app.add_error_handler(_global_error_handler)

    if webhook_base:
        port = int(os.environ.get("PORT", "8443"))
        webhook_url = f"{webhook_base}/webhook"
        print(f"[Бот] Режим webhook: {webhook_url}, порт {port}, GET / для cron", file=sys.stderr)
        _run_webhook_with_health(app, port, webhook_url)
    else:
        app.run_polling(allowed_updates=Update.ALL_TYPES)


async def _confirm_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Если нажали «Подтвердить», но ни текстовый поток, ни ConversationHandler не обработали — снять загрузку и подсказать."""
    query = update.callback_query
    try:
        await query.answer()
        await query.edit_message_text(
            "Сессия истекла или операция уже обработана. Начните заново: /start или введите операцию текстом (например: 5000 Сбербанк)."
        )
    except Exception:
        try:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="Сессия истекла. Начните заново: /start или введите операцию текстом.",
            )
        except Exception:
            pass


async def _cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("Операция отменена.")
    else:
        await update.message.reply_text("Операция отменена.")
    context.user_data.clear()
    return ConversationHandler.END


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await _cancel(update, context)


async def cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка нажатия кнопки «Отмена ❌» (callback) в любом шаге диалога."""
    return await _cancel(update, context)


if __name__ == "__main__":
    main()
