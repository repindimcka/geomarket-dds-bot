# Развёртывание бота: GitHub + Render.com

Пошаговая инструкция, чтобы бот работал 24/7 без вашего компьютера.

---

## Часть 1. Выложить код на GitHub

### 1.1. Установить Git (если ещё нет)

- **macOS:** откройте «Терминал» и выполните:  
  `xcode-select --install`  
  или установите Git с [git-scm.com](https://git-scm.com).
- Проверка: в терминале введите `git --version` — должна появиться версия.

### 1.2. Зарегистрироваться на GitHub

1. Зайдите на [github.com](https://github.com).
2. Нажмите **Sign up** и создайте аккаунт (логин, email, пароль).

### 1.3. Создать новый репозиторий на GitHub

1. Войдите в GitHub → справа вверху нажмите **+** → **New repository**.
2. **Repository name:** например `dds-telegram-bot`.
3. **Public**.
4. **НЕ** ставьте галочки «Add a README» / «Add .gitignore» — репозиторий создайте пустым.
5. Нажмите **Create repository**.

На следующей странице GitHub покажет команды — они понадобятся в шаге 1.5.

### 1.4. Открыть папку проекта в терминале

В Cursor откройте встроенный терминал (**Terminal → New Terminal**) или системный «Терминал» и перейдите в папку бота:

```bash
cd /Users/timofejmitrofanov/dds-telegram-bot
```

### 1.5. Инициализировать Git и отправить код на GitHub

Выполните по очереди (подставьте вместо `ВАШ_ЛОГИН` свой логин GitHub из шага 1.2):

```bash
git init
git add .
git status
```

В `git status` не должно быть файлов `.env` и `credentials.json` — они в `.gitignore`, в репозиторий не попадут (так и нужно).

Дальше:

```bash
git commit -m "Первый коммит: бот ДДС на Python"
git branch -M main
git remote add origin https://github.com/ВАШ_ЛОГИН/dds-telegram-bot.git
git push -u origin main
```

При первом `git push` браузер или терминал попросят войти в GitHub (логин и пароль или токен). Если просят **токен**: GitHub → Settings → Developer settings → Personal access tokens → создать токен с правом `repo` и вставить его вместо пароля.

После успешного `push` код будет на GitHub по адресу:  
`https://github.com/ВАШ_ЛОГИН/dds-telegram-bot`.

---

## Часть 2. Развернуть бота на Render.com (бесплатно)

На Render **бесплатный тариф есть только у Web Service**, а не у Background Worker. Поэтому бот настроен на режим **webhook**: Telegram сам отправляет обновления на ваш сервис. Создаём **Web Service** — так можно использовать бесплатный план.

### 2.1. Регистрация на Render

1. Зайдите на [render.com](https://render.com).
2. Нажмите **Get Started for Free**.
3. Войдите через **GitHub** или через Google (в последнем случае в настройках аккаунта Render подключите GitHub: Account Settings → Connected accounts → GitHub).

### 2.2. Создать Web Service (не Background Worker)

1. В личном кабинете Render нажмите **New +** → **Web Service**.
2. В списке репозиториев выберите **dds-telegram-bot** (если его нет — **Configure account** и дайте доступ к репо).
3. Заполните:
   - **Name:** например `dds-bot` (от этого имени будет URL: `https://dds-bot.onrender.com`).
   - **Region:** выберите ближайший (например Frankfurt).
   - **Branch:** `main`.
   - **Runtime:** Python 3.
   - **Build Command:**  
     `pip install -r requirements.txt`
   - **Start Command:**  
     `python run_on_render.py`
4. В блоке **Instance Type** выберите **Free** (бесплатный вариант).

### 2.3. Переменные окружения (Environment)

В том же сервисе откройте вкладку **Environment** и добавьте переменные:

| Key | Value |
|-----|--------|
| `TELEGRAM_BOT_TOKEN` | Токен бота от @BotFather |
| `GOOGLE_SHEET_ID` | ID вашей Google-таблицы |
| `GOOGLE_CREDENTIALS_PATH` | `credentials.json` |
| `CREDENTIALS_JSON` | **Весь текст** из файла `credentials.json` (от первой `{` до последней `}`). |
| `WEBHOOK_BASE_URL` | URL вашего сервиса: `https://dds-bot.onrender.com` (подставьте **точный** URL из шапки сервиса Render, например `https://dds-telegram-bot-38zf.onrender.com`). |
| `PYTHON_VERSION` | `3.12.7` (обязательно для совместимости с python-telegram-bot на Render). |

Опционально:

- `TELEGRAM_ALLOWED_IDS` — ваш Telegram user id или несколько через запятую, если нужно ограничить доступ.

**Важно:** `WEBHOOK_BASE_URL` должен совпадать с тем, что Render показывает как URL сервиса (после создания его видно в шапке сервиса). Без этой переменной бот не перейдёт в режим webhook и на Render не заработает. `PYTHON_VERSION=3.12.7` нужен, чтобы Render не использовал Python 3.14 (с ним бывают ошибки asyncio).

### 2.4. Сохранить и развернуть

1. Нажмите **Create Web Service**.
2. Render соберёт проект и запустит бота. В **Logs** должно появиться что-то вроде: `[Бот] Режим webhook: https://dds-bot.onrender.com/webhook ...`.
3. Напишите боту в Telegram (например /start). Если сервис только что «проснулся», первый ответ может прийти с задержкой 30–60 секунд — это нормально для бесплатного плана.

Если в логах ошибка про `credentials.json` или `CREDENTIALS_JSON` — проверьте, что переменная содержит полный JSON (начинается с `{`, заканчивается на `}`).

### 2.5. Про бесплатный план

- **Web Service** на бесплатном тарифе «засыпает» после примерно 15 минут без запросов.
- Когда кто-то пишет боту, Telegram отправляет запрос на ваш URL → Render будит сервис. Первый ответ после «сна» может быть с задержкой (cold start).
- Для одного бота этого обычно достаточно. Если нужна работа без задержек 24/7 — смотрите платные тарифы Render или другой хостинг.

---

## Краткий чеклист

- [ ] На Render создан **Web Service** (не Background Worker), репозиторий `dds-telegram-bot`.
- [ ] Instance Type: **Free**.
- [ ] Build: `pip install -r requirements.txt`, Start: `python run_on_render.py`.
- [ ] В Environment заданы: `TELEGRAM_BOT_TOKEN`, `GOOGLE_SHEET_ID`, `GOOGLE_CREDENTIALS_PATH=credentials.json`, `CREDENTIALS_JSON`, **`WEBHOOK_BASE_URL`** (например `https://dds-bot.onrender.com`).
- [ ] В Logs есть строка про «Режим webhook», бот в Telegram отвечает на /start.

Если на каком-то шаге что-то не получается — напишите, на каком шаге и что именно видите (сообщение об ошибке или скрин).
