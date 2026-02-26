import os
import asyncio
from dotenv import load_dotenv
load_dotenv()
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
import psycopg2
from telegram.constants import ParseMode 
import random 
from datetime import datetime
from telegram import ReplyKeyboardMarkup, KeyboardButton, WebAppInfo
from telegram.ext import MessageHandler, filters
import json
from datetime import datetime, timedelta
import os
print("MY_DATABASE_URL:", os.getenv("MY_DATABASE_URL"))

# --- КОНФИГУРАЦИЯ ---
BOT_TOKEN = os.getenv("BOT_TOKEN") 
SPONSORS = ["@sponsor1", "@sponsor2", "@sponsor3"]
PRIZE = "Telegram Premium на 6 месяцев или 1000 ⭐"
ADMINS = [514167463]  
BOT_USERNAME_FOR_REFLINK = "moy_giveaway_bot" 

# ГЛОБАЛЬНАЯ ПЕРЕМЕННАЯ (Статус работы бота)
IS_ACTIVE = True 

# --- Подключение к БД ---
def get_db_connection():
    DATABASE_URL = os.getenv("MY_DATABASE_URL")
    if not DATABASE_URL:
        return psycopg2.connect("postgresql://bot_user:12345@localhost/bot_db")
    return psycopg2.connect(DATABASE_URL)

def init_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Таблица пользователей
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        username TEXT,
                        ref_count INTEGER DEFAULT 0,
                        tickets INTEGER DEFAULT 0,
                        all_subscribed INTEGER DEFAULT 0,
                        last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                # Таблица рефералов
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS referrals (
                        referrer_id BIGINT,
                        referred_id BIGINT,
                        UNIQUE(referrer_id, referred_id)
                    )
                ''')
                # Таблица победителей
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS winners (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        username TEXT,
                        prize TEXT,
                        win_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                # НОВАЯ: Таблица подписок на каналы
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS channel_subscriptions (
                        user_id BIGINT,
                        channel_id TEXT,
                        subscribed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (user_id, channel_id)
                    )
                ''')
                conn.commit()
        print("✅ База данных подключена.")
    except Exception as e:
        print(f"❌ Ошибка БД: {e}")

# --- Вспомогательные функции ---
def mask_username(username: str) -> str:
    # 1. Если пользователь зарегистрировался без юзернейма
    if not username: 
        return "Без ника"
    
    # 2. Очищаем от знака @, если он случайно записался в базу
    username = username.lstrip('@')
    
    # 3. Если ник короткий (например, 3 буквы: "bot")
    if len(username) <= 3:
        return f"@{username[:1]}***" # Получится @b***
        
    # 4. Для нормальных ников (оставляем 2 первые и 1 последнюю)
    # Например: "alexander" -> "al" + "***" + "r" -> @al***r
    return f"@{username[:2]}***{username[-1]}"

def get_fortune_shortcut():
    return ReplyKeyboardMarkup(
        [[KeyboardButton(
            "🎰 Колесо фортуны",
            web_app=WebAppInfo(url="https://moygiveawaybot.ru/index.html")
        )]],
        resize_keyboard=True
    )

async def check_subscription(user_id, channel, context):
    try:
        member = await context.bot.get_chat_member(chat_id=channel, user_id=user_id)
        return member.status in ["member", "administrator", "creator"]
    except:
        return False

# --- ВАЖНО: ФУНКЦИЯ СИНХРОНИЗАЦИИ БИЛЕТОВ ---
# Она не просто считает, но и ЗАПИСЫВАЕТ результат в БД, чтобы Лидерборд видел актуальное число
def sync_tickets(user_id):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 1. Получаем текущее кол-во рефералов и статус подписки
                cur.execute("SELECT ref_count, all_subscribed FROM users WHERE user_id = %s", (user_id,))
                res = cur.fetchone()
                if not res: return 0
                
                ref_count, is_subscribed = res
                
                # 2. Логика подсчета:
                # Если не подписан -> билетов 0 (временно заморожены)
                # Если подписан -> 1 друг = 1 билет (макс 10)
                if is_subscribed == 1:
                    actual_tickets = min(10, ref_count)
                else:
                    actual_tickets = 0 

                # 3. ОБНОВЛЯЕМ БД (чтобы лидерборд видел это число)
                cur.execute("UPDATE users SET tickets = %s WHERE user_id = %s", (actual_tickets, user_id))
                conn.commit()
                
                return actual_tickets
    except Exception as e:
        print(f"Ошибка синхронизации билетов: {e}")
        return 0

# --- ГЕНЕРАЦИЯ ГЛАВНОГО МЕНЮ ---
async def get_start_text(user_id, first_name, context):
    channels_list = ""
    all_subs_ok = True
    
    # Проверка подписок с галочками и сохранением в БД
    for i, ch in enumerate(SPONSORS, 1):
        is_sub = await check_subscription(user_id, ch, context)
        if not is_sub:
            all_subs_ok = False
            icon = "❌"
        else:
            icon = "✅"
            # 🔥 Сохраняем факт подписки на этот канал
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO channel_subscriptions (user_id, channel_id)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                        """, (user_id, ch))
                        conn.commit()
            except Exception as e:
                print(f"Ошибка сохранения подписки на {ch}: {e}")
        channels_list += f"{i}. {ch} {icon}\n"

    # Обновляем общий статус подписки в users
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET all_subscribed = %s WHERE user_id = %s", (1 if all_subs_ok else 0, user_id))
                conn.commit()
    except: pass
    
    # Синхронизируем билеты
    sync_tickets(user_id)

    msg = (
        f"👋 <b>Привет, {first_name}!</b>\n\n"
        f"Добро пожаловать в наш регулярный Telegram Giveaway!\n\n"
        f"🎁 <b>Приз недели:</b>\n"
        f"{PRIZE} на твой счёт!\n\n"
        f"Как участвовать?\n"
        f"-----------------------------\n"
        f"1️⃣ <b>Подпишись на все каналы спонсоров:</b>\n"
        f"{channels_list}"
        f"2️⃣ <b>Пригласи минимум 1 друга</b> по своей реферальной ссылке (получишь её ниже)\n"
        f"3️⃣ <b>За каждого нового друга</b> — ещё <b>+1 билет</b> на розыгрыш (макс. 10)\n\n"
        f"⏳ <b>Новый розыгрыш — каждую неделю!</b>\n\n"
        f"❗️ <i>Чем больше рефералов — тем больше шансов на победу!</i>"
    )
    return msg

# --- START ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not IS_ACTIVE:
        pause_text = (
            "🏁 <b>РОЗЫГРЫШ ЗАВЕРШЕН!</b>\n\n"
            "Прямо сейчас мы подводим итоги и готовим новый сезон.\n"
            "Список каналов временно скрыт.\n\n"
            "🔔 <i>Ожидайте уведомления о старте нового конкурса!</i>"
        )
        await update.message.reply_text(pause_text, parse_mode=ParseMode.HTML)
        return

    user = update.effective_user
    uid = user.id
    name = user.first_name
    
    # Регистрация пользователя в БД
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (user_id, username) VALUES (%s, %s) ON CONFLICT DO NOTHING", 
                    (uid, name)
                )
                conn.commit()
    except Exception as e:
        print(f"Ошибка регистрации: {e}")

    # 🔗 Рефералка (ОБЯЗАТЕЛЬНО внутри функции!)
    if context.args:
        ref_str = context.args[0]
        if ref_str.isdigit() and int(ref_str) != uid:
            referrer = int(ref_str)
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO referrals (referrer_id, referred_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", 
                            (referrer, uid)
                        )
                        if cur.rowcount > 0:
                            cur.execute(
                                "UPDATE users SET ref_count = ref_count + 1 WHERE user_id = %s", 
                                (referrer,)
                            )
                            conn.commit()
            except Exception as e:
                print(f"Ошибка рефералки: {e}")

    # 🎡 Кнопка мини-приложения
    await update.message.reply_text(
        "Открой мини-приложение 'Колесо фортуны' кнопкой ниже:",
        reply_markup=ReplyKeyboardMarkup([
            [KeyboardButton("Колесо фортуны", web_app=WebAppInfo(url="https://moygiveawaybot.ru/index.html"))]
        ], resize_keyboard=True)
    )

    # 📋 Основное меню
    text = await get_start_text(uid, name, context)
    kb = [
        [InlineKeyboardButton("🔄 Проверить подписку", callback_data="check_sub")],
        [InlineKeyboardButton("🔗 Моя реферальная ссылка", callback_data="my_reflink")],
        [InlineKeyboardButton("🎫 Мои билеты", callback_data="my_tickets")],
        [
            InlineKeyboardButton("🏆 Лидерборд", callback_data="leaderboard"), 
            InlineKeyboardButton("🏅 Победители", callback_data="winners_list")
        ]
    ]
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
    
# --- КНОПКИ ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    
    if not IS_ACTIVE:
        await query.answer()
        await query.edit_message_text("🏁 Розыгрыш завершен. Идет подготовка нового этапа.", parse_mode=ParseMode.HTML)
        return

    uid = query.from_user.id
    data = query.data

    if data == "check_sub" or data == "back_to_main":
        await query.answer("Обновляю...")
        text = await get_start_text(uid, query.from_user.first_name, context)
        kb = [
        [InlineKeyboardButton("🔄 Проверить подписку", callback_data="check_sub")],
        [InlineKeyboardButton("🔗 Моя реферальная ссылка", callback_data="my_reflink")],
        [InlineKeyboardButton("🎫 Мои билеты", callback_data="my_tickets")],
        [
            InlineKeyboardButton("🏆 Лидерборд", callback_data="leaderboard"), 
            InlineKeyboardButton("🏅 Победители", callback_data="winners_list")
        ]
    ]
        try:
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
        except: pass

    elif data == "my_tickets":
        await query.answer()
        # 1. Сначала обновляем статус подписки (важно для подсчета)
        await get_start_text(uid, query.from_user.first_name, context)
        
        # 2. Теперь синхронизируем и получаем актуальное число
        tickets = sync_tickets(uid)
        
        # 3. Проверяем, подписан ли (для текста сообщения)
        is_sub = False
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT all_subscribed FROM users WHERE user_id = %s", (uid,))
                    res = cur.fetchone()
                    if res and res[0] == 1: is_sub = True
        except: pass

        if not is_sub:
            text = "⚠️ <b>Вы не подписаны на спонсоров!</b>\n\nВаши билеты временно заморожены (0).\nНажмите «Назад» и подпишитесь на каналы с ❌."
        else:
            text = f"🎫 <b>Ваши билеты: {tickets}</b>\n(Максимум 10, нужен минимум 1 друг)"

        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

    elif data == "my_reflink":
        await query.answer()
        link = f"https://t.me/{BOT_USERNAME_FOR_REFLINK}?start={uid}"
        text = f"🔗 <b>Ваша ссылка для приглашения:</b>\n\n<code>{link}</code>\n\nОтправляйте её друзьям!"
        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

    elif data == "leaderboard":
        await query.answer()
        # Синхронизируем текущего юзера перед показом
        sync_tickets(uid)
        
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT username, tickets FROM users WHERE tickets > 0 ORDER BY tickets DESC LIMIT 10")
                    rows = cur.fetchall()
            if not rows: res = "Пока пусто."
            else:
                res = "🏆 <b>ТОП-10 ПО БИЛЕТАМ:</b>\n\n"
                for i, r in enumerate(rows, 1):
                    res += f"{i}. {mask_username(r[0])} — {r[1]} 🎫\n"
        except: res = "Ошибка."
        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(res, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

    elif data == "winners_list":
        await query.answer()
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT username, win_date FROM winners ORDER BY win_date DESC LIMIT 15")
                    rows = cur.fetchall()
            
            if not rows:
                res = "📜 Список победителей пока пуст."
            else:
                res = "🏅 <b>ПОСЛЕДНИЕ 15 ПОБЕДИТЕЛЕЙ:</b>\n\n"
                for i, r in enumerate(rows, 1):
                    safe_name = mask_username(r[0])
                    date_str = r[1].strftime("%d.%m.%Y")
                    res += f"{i}. <b>{safe_name}</b> ({date_str})\n"
        except: res = "Ошибка."
        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(res, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)


# --- АДМИНКА ---

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    if not context.args:
        await update.message.reply_text("Введите текст.")
        return
    msg = " ".join(context.args)
    await update.message.reply_text("⏳ Рассылка...")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id FROM users")
                users = cur.fetchall()
        count = 0
        for u in users:
            try:
                await context.bot.send_message(u[0], msg)
                count += 1
                await asyncio.sleep(0.05)
            except: pass
        await update.message.reply_text(f"✅ Доставлено: {count}")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def fortune(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = [
        [KeyboardButton(
            "🎰 Колесо фортуны",
            web_app=WebAppInfo(url="https://moygiveawaybot.ru/index.html")
        )],
        [KeyboardButton("🔙 Назад")]
    ]
    markup = ReplyKeyboardMarkup(kb, resize_keyboard=True)
    await update.message.reply_text(
        "Жми на кнопку ниже и лови призы!",
        reply_markup=markup
    )

async def draw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        return

    try:
        # === СТАТИСТИКА ПО КАНАЛАМ ===
        stats_text = "📊 <b>Статистика по каналам-спонсорам:</b>\n\n"

        # Получаем общее число рефералов от активных участников
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(DISTINCT r.referred_id)
                    FROM referrals r
                    JOIN users u ON r.referrer_id = u.user_id
                    WHERE u.tickets > 0
                """)
                total_referrals = cur.fetchone()[0] or 0

        total_subscribed_to_any = set()

        for channel in SPONSORS:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Сколько рефералов подписались на ЭТОТ канал
                    cur.execute("""
                        SELECT COUNT(DISTINCT r.referred_id)
                        FROM referrals r
                        JOIN users u ON r.referrer_id = u.user_id
                        JOIN channel_subscriptions cs ON r.referred_id = cs.user_id
                        WHERE u.tickets > 0 AND cs.channel_id = %s
                    """, (channel,))
                    subscribed_count = cur.fetchone()[0] or 0

                    # Собираем всех, кто подписался хотя бы на один канал (для общего числа)
                    cur.execute("""
                        SELECT DISTINCT r.referred_id
                        FROM referrals r
                        JOIN users u ON r.referrer_id = u.user_id
                        JOIN channel_subscriptions cs ON r.referred_id = cs.user_id
                        WHERE u.tickets > 0
                    """)
                    all_subscribed = {row[0] for row in cur.fetchall()}
                    total_subscribed_to_any = all_subscribed

            stats_text += (
                f"🔹 <b>{channel}</b>\n"
                f"   ➤ Перешли (рефералы): {total_referrals}\n"
                f"   ➤ Подписались на канал: {subscribed_count}\n\n"
            )

        stats_text += f"✅ <b>Всего рефералов, подписавшихся хотя бы на 1 канал:</b> {len(total_subscribed_to_any)}\n"
        await update.message.reply_text(stats_text, parse_mode=ParseMode.HTML)

        # === ВЫБОР 2 ПОБЕДИТЕЛЕЙ ===
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id, username, tickets FROM users WHERE tickets > 0 AND all_subscribed = 1")
                rows = cur.fetchall()

        if len(rows) < 2:
            await update.message.reply_text(
                f"❌ Недостаточно участников для выбора 2 победителей (нужно минимум 2, сейчас: {len(rows)})."
            )
            return

        # Создаём "лотерею": каждый билет = 1 шанс
        pool = []
        for r in rows:
            pool.extend([r] * r[2])

        if len(pool) < 2:
            await update.message.reply_text("❌ Недостаточно билетов для двух победителей.")
            return

        # Выбираем двух уникальных победителей
        winner1 = random.choice(pool)
        pool_without_winner1 = [p for p in pool if p[0] != winner1[0]]
        if not pool_without_winner1:
            await update.message.reply_text("⚠️ Все билеты у одного участника. Второй победитель невозможен.")
            return
        winner2 = random.choice(pool_without_winner1)

        winners = [winner1, winner2]

        # Сохраняем в БД
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    for wid, wname, wtickets in winners:
                        cur.execute("INSERT INTO winners (user_id, username, prize) VALUES (%s, %s, %s)", 
                                   (wid, wname, PRIZE))
                    conn.commit()
        except Exception as e:
            print(f"Ошибка сохранения победителей: {e}")

        # Отправляем админу
        result_msg = "🎉 <b>ПОБЕДИТЕЛИ РОЗЫГРЫША:</b>\n\n"
        for i, (wid, wname, wtickets) in enumerate(winners, 1):
            result_msg += f"{i}. @{wname or 'Нет ника'} (ID: <code>{wid}</code>) — {wtickets} 🎫\n"

        await update.message.reply_text(result_msg, parse_mode=ParseMode.HTML)

        # Отправляем ЛС победителям
        win_msg = (
            f"🎉 <b>ПОЗДРАВЛЯЕМ! ВЫ ВЫИГРАЛИ!</b> 🎁\n\n"
            f"В розыгрыше приза: <b>{PRIZE}</b>\n"
            f"Удача улыбнулась именно вам! 🥳\n\n"
            f"❗️ <b>ЧТО ДЕЛАТЬ ДАЛЬШЕ?</b>\n"
            f"Свяжитесь с администратором для получения приза.\n"
            f"👉 <b>Написать:</b> @moderatorgive_bot\n\n"
            f"⏳ <b>Важно:</b> У вас есть ровно <b>48 часов</b>.\n"
            f"<i>По истечении этого срока приз аннулируется!</i>"
        )

        success_count = 0
        for wid, _, _ in winners:
            try:
                await context.bot.send_message(wid, win_msg, parse_mode=ParseMode.HTML)
                success_count += 1
            except:
                pass

        await update.message.reply_text(f"✅ ЛС отправлено {success_count} из 2 победителям.")

    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка в /draw: {e}")
        import traceback
        print(traceback.format_exc())

async def stop_giveaway(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    global IS_ACTIVE
    IS_ACTIVE = False
    await update.message.reply_text("⛔️ <b>ПАУЗА</b>", parse_mode=ParseMode.HTML)

async def resume_giveaway(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    global IS_ACTIVE
    IS_ACTIVE = True
    await update.message.reply_text("▶️ <b>СТАРТ</b>", parse_mode=ParseMode.HTML)

async def reset_season(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET tickets = 0, ref_count = 0")
                conn.commit()
        await update.message.reply_text("✅ <b>Сезон сброшен!</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

# --- НОВАЯ ФУНКЦИЯ СТАТИСТИКИ ---
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 1. Всего людей в базе
                cur.execute("SELECT COUNT(*) FROM users")
                total_users = cur.fetchone()[0]
                
                # 2. Активные участники (подписаны + есть билеты)
                cur.execute("SELECT COUNT(*) FROM users WHERE tickets > 0 AND all_subscribed = 1")
                active_participants = cur.fetchone()[0]
                
                # 3. Общее количество билетов
                cur.execute("SELECT SUM(tickets) FROM users")
                total_tickets = cur.fetchone()[0] or 0

        text = (
            f"📊 <b>СТАТИСТИКА БОТА:</b>\n\n"
            f"👥 <b>Всего пользователей:</b> {total_users}\n"
            f"✅ <b>Активных участников:</b> {active_participants}\n"
            f"🎫 <b>Всего билетов в игре:</b> {total_tickets}\n\n"
            f"<i>Это ваши цифры для продажи рекламы!</i> 💰"
        )
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"Ошибка получения статистики: {e}")

async def handle_webapp_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    now = datetime.utcnow()
    today = now.date()

    try:
        # Получаем данные из Колеса (чтобы понять, крутит он бесплатно или за билеты)
        # Допустим, если юзер хочет сжечь билеты, веб-апп пришлет {"action": "buy_spin"}
        data_str = update.effective_message.web_app_data.data
        action = ""
        try:
            parsed_data = json.loads(data_str)
            action = parsed_data.get("action", "")
        except: pass

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Достаем всю экономику юзера
                cur.execute("""
                    SELECT last_fortune_time, tickets, pity_counter, streak_days, last_spin_date, chance_multiplier 
                    FROM users WHERE user_id = %s
                """, (user_id,))
                res = cur.fetchone()
                
                if not res:
                    await update.effective_message.reply_text("❌ Ошибка профиля.")
                    return
                
                last_spin_time, tickets, pity, streak, last_spin_date, multiplier = res
                
                # --- 1. ПРОВЕРКА КУЛДАУНА И "СГОРАНИЯ" БИЛЕТОВ ---
                can_spin = True
                wait_msg = ""
                cost_tickets = 0

                if last_spin_time:
                    delta = now - last_spin_time
                    if delta < timedelta(hours=6):
                        if action == "buy_spin":
                            # Механика Сгорания: юзер хочет крутить прямо сейчас за 2 билета
                            if tickets >= 2:
                                cost_tickets = 2
                            else:
                                can_spin = False
                                wait_msg = "❌ У вас недостаточно билетов для платного прокрута (нужно 2 🎫)."
                        else:
                            can_spin = False
                            h_left = 5 - delta.seconds // 3600
                            m_left = (3600 - (delta.seconds % 3600)) // 60
                            wait_msg = f"⏳ Колесо заряжается! Ждите {h_left}ч {m_left}м.\n\n🔥 *Не хотите ждать?* Нажмите кнопку ниже, чтобы покрутить сейчас за 2 билета!"

                if not can_spin:
                    # Если не может крутить, предлагаем купить прокрут (если это не было ошибкой покупки)
                    kb = []
                    if "Колесо заряжается" in wait_msg:
                        kb = [[KeyboardButton("🔥 Крутить за 2 билета", web_app=WebAppInfo(url="https://moygiveawaybot.ru/index.html?action=buy_spin"))]]
                    
                    markup = ReplyKeyboardMarkup(kb, resize_keyboard=True) if kb else None
                    await update.effective_message.reply_text(wait_msg, reply_markup=markup, parse_mode=ParseMode.HTML)
                    return

                # --- 2. ЕЖЕДНЕВНЫЙ СТРИК (Streak) ---
                is_golden_wheel = False
                if last_spin_date == today:
                    pass # Стрик не меняется, крутит в тот же день
                elif last_spin_date == today - timedelta(days=1):
                    streak += 1 # Подряд!
                else:
                    streak = 1 # Пропустил день, сброс
                
                if streak >= 7:
                    is_golden_wheel = True
                    streak = 0 # Сброс после Золотого колеса
                    
                # --- 3. МАТЕМАТИКА ПРИЗОВ И PITY TIMER (ГАРАНТ) ---
                prize_text = ""
                won_tickets = 0
                won_multiplier = 0.0
                
                if is_golden_wheel:
                    # ЗОЛОТОЕ КОЛЕСО (7-й день) - Пустых нет!
                    choices = ["t_3", "t_5", "m_20", "m_50"]
                    weights = [40, 30, 20, 10]
                    pity = 0 # Сбрасываем гарант
                elif pity >= 4:
                    # СИСТЕМА ГАРАНТА (4 раза было пусто)
                    choices = ["t_1", "t_2", "m_5", "m_10"]
                    weights = [40, 30, 20, 10]
                    pity = 0
                    prize_text = "🛡 <b>Сработала система Гаранта!</b>\n\n"
                else:
                    # ОБЫЧНОЕ КОЛЕСО
                    choices = ["empty", "t_1", "t_2", "m_5", "m_10", "m_25"]
                    weights = [25, 30, 15, 15, 10, 5]
                    
                # Крутим рулетку на сервере
                result = random.choices(choices, weights=weights, k=1)[0]
                
                # --- 4. РАСПРЕДЕЛЕНИЕ ПРИЗОВ ---
                if result == "empty":
                    pity += 1
                    prize_text = "✨ В этот раз пусто...\nНо шкала Гаранта заполняется! Скоро точно повезет."
                elif result == "t_1":
                    won_tickets = 1
                    pity = 0
                    prize_text += "🎉 Поздравляем! Вы выиграли <b>+1 Билет</b>!"
                elif result == "t_2":
                    won_tickets = 2
                    pity = 0
                    prize_text += "🔥 Ого! Вы выиграли <b>+2 Билета</b>!"
                elif result == "t_3":
                    won_tickets = 3
                    prize_text = "🌟 ЗОЛОТОЕ КОЛЕСО: Вы выиграли <b>+3 Билета</b>!"
                elif result == "t_5":
                    won_tickets = 5
                    prize_text = "🌟 ЗОЛОТОЕ КОЛЕСО: Вы выиграли <b>+5 Билетов</b>!"
                elif result == "m_5":
                    won_multiplier = 0.05
                    pity = 0
                    prize_text += "📈 Бафф: <b>+5% к шансам на победу</b> в финале недели!"
                elif result == "m_10":
                    won_multiplier = 0.10
                    pity = 0
                    prize_text += "🚀 Бафф: <b>+10% к шансам на победу</b>!"
                elif result == "m_20":
                    won_multiplier = 0.20
                    prize_text = "🌟 ЗОЛОТОЕ КОЛЕСО: <b>+20% к шансам на победу</b>!"
                elif result == "m_25":
                    won_multiplier = 0.25
                    pity = 0
                    prize_text += "🏆 ДЖЕКПОТ! <b>+25% к шансам на победу</b>!"
                elif result == "m_50":
                    won_multiplier = 0.50
                    prize_text = "👑 МЕГА-ДЖЕКПОТ ЗОЛОТОГО КОЛЕСА: <b>+50% к шансам на победу</b>!"

                # --- 5. ОБНОВЛЯЕМ БАЗУ ДАННЫХ ---
                new_tickets = tickets - cost_tickets + won_tickets
                new_multiplier = multiplier + won_multiplier
                
                cur.execute("""
                    UPDATE users 
                    SET last_fortune_time = %s, tickets = %s, pity_counter = %s, 
                        streak_days = %s, last_spin_date = %s, chance_multiplier = %s
                    WHERE user_id = %s
                """, (now, new_tickets, pity, streak, today, new_multiplier, user_id))
                
                conn.commit()
                
                # --- 6. ОТПРАВЛЯЕМ ИТОГ ЮЗЕРУ ---
                header = f"🔥 <b>Стрик: {streak} дн.</b> | 📈 <b>Множитель шансов: x{new_multiplier:.2f}</b>\n{'—'*20}\n"
                
                if cost_tickets > 0:
                    header = f"💳 <i>Списано 2 билета за внеочередной прокрут.</i>\n\n" + header
                    
                await update.effective_message.reply_text(header + prize_text, parse_mode=ParseMode.HTML)
                
    except Exception as e:
        await update.effective_message.reply_text("❌ Ошибка обработки. Напишите админу.")
        print(f"Ошибка WebApp: {e}")

def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("draw", draw))
    app.add_handler(CommandHandler("stop", stop_giveaway))
    app.add_handler(CommandHandler("resume", resume_giveaway))
    app.add_handler(CommandHandler("reset_season", reset_season))
    app.add_handler(CommandHandler("stats", stats)) 
    app.add_handler(CommandHandler("fortune", fortune))
    app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, handle_webapp_data))
    
    print("Бот запущен...")
    app.run_polling()

if __name__ == "__main__":
    main()
