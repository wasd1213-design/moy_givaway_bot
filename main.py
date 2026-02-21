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

# --- КОНФИГУРАЦИЯ ---
BOT_TOKEN = os.getenv("BOT_TOKEN") 
SPONSORS = ["@openbusines", "@SAGkatalog", "@pro_teba_lubimyu"]
PRIZE = "🎁 Telegram Premium на 6 месяцев ИЛИ 1000 ⭐"
ADMINS = [514167463]  
BOT_USERNAME_FOR_REFLINK = "moy_giveaway_bot" 

# ГЛОБАЛЬНАЯ ПЕРЕМЕННАЯ (Статус работы бота)
# True = Работает, False = Технический перерыв
IS_ACTIVE = True 

# --- Подключение к БД ---
def get_db_connection():
    DATABASE_URL = os.getenv("MY_DATABASE_URL")
    if not DATABASE_URL:
        # Резервный адрес (локальный)
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
                # Таблица победителей (НОВАЯ)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS winners (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        username TEXT,
                        prize TEXT,
                        win_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                conn.commit()
        print("✅ База данных подключена.")
    except Exception as e:
        print(f"❌ Ошибка БД: {e}")

# --- Вспомогательные функции ---
def mask_username(username: str) -> str:
    """Скрывает никнейм (Al**ex)"""
    if not username: return "Us**er"
    if len(username) <= 2: return username[0] + "**"
    return username[0] + "**" + username[-1]

async def check_subscription(user_id, channel, context):
    try:
        member = await context.bot.get_chat_member(chat_id=channel, user_id=user_id)
        return member.status in ["member", "administrator", "creator"]
    except:
        return False

def calculate_tickets(user_id):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT ref_count, all_subscribed FROM users WHERE user_id = %s", (user_id,))
                res = cursor.fetchone()
                if not res: return 0
                count, sub = res
                if sub == 0: return 0
                return min(10, count) # 1 друг = 1 билет (макс 10)
    except:
        return 0

# --- ГЛАВНОЕ МЕНЮ И СТАТУС ---
async def build_status_message(user_id, first_name, context):
    subs = []
    unsubs = []
    
    # Проверка подписок
    for ch in SPONSORS:
        if await check_subscription(user_id, ch, context):
            subs.append(f"✅ {ch}")
        else:
            unsubs.append(f"❌ {ch}")
    
    all_ok = (len(unsubs) == 0)
    
    # Обновление данных в БД
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO users (user_id, username, all_subscribed, last_checked) VALUES (%s, %s, %s, NOW()) "
                    "ON CONFLICT (user_id) DO UPDATE SET username=%s, all_subscribed=%s, last_checked=NOW()",
                    (user_id, first_name, 1 if all_ok else 0, first_name, 1 if all_ok else 0)
                )
                conn.commit()
        
        tickets = calculate_tickets(user_id)
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET tickets = %s WHERE user_id = %s", (tickets, user_id))
                conn.commit()
    except Exception as e:
        print(f"Error update: {e}")
        tickets = 0

    # ЛОГИКА ОТОБРАЖЕНИЯ (Оригинальная)
    if not all_ok:
        msg = (
            "⚠️ <b>ВЫ НЕ УЧАСТВУЕТЕ!</b>\n\n"
            "Для участия подпишитесь на каналы:\n" +
            "\n".join(unsubs) + "\n\n"
            "После подписки нажмите кнопку «🔄 Обновить статус»"
        )
    else:
        msg = (
            f"👋 Привет, {first_name}!\n\n"
            f"🎁 <b>Розыгрыш:</b> {PRIZE}\n\n"
            f"✅ Вы подписаны на все каналы!\n"
            f"🎫 Ваши билеты: <b>{tickets}</b> / 10\n"
            f"👇 Жми кнопки ниже:"
        )

    # КНОПКИ (С добавленной кнопкой победителей)
    kb = [
        [InlineKeyboardButton("🔗 Моя ссылка", callback_data="my_reflink")],
        [InlineKeyboardButton("🎫 Мои билеты", callback_data="my_tickets")],
        [InlineKeyboardButton("🏅 Прошлые победители", callback_data="winners_list")], 
        [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")],
        [InlineKeyboardButton("🏆 Лидерборд", callback_data="leaderboard")],
        [InlineKeyboardButton("📜 Условия розыгрыша", callback_data="rules")]
    ]
    return msg, InlineKeyboardMarkup(kb)

# --- START ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 1. ПРОВЕРКА НА ПАУЗУ
    if not IS_ACTIVE:
        pause_text = (
            "🏁 <b>РОЗЫГРЫШ ЗАВЕРШЕН!</b>\n\n"
            "Прямо сейчас мы подводим итоги и готовим новый сезон.\n"
            "🔔 <i>Ожидайте уведомления о старте нового конкурса!</i>"
        )
        await update.message.reply_text(pause_text, parse_mode=ParseMode.HTML)
        return

    user = update.effective_user
    uid = user.id
    name = user.first_name
    
    # Регистрация
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users (user_id, username) VALUES (%s, %s) ON CONFLICT DO NOTHING", (uid, name))
                conn.commit()
    except: pass

    # Рефералка
    if context.args:
        ref_str = context.args[0]
        if ref_str.isdigit() and int(ref_str) != uid:
            referrer = int(ref_str)
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("INSERT INTO referrals (referrer_id, referred_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (referrer, uid))
                        if cur.rowcount > 0:
                            cur.execute("UPDATE users SET ref_count = ref_count + 1 WHERE user_id = %s", (referrer,))
                            conn.commit()
            except: pass

    text, markup = await build_status_message(uid, name, context)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

# --- ОБРАБОТЧИК КНОПОК ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # 2. ПРОВЕРКА НА ПАУЗУ
    if not IS_ACTIVE:
        await query.edit_message_text("🏁 Розыгрыш завершен. Идет подготовка нового этапа.", parse_mode=ParseMode.HTML)
        return
    
    uid = query.from_user.id
    name = query.from_user.first_name
    data = query.data

    if data == "refresh_status" or data == "back_to_main":
        text, markup = await build_status_message(uid, name, context)
        try:
            await query.edit_message_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        except: pass

    elif data == "my_tickets":
        tickets = calculate_tickets(uid)
        text = f"🎫 Ваши билеты: <b>{tickets}</b> (макс 10)"
        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

    elif data == "my_reflink":
        link = f"https://t.me/{BOT_USERNAME_FOR_REFLINK}?start={uid}"
        text = f"🔗 Ссылка для друзей:\n<code>{link}</code>\n\n+1 друг = +1 билет"
        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

    # --- СПИСОК ПОБЕДИТЕЛЕЙ (НОВОЕ) ---
    elif data == "winners_list":
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT username, win_date FROM winners ORDER BY win_date DESC LIMIT 15")
                    rows = cur.fetchall()
            
            if not rows:
                res = "📜 Список победителей пока пуст.\nСтаньте первым!"
            else:
                res = "🏅 <b>ПОСЛЕДНИЕ 15 ПОБЕДИТЕЛЕЙ:</b>\n\n"
                for i, r in enumerate(rows, 1):
                    safe_name = mask_username(r[0])
                    date_str = r[1].strftime("%d.%m.%Y")
                    res += f"{i}. <b>{safe_name}</b> ({date_str})\n"
        except: res = "Ошибка загрузки."
        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(res, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

    elif data == "leaderboard":
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

    elif data == "rules":
        text = (
            "📜 <b>Условия розыгрыша:</b>\n\n"
            "1. Подписка на спонсоров обязательна.\n"
            "2. <b>Приглашение 1 друга обязательно</b> для получения билета.\n"
            "3. Запрещена накрутка.\n"
            "4. Приз вручается в течение 48 часов."
        )
        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

# --- АДМИН ПАНЕЛЬ ---

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
        await update.message.reply_text(f"✅ Отправлено: {count}")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

# --- ФУНКЦИЯ РОЗЫГРЫША (ОБНОВЛЕННАЯ) ---
async def draw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id, username, tickets FROM users WHERE tickets > 0 AND all_subscribed = 1")
                rows = cur.fetchall()
        
        if not rows:
            await update.message.reply_text("Нет участников.")
            return

        pool = []
        for r in rows:
            pool.extend([r] * r[2]) 
        
        winner = random.choice(pool)
        wid, wname, wtickets = winner
        
        # 1. ЗАПИСЬ В БД
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO winners (user_id, username, prize) VALUES (%s, %s, %s)", (wid, wname, PRIZE))
                    conn.commit()
        except: pass

        # 2. СООБЩЕНИЕ АДМИНУ
        await update.message.reply_text(
            f"🎉 <b>ПОБЕДИТЕЛЬ:</b> @{wname or 'Нет ника'} (ID: <code>{wid}</code>)\n"
            f"Билетов: {wtickets}\n"
            f"✅ Записан в БД.\n"
            f"📨 Отправляю сообщение в ЛС...", 
            parse_mode=ParseMode.HTML
        )

        # 3. СООБЩЕНИЕ ПОБЕДИТЕЛЮ
        win_msg = (
            f"🎉 <b>ПОЗДРАВЛЯЕМ! ВЫ ВЫИГРАЛИ!</b> 🎁\n\n"
            f"В розыгрыше приза: <b>{PRIZE}</b>\n"
            f"Удача улыбнулась именно вам! 🥳\n\n"
            f"❗️ <b>ЧТО ДЕЛАТЬ ДАЛЬШЕ?</b>\n"
            f"Свяжитесь с модератором для получения приза.\n"
            f"👉 <b>Написать модератору:</b> @AddkatalogBot\n\n"
            f"⏳ <b>Важно:</b> У вас есть ровно <b>48 часов</b>.\n"
            f"<i>По истечении этого срока приз аннулируется!</i>"
        )
        try:
            await context.bot.send_message(wid, win_msg, parse_mode=ParseMode.HTML)
            await update.message.reply_text("✅ Сообщение доставлено победителю.")
        except:
            await update.message.reply_text("⚠️ Не удалось написать победителю (ЛС закрыто).")

    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

# --- УПРАВЛЕНИЕ ПАУЗОЙ ---
async def stop_giveaway(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    global IS_ACTIVE
    IS_ACTIVE = False
    await update.message.reply_text("⛔️ <b>РОЗЫГРЫШ ОСТАНОВЛЕН!</b>\nТеперь бот пишет пользователям, что идет подсчет итогов.", parse_mode=ParseMode.HTML)

async def resume_giveaway(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    global IS_ACTIVE
    IS_ACTIVE = True
    await update.message.reply_text("▶️ <b>РОЗЫГРЫШ ВОЗОБНОВЛЕН!</b>", parse_mode=ParseMode.HTML)

async def reset_season(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET tickets = 0, ref_count = 0")
                conn.commit()
        await update.message.reply_text("✅ <b>Сезон сброшен!</b> Билеты = 0.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def help_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    text = "🛠 /draw, /stop, /resume, /reset_season, /broadcast"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(CommandHandler("admin", help_admin))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("draw", draw))
    app.add_handler(CommandHandler("stop", stop_giveaway))
    app.add_handler(CommandHandler("resume", resume_giveaway))
    app.add_handler(CommandHandler("reset_season", reset_season))
    print("Бот запущен...")
    app.run_polling()

if __name__ == "__main__":
    main()
