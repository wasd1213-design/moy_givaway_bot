import os
import asyncio
from dotenv import load_dotenv
load_dotenv()
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
import psycopg2
from telegram.constants import ParseMode 
import random 

# --- КОНФИГУРАЦИЯ ---
# Токен бота берется из .env
BOT_TOKEN = os.getenv("BOT_TOKEN") 

# Каналы-спонсоры
SPONSORS = ["@openbusines", "@SAGkatalog", "@pro_teba_lubimyu"]

# Текст приза
PRIZE = "🎁 Telegram Premium на 6 месяцев ИЛИ 1000 ⭐"

# ID Админов (замените на свой ID)
ADMINS = [514167463]  

# Юзернейм вашего бота (без @)
BOT_USERNAME_FOR_REFLINK = "moy_giveaway_bot" 

# --- База данных ---
def get_db_connection():
    DATABASE_URL = os.getenv("MY_DATABASE_URL")
    if not DATABASE_URL:
        # Стандартный локальный адрес, если переменной нет
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
                        last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                # Таблица рефералов
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS referrals (
                        referrer_id BIGINT,
                        referred_id BIGINT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(referrer_id, referred_id)
                    )
                ''')
                conn.commit()
        print("✅ База данных подключена.")
    except Exception as e:
        print(f"❌ Ошибка БД: {e}")

# --- Вспомогательные функции ---
def mask_username(username: str) -> str:
    if not username: return "User"
    if len(username) <= 2: return username + "*"
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
                return min(10, count)
    except:
        return 0

# --- Сообщение статуса ---
async def build_status_message(user_id, first_name, context):
    subs = []
    unsubs = []
    
    for ch in SPONSORS:
        if await check_subscription(user_id, ch, context):
            subs.append(f"✅ {ch}")
        else:
            unsubs.append(f"❌ {ch}")
    
    all_ok = (len(unsubs) == 0)
    
    # Обновляем статус в БД
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO users (user_id, username, all_subscribed, last_checked) VALUES (%s, %s, %s, NOW()) "
                    "ON CONFLICT (user_id) DO UPDATE SET username=%s, all_subscribed=%s, last_checked=NOW()",
                    (user_id, first_name, 1 if all_ok else 0, first_name, 1 if all_ok else 0)
                )
                conn.commit()
        
        # Обновляем билеты
        tickets = calculate_tickets(user_id)
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET tickets = %s WHERE user_id = %s", (tickets, user_id))
                conn.commit()

    except Exception as e:
        print(f"Update User Error: {e}")
        tickets = 0

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
            f"👥 Друзей приглашено: {tickets}\n\n"
            f"👇 Жми кнопки ниже, чтобы получить ссылку!"
        )

    kb = [
        [InlineKeyboardButton("🔗 Моя ссылка для друзей", callback_data="my_reflink")],
        [InlineKeyboardButton("🎫 Мои билеты", callback_data="my_tickets")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")],
        [InlineKeyboardButton("🏆 Лидерборд", callback_data="leaderboard")],
        [InlineKeyboardButton("📜 Правила", callback_data="rules")]
    ]
    return msg, InlineKeyboardMarkup(kb)

# --- Обработчик /start ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid = user.id
    name = user.first_name
    
    # Регистрируем пользователя
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (user_id, username) VALUES (%s, %s) ON CONFLICT (user_id) DO NOTHING",
                    (uid, name)
                )
                conn.commit()
    except Exception as e:
        print(f"DB Start Error: {e}")

    # Проверка реферала
    if context.args:
        ref_id_str = context.args[0]
        if ref_id_str.isdigit() and int(ref_id_str) != uid:
            referrer = int(ref_id_str)
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO referrals (referrer_id, referred_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            (referrer, uid)
                        )
                        if cur.rowcount > 0:
                            cur.execute("UPDATE users SET ref_count = ref_count + 1 WHERE user_id = %s", (referrer,))
                            conn.commit()
            except Exception as e:
                print(f"Ref Error: {e}")

    text, markup = await build_status_message(uid, name, context)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

# --- Кнопки ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
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
        text = f"🎫 <b>Ваши билеты: {tickets}</b>\n\nМаксимум 10 билетов."
        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

    elif data == "my_reflink":
        link = f"https://t.me/{BOT_USERNAME_FOR_REFLINK}?start={uid}"
        text = f"🔗 <b>Ваша ссылка:</b>\n\n<code>{link}</code>\n\nЗа каждого друга +1 билет!"
        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

    elif data == "leaderboard":
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT username, tickets FROM users WHERE tickets > 0 ORDER BY tickets DESC LIMIT 10")
                    rows = cur.fetchall()
            
            if not rows: res = "Пока пусто."
            else:
                res = "🏆 <b>ТОП УЧАСТНИКОВ:</b>\n\n"
                for i, r in enumerate(rows, 1):
                    res += f"{i}. {mask_username(r[0])} — {r[1]} 🎫\n"
        except: res = "Ошибка."
        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(res, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

    elif data == "rules":
        text = "📜 <b>Правила:</b>\n1. Подписка обязательна.\n2. Приз выдается в течение 48 часов."
        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)


# --- АДМИН ПАНЕЛЬ ---

# 1. Реклама
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return

    if not context.args:
        await update.message.reply_text("❌ Введите текст.\nПример: `/broadcast Привет всем!`")
        return

    msg_text = " ".join(context.args)
    await update.message.reply_text("⏳ Рассылка началась...")

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id FROM users")
                users = cur.fetchall()
        
        count = 0
        for user in users:
            try:
                await context.bot.send_message(chat_id=user[0], text=msg_text)
                count += 1
                await asyncio.sleep(0.05)
            except: pass
        
        await update.message.reply_text(f"✅ Рассылка завершена! Доставлено: {count}")

    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

# 2. РОЗЫГРЫШ (ОБНОВЛЕННАЯ ФУНКЦИЯ)
async def draw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return

    try:
        # Выбираем участников
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id, username, tickets FROM users WHERE tickets > 0 AND all_subscribed = 1")
                rows = cur.fetchall()
        
        if not rows:
            await update.message.reply_text("🤷‍♂️ Нет участников с билетами.")
            return

        # Взвешенный рандом
        pool = []
        for r in rows:
            pool.extend([r] * r[2]) # Добавляем столько раз, сколько билетов

        winner = random.choice(pool) # (user_id, username, tickets)
        winner_id = winner[0]
        winner_name = winner[1] or "Без ника"
        winner_tickets = winner[2]
        
        # 1. Сообщение Админу
        admin_text = (
            f"🎉 <b>ПОБЕДИТЕЛЬ ОПРЕДЕЛЕН!</b>\n\n"
            f"👤 @{winner_name} (ID: <code>{winner_id}</code>)\n"
            f"🎫 Билетов: {winner_tickets}\n"
            f"✅ Сообщение победителю отправляется..."
        )
        await update.message.reply_text(admin_text, parse_mode=ParseMode.HTML)

        # 2. Сообщение Победителю (То, что вы просили)
        winner_text = (
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
            await context.bot.send_message(chat_id=winner_id, text=winner_text, parse_mode=ParseMode.HTML)
            await update.message.reply_text("✅ Победитель получил уведомление в личку!")
        except Exception as e:
            await update.message.reply_text(f"⚠️ Не удалось отправить сообщение победителю (возможно, бот заблокирован): {e}")

    except Exception as e:
        await update.message.reply_text(f"Ошибка розыгрыша: {e}")

# 3. Сброс сезона (оставляет людей в базе, обнуляет билеты)
async def reset_season(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET tickets = 0, ref_count = 0")
                # cur.execute("DELETE FROM referrals") # Раскомментируйте, если можно приглашать тех же друзей
                conn.commit()
        
        await update.message.reply_text("✅ <b>СЕЗОН ОБНУЛЕН!</b>\nБилеты сброшены. Пользователи остались в базе.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

# --- ЗАПУСК ---
def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("draw", draw))
    app.add_handler(CommandHandler("reset_season", reset_season))

    print("Бот запущен...")
    app.run_polling()

if __name__ == "__main__":
    main()
