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
                return min(10, count) # 1 друг = 1 билет
    except:
        return 0

# --- ГЕНЕРАЦИЯ ГЛАВНОГО МЕНЮ (С ПРОВЕРКОЙ ГАЛОЧЕК) ---
async def get_start_text(user_id, first_name, context):
    channels_list = ""
    all_subs_ok = True
    
    # Проходим по каждому спонсору и ставим галочку или крестик
    for i, ch in enumerate(SPONSORS, 1):
        is_sub = await check_subscription(user_id, ch, context)
        if not is_sub:
            all_subs_ok = False
            icon = "❌"
        else:
            icon = "✅"
        channels_list += f"{i}. {ch} {icon}\n"

    # Обновляем статус в БД (чтобы потом билеты считались верно)
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET all_subscribed = %s WHERE user_id = %s", (1 if all_subs_ok else 0, user_id))
                conn.commit()
    except: pass

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
    # ПРОВЕРКА НА ПАУЗУ
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
    
    # Регистрация
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users (user_id, username) VALUES (%s, %s) ON CONFLICT DO NOTHING", (uid, name))
                conn.commit()
    except: pass

    # Реферальная система
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

    # Генерируем текст с актуальными галочками
    text = await get_start_text(uid, name, context)
    
    kb = [
        [InlineKeyboardButton("🎫 Мои билеты", callback_data="my_tickets")],
        [InlineKeyboardButton("🔗 Моя реферальная ссылка", callback_data="my_reflink")],
        [InlineKeyboardButton("🏆 Лидерборд", callback_data="leaderboard")],
        [InlineKeyboardButton("🏅 Прошлые победители", callback_data="winners_list")],
        [InlineKeyboardButton("🔄 Проверить подписку", callback_data="check_sub")]
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

    # --- ЛОГИКА ОБНОВЛЕНИЯ МЕНЮ (ПРОВЕРКА ПОДПИСКИ) ---
    if data == "check_sub" or data == "back_to_main":
        await query.answer("Обновляю...")
        # Генерируем текст заново (проверяя подписки)
        text = await get_start_text(uid, query.from_user.first_name, context)
        
        kb = [
            [InlineKeyboardButton("🎫 Мои билеты", callback_data="my_tickets")],
            [InlineKeyboardButton("🔗 Моя реферальная ссылка", callback_data="my_reflink")],
            [InlineKeyboardButton("🏆 Лидерборд", callback_data="leaderboard")],
            [InlineKeyboardButton("🏅 Прошлые победители", callback_data="winners_list")],
            [InlineKeyboardButton("🔄 Проверить подписку", callback_data="check_sub")]
        ]
        try:
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
        except:
            pass # Если текст не изменился, игнорируем ошибку телеграма

    elif data == "my_tickets":
        await query.answer()
        # Обновляем статус подписки (чтобы билеты были актуальны)
        await get_start_text(uid, query.from_user.first_name, context)
        
        tickets = calculate_tickets(uid)
        
        # Проверяем в БД, подписан ли он
        is_subscribed = False
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT all_subscribed FROM users WHERE user_id = %s", (uid,))
                    res = cur.fetchone()
                    if res and res[0] == 1: is_subscribed = True
        except: pass

        if not is_subscribed:
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


# --- АДМИНСКИЕ ФУНКЦИИ (ПОЛНЫЕ) ---

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    if not context.args:
        await update.message.reply_text("Введите текст для рассылки.")
        return
    msg = " ".join(context.args)
    await update.message.reply_text("⏳ Рассылка запущена...")
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
                await asyncio.sleep(0.05) # Анти-спам задержка
            except: pass
        await update.message.reply_text(f"✅ Рассылка завершена. Доставлено: {count}")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def draw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Только подписанные и с билетами > 0
                cur.execute("SELECT user_id, username, tickets FROM users WHERE tickets > 0 AND all_subscribed = 1")
                rows = cur.fetchall()
        
        if not rows:
            await update.message.reply_text("Нет участников для розыгрыша.")
            return

        # Лотерея с весами (больше билетов = больше шансов)
        pool = []
        for r in rows:
            pool.extend([r] * r[2]) 
        
        winner = random.choice(pool)
        wid, wname, wtickets = winner
        
        # 1. Запись в БД
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO winners (user_id, username, prize) VALUES (%s, %s, %s)", (wid, wname, PRIZE))
                    conn.commit()
        except: pass

        # 2. Админу
        await update.message.reply_text(
            f"🎉 <b>ПОБЕДИТЕЛЬ:</b> @{wname or 'Нет ника'} (ID: <code>{wid}</code>)\n"
            f"Билетов: {wtickets}\n"
            f"✅ Сообщение победителю отправляется...", 
            parse_mode=ParseMode.HTML
        )

        # 3. Победителю
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
            await update.message.reply_text("✅ Сообщение успешно доставлено победителю.")
        except:
            await update.message.reply_text("⚠️ Не удалось написать победителю (ЛС закрыто).")

    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def stop_giveaway(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    global IS_ACTIVE
    IS_ACTIVE = False
    await update.message.reply_text("⛔️ <b>РОЗЫГРЫШ ОСТАНОВЛЕН!</b>\nРежим паузы активирован.", parse_mode=ParseMode.HTML)

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
        await update.message.reply_text("✅ <b>Сезон сброшен!</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

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

    print("Бот запущен...")
    app.run_polling()

if __name__ == "__main__":
    main()
