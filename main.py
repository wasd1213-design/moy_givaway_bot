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
    if not username: return "User"
    if len(username) <= 2: return username + "*"
    return username[0] + "**" + username[-1]

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

    if data == "check_sub" or data == "back_to_main":
        await query.answer("Обновляю...")
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
    
    print("Бот запущен...")
    app.run_polling()

if __name__ == "__main__":
    main()
