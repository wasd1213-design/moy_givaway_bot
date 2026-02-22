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
BOT_TOKEN = os.getenv("BOT_TOKEN") 

# ❗️ ВАЖНО: Если вы меняете каналы, старайтесь сохранять порядок или очищать базу, 
# так как статистика привязывается к позиции канала в списке (1-й, 2-й, 3-й).
SPONSORS = ["@openbusines", "@SAGkatalog", "@pro_teba_lubimyu"]

PRIZE = "Telegram Premium на 6 месяцев или 1000 ⭐"
ADMINS = [514167463]  # ID Админа
BOT_USERNAME_FOR_REFLINK = "moy_giveaway_bot" 
WINNERS_COUNT = 2     # Количество победителей

# Глобальный переключатель
IS_ACTIVE = True 

# --- ПОДКЛЮЧЕНИЕ К БД ---
def get_db_connection():
    DATABASE_URL = os.getenv("MY_DATABASE_URL")
    if not DATABASE_URL:
        # Если запускаете локально без .env переменной базы
        return psycopg2.connect("postgresql://bot_user:12345@localhost/bot_db")
    return psycopg2.connect(DATABASE_URL)

def init_db():
    """Создает таблицы и обновляет их структуру автоматически"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 1. Основная таблица пользователей
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
                
                # 2. АВТОМАТИЧЕСКОЕ ДОБАВЛЕНИЕ КОЛОНОК ДЛЯ СТАТИСТИКИ
                # Бот создаст sub_channel_1, sub_channel_2 и т.д.
                for i in range(len(SPONSORS)):
                    col_name = f"sub_channel_{i+1}" 
                    # Попытка добавить колонку, если её нет
                    cursor.execute(f"ALTER TABLE users ADD COLUMN IF NOT EXISTS {col_name} INTEGER DEFAULT 0")
                
                # 3. Таблица рефералов
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS referrals (
                        referrer_id BIGINT,
                        referred_id BIGINT,
                        UNIQUE(referrer_id, referred_id)
                    )
                ''')
                # 4. Таблица победителей
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
        print("✅ База данных готова и обновлена.")
    except Exception as e:
        print(f"❌ Ошибка инициализации БД: {e}")

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
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

# --- ГЛАВНАЯ ФУНКЦИЯ СИНХРОНИЗАЦИИ ---
# Проверяет подписки и сразу сохраняет статистику в БД
async def sync_user_status(user_id, context):
    try:
        all_subs_ok = True
        channel_statuses = [] # [1, 0, 1] ...

        # Проходим по списку спонсоров
        for ch in SPONSORS:
            is_sub = await check_subscription(user_id, ch, context)
            if not is_sub:
                all_subs_ok = False
                channel_statuses.append(0)
            else:
                channel_statuses.append(1)

        # Пишем результаты в БД
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Формируем динамический SQL запрос для обновления колонок sub_channel_X
                # Пример: "sub_channel_1 = %s, sub_channel_2 = %s"
                sql_update_channels = ", ".join([f"sub_channel_{i+1} = %s" for i in range(len(SPONSORS))])
                
                # Обновляем статусы каналов и общий статус
                query = f"UPDATE users SET {sql_update_channels}, all_subscribed = %s WHERE user_id = %s"
                params = (*channel_statuses, 1 if all_subs_ok else 0, user_id)
                cur.execute(query, params)
                
                # Пересчитываем билеты
                cur.execute("SELECT ref_count FROM users WHERE user_id = %s", (user_id,))
                res = cur.fetchone()
                ref_count = res[0] if res else 0
                
                # Билеты даем только если подписан НА ВСЕХ
                if all_subs_ok:
                    actual_tickets = min(10, ref_count) # Максимум 10 билетов
                else:
                    actual_tickets = 0 # Замораживаем билеты
                
                cur.execute("UPDATE users SET tickets = %s WHERE user_id = %s", (actual_tickets, user_id))
                conn.commit()
                
        return actual_tickets, channel_statuses
    except Exception as e:
        print(f"Ошибка Sync: {e}")
        return 0, [0]*len(SPONSORS)

# --- ГЕНЕРАЦИЯ ТЕКСТА МЕНЮ ---
async def get_start_text(user_id, first_name, context):
    # При вызове меню мы обновляем данные в БД (проверка занимает 1-2 сек)
    tickets, statuses = await sync_user_status(user_id, context)
    
    channels_list = ""
    for i, ch in enumerate(SPONSORS):
        icon = "✅" if statuses[i] == 1 else "❌"
        channels_list += f"{i+1}. {ch} {icon}\n"

    msg = (
        f"👋 <b>Привет, {first_name}!</b>\n\n"
        f"🎁 <b>Приз недели:</b>\n{PRIZE}\n\n"
        f"👇 <b>Для участия:</b>\n"
        f"{channels_list}\n"
        f"✅ Подпишись на все каналы и пригласи друга!\n"
        f"🎫 <b>Твои билеты:</b> {tickets} (Макс. 10)"
    )
    return msg

# --- КОМАНДА /START ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not IS_ACTIVE:
        await update.message.reply_text("🏁 <b>Розыгрыш на паузе или завершен.</b>", parse_mode=ParseMode.HTML)
        return

    user = update.effective_user
    uid = user.id
    name = user.first_name
    
    # Регистрация юзера
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users (user_id, username) VALUES (%s, %s) ON CONFLICT DO NOTHING", (uid, name))
                conn.commit()
    except: pass

    # Обработка реферальной ссылки
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

    # Индикатор загрузки
    wait_msg = await update.message.reply_text("⏳ Проверяю подписки...")
    
    # Получаем текст меню (внутри происходит проверка подписок)
    text = await get_start_text(uid, name, context)
    
    # Удаляем сообщение "Загрузка"
    try:
        await context.bot.delete_message(chat_id=uid, message_id=wait_msg.message_id)
    except: pass
    
    kb = [
        [InlineKeyboardButton("🎫 Мои билеты", callback_data="my_tickets")],
        [InlineKeyboardButton("🔗 Моя реферальная ссылка", callback_data="my_reflink")],
        [InlineKeyboardButton("🏆 Лидерборд", callback_data="leaderboard")],
        [InlineKeyboardButton("🔄 Проверить подписку", callback_data="check_sub")]
    ]
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))

# --- ОБРАБОТЧИК КНОПОК ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    data = query.data

    if not IS_ACTIVE:
        await query.answer()
        await query.edit_message_text("🏁 Розыгрыш завершен.", parse_mode=ParseMode.HTML)
        return

    if data == "check_sub" or data == "back_to_main":
        await query.answer("Проверяю...")
        text = await get_start_text(uid, query.from_user.first_name, context)
        kb = [
            [InlineKeyboardButton("🎫 Мои билеты", callback_data="my_tickets")],
            [InlineKeyboardButton("🔗 Моя реферальная ссылка", callback_data="my_reflink")],
            [InlineKeyboardButton("🏆 Лидерборд", callback_data="leaderboard")],
            [InlineKeyboardButton("🔄 Проверить подписку", callback_data="check_sub")]
        ]
        try: await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(kb))
        except: pass

    elif data == "my_tickets":
        await query.answer()
        # Берем билеты напрямую из базы (быстро)
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT tickets FROM users WHERE user_id = %s", (uid,))
                res = cur.fetchone()
                t = res[0] if res else 0
        
        text = f"🎫 <b>Ваши активные билеты: {t}</b>\n\nЕсли билетов 0, но вы приглашали друзей — проверьте подписку на каналы!"
        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
    
    elif data == "my_reflink":
        await query.answer()
        link = f"https://t.me/{BOT_USERNAME_FOR_REFLINK}?start={uid}"
        text = f"🔗 <b>Ваша ссылка для приглашения:</b>\n\n<code>{link}</code>\n\nОтправьте её другу. Как только он запустит бота, вам засчитается друг."
        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)

    elif data == "leaderboard":
        await query.answer()
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT username, tickets FROM users WHERE tickets > 0 ORDER BY tickets DESC LIMIT 10")
                    rows = cur.fetchall()
            
            if not rows:
                res = "Пока пусто."
            else:
                res = "🏆 <b>ТОП-10 УЧАСТНИКОВ:</b>\n\n"
                for i, r in enumerate(rows):
                    res += f"{i+1}. {mask_username(r[0])} — {r[1]} 🎫\n"
        except: res = "Ошибка данных."
        kb = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
        await query.edit_message_text(res, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)


# --- ФУНКЦИЯ РОЗЫГРЫША (DRAW) ---
async def draw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    
    await update.message.reply_text("🎲 <b>Запускаю розыгрыш...</b>", parse_mode=ParseMode.HTML)

    # 1. Сбор участников
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Берем только тех, у кого билетов > 0 (значит, они точно подписаны на всех)
                cur.execute("SELECT user_id, username, tickets FROM users WHERE tickets > 0 AND all_subscribed = 1")
                rows = cur.fetchall()
        
        participants_count = len(rows) # Количество уникальных участников финала
        
        if not rows:
            await update.message.reply_text("⚠️ Нет участников, выполнивших условия (билетов > 0).")
            return

        # 2. Выбор победителей (с весами)
        selected_winners = []
        pool = list(rows) # Копия списка
        
        for _ in range(WINNERS_COUNT):
            if not pool: break # Если участники кончились

            # Создаем "барабан": чем больше билетов, тем больше раз имя в барабане
            weighted_pool = []
            for r in pool: 
                weighted_pool.extend([r]*r[2]) 
            
            if not weighted_pool: break

            # Крутим барабан
            winner = random.choice(weighted_pool)
            selected_winners.append(winner)
            
            # Убираем победителя из списка, чтобы он не выиграл второй приз
            pool = [p for p in pool if p[0] != winner[0]]

        # 3. Объявление результатов
        res_text = "🎉 <b>ИТОГИ РОЗЫГРЫША:</b>\n\n"
        for i, w in enumerate(selected_winners, 1):
            wid, wname, wtickets = w
            
            # Запись в историю победителей
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("INSERT INTO winners (user_id, username, prize) VALUES (%s, %s, %s)", (wid, wname, PRIZE))
                        conn.commit()
            except: pass
            
            # Отправка ЛС
            try:
                await context.bot.send_message(wid, f"🎉 <b>ПОЗДРАВЛЯЕМ!</b>\nВы выиграли приз: {PRIZE}!\nСвяжитесь с админом.", parse_mode=ParseMode.HTML)
            except: pass
            
            res_text += f"🏆 <b>Место {i}:</b> @{wname or 'user'} (ID {wid}) — {wtickets} 🎫\n"
        
        await update.message.reply_text(res_text + "\n✅ Победители оповещены.", parse_mode=ParseMode.HTML)

        # 4. МГНОВЕННЫЙ ОТЧЕТ ПО КАНАЛАМ
        # Теперь мы не опрашиваем Телеграм, а просто смотрим нашу базу, где данные уже собраны
        
        stats_text = (
            f"📊 <b>ОТЧЕТ ПО ТРАФИКУ:</b>\n\n"
            f"👥 Всего участников финала (подписаны на всех): {participants_count}\n"
            f"📉 <b>Детальная статистика подписок:</b>\n"
            f"(Учитываются все, кто нажимал 'Проверить подписку')\n\n"
        )
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for i, ch in enumerate(SPONSORS):
                    col_name = f"sub_channel_{i+1}"
                    # Считаем, сколько людей имеют "1" в колонке этого канала
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM users WHERE {col_name} = 1")
                        count = cur.fetchone()[0]
                    except: count = 0
                    
                    stats_text += f"👉 <b>{ch}</b>: {count} подписчиков\n"
        
        await update.message.reply_text(stats_text, parse_mode=ParseMode.HTML)

    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при розыгрыше: {e}")

# --- АДМИНСКИЕ КОМАНДЫ ---
async def stop_giveaway(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    global IS_ACTIVE
    IS_ACTIVE = False
    await update.message.reply_text("⛔️ <b>Розыгрыш остановлен (ПАУЗА).</b>", parse_mode=ParseMode.HTML)

async def resume_giveaway(update:S: return
    global IS_ACTIVE
    IS_ACTIVE = True
    await update.message.reply_text("▶️ <b>Розыгрыш возобновлен.</b>", parse_mode=ParseMode.HTML)

async def reset_season(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Обнуляем билеты и рефералов, но оставляем юзеров
                cur.execute("UPDATE users SET tickets = 0, ref_count = 0")
                # Также можно сбросить и подписки, чтобы заставить их проверить заново
                cur.execute("UPDATE users SET all_subscribed = 0")
                conn.commit()
        await update.message.reply_text("✅ <b>Новый сезон!</b> Билеты и рефералы сброшены.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users")
                total = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM users WHERE tickets > 0")
                active = cur.fetchone()[0]
        await update.message.reply_text(f"📊 <b>База:</b> {total} чел.\n✅ <b>С билетами:</b> {active} чел.", parse_mode=ParseMode.HTML)
    except:
        await update.message.reply_text("Ошибка БД.")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS: return
    if not context.args: return
    msg = " ".join(context.args)
    await update.message.reply_text("⏳ Рассылка...")
    # Тут простая рассылка, для тысяч юзеров лучше делать с задержкой
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users")
            users = cur.fetchall()
    for u in users:
        try:
            await context.bot.send_message(u[0], msg)
            await asyncio.sleep(0.05) # 20 сообщений в секунду
        except: pass
    await update.message.reply_text("✅ Рассылка завершена.")

def main():
    # Инициализация БД при запуске
    init_db()
    
    app = Application.builder().token(BOT_TOKEN).build()
    
    # Хендлеры
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    
    # Админка
    app.add_handler(CommandHandler("draw", draw))
    app.add_handler(CommandHandler("stop", stop_giveaway))
    app.add_handler(CommandHandler("resume", resume_giveaway))
    app.add_handler(CommandHandler("stats", stats)) 
    app.add_handler(CommandHandler("reset_season", reset_season))
    app.add_handler(CommandHandler("broadcast", broadcast))

    print("Бот запущен...")
    app.run_polling()

if __name__ == "__main__":
    main()
