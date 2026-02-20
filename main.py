import os
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
import psycopg2
from telegram.constants import ParseMode 
import random 

# --- Вспомогательная функция для маскировки имени пользователя ---
def mask_username(username: str) -> str:
    if not username:
        return "Без ника"
    if len(username) <= 2:
        return username[0] + "**"
    return username[0] + "**" + username[-1]

# --- Конфигурация ---
# ! ВАЖНО: Если вы разворачиваете на Railway или аналогичной платформе,
# ! БОТ ТОКЕН лучше хранить в переменной окружения Railway (Environment Variable) с именем BOT_TOKEN.
# ! Тогда строчка BOT_TOKEN = os.getenv("BOT_TOKEN") автоматически его подхватит.
# ! Если запускаете локально, то можно оставить как есть, но это менее безопасно для продакшена.
BOT_TOKEN = os.getenv("BOT_TOKEN", "8576715226:AAGvd7NOy4kA98Gdn6ZVdgkIzAWtZjAgI8s") # Ваш токен, как в первом сообщении

SPONSORS = ["@openbusines", "@SAGkatalog", "@pro_teba_lubimyu"]  # Ваши каналы-спонсоры
PRIZE = "🎁 Telegram Premium на 6 месяцев ИЛИ 1000 ⭐" # Обновленный приз
ADMINS = [514167463]  # Ваши user_id админов (можно узнать через @getmyid_bot в Telegram)

# ! ВАЖНО: ЗАМЕНИТЕ ЭТОТ ЮЗЕРНЕЙМ НА АКТУАЛЬНЫЙ ЮЗЕРНЕЙМ ВАШЕГО БОТА!
BOT_USERNAME_FOR_REFLINK = "moy_giveaway_bot" 

# --- Подключение к PostgreSQL ---
def get_db_connection():
    # ! ВАЖНО: MY_DATABASE_URL должен быть настроен как переменная окружения
    # ! в вашей среде развертывания (например, Railway).
    DATABASE_URL = os.getenv("MY_DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("MY_DATABASE_URL не установлен. Настройте его как переменную окружения (например, в Railway).")
    return psycopg2.connect(DATABASE_URL, sslmode='require')

# --- Инициализация базы данных ---
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
        print("✅ База данных успешно инициализирована или уже существует.")
    except Exception as e:
        print(f"❌ Ошибка при инициализации базы данных: {e}")


# --- Проверка подписки на канал ---
async def check_subscription(user_id, channel, context):
    try:
        chat_member = await context.bot.get_chat_member(chat_id=channel, user_id=user_id)
        return chat_member.status in ["member", "administrator", "creator"]
    except Exception as e:
        print(f"[ERROR] check_subscription для канала: {channel}, user_id: {user_id}, ошибка: {e}")
        return False

# --- Расчёт билетов ---
def calculate_tickets(user_id):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT ref_count, all_subscribed FROM users WHERE user_id = %s", (user_id,))
                result = cursor.fetchone()
                
                if not result:
                    return 0
                
                ref_count, all_subscribed_status = result
                
                if all_subscribed_status == 0:
                    return 0
                if ref_count < 1:
                    return 0
                
                return min(10, ref_count)
    except Exception as e:
        print(f"[ERROR] calculate_tickets для user_id {user_id}: {e}")
        return 0

# --- Формирование сообщения статуса ---
async def build_status_message(user_id, first_name_tg, context):
    # Проверяем подписки
    subscribed_channels = []
    unsubscribed_channels = []
    
    for channel in SPONSORS:
        if await check_subscription(user_id, channel, context):
            subscribed_channels.append(f"✅ {channel}")
        else:
            unsubscribed_channels.append(f"❌ {channel}")
    
    all_subscribed = len(unsubscribed_channels) == 0
    
    # Сохраняем статус и обновляем имя пользователя в БД
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO users (user_id, username, all_subscribed, last_checked) VALUES (%s, %s, %s, CURRENT_TIMESTAMP) "
                    "ON CONFLICT (user_id) DO UPDATE SET username = %s, all_subscribed = %s, last_checked = CURRENT_TIMESTAMP",
                    (user_id, first_name_tg, 1 if all_subscribed else 0,
                     first_name_tg, 1 if all_subscribed else 0)
                )
                conn.commit()
    except Exception as e:
        print(f"[ERROR] build_status_message DB update для user {user_id}: {e}")
    
    tickets = calculate_tickets(user_id)
    
    # Формируем текст
    if not all_subscribed:
        status_text = (
            "⚠️ Вы не подписаны на все каналы!\n\n"
            "Подпишитесь, чтобы участвовать:\n" +
            "\n".join(unsubscribed_channels) + "\n\n" +
            "✅ Подписаны:\n" + "\n".join(subscribed_channels)
        )
    else:
        status_text = (
            f"🎉 Привет, {first_name_tg}!\n\n"
            f"🎁 Приз этой недели:\n{PRIZE}\n\n"
            f"✅ Вы подписаны на все каналы!\n" +
            "\n".join(subscribed_channels) + "\n\n"
            f"🎫 Ваши билеты: {tickets} / 10\n"
            f"👥 Рефералов: {tickets if tickets > 0 else 0} (минимум 1 для участия)\n\n"
            f"💡 Каждый новый реферал = +1 билет (макс. 10)"
        )
    
    # Кнопки для главного меню статуса
    keyboard = [
        [InlineKeyboardButton("🎫 Мои билеты", callback_data="my_tickets")],
        [InlineKeyboardButton("🔗 Моя реферальная ссылка", callback_data="my_reflink")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")],
        [InlineKeyboardButton("🏅 Лидерборд", callback_data="leaderboard")],
        [InlineKeyboardButton("🏆 Условия розыгрыша", callback_data="rules")]
    ]
    return status_text, InlineKeyboardMarkup(keyboard)

# --- Команда /start ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    first_name_tg = user.first_name
    username_db = user.username or f"user_{user_id}"

    # 1. Сохраняем/обновляем пользователя в базе данных
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO users (user_id, username) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET username = %s",
                    (user_id, username_db, username_db)
                )
                conn.commit()
    except Exception as e:
        print(f"[ERROR] start command DB insert/update для user {user_id}: {e}")

    # 2. Обработка реферала
    if context.args:
        referrer_id_str = context.args[0]
        if referrer_id_str.isdigit() and int(referrer_id_str) != user_id:
            referrer_id = int(referrer_id_str)
            try:
                subscribed_any = False
                for channel in SPONSORS:
                    if await check_subscription(user_id, channel, context):
                        subscribed_any = True
                        break
                
                if subscribed_any:
                    with get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(
                                "INSERT INTO referrals (referrer_id, referred_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                                (referrer_id, user_id)
                            )
                            if cursor.rowcount > 0: # Если новая запись была вставлена
                                cursor.execute(
                                    "UPDATE users SET ref_count = ref_count + 1 WHERE user_id = %s",
                                    (referrer_id,)
                                )
                                print(f"[REF] Пользователь {user_id} засчитан как реферал для {referrer_id}")
                            else:
                                print(f"[REF] Пользователь {user_id} уже был рефералом для {referrer_id} (пропущено)")
                            conn.commit()
                else:
                    await update.message.reply_text(
                        "Чтобы считаться рефералом, нужно подписаться хотя бы на 1 канал-спонсора!",
                        reply_to_message_id=update.message.message_id
                    )
            except Exception as e:
                print(f"[ERROR] Ошибка при обработке реферала {user_id} от {referrer_id}: {e}")

    # 3. Приветственное сообщение
    welcome_text = (
        f"👋 <b>Привет, {first_name_tg}!</b>\n\n"
        "Добро пожаловать в наш регулярный <b>Telegram Giveaway!</b>\n\n"
        "🎁 <b>Приз недели:</b>\n"
        f"{PRIZE}\n\n"
        "Как участвовать?\n"
        "-----------------------\n"
        "1️⃣ <b>Подпишись на все каналы спонсоров:</b>\n"
        + ''.join(f"{i+1}. <a href='https://t.me/{chan.replace('@', '')}'>{chan}</a>\n" for i, chan in enumerate(SPONSORS) if chan) +
        "2️⃣ <b>Пригласи минимум 1 друга по своей реферальной ссылке</b> (получишь её ниже)\n"
        "3️⃣ <b>За каждого нового друга — ещё +1 билет на розыгрыш (макс. 10)</b>\n\n"
        "⏳ <b>Новый розыгрыш — каждую неделю!</b>\n\n"
        "❗️ <i>Чем больше рефералов — тем больше шансов на победу!</i>\n"
    )

    await update.message.reply_text(
        welcome_text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )

    # 4. Отправляем сообщение со статусом пользователя.
    text, markup = await build_status_message(user_id, first_name_tg, context)
    await update.message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)

# --- Обработчик кнопок ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    user_id = user.id
    first_name_tg = user.first_name
    
    if query.data == "refresh_status":
        text, markup = await build_status_message(user_id, first_name_tg, context)
        await query.edit_message_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
    
    elif query.data == "my_tickets":
        tickets = calculate_tickets(user_id)
        ref_count_from_db = 0
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT ref_count FROM users WHERE user_id = %s", (user_id,))
                    result = cursor.fetchone()
                    if result:
                        ref_count_from_db = result[0]
        except Exception as e:
            print(f"[ERROR] my_tickets ref_count fetch для user {user_id}: {e}")

        status = "✅ Вы участвуете!" if tickets > 0 else "⏳ Нужно 1 реферал для участия"
        text = (
            f"🎫 Ваши билеты: {tickets} / 10\n"
            f"👥 Ваши рефералы: {ref_count_from_db}\n"
            f"{status}\n\n"
            f"💡 Каждый новый реферал = +1 билет (макс. 10)"
        )
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
        ]), parse_mode=ParseMode.HTML)
 
    elif query.data == "leaderboard":
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT username, tickets FROM users WHERE tickets > 0 ORDER BY tickets DESC LIMIT 10")
                    rows = cursor.fetchall()
        except Exception as e:
            await query.edit_message_text("Ошибка при получении лидерборда.", parse_mode=ParseMode.HTML)
            print(f"[ERROR] leaderboard callback: {e}")
            return
        
        if not rows:
            await query.edit_message_text("Пока никто не заработал билеты.", parse_mode=ParseMode.HTML)
            return

        text = "<b>🏆 Лидерборд по билетам:</b>\n\n"
        for i, row in enumerate(rows, 1):
            username_from_db = row[0] or ""
            masked = mask_username(username_from_db)
            tickets = row[1]
            text += f"{i}. <b>{masked}</b> — {tickets} билетов\n"

        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
            ])
        )
        
    elif query.data == "my_reflink":
        # Используем BOT_USERNAME_FOR_REFLINK
        link = f"https://t.me/{BOT_USERNAME_FOR_REFLINK}?start={user_id}"
        text = (
            f"🔗 Ваша реферальная ссылка:\n\n<code>{link}</code>\n\n"
            f"📤 Отправьте друзьям! Каждый, кто перейдёт и запустит бота, засчитается как реферал.\n"
            f"💡 Чем больше друзей — тем больше билетов!"
        )
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
        ]), parse_mode=ParseMode.HTML)
    
    elif query.data == "rules":
        text = (
            "📜 <b>ПРАВИЛА РОЗЫГРЫША:</b>\n\n"
            "1️⃣ Подпишитесь на все каналы спонсоров\n"
            "2️⃣ Пригласите минимум 1 друга по вашей ссылке\n"
            "3️⃣ Каждый дополнительный реферал = +1 билет (макс. 10)\n"
            "4️⃣ Розыгрыш каждые 7 дней\n"
            "5️⃣ Победители связываются с админом в течение 48 часов\n\n"
            "⚠️ Приз аннулируется при отсутствии контакта!"
        )
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
        ]), parse_mode=ParseMode.HTML)
    
    elif query.data == "back_to_main":
        text, markup = await build_status_message(user_id, first_name_tg, context)
        await query.edit_message_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)

# --- Команды администратора ---

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("Нет доступа!")
        return
    await update.message.reply_text(
        "🔒 Админ-панель:\n"
        "/draw — запустить розыгрыш\n"
        "/stats — показать участников\n"
        "/leaderboard — показать лидерборд (как для пользователя)"
    )

# Команда для розыгрыша (с взвешенным выбором)
async def draw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("Нет доступа!")
        return
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT user_id, username, tickets FROM users WHERE tickets > 0")
                participants_data = cursor.fetchall()
        
        if not participants_data:
            await update.message.reply_text("Нет участников для розыгрыша.")
            return
        
        ticket_pool = []
        for p_id, p_username, p_tickets in participants_data:
            ticket_pool.extend([(p_id, p_username)] * p_tickets)
        
        if not ticket_pool:
            await update.message.reply_text("Ошибка при формировании пула билетов. Возможно, нет активных билетов.")
            return

        winner_id, winner_username = random.choice(ticket_pool)
        
        winner_total_tickets = next((p[2] for p in participants_data if p[0] == winner_id), 0)

        await update.message.reply_text(
            f"🎉 Победитель: @{winner_username or 'user_' + str(winner_id)} (ID: <code>{winner_id}</code>), билетов: <b>{winner_total_tickets}</b>",
            parse_mode=ParseMode.HTML
        )

    except Exception as e:
        print(f"[ERROR] draw command: {e}")
        await update.message.reply_text("Произошла ошибка при проведении розыгрыша.")

# Команда для просмотра всех участников
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("Нет доступа!")
        return
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT user_id, username, ref_count, tickets, all_subscribed FROM users ORDER BY tickets DESC, ref_count DESC")
                participants = cursor.fetchall()
        
        if not participants:
            await update.message.reply_text("Нет зарегистрированных пользователей.")
            return
        
        text = "<b>🎫 Статистика пользователей:</b>\n\n"
        for i, u in enumerate(participants):
            user_id_db, username_db, ref_count_db, tickets_db, all_subscribed_db = u
            masked_username = mask_username(username_db or f"user_{user_id_db}")
            status_sub = "✅" if all_subscribed_db == 1 else "❌"
            text += (
                f"{i+1}. <b>{masked_username}</b> (ID: <code>{user_id_db}</code>)\n"
                f"   Рефералов: {ref_count_db}, Билетов: {tickets_db}, Подписка: {status_sub}\n"
            )
            
            if len(text) > 3000 and i < len(participants) - 1:
                await update.message.reply_text(text, parse_mode=ParseMode.HTML)
                text = "<b>(Продолжение статистики)</b>\n\n"

        await update.message.reply_text(text, parse_mode=ParseMode.HTML)

    except Exception as e:
        print(f"[ERROR] stats command: {e}")
        await update.message.reply_text("Произошла ошибка при получении статистики.")

# Команда /leaderboard для админов и пользователей
async def leaderboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT username, tickets FROM users WHERE tickets > 0 ORDER BY tickets DESC LIMIT 10")
                rows = cursor.fetchall()
    except Exception as e:
        await update.message.reply_text("Ошибка при получении лидерборда.")
        print(f"[ERROR] leaderboard command: {e}")
        return

    if not rows:
        await update.message.reply_text("Пока никто не заработал билеты.")
        return

    text = "<b>🏆 Лидерборд по билетам:</b>\n\n"
    for i, row in enumerate(rows, 1):
        username_from_db = row[0] or ""
        masked = mask_username(username_from_db)
        tickets = row[1]
        text += f"{i}. <b>{masked}</b> — {tickets} билетов\n"

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


# --- Запуск бота ---
def main():
    init_db()
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))

    # Добавляем обработчики для админ-команд
    application.add_handler(CommandHandler("admin", admin_panel))
    application.add_handler(CommandHandler("draw", draw))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("leaderboard", leaderboard_command))
    
    print("✅ Бот запущен!")
    application.run_polling()

if __name__ == "__main__":
    main()
