import os
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
import psycopg2
from psycopg2.extras import RealDictCursor

# ====== ВАШ ТОКЕН ======
BOT_TOKEN = os.getenv("BOT_TOKEN", "8576715226:AAGPd2BSCT8mDm6hMp-1c1XYS-7PL0QAG3E")

# ====== НАСТРОЙКИ ======
SPONSORS = ["@openbusines", "@SAGkatalog", ""]  # ← ЗАМЕНИТЕ НА СВОИ КАНАЛЫ!
PRIZE = "🎁 Telegram Premium на 6 месяцев ИЛИ 1500 ⭐"

# Подключение к PostgreSQL
def get_db_connection():
    DATABASE_URL = os.getenv("MY_DATABASE_URL")  # ← вот оно!
    if not DATABASE_URL:
        raise ValueError("MY_DATABASE_URL не установлен. Настройте в Railway.")
    return psycopg2.connect(DATABASE_URL, sslmode='require')
# Инициализация базы данных
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
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
    cursor.close()
    conn.close()

# Проверка подписки на канал
async def check_subscription(user_id, channel, context):
    try:
        chat_member = await context.bot.get_chat_member(chat_id=channel, user_id=user_id)
        
        # Логирование результата запроса
        print(f"[DEBUG] Проверка {channel} для user_id {user_id} — статус: {chat_member.status}")
        
        return chat_member.status in ["member", "administrator", "creator"]
    except Exception as e:
        # Логировать именно user_id, канал и ошибку!
        print(f"[ERROR] channel: {channel}, user_id: {user_id}, ошибка: {e}")
        return False

# Расчёт билетов
def calculate_tickets(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ref_count, all_subscribed FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if not result or result[1] == 0:
        return 0
    if result[0] < 2:
        return 0
    return min(10, 1 + max(0, result[0] - 2))

# Формирование сообщения статуса
async def build_status_message(user_id, username, context):
    # Проверяем подписки
    subscribed_channels = []
    unsubscribed_channels = []
    
    for channel in SPONSORS:
        if await check_subscription(user_id, channel, context):
            subscribed_channels.append(f"✅ {channel}")
        else:
            unsubscribed_channels.append(f"❌ {channel}")
    
    all_subscribed = len(unsubscribed_channels) == 0
    
    # Сохраняем статус в БД
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO users (user_id, username, all_subscribed, last_checked) VALUES (%s, %s, %s, CURRENT_TIMESTAMP) ON CONFLICT (user_id) DO UPDATE SET all_subscribed = %s, last_checked = CURRENT_TIMESTAMP",
        (user_id, username, 1 if all_subscribed else 0, 1 if all_subscribed else 0)
    )
    conn.commit()
    cursor.close()
    conn.close()
    
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
            f"🎉 Привет, {username}!\n\n"
            f"🎁 Приз этой недели:\n{PRIZE}\n\n"
            f"✅ Вы подписаны на все каналы!\n" +
            "\n".join(subscribed_channels) + "\n\n"
            f"🎫 Ваши билеты: {tickets} / 10\n"
            f"👥 Рефералов: {tickets + 1 if tickets > 0 else 0} (минимум 2 для участия)\n\n"
            f"💡 Каждый новый реферал = +1 билет (макс. 10)"
        )
    
    # Кнопки
    keyboard = [
        [InlineKeyboardButton("🎫 Мои билеты", callback_data="my_tickets")],
        [InlineKeyboardButton("🔗 Моя реферальная ссылка", callback_data="my_reflink")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")],
        [InlineKeyboardButton("🏆 Условия розыгрыша", callback_data="rules")]
    ]
    return status_text, InlineKeyboardMarkup(keyboard)

# Команда /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print("[DEBUG] Запущена команда /start")
    user = update.effective_user
    user_id = user.id
    username = user.first_name
    
    # Сохраняем пользователя
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO users (user_id, username) VALUES (%s, %s) ON CONFLICT (user_id) DO NOTHING",
        (user_id, user.username or f"user_{user_id}")
    )
    
    # Обработка реферала с антибот-защитой (подписка хотя бы на 1 канал)
if context.args:
    referrer_id = context.args[0]
    if referrer_id.isdigit() and int(referrer_id) != user_id:
        try:
            # Проверяем: подписан ли пользователь хотя бы на один канал-спонсор
            subscribed_any = False
            for channel in SPONSORS:
                if channel and await check_subscription(user_id, channel, context):
                    subscribed_any = True
                    break
            if subscribed_any:
                cursor.execute(
                    "INSERT INTO referrals (referrer_id, referred_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (int(referrer_id), user_id)
                )
                cursor.execute(
                    "UPDATE users SET ref_count = ref_count + 1 WHERE user_id = %s",
                    (int(referrer_id),)
                )
                print(f"[REF] Пользователь {user_id} засчитан как реферал для {referrer_id}")
            else:
                else:
    try:
        await update.message.reply_text(
            "Чтобы считаться рефералом, нужно подписаться хотя бы на 1 канал-спонсора!"
        )
    except Exception:
        pass
        except Exception as e:
            print(f"Ошибка реферала: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    # Отправляем статус
    text, markup = await build_status_message(user_id, username, context)
    await update.message.reply_text(text, reply_markup=markup)

# Обработчик кнопок
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print("[DEBUG] Обработчик кнопки вызван")
    query = update.callback_query
    await query.answer()
    user = query.from_user
    user_id = user.id
    username = user.first_name
    
    if query.data == "refresh_status":
        text, markup = await build_status_message(user_id, username, context)
        await query.edit_message_text(text, reply_markup=markup)
    
    elif query.data == "my_tickets":
        tickets = calculate_tickets(user_id)
        status = "✅ Вы участвуете!" if tickets > 0 else "⏳ Нужно 2 реферала для участия"
        text = f"🎫 Ваши билеты: {tickets} / 10\n{status}\n\n💡 Каждый новый реферал = +1 билет!"
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
        ]))
    
    elif query.data == "my_reflink":
        link = f"https://t.me/moy_giveaway_bot?start={user_id}"  # ← ЗАМЕНИТЕ НА ЮЗЕРНЕЙМ ВАШЕГО БОТА!
        text = (
            f"🔗 Ваша реферальная ссылка:\n\n{link}\n\n"
            f"📤 Отправьте друзьям! Каждый, кто перейдёт и запустит бота, засчитается как реферал.\n"
            f"💡 Чем больше друзей — тем больше билетов!"
        )
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
        ]))
    
    elif query.data == "rules":
        text = (
            "📜 ПРАВИЛА РОЗЫГРЫША:\n\n"
            "1️⃣ Подпишитесь на все 3 канала спонсоров\n"
            "2️⃣ Пригласите минимум 2 друзей по вашей ссылке\n"
            "3️⃣ Каждый дополнительный реферал = +1 билет (макс. 10)\n"
            "4️⃣ Розыгрыш каждые 7 дней\n"
            "5️⃣ Победители связываются с админом (@ваш_юзернейм) в течение 48 часов\n\n"
            "⚠️ Приз аннулируется при отсутствии контакта!"
        )
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
        ]))
    
    elif query.data == "back_to_main":
        text, markup = await build_status_message(user_id, username, context)
        await query.edit_message_text(text, reply_markup=markup)

# Запуск бота
def main():
    init_db()
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))

# Добавь ЭТИ три строки:
    application.add_handler(CommandHandler("admin", admin_panel))
    application.add_handler(CommandHandler("draw", draw))
    application.add_handler(CommandHandler("stats", stats))
    
    print("✅ Бот запущен с PostgreSQL!")
    application.run_polling()

import random

# Список user_id админов (укажи свои!)
ADMINS = [514167463]  # ← сюда добавь свои user_id (можно узнать через @getmyid_bot в Telegram)

# Команда /admin - простая админ-панель
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("Нет доступа!")
        return
    await update.message.reply_text(
        "🔒 Админ-панель:\n"
        "/draw — запустить розыгрыш\n"
        "/stats — показать участников"
    )

# Команда для розыгрыша
async def draw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("Нет доступа!")
        return
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, username, tickets FROM users WHERE tickets > 0")
    participants = cursor.fetchall()
    cursor.close()
    conn.close()
    if not participants:
        await update.message.reply_text("Нет участников для розыгрыша.")
        return
    winner = random.choice(participants)
    await update.message.reply_text(
        f"🎉 Победитель: @{winner[1] or 'user_' + str(winner[0])} (id: {winner[0]}), билетов: {winner[2]}"
    )

# Команда для просмотра всех участников
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("Нет доступа!")
        return
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, username, tickets FROM users WHERE tickets > 0 ORDER BY tickets DESC")
    participants = cursor.fetchall()
    cursor.close()
    conn.close()
    if not participants:
        await update.message.reply_text("Нет участников, соответствующих условиям.")
        return
    text = "\n".join([f"{i+1}. @{u[1] or 'user_' + str(u[0])} — билетов: {u[2]}" for i, u in enumerate(participants)])
    await update.message.reply_text("🎫 Участники:\n" + text)

if __name__ == "__main__":
    main()
