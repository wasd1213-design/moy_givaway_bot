import sqlite3
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from flask import Flask, request
import threading
import os

# Токен из переменных окружения Railway
BOT_TOKEN = os.getenv("BOT_TOKEN", "8576715226:AAGPd2BSCT8mDm6hMp-1c1XYS-7PL0QAG3E")

# Настройки
SPONSORS = ["@openbusines", "@sponsor2", "@sponsor3"]  # ← ЗАМЕНИТЕ НА СВОИ!
PRIZE = "🎁 Telegram Premium на 6 месяцев ИЛИ 1500 ⭐"

# Инициализация базы данных
def init_db():
    conn = sqlite3.connect('giveaway.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            ref_count INTEGER DEFAULT 0,
            tickets INTEGER DEFAULT 0,
            joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS referrals (
            referrer_id INTEGER,
            referred_id INTEGER,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(referrer_id, referred_id)
        )
    ''')
    conn.commit()
    conn.close()

# Проверка подписки
async def check_subscription(user_id, channel, context):
    try:
        chat_id = channel.lstrip('@')
        chat_member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        return chat_member.status in ["member", "administrator", "creator"]
    except:
        return False

# Расчёт билетов
def calculate_tickets(user_id):
    conn = sqlite3.connect('giveaway.db')
    cursor = conn.cursor()
    cursor.execute("SELECT ref_count FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    if not result or result[0] < 2:
        return 0
    return min(10, 1 + max(0, result[0] - 2))

# Формирование сообщения
async def build_status_message(user_id, username, context):
    subscribed_channels = []
    unsubscribed_channels = []
    
    for channel in SPONSORS:
        if await check_subscription(user_id, channel, context):
            subscribed_channels.append(f"✅ {channel}")
        else:
            unsubscribed_channels.append(f"❌ {channel}")
    
    all_subscribed = len(unsubscribed_channels) == 0
    tickets = calculate_tickets(user_id) if all_subscribed else 0
    
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
            f"👥 Рефералов: {tickets + 1 if tickets > 0 else 0}\n\n"
            f"💡 Каждый новый реферал = +1 билет (макс. 10)"
        )
    
    keyboard = [
        [InlineKeyboardButton("🎫 Мои билеты", callback_data="my_tickets")],
        [InlineKeyboardButton("🔗 Моя реферальная ссылка", callback_data="my_reflink")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")],
        [InlineKeyboardButton("🏆 Условия розыгрыша", callback_data="rules")]
    ]
    return status_text, InlineKeyboardMarkup(keyboard)

# Команда /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    username = user.first_name
    
    conn = sqlite3.connect('giveaway.db')
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)", (user_id, user.username or f"user_{user_id}"))
    
    if context.args:
        referrer_id = context.args[0]
        if referrer_id.isdigit() and int(referrer_id) != user_id:
            try:
                cursor.execute("INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?, ?)", (int(referrer_id), user_id))
                cursor.execute("UPDATE users SET ref_count = ref_count + 1 WHERE user_id = ?", (int(referrer_id),))
            except:
                pass
    
    conn.commit()
    conn.close()
    
    text, markup = await build_status_message(user_id, username, context)
    await update.message.reply_text(text, reply_markup=markup)

# Обработчик кнопок
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        status = "✅ Вы участвуете!" if tickets > 0 else "⏳ Нужно 2 реферала"
        text = f"🎫 Ваши билеты: {tickets} / 10\n{status}\n\n💡 Каждый новый реферал = +1 билет!"
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
        ]))
    
    elif query.data == "my_reflink":
        link = f"https://t.me/@moy_giveaway_bot?start={user_id}"  # ← ЗАМЕНИТЕ!
        text = f"🔗 Ваша реферальная ссылка:\n\n{link}\n\n📤 Отправьте друзьям!"
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]
        ]))
    
    elif query.data == "rules":
        text = "📜 ПРАВИЛА РОЗЫГРЫША:\n\n1️⃣ Подпишитесь на все 3 канала\n2️⃣ Пригласите минимум 2 друзей\n3️⃣ Каждый реферал = +1 билет (макс. 10)\n4️⃣ Розыгрыш каждые 7 дней"
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
    
    print("✅ Бот запущен на Railway!")
    application.run_polling()

# Flask веб-сервер для Railway
app = Flask(__name__)

@app.route('/')
def index():
    return "Bot is running!"

def run_bot():
    main()

if __name__ == "__main__":
    # Запускаем бота в отдельном потоке
    bot_thread = threading.Thread(target=run_bot)
    bot_thread.daemon = True
    bot_thread.start()
    
    # Запускаем веб-сервер
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8000)))
