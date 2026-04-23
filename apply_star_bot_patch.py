from pathlib import Path
import re
import sys
import py_compile

main_path = Path("main.py")
spin_path = Path("is_can_spin_server.py")

if not main_path.exists():
    print("ERROR: main.py not found")
    sys.exit(1)

if not spin_path.exists():
    print("ERROR: is_can_spin_server.py not found")
    sys.exit(1)

main = main_path.read_text(encoding="utf-8")
spin = spin_path.read_text(encoding="utf-8")

# -----------------------------
# main.py patches
# -----------------------------

# 1) constants
main = re.sub(
    r"PREMIUM_COST = 1000\nWITHDRAW_MIN = 700",
    "PREMIUM_COST = 1000\nWITHDRAW_MIN = 700\nFIRST_INVITED_FRIEND_BONUS = 20\nFIRST_INVITED_FRIEND_BONUS_PERCENT = 5",
    main,
    count=1,
)

# 2) FAQ start/wheel text
main = main.replace(
'''    "start": {
        "title": "🔰 Начало работы",
        "text": (
            "❓ <b>Как начать пользоваться ботом?</b>\\n"
            "• Нажмите /start или кнопку «🌠 Звёздное Колесо» в меню\\n"
            "• Подпишитесь на всех активных спонсоров\\n"
            "• Пригласите 2 активных друга\\n"
            "• После выполнения условий доступ к Колесу откроется автоматически ✅\\n\\n"
            "❓ <b>Кто считается активным другом?</b>\\n"
            "Активный друг — это пользователь, который:\\n"
            "• перешёл по вашей ссылке\\n"
            "• подписался на 2 основных спонсорских канала\\n\\n"
            "⚠️ Временный спонсор (слот 3) не влияет на активацию друга.\\n\\n"
            "❓ <b>Где взять свою ссылку?</b>\\n"
            "Откройте «👤 Профиль» — там будет ваша персональная ссылка для приглашений."
        ),
    },''',
'''    "start": {
        "title": "🔰 Начало работы",
        "text": (
            "❓ <b>Как начать пользоваться ботом?</b>\\n"
            "• Нажмите /start\\n"
            "• Сделайте приветственный спин\\n"
            "• Подпишитесь на 2 основных спонсорских канала\\n"
            "• После подписки доступ к Колесу откроется автоматически ✅\\n\\n"
            "❓ <b>Как работают друзья?</b>\\n"
            "• Приглашённый друг — это пользователь, который перешёл по вашей ссылке и запустил бота\\n"
            "• Активный друг — это приглашённый пользователь, который подписался на 2 основных спонсоров\\n\\n"
            "🎁 <b>Бонус за первого приглашённого друга:</b> +20⭐ и +5% к шансу\\n\\n"
            "❓ <b>Где взять свою ссылку?</b>\\n"
            "Откройте «👤 Профиль» — там будет ваша персональная ссылка и кнопка поделиться."
        ),
    },'''
)

main = main.replace(
'''    "wheel": {
        "title": "🌠 Звёздное Колесо",
        "text": (
            "❓ <b>Как крутить Колесо?</b>\\n"
            f"• Бесплатно: 1 раз в 6 часов\\n"
            f"• Дополнительный спин: за {EXTRA_SPIN_COST} ⭐\\n"
            "• Открыть Колесо можно через кнопку в меню или WebApp\\n\\n"
            "❓ <b>Почему Колесо недоступно?</b>\\n"
            "Проверьте:\\n"
            "• подписку на всех активных спонсоров\\n"
            "• наличие 2 активных друзей\\n"
            "• не действует ли ещё таймер 6 часов\\n\\n"
            "❓ <b>Как обновить статус?</b>\\n"
            "Нажмите «🔄 Обновить статус» под главным сообщением или введите /start."
        ),
    },''',
'''    "wheel": {
        "title": "🌠 Звёздное Колесо",
        "text": (
            "❓ <b>Как крутить Колесо?</b>\\n"
            f"• Бесплатно: 1 раз в 6 часов\\n"
            f"• Дополнительный спин: за {EXTRA_SPIN_COST} ⭐\\n"
            "• Открыть Колесо можно после подписки на 2 основных спонсоров\\n\\n"
            "❓ <b>Почему Колесо недоступно?</b>\\n"
            "Проверьте:\\n"
            "• подписку на всех активных спонсоров\\n"
            "• не действует ли ещё таймер 6 часов\\n\\n"
            "❓ <b>Как зарабатывать больше?</b>\\n"
            "Приглашайте друзей: первый приглашённый друг даёт +20⭐ и +5% к шансу, а активные друзья повышают уровень."
        ),
    },'''
)

# 3) new level logic
main = re.sub(
    r"""def get_level_info\(ref_count: int\):\n(?:    .*\n)+?    return \{\n            "name": "Bronze",\n            "emoji": "🥉",\n            "bonus_percent": 0,\n            "next_target": 5,\n            "next_name": "Silver",\n        \}\n""",
'''def get_level_info(ref_count: int):
    if ref_count >= 12:
        return {
            "name": "Diamond",
            "emoji": "💎",
            "bonus_percent": 60,
            "next_target": None,
            "next_name": None,
        }
    if ref_count >= 8:
        return {
            "name": "Gold",
            "emoji": "🥇",
            "bonus_percent": 35,
            "next_target": 12,
            "next_name": "Diamond",
        }
    if ref_count >= 4:
        return {
            "name": "Silver",
            "emoji": "🥈",
            "bonus_percent": 15,
            "next_target": 8,
            "next_name": "Gold",
        }
    return {
        "name": "Bronze",
        "emoji": "🥉",
        "bonus_percent": 0,
        "next_target": 4,
        "next_name": "Silver",
    }
''',
    main,
    count=1,
    flags=re.MULTILINE
)

# 4) welcome inline text remains, no patch needed

# 5) DB columns in CREATE TABLE users
main = main.replace(
"                        lifetime_ref_count INT DEFAULT 0,\n",
"                        lifetime_ref_count INT DEFAULT 0,\n                        invited_ref_count INT DEFAULT 0,\n",
)

main = main.replace(
"                        welcome_spin_used BOOLEAN DEFAULT FALSE\n",
"                        welcome_spin_used BOOLEAN DEFAULT FALSE,\n                        paid_spins INT DEFAULT 0,\n                        first_invite_bonus_paid BOOLEAN DEFAULT FALSE\n",
)

# 6) alter statements
main = main.replace(
'''                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS lifetime_ref_count INT DEFAULT 0",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS weekly_hold_bonus_count INT DEFAULT 0",''',
'''                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS lifetime_ref_count INT DEFAULT 0",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS invited_ref_count INT DEFAULT 0",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS paid_spins INT DEFAULT 0",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS first_invite_bonus_paid BOOLEAN DEFAULT FALSE",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS weekly_hold_bonus_count INT DEFAULT 0",'''
)

# 7) count_valid_refs rewrite
pattern = r"""async def count_valid_refs\(referrer_id: int, context: ContextTypes\.DEFAULT_TYPE\) -> int:\n(?:.*\n)+?    return active_count\n"""
replacement = '''async def count_valid_refs(referrer_id: int, context: ContextTypes.DEFAULT_TYPE) -> int:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    active_count = 0
    invited_count = 0

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT referred_id FROM referrals WHERE referrer_id=%s",
                (referrer_id,),
            )
            rows = cur.fetchall()

            main_sponsors = await get_active_sponsors(include_temp=False)

            for (referred_id,) in rows:
                invited_count += 1
                valid_now = True

                if len(main_sponsors) < 2:
                    valid_now = False
                else:
                    for sponsor in main_sponsors:
                        if not await check_subscription(referred_id, sponsor["channel_username"], context):
                            valid_now = False
                            break

                if valid_now:
                    cur.execute(
                        """
                        UPDATE referrals
                        SET is_valid = TRUE,
                            checked_at = %s,
                            inactive_since = NULL
                        WHERE referrer_id = %s AND referred_id = %s
                        """,
                        (now, referrer_id, referred_id),
                    )
                    active_count += 1
                else:
                    cur.execute(
                        """
                        UPDATE referrals
                        SET is_valid = FALSE,
                            checked_at = %s
                        WHERE referrer_id = %s AND referred_id = %s
                        """,
                        (now, referrer_id, referred_id),
                    )

            cur.execute(
                """
                UPDATE users
                SET active_ref_count = %s,
                    invited_ref_count = %s
                WHERE user_id = %s
                RETURNING COALESCE(first_invite_bonus_paid, FALSE)
                """,
                (active_count, invited_count, referrer_id),
            )
            row = cur.fetchone()
            first_bonus_paid = bool(row[0]) if row else False

            if invited_count >= 1 and not first_bonus_paid:
                cur.execute(
                    """
                    UPDATE users
                    SET tickets = COALESCE(tickets, 0) + %s,
                        activation_bonus_percent = CASE
                            WHEN COALESCE(activation_bonus_percent, 0) < %s THEN %s
                            ELSE activation_bonus_percent
                        END,
                        first_invite_bonus_paid = TRUE
                    WHERE user_id = %s
                    """,
                    (
                        FIRST_INVITED_FRIEND_BONUS,
                        FIRST_INVITED_FRIEND_BONUS_PERCENT,
                        FIRST_INVITED_FRIEND_BONUS_PERCENT,
                        referrer_id,
                    ),
                )
                conn.commit()
                try:
                    await context.bot.send_message(
                        chat_id=referrer_id,
                        text=(
                            "🎉 <b>Поздравляем!</b>\\n\\n"
                            f"Вы пригласили первого друга и получили <b>+{FIRST_INVITED_FRIEND_BONUS}⭐</b> "
                            f"и <b>+{FIRST_INVITED_FRIEND_BONUS_PERCENT}%</b> к шансу звёздных секторов."
                        ),
                        parse_mode=ParseMode.HTML,
                    )
                except Exception as e:
                    print("first invite reward notify error:", e)
            else:
                conn.commit()

    return active_count
'''
main = re.sub(pattern, replacement, main, count=1, flags=re.MULTILINE)

# 8) get_user_state select invited_ref_count
main = main.replace(
'''                    COALESCE(activated, FALSE),
                    COALESCE(active_ref_count, 0),
                    COALESCE(tickets, 0),''',
'''                    COALESCE(activated, FALSE),
                    COALESCE(active_ref_count, 0),
                    COALESCE(invited_ref_count, 0),
                    COALESCE(tickets, 0),'''
)

main = main.replace(
'''    ref_count = 0
    stars = 0''',
'''    ref_count = 0
    invited_ref_count = 0
    stars = 0'''
)

main = main.replace(
'''            activated,
            ref_count,
            stars,''',
'''            activated,
            ref_count,
            invited_ref_count,
            stars,''',
)

main = main.replace(
'''        "ref_count": ref_count,
        "stars": stars,''',
'''        "ref_count": ref_count,
        "invited_ref_count": invited_ref_count,
        "stars": stars,''',
)

# 9) build invite text with share info
main = re.sub(
    r"""def build_invite_text\(user_id: int\):\n    reflink = f"https://t\.me/\{BOT_USERNAME_FOR_REFLINK\}\?start=\{user_id\}"\n    return \(\n(?:.*\n)+?    \)\n""",
'''def build_invite_text(user_id: int):
    reflink = f"https://t.me/{BOT_USERNAME_FOR_REFLINK}?start={user_id}"
    share_text = f"Здесь можно получать звёзды и обменивать их на призы ⭐\\n\\nМоя ссылка: {reflink}"
    return (
        "📨 <b>Ваша ссылка для приглашения</b>\\n\\n"
        f"<code>{reflink}</code>\\n\\n"
        "👥 <b>Как считаются друзья:</b>\\n"
        "• Приглашённый друг — просто зашёл по вашей ссылке и запустил бота\\n"
        "• Активный друг — подписался на 2 основных спонсоров\\n\\n"
        f"📤 <b>Текст для отправки:</b>\\n<code>{share_text}</code>"
    )
''',
    main,
    count=1,
    flags=re.MULTILINE
)

# 10) add invite keyboard helper
insert_after = 'def build_invite_text(user_id: int):'
if 'def get_invite_inline(user_id: int):' not in main:
    idx = main.find(insert_after)
    if idx != -1:
        end = main.find('\n\nasync def get_welcome_start_text', idx)
        block = '''

def get_invite_inline(user_id: int):
    reflink = f"https://t.me/{BOT_USERNAME_FOR_REFLINK}?start={user_id}"
    share_text = f"Здесь можно получать звёзды и обменивать их на призы ⭐\\n\\nМоя ссылка: {reflink}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📤 Поделиться ссылкой", switch_inline_query=share_text)],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")],
    ])
'''
        main = main[:end] + block + main[end:]

# 11) welcome text
main = re.sub(
    r"""async def get_welcome_start_text\(user_id, first_name, context\):\n    state = await get_user_state\(user_id, context\)\n    return \(\n(?:.*\n)+?    \)\n""",
'''async def get_welcome_start_text(user_id, first_name, context):
    state = await get_user_state(user_id, context)
    return (
        f"👋 <b>Привет, {first_name}!</b>\\n\\n"
        f"Тебе уже доступен <b>приветственный спин</b> 🎁\\n\\n"
        f"Забирай первые звёзды прямо сейчас и запускай Колесо 👇\\n\\n"
        f"⭐ <b>Баланс:</b> {state['stars']}"
    )
''',
    main,
    count=1,
    flags=re.MULTILINE
)

# 12) get_main_inline should show invite/share for all
main = re.sub(
    r"""def get_main_inline\(activated: bool\):\n    if not activated:\n        return InlineKeyboardMarkup\(\[\n            \[InlineKeyboardButton\("📢 Подписаться на спонсоров", callback_data="show_sponsors"\)\],\n            \[InlineKeyboardButton\("📨 Моя ссылка", callback_data="show_invite"\)\],\n            \[InlineKeyboardButton\("🔄 Обновить статус", callback_data="check_sub"\)\],\n        \]\)\n\n    return InlineKeyboardMarkup\(\[\n        \[InlineKeyboardButton\("🔄 Обновить статус", callback_data="check_sub"\)\]\n    \]\)\n""",
'''def get_main_inline(activated: bool):
    if not activated:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Подписаться на спонсоров", callback_data="show_sponsors")],
            [InlineKeyboardButton("📨 Моя ссылка", callback_data="show_invite")],
            [InlineKeyboardButton("🔄 Обновить статус", callback_data="check_sub")],
        ])

    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📨 Моя ссылка", callback_data="show_invite")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data="check_sub")]
    ])
''',
    main,
    count=1,
    flags=re.MULTILINE
)

# 13) support old callback aliases
main = main.replace(
'        if data in ("check_sub", "back_to_main"):',
'        if data in ("check_sub", "check_subs", "back_to_main"):'
)
main = main.replace(
'        elif data == "show_invite":',
'        elif data in ("show_invite", "my_ref_link"):'
)
main = main.replace(
'''                build_invite_text(uid),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_main")]]
                ),''',
'''                build_invite_text(uid),
                parse_mode=ParseMode.HTML,
                reply_markup=get_invite_inline(uid),'''
)

# 14) activation now by subscription in get_user_state
main = main.replace(
'''            cur.execute(
                "UPDATE users SET all_subscribed = %s WHERE user_id = %s",
                (1 if all_subs_ok else 0, user_id),
            )''',
'''            cur.execute(
                "UPDATE users SET all_subscribed = %s WHERE user_id = %s",
                (1 if all_subs_ok else 0, user_id),
            )
            main_sponsors = [s for s in sponsors if s["sponsor_type"] == "main"]
            should_activate = all_subs_ok and len(main_sponsors) >= 2
            cur.execute(
                """
                UPDATE users
                SET activated = %s
                WHERE user_id = %s
                """,
                (should_activate, user_id),
            )'''
)

# 15) reply menu unlock profile/help before activation, wheel locked only by welcome/subscription state
main = re.sub(
    r"""def get_reply_menu\(user_id: int, activated: bool, bonus_percent: int = 0, welcome_spin_used: bool = True\):\n(?:.*\n)+?    return ReplyKeyboardMarkup\(\n        \[\n            \[\n                KeyboardButton\(\n                    f"🌠 Звёздное Колесо \(\+\{bonus_percent\}%\)",\n                    web_app=WebAppInfo\(url=f"\{WEBAPP_URL\}\?user_id=\{user_id\}"\),\n                \),\n            \],\n            \[\n                KeyboardButton\("👤 Профиль"\),\n                KeyboardButton\("🔄 Обмен звёзд"\),\n            \],\n            \[\n                KeyboardButton\("🏆 Лидерборд"\),\n                KeyboardButton\("❓ Помощь"\),\n            \],\n        \],\n        resize_keyboard=True,\n    \)\n""",
'''def get_reply_menu(user_id: int, activated: bool, bonus_percent: int = 0, welcome_spin_used: bool = True):
    if not welcome_spin_used:
        return ReplyKeyboardMarkup(
            [
                [KeyboardButton("🌠 Звёздное Колесо")],
                [KeyboardButton("👤 Профиль"), KeyboardButton("🔒 Обмен звёзд")],
                [KeyboardButton("🏆 Лидерборд"), KeyboardButton("❓ Помощь")],
            ],
            resize_keyboard=True,
        )

    if not activated:
        return ReplyKeyboardMarkup(
            [
                [KeyboardButton("🔒 Звёздное Колесо")],
                [KeyboardButton("👤 Профиль"), KeyboardButton("🔒 Обмен звёзд")],
                [KeyboardButton("🏆 Лидерборд"), KeyboardButton("❓ Помощь")],
            ],
            resize_keyboard=True,
        )

    return ReplyKeyboardMarkup(
        [
            [
                KeyboardButton(
                    f"🌠 Звёздное Колесо (+{bonus_percent}%)",
                    web_app=WebAppInfo(url=f"{WEBAPP_URL}?user_id={user_id}"),
                ),
            ],
            [
                KeyboardButton("👤 Профиль"),
                KeyboardButton("🔄 Обмен звёзд"),
            ],
            [
                KeyboardButton("🏆 Лидерборд"),
                KeyboardButton("❓ Помощь"),
            ],
        ],
        resize_keyboard=True,
    )
''',
    main,
    count=1,
    flags=re.MULTILINE
)

# 16) locked wheel text
main = main.replace(
'''        await update.message.reply_text(
            (
                f"🔒 <b>Звёздное Колесо пока недоступно</b>\\n\\n"
                f"Чтобы открыть доступ, нужно выполнить условия:\\n"
                f"• Подписки на спонсоров: <b>{subs_status}</b>\\n"
                f"• Активные друзья: <b>{state['ref_count']}/2</b>\\n\\n"
                f"После выполнения условий нажмите <b>«Обновить статус»</b> на главном экране."
            ),
            parse_mode=ParseMode.HTML,
        )''',
'''        await update.message.reply_text(
            (
                f"🔒 <b>Звёздное Колесо пока недоступно</b>\\n\\n"
                f"Чтобы открыть доступ, нужно:\\n"
                f"• подписаться на всех активных спонсоров\\n\\n"
                f"Текущий статус подписок: <b>{subs_status}</b>\\n\\n"
                f"После подписки нажмите <b>«Обновить статус»</b> на главном экране."
            ),
            parse_mode=ParseMode.HTML,
        )'''
)

# 17) exchange locked text
main = main.replace(
'''        await update.message.reply_text(
            (
                f"🔒 <b>Обмен звёзд пока недоступен</b>\\n\\n"
                f"Сначала откройте Звёздное Колесо:\\n"
                f"1️⃣ Подпишитесь на всех активных спонсоров\\n"
                f"2️⃣ Пригласите 2 активных друзей\\n\\n"
                f"После активации Колеса раздел обмена станет доступен."
            ),
            parse_mode=ParseMode.HTML,
        )''',
'''        await update.message.reply_text(
            (
                f"🔒 <b>Обмен звёзд пока недоступен</b>\\n\\n"
                f"Сначала откройте Звёздное Колесо:\\n"
                f"1️⃣ Подпишитесь на всех активных спонсоров\\n"
                f"2️⃣ Нажмите «Обновить статус»\\n\\n"
                f"После активации Колеса раздел обмена станет доступен."
            ),
            parse_mode=ParseMode.HTML,
        )'''
)

# 18) start text not activated
pattern = r"""async def get_start_text\(user_id, first_name, context\):\n    state = await get_user_state\(user_id, context\)\n\n    if not state\["activated"\]:\n        subs_status = "✅ выполнены" if state\["all_subs_ok"\] else "❌ не выполнены"\n\n        return \(\n(?:.*\n)+?        \)\n\n    progress = get_level_progress_data\(state\["ref_count"\]\)\n"""
replacement = '''async def get_start_text(user_id, first_name, context):
    state = await get_user_state(user_id, context)

    if not state["activated"]:
        subs_status = "✅ выполнены" if state["all_subs_ok"] else "❌ не выполнены"

        return (
            f"👋 <b>Привет, {first_name}!</b>\\n\\n"
            f"После подписки на 2 основных спонсоров <b>Колесо откроется сразу</b> ✅\\n\\n"
            f"⭐ <b>Баланс:</b> {state['stars']}\\n\\n"
            f"📌 <b>Текущий статус:</b>\\n"
            f"• Подписки: <b>{subs_status}</b>\\n"
            f"• Статус колеса: <b>🔒 не активировано</b>\\n\\n"
            f"🎁 <b>Что будет после активации:</b>\\n"
            f"• Бесплатное вращение каждые 6 часов\\n"
            f"• Возможность копить звёзды и обменивать их на призы\\n\\n"
            f"👥 <b>Бонус за первого приглашённого друга:</b> +{FIRST_INVITED_FRIEND_BONUS}⭐ и +{FIRST_INVITED_FRIEND_BONUS_PERCENT}% к шансу\\n\\n"
            f"После подписки нажмите <b>«Обновить статус»</b> 👇"
        )

    progress = get_level_progress_data(state["ref_count"])
'''
main = re.sub(pattern, replacement, main, count=1, flags=re.MULTILINE)

# 19) start text activated block tweaks
main = main.replace(
'''        f"✅ <b>Звёздное Колесо уже активировано</b>\\n"
        f"Теперь твоя задача — <b>повышать уровень, увеличивать бонус и зарабатывать больше звёзд</b> ⭐\\n\\n"''',
'''        f"✅ <b>Колесо открыто</b>\\n"
        f"Теперь можно крутить его каждые 6 часов, получать звёзды и усиливать шанс через друзей ⭐\\n\\n"'''
)
main = main.replace(
'''        f"1️⃣ Крути <b>Звёздное Колесо</b> и собирай звёзды\\n"
        f"2️⃣ Приглашай новых <b>активных друзей</b> и повышай уровень\\n"
        f"3️⃣ Используй звёзды на <b>бусты, обмен и продвижение</b>\\n\\n"''',
'''        f"1️⃣ Крути <b>Звёздное Колесо</b> и собирай звёзды\\n"
        f"2️⃣ Приглашай друзей и повышай уровень\\n"
        f"3️⃣ Используй звёзды на <b>бусты, обмен и продвижение</b>\\n\\n"'''
)
main = main.replace(
'''        f"👥 <b>Активные друзья:</b> {state['ref_count']}\\n"''',
'''        f"👥 <b>Приглашённые друзья:</b> {state['invited_ref_count']}\\n"
        f"✅ <b>Активные друзья:</b> {state['ref_count']}\\n"'''
)

# 20) progress logic 4/8/12
main = re.sub(
    r"""def get_level_progress_data\(ref_count: int\):\n(?:.*\n)+?    return \{\n        "level": level,\n        "progress_label": \(\n            f"📈 <b>До уровня \{level\['next_name'\]\}</b>: "\n            f"<b>\{ref_count\}/\{target\}</b>"\n        \),\n        "progress_bar": progress_bar,\n        "remaining_text": format_friends_left\(left\),\n    \}\n""",
'''def get_level_progress_data(ref_count: int):
    level = get_level_info(ref_count)

    if level["name"] == "Bronze":
        base = 0
        target = 4
        current_in_level = ref_count
    elif level["name"] == "Silver":
        base = 4
        target = 8
        current_in_level = ref_count - base
    elif level["name"] == "Gold":
        base = 8
        target = 12
        current_in_level = ref_count - base
    else:
        return {
            "level": level,
            "progress_label": "📈 <b>Максимальный уровень достигнут</b>",
            "progress_bar": "🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩",
            "remaining_text": "",
        }

    step_target = target - base
    progress_bar = make_progress_bar(current_in_level, step_target, length=10)
    left = max(0, target - ref_count)

    return {
        "level": level,
        "progress_label": (
            f"📈 <b>До уровня {level['next_name']}</b>: "
            f"<b>{ref_count}/{target}</b>"
        ),
        "progress_bar": progress_bar,
        "remaining_text": format_friends_left(left),
    }
''',
    main,
    count=1,
    flags=re.MULTILINE
)

# 21) profile show invited + active
main = main.replace(
'''        f"👥 <b>Активные друзья:</b> {state['ref_count']}\\n"''',
'''        f"👥 <b>Приглашённые друзья:</b> {state['invited_ref_count']}\\n"
        f"✅ <b>Активные друзья:</b> {state['ref_count']}\\n"''',
    1
)

# 22) show invite keyboard in callback already done

# -----------------------------
# is_can_spin_server.py patches
# -----------------------------

# 23) level logic in server
spin = re.sub(
    r"""def get_level_info\(ref_count: int\):\n(?:.*\n)+?    return \{\n            "name": "Bronze",\n            "emoji": "🥉",\n            "bonus_percent": 0,\n            "multiplier": 1\.00,\n        \}\n""",
'''def get_level_info(ref_count: int):
    if ref_count >= 12:
        return {
            "name": "Diamond",
            "emoji": "🌟",
            "bonus_percent": 60,
            "multiplier": 1.60,
        }
    if ref_count >= 8:
        return {
            "name": "Gold",
            "emoji": "🥇",
            "bonus_percent": 35,
            "multiplier": 1.35,
        }
    if ref_count >= 4:
        return {
            "name": "Silver",
            "emoji": "🥈",
            "bonus_percent": 15,
            "multiplier": 1.15,
        }
    return {
        "name": "Bronze",
        "emoji": "🥉",
        "bonus_percent": 0,
        "multiplier": 1.00,
    }
''',
    spin,
    count=1,
    flags=re.MULTILINE
)

# 24) callback_data fix in send_post_welcome_message
spin = spin.replace(
'"callback_data": "my_ref_link"',
'"callback_data": "show_invite"'
)
spin = spin.replace(
'"callback_data": "check_subs"',
'"callback_data": "check_sub"'
)

# 25) onboarding text after welcome
spin = re.sub(
    r"""def send_post_welcome_message\(user_id: int, stars: int\):\n    text = \(\n(?:.*\n)+?    \)\n""",
'''def send_post_welcome_message(user_id: int, stars: int):
    text = (
        "🎉 <b>Приветственный спин получен!</b>\\n\\n"
        "Теперь, чтобы открыть постоянный доступ к <b>Звёздному Колесу</b>:\\n"
        "1. Подпишитесь на 2 основных спонсоров\\n"
        "2. Нажмите <b>«Обновить статус»</b>\\n\\n"
        "После подписки колесо откроется сразу ✅\\n\\n"
        "👥 Пригласите 1 друга и получите <b>+20⭐</b> и <b>+5%</b> к шансу\\n\\n"
        f"⭐ <b>Баланс:</b> {stars}"
    )
''',
    spin,
    count=1,
    flags=re.MULTILINE
)

# 26) spin access not by activated refs but by subscribed/activated flag that main now sets from subscriptions
spin = spin.replace(
'''                        "message": "Пригласите 2 активных реферала для открытия Звёздного Колеса.",''',
'''                        "message": "Подпишитесь на 2 основных спонсоров для открытия Звёздного Колеса.",'''
)
spin = spin.replace(
'''                        "message": "Нет 2 активных рефералов.",''',
'''                        "message": "Колесо ещё не активировано. Подпишитесь на 2 основных спонсоров и обновите статус.",'''
)
spin = spin.replace(
'''                        "message": "Не выполнено условие по 2 активным рефералам.",''',
'''                        "message": "Колесо ещё не активировано. Подпишитесь на 2 основных спонсоров и обновите статус.",'''
)

# Write files
main_path.write_text(main, encoding="utf-8")
spin_path.write_text(spin, encoding="utf-8")

# Syntax check
errors = []
for path in [main_path, spin_path]:
    try:
        py_compile.compile(str(path), doraise=True)
        print(f"OK syntax: {path}")
    except Exception as e:
        errors.append((str(path), str(e)))

if errors:
    print("PATCH_APPLIED_WITH_ERRORS")
    for f, e in errors:
        print(f"{f}: {e}")
    sys.exit(2)

print("PATCH_APPLIED_SUCCESSFULLY")
