from pathlib import Path
import re
import sys
import py_compile
from datetime import datetime

main_path = Path("main.py")
spin_path = Path("is_can_spin_server.py")

if not main_path.exists() or not spin_path.exists():
    print("ERROR: main.py or is_can_spin_server.py not found")
    sys.exit(1)

main = main_path.read_text(encoding="utf-8")
spin = spin_path.read_text(encoding="utf-8")

ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
Path(f"main.py.safe_bak.{ts}").write_text(main, encoding="utf-8")
Path(f"is_can_spin_server.py.safe_bak.{ts}").write_text(spin, encoding="utf-8")

def ensure_once(text, needle, insert_after, block):
    if needle in text:
        return text
    pos = text.find(insert_after)
    if pos == -1:
        raise RuntimeError(f"insert_after not found: {insert_after}")
    pos += len(insert_after)
    return text[:pos] + block + text[pos:]

# =========================================================
# main.py
# =========================================================

# 1. New constants
if "FIRST_INVITED_FRIEND_BONUS = 20" not in main:
    main = main.replace(
        "CHANNEL_PROMO_PRIORITY_COST = 300\n",
        "CHANNEL_PROMO_PRIORITY_COST = 300\n\nFIRST_INVITED_FRIEND_BONUS = 20\nFIRST_INVITED_FRIEND_BONUS_PERCENT = 5\n",
        1
    )

# 2. Add new columns into CREATE TABLE users
if "invited_ref_count INT DEFAULT 0" not in main:
    main = main.replace(
        "                        lifetime_ref_count INT DEFAULT 0,\n",
        "                        lifetime_ref_count INT DEFAULT 0,\n                        invited_ref_count INT DEFAULT 0,\n",
        1
    )

if "paid_spins INT DEFAULT 0" not in main:
    main = main.replace(
        "                        welcome_spin_used BOOLEAN DEFAULT FALSE\n",
        "                        welcome_spin_used BOOLEAN DEFAULT FALSE,\n                        paid_spins INT DEFAULT 0,\n                        first_invite_bonus_paid BOOLEAN DEFAULT FALSE\n",
        1
    )

# 3. Add ALTER TABLE statements
if '"ALTER TABLE users ADD COLUMN IF NOT EXISTS invited_ref_count INT DEFAULT 0"' not in main:
    main = main.replace(
        '"ALTER TABLE users ADD COLUMN IF NOT EXISTS lifetime_ref_count INT DEFAULT 0",\n',
        '"ALTER TABLE users ADD COLUMN IF NOT EXISTS lifetime_ref_count INT DEFAULT 0",\n'
        '                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS invited_ref_count INT DEFAULT 0",\n'
        '                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS paid_spins INT DEFAULT 0",\n'
        '                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS first_invite_bonus_paid BOOLEAN DEFAULT FALSE",\n',
        1
    )

# 4. Level logic in main.py
main = re.sub(
    r'def get_level_info\(ref_count: int\):\n(?:    .*\n)+?def get_main_inline',
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

def get_main_inline''',
    main,
    count=1,
    flags=re.MULTILINE
)

# 5. Add invite inline keyboard helper
if "def get_invite_inline(user_id: int):" not in main:
    insert_after = "def build_invite_text(user_id: int):"
    start = main.find(insert_after)
    if start == -1:
        raise RuntimeError("build_invite_text not found")
    end = main.find("\n\nasync def get_welcome_start_text", start)
    if end == -1:
        raise RuntimeError("get_welcome_start_text not found")
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

# 6. Replace build_invite_text safely
main = re.sub(
    r'def build_invite_text\(user_id: int\):\n(?:    .*\n)+?(?=\nasync def get_welcome_start_text|\ndef get_invite_inline)',
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

# 7. get_main_inline
main = re.sub(
    r'def get_main_inline\(activated: bool\):\n(?:    .*\n)+?def get_exchange_inline',
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

def get_exchange_inline''',
    main,
    count=1,
    flags=re.MULTILINE
)

# 8. Support old callback names too
main = main.replace(
    'if data in ("check_sub", "back_to_main"):',
    'if data in ("check_sub", "check_subs", "back_to_main"):'
)
main = main.replace(
    'elif data == "show_invite":',
    'elif data in ("show_invite", "my_ref_link"):'
)

# 9. Replace invite callback keyboard
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

# 10. get_welcome_start_text
main = re.sub(
    r'async def get_welcome_start_text\(user_id, first_name, context\):\n(?:    .*\n)+?(?=\ndef get_reply_menu)',
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

# 11. get_reply_menu
main = re.sub(
    r'def get_reply_menu\(user_id: int, activated: bool, bonus_percent: int = 0, welcome_spin_used: bool = True\):\n(?:    .*\n)+?(?=\n\nasync def get_start_text)',
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

# 12. get_level_progress_data
main = re.sub(
    r'def get_level_progress_data\(ref_count: int\):\n(?:    .*\n)+?(?=\ndef get_faq_keyboard)',
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

# 13. Update get_user_state SELECT
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
    1
)

main = main.replace(
    '''        "ref_count": ref_count,
        "stars": stars,''',
    '''        "ref_count": ref_count,
        "invited_ref_count": invited_ref_count,
        "stars": stars,''',
    1
)

# 14. Activation now based on subscription
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

# 15. Rewrite count_valid_refs
main = re.sub(
    r'async def count_valid_refs\(referrer_id: int, context: ContextTypes.DEFAULT_TYPE\) -> int:\n(?:.*\n)+?    return active_count\n',
    '''async def count_valid_refs(referrer_id: int, context: ContextTypes.DEFAULT_TYPE) -> int:
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
''',
    main,
    count=1,
    flags=re.MULTILINE
)

# 16. start text
main = re.sub(
    r'async def get_start_text\(user_id, first_name, context\):\n(?:.*\n)+?(?=\nasync def show_profile)',
    '''async def get_start_text(user_id, first_name, context):
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
    reflink = f"https://t.me/{BOT_USERNAME_FOR_REFLINK}?start={user_id}"

    if not state["all_subs_ok"]:
        wheel_access = "❌ <b>Звёздное Колесо недоступно: подпишитесь на всех активных спонсоров</b>"
    else:
        wheel_access = "✅ <b>Звёздное Колесо доступно</b>"

    return (
        f"👋 <b>Привет, {first_name}!</b>\\n\\n"
        f"✅ <b>Колесо открыто</b>\\n"
        f"Теперь можно крутить его каждые 6 часов, получать звёзды и усиливать шанс через друзей ⭐\\n\\n"
        f"⭐ <b>Баланс:</b> {state['stars']}\\n\\n"
        f"🚀 <b>Что делать сейчас:</b>\\n"
        f"1️⃣ Крути <b>Звёздное Колесо</b> и собирай звёзды\\n"
        f"2️⃣ Приглашай друзей и повышай уровень\\n"
        f"3️⃣ Используй звёзды на <b>бусты, обмен и продвижение</b>\\n\\n"
        f"💡 <b>Как зарабатывать больше:</b>\\n"
        f"• Бесплатное вращение — 1 раз в 6 часов\\n"
        f"• Начисление каждую неделю по {WEEKLY_HOLD_BONUS}⭐ (если не отписались от спонсоров)\\n"
        f"• Чем выше уровень, тем выше шанс выпадения звёздных секторов\\n\\n"
        f"📌 <b>Активные спонсоры:</b>\\n{state['channels_list']}\\n"
        f"👥 <b>Приглашённые друзья:</b> {state['invited_ref_count']}\\n"
        f"✅ <b>Активные друзья:</b> {state['ref_count']}\\n"
        f"🔗 <b>Ваша ссылка для приглашения друзей:</b>\\n"
        f"<code>{reflink}</code>\\n\\n"
        f"🏅 <b>Уровень:</b> {state['level']['emoji']} {state['level']['name']}\\n"
        f"{progress['progress_label']}\\n"
        f"{progress['progress_bar']} {progress['remaining_text']}\\n"
        f"✨ <b>Бонус за первого друга:</b> +{state['activation_bonus_percent']}%\\n"
        f"🏅 <b>Бонус уровня:</b> +{state['level']['bonus_percent']}%\\n"
        f"🌠 <b>Буст:</b> "
        f"{('+' + str(state['boost_percent']) + '% (осталось ' + str(state['boost_spins_left']) + ' спина)') if state['boost_spins_left'] > 0 else 'нет'}\\n"
        f"🌠 <b>Общий бонус к Звёздному Колесу:</b> +{state['total_bonus_percent']}%\\n"
        f"📌 <b>Этот бонус повышает шанс выпадения звёздных секторов</b>\\n\\n"
        f"{wheel_access}"
    )

async def show_profile''',
    main,
    count=1,
    flags=re.MULTILINE
)

# 17. show_profile invited+active
main = main.replace(
    'f"👥 <b>Активные друзья:</b> {state[\'ref_count\']}\\n"',
    'f"👥 <b>Приглашённые друзья:</b> {state[\'invited_ref_count\']}\\n"\n'
    '        f"✅ <b>Активные друзья:</b> {state[\'ref_count\']}\\n"',
    1
)

# 18. locked wheel text
main = main.replace(
    '''        await update.message.reply_text(
            (
                f"🔒 <b>Звёздное Колесо пока недоступно</b>\n\n"
                f"Чтобы открыть доступ, нужно выполнить условия:\n"
                f"• Подписки на спонсоров: <b>{subs_status}</b>\n"
                f"• Активные друзья: <b>{state['ref_count']}/2</b>\n\n"
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

# 19. locked exchange text
main = main.replace(
    '''        await update.message.reply_text(
            (
                f"🔒 <b>Обмен звёзд пока недоступен</b>\n\n"
                f"Сначала откройте Звёздное Колесо:\n"
                f"1️⃣ Подпишитесь на всех активных спонсоров\n"
                f"2️⃣ Пригласите 2 активных друзей\n\n"
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

# 20. FAQ text точечно
main = main.replace(
    "• Пригласите 2 активных друга\\n",
    "• Подпишитесь на 2 основных спонсорских канала\\n",
)
main = main.replace(
    "• После выполнения условий доступ к Колесу откроется автоматически ✅\\n\\n",
    "• После подписки доступ к Колесу откроется автоматически ✅\\n\\n",
)
main = main.replace(
    "Активный друг — это пользователь, который:\\n"
    "• перешёл по вашей ссылке\\n"
    "• подписался на 2 основных спонсорских канала\\n\\n",
    "Приглашённый друг — это пользователь, который перешёл по вашей ссылке и запустил бота.\\n"
    "Активный друг — это приглашённый пользователь, который подписался на 2 основных спонсорских канала.\\n\\n"
    "🎁 Бонус за первого приглашённого друга: +20⭐ и +5% к шансу\\n\\n",
)
main = main.replace(
    "• наличие 2 активных друзей\\n",
    "",
)
main = main.replace(
    "• Открыть Колесо можно через кнопку в меню или WebApp\\n\\n",
    "• Открыть Колесо можно после подписки на 2 основных спонсоров\\n\\n",
)

# =========================================================
# is_can_spin_server.py
# =========================================================

# 21. Level logic in server
spin = re.sub(
    r'def get_level_info\(ref_count: int\):\n(?:    .*\n)+?def get_wheel_weights_by_bonus',
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

def get_wheel_weights_by_bonus''',
    spin,
    count=1,
    flags=re.MULTILINE
)

# 22. callback_data fix
spin = spin.replace('"callback_data": "my_ref_link"', '"callback_data": "show_invite"')
spin = spin.replace('"callback_data": "check_subs"', '"callback_data": "check_sub"')

# 23. welcome post message text
spin = re.sub(
    r'def send_post_welcome_message\(user_id: int, stars: int\):\n(?:    .*\n)+?    reply_markup = \{',
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

    reply_markup = {''',
    spin,
    count=1,
    flags=re.MULTILINE
)

# 24. Messages about activation in server
spin = spin.replace(
    '"message": "Пригласите 2 активных реферала для открытия Звёздного Колеса.",',
    '"message": "Подпишитесь на 2 основных спонсоров для открытия Звёздного Колеса.",'
)
spin = spin.replace(
    '"message": "Нет 2 активных рефералов.",',
    '"message": "Колесо ещё не активировано. Подпишитесь на 2 основных спонсоров и обновите статус.",'
)
spin = spin.replace(
    '"message": "Не выполнено условие по 2 активным рефералам.",',
    '"message": "Колесо ещё не активировано. Подпишитесь на 2 основных спонсоров и обновите статус.",'
)

# Save files
main_path.write_text(main, encoding="utf-8")
spin_path.write_text(spin, encoding="utf-8")

# Syntax check
failed = []
for f in ["main.py", "is_can_spin_server.py"]:
    try:
        py_compile.compile(f, doraise=True)
        print(f"OK syntax: {f}")
    except Exception as e:
        failed.append((f, str(e)))

if failed:
    print("PATCH_FAILED")
    for f, e in failed:
        print(f"{f}: {e}")
    sys.exit(2)

print("PATCH_OK")
