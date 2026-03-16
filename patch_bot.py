from pathlib import Path

path = Path("/root/starearn_bot/main.py")
text = path.read_text(encoding="utf-8")

# 1. imports
if "import time" not in text:
    text = text.replace(
        "import os\nimport asyncio\nfrom datetime import datetime, timedelta, timezone\n",
        "import os\nimport asyncio\nimport time\nimport logging\nfrom datetime import datetime, timedelta, timezone\n"
    )

# 2. logging setup
if 'logger = logging.getLogger(__name__)' not in text:
    text = text.replace(
        "load_dotenv()\n",
        "load_dotenv()\n\nlogging.basicConfig(\n    level=logging.INFO,\n    format=\"%(asctime)s | %(levelname)s | %(message)s\"\n)\nlogger = logging.getLogger(__name__)\n"
    )

# 3. replace faq_callback
old_faq = '''async def faq_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    selected_key = None
    if query.data.startswith("faq:"):
        selected_key = query.data.split(":", 1)[1]

    try:
        await query.edit_message_text(
            build_faq_text(selected_key),
            parse_mode=ParseMode.HTML,
            reply_markup=get_faq_keyboard(),
        )
    except Exception as e:
        if "not modified" in str(e).lower():
            pass
        else:
            raise e
'''
new_faq = '''async def faq_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    start_time = time.perf_counter()
    query = update.callback_query

    logger.info("faq_callback START | user_id=%s | data=%s", query.from_user.id, query.data)

    t1 = time.perf_counter()
    await query.answer()
    logger.info("faq_callback query.answer DONE | %.3f sec", time.perf_counter() - t1)

    selected_key = None
    if query.data.startswith("faq:"):
        selected_key = query.data.split(":", 1)[1]

    try:
        t2 = time.perf_counter()
        await query.edit_message_text(
            build_faq_text(selected_key),
            parse_mode=ParseMode.HTML,
            reply_markup=get_faq_keyboard(),
        )
        logger.info("faq_callback edit_message_text DONE | %.3f sec", time.perf_counter() - t2)
        logger.info("faq_callback FINISH | total=%.3f sec", time.perf_counter() - start_time)
    except Exception as e:
        if "not modified" in str(e).lower():
            logger.info("faq_callback NOT MODIFIED | total=%.3f sec", time.perf_counter() - start_time)
            pass
        else:
            logger.exception("faq_callback ERROR | %s", e)
            raise e
'''
if old_faq in text:
    text = text.replace(old_faq, new_faq)

# 4. replace get_user_state
old_get_user_state = '''async def get_user_state(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    sponsors = await get_active_sponsors(include_temp=True)

    all_subs_ok = True
    channels_list = ""

    if not sponsors:
        channels_list = "Список спонсоров пока не настроен.\\n"
        all_subs_ok = False

    for sponsor in sponsors:
        ch = sponsor["channel_username"]
        is_sub = await check_subscription(user_id, ch, context)

        if is_sub:
            icon = "✅"
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO channel_subscriptions (user_id, channel_id)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                            """,
                            (user_id, ch),
                        )
                        conn.commit()
            except Exception as e:
                print("channel_subscriptions save error:", e)
        else:
            icon = "❌"
            all_subs_ok = False

        channels_list += f"• {ch} {icon}\\n"

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET all_subscribed = %s WHERE user_id = %s",
                (1 if all_subs_ok else 0, user_id),
            )
            cur.execute(
                """
                SELECT
                    COALESCE(activated, FALSE),
                    COALESCE(lifetime_ref_count, 0),
                    COALESCE(tickets, 0),
                    COALESCE(weekly_hold_bonus_count, 0),
                    last_fortune_time,
                    COALESCE(last_level_notified, 'Bronze')
                FROM users
                WHERE user_id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()
            conn.commit()

    activated = False
    ref_count = 0
    stars = 0
    weekly_hold_bonus_count = 0
    last_fortune_time = None
    last_level_notified = "Bronze"

    if row:
        (
            activated,
            ref_count,
            stars,
            weekly_hold_bonus_count,
            last_fortune_time,
            last_level_notified,
        ) = row

    level = get_level_info(ref_count)

    return {
        "activated": activated,
        "all_subs_ok": all_subs_ok,
        "channels_list": channels_list,
        "ref_count": ref_count,
        "stars": stars,
        "weekly_hold_bonus_count": weekly_hold_bonus_count,
        "last_fortune_time": to_naive_utc(last_fortune_time),
        "level": level,
        "last_level_notified": last_level_notified,
    }
'''
new_get_user_state = '''async def get_user_state(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    start_time = time.perf_counter()
    logger.info("get_user_state START | user_id=%s", user_id)

    sponsors = await get_active_sponsors(include_temp=True)

    all_subs_ok = True
    channels_list = ""

    if not sponsors:
        channels_list = "Список спонсоров пока не настроен.\\n"
        all_subs_ok = False

    for sponsor in sponsors:
        ch = sponsor["channel_username"]

        t_sub = time.perf_counter()
        is_sub = await check_subscription(user_id, ch, context)
        logger.info("check_subscription DONE | user_id=%s | channel=%s | %.3f sec", user_id, ch, time.perf_counter() - t_sub)

        if is_sub:
            icon = "✅"
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO channel_subscriptions (user_id, channel_id)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                            """,
                            (user_id, ch),
                        )
                        conn.commit()
            except Exception as e:
                logger.exception("channel_subscriptions save error | %s", e)
        else:
            icon = "❌"
            all_subs_ok = False

        channels_list += f"• {ch} {icon}\\n"

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET all_subscribed = %s WHERE user_id = %s",
                (1 if all_subs_ok else 0, user_id),
            )
            cur.execute(
                """
                SELECT
                    COALESCE(activated, FALSE),
                    COALESCE(lifetime_ref_count, 0),
                    COALESCE(tickets, 0),
                    COALESCE(weekly_hold_bonus_count, 0),
                    last_fortune_time,
                    COALESCE(last_level_notified, 'Bronze')
                FROM users
                WHERE user_id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()
            conn.commit()

    activated = False
    ref_count = 0
    stars = 0
    weekly_hold_bonus_count = 0
    last_fortune_time = None
    last_level_notified = "Bronze"

    if row:
        (
            activated,
            ref_count,
            stars,
            weekly_hold_bonus_count,
            last_fortune_time,
            last_level_notified,
        ) = row

    level = get_level_info(ref_count)

    logger.info("get_user_state FINISH | user_id=%s | total=%.3f sec", user_id, time.perf_counter() - start_time)

    return {
        "activated": activated,
        "all_subs_ok": all_subs_ok,
        "channels_list": channels_list,
        "ref_count": ref_count,
        "stars": stars,
        "weekly_hold_bonus_count": weekly_hold_bonus_count,
        "last_fortune_time": to_naive_utc(last_fortune_time),
        "level": level,
        "last_level_notified": last_level_notified,
    }
'''
if old_get_user_state in text:
    text = text.replace(old_get_user_state, new_get_user_state)

# 5. replace count_valid_refs
old_count_valid_refs = '''async def count_valid_refs(referrer_id: int, context: ContextTypes.DEFAULT_TYPE) -> int:
    valid_count = 0

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT referred_id, COALESCE(is_valid, FALSE) FROM referrals WHERE referrer_id=%s",
                (referrer_id,),
            )
            rows = cur.fetchall()

            main_sponsors = await get_active_sponsors(include_temp=False)

            for referred_id, is_valid in rows:
                if is_valid:
                    valid_count += 1
                    continue

                if len(main_sponsors) < 2:
                    continue

                subscribed_all_main = True
                for sponsor in main_sponsors:
                    if not await check_subscription(referred_id, sponsor["channel_username"], context):
                        subscribed_all_main = False
                        break

                if subscribed_all_main:
                    cur.execute(
                        """
                        UPDATE referrals
                        SET is_valid = TRUE, checked_at = %s
                        WHERE referrer_id = %s AND referred_id = %s
                        """,
                        (utcnow(), referrer_id, referred_id),
                    )
                    valid_count += 1

            cur.execute(
                "UPDATE users SET lifetime_ref_count = %s WHERE user_id = %s",
                (valid_count, referrer_id),
            )

            if valid_count >= 2:
                cur.execute(
                    "UPDATE users SET activated = TRUE WHERE user_id = %s",
                    (referrer_id,),
                )

            conn.commit()

    return valid_count
'''
new_count_valid_refs = '''async def count_valid_refs(referrer_id: int, context: ContextTypes.DEFAULT_TYPE) -> int:
    start_time = time.perf_counter()
    logger.info("count_valid_refs START | referrer_id=%s", referrer_id)

    valid_count = 0

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT referred_id, COALESCE(is_valid, FALSE) FROM referrals WHERE referrer_id=%s",
                (referrer_id,),
            )
            rows = cur.fetchall()

            main_sponsors = await get_active_sponsors(include_temp=False)

            for referred_id, is_valid in rows:
                if is_valid:
                    valid_count += 1
                    continue

                if len(main_sponsors) < 2:
                    continue

                subscribed_all_main = True
                for sponsor in main_sponsors:
                    t_sub = time.perf_counter()
                    ok = await check_subscription(referred_id, sponsor["channel_username"], context)
                    logger.info(
                        "count_valid_refs check_subscription | referrer_id=%s | referred_id=%s | channel=%s | %.3f sec",
                        referrer_id,
                        referred_id,
                        sponsor["channel_username"],
                        time.perf_counter() - t_sub
                    )
                    if not ok:
                        subscribed_all_main = False
                        break

                if subscribed_all_main:
                    cur.execute(
                        """
                        UPDATE referrals
                        SET is_valid = TRUE, checked_at = %s
                        WHERE referrer_id = %s AND referred_id = %s
                        """,
                        (utcnow(), referrer_id, referred_id),
                    )
                    valid_count += 1

            cur.execute(
                "UPDATE users SET lifetime_ref_count = %s WHERE user_id = %s",
                (valid_count, referrer_id),
            )

            if valid_count >= 2:
                cur.execute(
                    "UPDATE users SET activated = TRUE WHERE user_id = %s",
                    (referrer_id,),
                )

            conn.commit()

    logger.info(
        "count_valid_refs FINISH | referrer_id=%s | valid_count=%s | total=%.3f sec",
        referrer_id,
        valid_count,
        time.perf_counter() - start_time
    )

    return valid_count
'''
if old_count_valid_refs in text:
    text = text.replace(old_count_valid_refs, new_count_valid_refs)

# 6. replace button_handler
old_button_handler = '''async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        if not IS_ACTIVE:
            await query.edit_message_text("⛔️ Бот временно на паузе.", parse_mode=ParseMode.HTML)
            return

        uid = query.from_user.id
        data = query.data

        if data in ("check_sub", "back_to_main"):
            decay_result = await apply_inactivity_decay(uid, context)
            await count_valid_refs(uid, context)
            await recount_temp_order_progress(context)
            text = await get_start_text(uid, query.from_user.first_name, context)

            if decay_result.get("decayed", 0) > 0:
                text += (
                    f"\\n\\n⚠️ <b>За период неактивности было списано:</b> {decay_result['decayed']}⭐\\n"
                    f"Чтобы сохранять баланс, заходите в бот хотя бы 1 раз в 7 дней."
                )

            try:
                await query.edit_message_text(
                    text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_main_inline(),
                )
            except Exception as e:
                if "not modified" in str(e).lower():
                    pass
                else:
                    raise e

        elif data == "profile":
            await show_profile(query, uid, query.from_user.first_name, context, edit=True)

        elif data == "exchange":
            state = await get_user_state(uid, context)
            text = (
                f"🔄 <b>Обмен звёзд</b>\\n"
                f"⭐ Ваш баланс: <b>{state['stars']}</b>\\n"
                f"Выберите нужное действие:"
            )
            try:
                await query.edit_message_text(
                    text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_exchange_inline(),
                )
            except Exception as e:
                if "not modified" in str(e).lower():
                    pass
                else:
                    raise e

        elif data == "exchange_premium":
            state = await get_user_state(uid, context)

            if state["stars"] < PREMIUM_COST:
                await query.edit_message_text(
                    "❌ <b>Недостаточно звёзд</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                    ),
                )
                return

            if state["level"]["name"] != "Diamond":
                await query.edit_message_text(
                    "❌ <b>Доступно только для Diamond-уровня Звёздного Колеса!</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                    ),
                )
                return

            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET tickets = tickets - %s WHERE user_id = %s RETURNING tickets",
                        (PREMIUM_COST, uid),
                    )
                    new_balance = int(cur.fetchone()[0])

                    cur.execute(
                        """
                        INSERT INTO exchange_requests (user_id, username, exchange_type, stars_amount)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (uid, query.from_user.username, "premium_3m", PREMIUM_COST),
                    )
                    conn.commit()

            await notify_admins(
                context,
                (
                    f"💎 <b>Новая заявка на Telegram Premium 3 мес</b>\\n\\n"
                    f"👤 Пользователь: {display_username(query.from_user.username)}\\n"
                    f"🆔 ID: <code>{uid}</code>\\n"
                    f"⭐ Списано: <b>{PREMIUM_COST}</b>\\n"
                    f"💰 Остаток: <b>{new_balance}</b>"
                ),
            )

            await query.edit_message_text(
                "✅ Заявка на Telegram Premium создана. Администратор получил уведомление.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                ),
            )

        elif data == "exchange_withdraw":
            state = await get_user_state(uid, context)

            if state["stars"] < WITHDRAW_MIN:
                await query.edit_message_text(
                    f"❌ <b>Недостаточно звёзд</b>\\nМинимум для вывода: <b>{WITHDRAW_MIN} ⭐</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                    ),
                )
                return

            if state["level"]["name"] != "Diamond":
                await query.edit_message_text(
                    "❌ <b>Доступно только для Diamond-уровня Звёздного Колеса!</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                    ),
                )
                return

            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET tickets = tickets - %s WHERE user_id = %s RETURNING tickets",
                        (WITHDRAW_MIN, uid),
                    )
                    new_balance = int(cur.fetchone()[0])

                    cur.execute(
                        """
                        INSERT INTO exchange_requests (user_id, username, exchange_type, stars_amount)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (uid, query.from_user.username, "withdraw", WITHDRAW_MIN),
                    )
                    conn.commit()

            await notify_admins(
                context,
                (
                    f"💸 <b>Новая заявка на вывод звёзд</b>\\n\\n"
                    f"👤 Пользователь: {display_username(query.from_user.username)}\\n"
                    f"🆔 ID: <code>{uid}</code>\\n"
                    f"⭐ Списано: <b>{WITHDRAW_MIN}</b>\\n"
                    f"💰 Остаток: <b>{new_balance}</b>"
                ),
            )

            await query.edit_message_text(
                "✅ Заявка на вывод создана. Администратор получил уведомление.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                ),
            )

        elif data in ("exchange_promo", "exchange_promo_priority"):
            stars_cost = CHANNEL_PROMO_COST if data == "exchange_promo" else CHANNEL_PROMO_PRIORITY_COST
            priority_level = 0 if data == "exchange_promo" else 1

            state = await get_user_state(uid, context)
            if state["stars"] < stars_cost:
                await query.edit_message_text(
                    "❌ <b>Недостаточно звёзд</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                    ),
                )
                return

            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET tickets = tickets - %s WHERE user_id = %s RETURNING tickets",
                        (stars_cost, uid),
                    )
                    new_balance = int(cur.fetchone()[0])

                    cur.execute(
                        """
                        INSERT INTO sponsor_orders (
                            user_id, username, target_subscribers, counted_subscribers,
                            active_subscribers, priority_level, stars_amount, status, placed_in_slot
                        )
                        VALUES (%s, %s, %s, 0, 0, %s, %s, 'waiting_link', FALSE)
                        RETURNING id
                        """,
                        (uid, query.from_user.username, 100, priority_level, stars_cost),
                    )
                    order_id = int(cur.fetchone()[0])

                    cur.execute(
                        """
                        INSERT INTO exchange_requests (user_id, username, exchange_type, stars_amount)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            uid,
                            query.from_user.username,
                            "sponsor_channel_priority" if priority_level == 1 else "sponsor_channel",
                            stars_cost,
                        ),
                    )
                    conn.commit()

            context.user_data["waiting_sponsor_order_id"] = order_id

            await notify_admins(
                context,
                (
                    f"📢 <b>Новая заявка на размещение канала</b>\\n\\n"
                    f"Заказ #{order_id}\\n"
                    f"👤 Пользователь: {display_username(query.from_user.username)}\\n"
                    f"🆔 ID: <code>{uid}</code>\\n"
                    f"⭐ Списано: <b>{stars_cost}</b>\\n"
                    f"💰 Остаток: <b>{new_balance}</b>\\n"
                    f"Тип: <b>{'вне очереди' if priority_level == 1 else 'обычный'}</b>\\n\\n"
                    f"Пользователь должен прислать username канала."
                ),
            )

            await query.edit_message_text(
                "✅ Заявка создана.\\n\\n"
                "Теперь отправьте <b>username канала</b> одним сообщением.\\n"
                "Пример: <code>@mychannel</code>\\n\\n"
                "⚠️ Бот должен быть добавлен администратором в канал.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                ),
            )

        else:
            await query.edit_message_text("Неизвестное действие.")

    except Exception as e:
        try:
            await query.edit_message_text(f"Ошибка: {e}")
        except Exception:
            pass
'''
new_button_handler = '''async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    start_time = time.perf_counter()
    query = update.callback_query
    uid = query.from_user.id
    data = query.data

    logger.info("button_handler START | user_id=%s | data=%s", uid, data)

    t_answer = time.perf_counter()
    await query.answer()
    logger.info("button_handler query.answer DONE | %.3f sec", time.perf_counter() - t_answer)

    try:
        if not IS_ACTIVE:
            t_pause = time.perf_counter()
            await query.edit_message_text("⛔️ Бот временно на паузе.", parse_mode=ParseMode.HTML)
            logger.info("button_handler paused edit DONE | %.3f sec", time.perf_counter() - t_pause)
            logger.info("button_handler FINISH | user_id=%s | data=%s | total=%.3f sec", uid, data, time.perf_counter() - start_time)
            return

        if data in ("check_sub", "back_to_main"):
            t1 = time.perf_counter()
            decay_result = await apply_inactivity_decay(uid, context)
            logger.info("apply_inactivity_decay DONE | %.3f sec", time.perf_counter() - t1)

            t2 = time.perf_counter()
            await count_valid_refs(uid, context)
            logger.info("count_valid_refs DONE | %.3f sec", time.perf_counter() - t2)

            t3 = time.perf_counter()
            text = await get_start_text(uid, query.from_user.first_name, context)
            logger.info("get_start_text DONE | %.3f sec", time.perf_counter() - t3)

            if decay_result.get("decayed", 0) > 0:
                text += (
                    f"\\n\\n⚠️ <b>За период неактивности было списано:</b> {decay_result['decayed']}⭐\\n"
                    f"Чтобы сохранять баланс, заходите в бот хотя бы 1 раз в 7 дней."
                )

            try:
                t4 = time.perf_counter()
                await query.edit_message_text(
                    text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_main_inline(),
                )
                logger.info("edit_message_text main DONE | %.3f sec", time.perf_counter() - t4)
            except Exception as e:
                if "not modified" in str(e).lower():
                    logger.info("edit_message_text main NOT MODIFIED")
                    pass
                else:
                    raise e

        elif data == "profile":
            t1 = time.perf_counter()
            await show_profile(query, uid, query.from_user.first_name, context, edit=True)
            logger.info("show_profile DONE | %.3f sec", time.perf_counter() - t1)

        elif data == "exchange":
            t1 = time.perf_counter()
            state = await get_user_state(uid, context)
            logger.info("get_user_state for exchange DONE | %.3f sec", time.perf_counter() - t1)

            text = (
                f"🔄 <b>Обмен звёзд</b>\\n"
                f"⭐ Ваш баланс: <b>{state['stars']}</b>\\n"
                f"Выберите нужное действие:"
            )
            try:
                t2 = time.perf_counter()
                await query.edit_message_text(
                    text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_exchange_inline(),
                )
                logger.info("edit_message_text exchange DONE | %.3f sec", time.perf_counter() - t2)
            except Exception as e:
                if "not modified" in str(e).lower():
                    logger.info("edit_message_text exchange NOT MODIFIED")
                    pass
                else:
                    raise e

        elif data == "exchange_premium":
            t1 = time.perf_counter()
            state = await get_user_state(uid, context)
            logger.info("get_user_state premium DONE | %.3f sec", time.perf_counter() - t1)

            if state["stars"] < PREMIUM_COST:
                await query.edit_message_text(
                    "❌ <b>Недостаточно звёзд</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                    ),
                )
                logger.info("button_handler FINISH | user_id=%s | data=%s | total=%.3f sec", uid, data, time.perf_counter() - start_time)
                return

            if state["level"]["name"] != "Diamond":
                await query.edit_message_text(
                    "❌ <b>Доступно только для Diamond-уровня Звёздного Колеса!</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                    ),
                )
                logger.info("button_handler FINISH | user_id=%s | data=%s | total=%.3f sec", uid, data, time.perf_counter() - start_time)
                return

            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET tickets = tickets - %s WHERE user_id = %s RETURNING tickets",
                        (PREMIUM_COST, uid),
                    )
                    new_balance = int(cur.fetchone()[0])

                    cur.execute(
                        """
                        INSERT INTO exchange_requests (user_id, username, exchange_type, stars_amount)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (uid, query.from_user.username, "premium_3m", PREMIUM_COST),
                    )
                    conn.commit()

            await notify_admins(
                context,
                (
                    f"💎 <b>Новая заявка на Telegram Premium 3 мес</b>\\n\\n"
                    f"👤 Пользователь: {display_username(query.from_user.username)}\\n"
                    f"🆔 ID: <code>{uid}</code>\\n"
                    f"⭐ Списано: <b>{PREMIUM_COST}</b>\\n"
                    f"💰 Остаток: <b>{new_balance}</b>"
                ),
            )

            await query.edit_message_text(
                "✅ Заявка на Telegram Premium создана. Администратор получил уведомление.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                ),
            )

        elif data == "exchange_withdraw":
            t1 = time.perf_counter()
            state = await get_user_state(uid, context)
            logger.info("get_user_state withdraw DONE | %.3f sec", time.perf_counter() - t1)

            if state["stars"] < WITHDRAW_MIN:
                await query.edit_message_text(
                    f"❌ <b>Недостаточно звёзд</b>\\nМинимум для вывода: <b>{WITHDRAW_MIN} ⭐</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                    ),
                )
                logger.info("button_handler FINISH | user_id=%s | data=%s | total=%.3f sec", uid, data, time.perf_counter() - start_time)
                return

            if state["level"]["name"] != "Diamond":
                await query.edit_message_text(
                    "❌ <b>Доступно только для Diamond-уровня Звёздного Колеса!</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                    ),
                )
                logger.info("button_handler FINISH | user_id=%s | data=%s | total=%.3f sec", uid, data, time.perf_counter() - start_time)
                return

            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET tickets = tickets - %s WHERE user_id = %s RETURNING tickets",
                        (WITHDRAW_MIN, uid),
                    )
                    new_balance = int(cur.fetchone()[0])

                    cur.execute(
                        """
                        INSERT INTO exchange_requests (user_id, username, exchange_type, stars_amount)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (uid, query.from_user.username, "withdraw", WITHDRAW_MIN),
                    )
                    conn.commit()

            await notify_admins(
                context,
                (
                    f"💸 <b>Новая заявка на вывод звёзд</b>\\n\\n"
                    f"👤 Пользователь: {display_username(query.from_user.username)}\\n"
                    f"🆔 ID: <code>{uid}</code>\\n"
                    f"⭐ Списано: <b>{WITHDRAW_MIN}</b>\\n"
                    f"💰 Остаток: <b>{new_balance}</b>"
                ),
            )

            await query.edit_message_text(
                "✅ Заявка на вывод создана. Администратор получил уведомление.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                ),
            )

        elif data in ("exchange_promo", "exchange_promo_priority"):
            stars_cost = CHANNEL_PROMO_COST if data == "exchange_promo" else CHANNEL_PROMO_PRIORITY_COST
            priority_level = 0 if data == "exchange_promo" else 1

            t1 = time.perf_counter()
            state = await get_user_state(uid, context)
            logger.info("get_user_state promo DONE | %.3f sec", time.perf_counter() - t1)

            if state["stars"] < stars_cost:
                await query.edit_message_text(
                    "❌ <b>Недостаточно звёзд</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                    ),
                )
                logger.info("button_handler FINISH | user_id=%s | data=%s | total=%.3f sec", uid, data, time.perf_counter() - start_time)
                return

            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET tickets = tickets - %s WHERE user_id = %s RETURNING tickets",
                        (stars_cost, uid),
                    )
                    new_balance = int(cur.fetchone()[0])

                    cur.execute(
                        """
                        INSERT INTO sponsor_orders (
                            user_id, username, target_subscribers, counted_subscribers,
                            active_subscribers, priority_level, stars_amount, status, placed_in_slot
                        )
                        VALUES (%s, %s, %s, 0, 0, %s, %s, 'waiting_link', FALSE)
                        RETURNING id
                        """,
                        (uid, query.from_user.username, 100, priority_level, stars_cost),
                    )
                    order_id = int(cur.fetchone()[0])

                    cur.execute(
                        """
                        INSERT INTO exchange_requests (user_id, username, exchange_type, stars_amount)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            uid,
                            query.from_user.username,
                            "sponsor_channel_priority" if priority_level == 1 else "sponsor_channel",
                            stars_cost,
                        ),
                    )
                    conn.commit()

            context.user_data["waiting_sponsor_order_id"] = order_id

            await notify_admins(
                context,
                (
                    f"📢 <b>Новая заявка на размещение канала</b>\\n\\n"
                    f"Заказ #{order_id}\\n"
                    f"👤 Пользователь: {display_username(query.from_user.username)}\\n"
                    f"🆔 ID: <code>{uid}</code>\\n"
                    f"⭐ Списано: <b>{stars_cost}</b>\\n"
                    f"💰 Остаток: <b>{new_balance}</b>\\n"
                    f"Тип: <b>{'вне очереди' if priority_level == 1 else 'обычный'}</b>\\n\\n"
                    f"Пользователь должен прислать username канала."
                ),
            )

            await query.edit_message_text(
                "✅ Заявка создана.\\n\\n"
                "Теперь отправьте <b>username канала</b> одним сообщением.\\n"
                "Пример: <code>@mychannel</code>\\n\\n"
                "⚠️ Бот должен быть добавлен администратором в канал.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="exchange")]]
                ),
            )

        else:
            await query.edit_message_text("Неизвестное действие.")

        logger.info(
            "button_handler FINISH | user_id=%s | data=%s | total=%.3f sec",
            uid,
            data,
            time.perf_counter() - start_time
        )

    except Exception as e:
        logger.exception("button_handler ERROR | user_id=%s | data=%s | %s", uid, data, e)
        try:
            await query.edit_message_text(f"Ошибка: {e}")
        except Exception:
            pass
'''
if old_button_handler in text:
    text = text.replace(old_button_handler, new_button_handler)

# 7. increase timeouts in main()
text = text.replace(
    'app = Application.builder().token(BOT_TOKEN).build()',
    'app = (\n        Application.builder()\n        .token(BOT_TOKEN)\n        .connect_timeout(20)\n        .read_timeout(20)\n        .write_timeout(20)\n        .pool_timeout(20)\n        .build()\n    )'
)

path.write_text(text, encoding="utf-8")
print("PATCH_OK")
