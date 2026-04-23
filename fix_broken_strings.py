from pathlib import Path
import re
import py_compile
import sys

p = Path("main.py")
text = p.read_text(encoding="utf-8")

# --- fix 1: broken congratulation text in count_valid_refs()
text = re.sub(
    r'text=\(\s*"🎉 <b>Поздравляем!</b>[\s\S]*?parse_mode=ParseMode\.HTML,',
    '''text=(
                            "🎉 <b>Поздравляем!</b>\\n\\n"
                            f"Вы пригласили первого друга и получили <b>+{FIRST_INVITED_FRIEND_BONUS}⭐</b> "
                            f"и <b>+{FIRST_INVITED_FRIEND_BONUS_PERCENT}%</b> к шансу звёздных секторов."
                        ),
                        parse_mode=ParseMode.HTML,''',
    text,
    count=1
)

# --- fix 2: broken line in get_start_text() for first invited friend bonus
text = re.sub(
    r'f"👥 <b>Бонус за первого приглашённого друга:</b> \+\{FIRST_INVITED_FRIEND_BONUS\}⭐ и \+\{FIRST_INVITED_FRIEND_BONUS_PERCENT\}% к шансу\s*',
    'f"👥 <b>Бонус за первого приглашённого друга:</b> +{FIRST_INVITED_FRIEND_BONUS}⭐ и +{FIRST_INVITED_FRIEND_BONUS_PERCENT}% к шансу\\n\\n"\n',
    text,
    count=1
)

# --- fix 3: broken invited_ref_count line in get_start_text/show_profile
text = re.sub(
    r'f"👥 <b>Приглашённые друзья:</b> \{state\[\'invited_ref_count\'\]\}\s*',
    'f"👥 <b>Приглашённые друзья:</b> {state[\'invited_ref_count\']}\\\\n"\n',
    text,
    count=1
)

# --- fix 4: if another broken invited line exists later
text = re.sub(
    r'f"👥 <b>Приглашённые друзья:</b> \{state\[\'invited_ref_count\'\]\}\\n"\s*\n\s*f"✅ <b>Активные друзья:</b>',
    'f"👥 <b>Приглашённые друзья:</b> {state[\'invited_ref_count\']}\\\\n"\n        f"✅ <b>Активные друзья:</b>',
    text,
    count=2
)

p.write_text(text, encoding="utf-8")

try:
    py_compile.compile("main.py", doraise=True)
    print("FIX_OK")
except Exception as e:
    print("FIX_FAILED")
    print(e)
    sys.exit(1)
