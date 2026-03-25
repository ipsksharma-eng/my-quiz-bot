#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          QuizBot Pro — v5.1  (SPEED + fpdf2 Hindi Perfect PDF)              ║
║  ⚡ SPEED FIX: Combined DB calls — 3x faster quiz creation                  ║
║  ⚡ SPEED FIX: No extra DB check in auto-advance — 1-2s faster              ║
║  ⚡ SPEED FIX: Reduced timer delay 0.15s → 0.05s                            ║
║  ⚡ SPEED FIX: Instant "Saving..." feedback before DB write                 ║
║  ✅ HINDI FIX: fpdf2 + uharfbuzz — perfect Devanagari text shaping          ║
║  ✅ HTML Fix: InputFile wrapper + seek(0) + parse_mode=None                  ║
║  ✅ Speed Fix: User cache added — no extra DB calls per message              ║
║  ✅ Auto-Restart Loop: Prevents bot from dying due to network/API errors     ║
║  ✅ Activity Logging: Saves all errors and crashes to bot_activity.log       ║
║  ✅ Last Quiz Memory: Bare /start in group restarts the previous quiz        ║
║  ✅ Fixed Timer: Owner answers no longer skip the timer                      ║
║  ✅ Auto-Advance Safety: Database locks & poll errors won't halt the quiz    ║
║  ✅ Smart Reference Split: Kathan/Statements auto-split into ref_text        ║
║  ✅ Render Keep-Alive: Flask server integrated for 24/7 uptime               ║
║  ✅ New Leaderboard: 1st center podium, 2nd-3rd sides, 3-line per student   ║
║  ✅ /quizpdf → fpdf2 PDF: dark-blue header, answer green box each Q          ║
║  ✅ /testseries → same PDF (alias)                                           ║
║  ✅ Hindi font: NotoSansDevanagari + HarfBuzz shaping = perfect rendering    ║
║  ⚡ FAST DATABASE: Connection Pooling added for instant replies               ║
║  ⚡ FAST POLLING: Timeout reduced for quicker Telegram responses             ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import telebot, json, re, os, random, string, html as html_mod, threading, logging, time, io
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import pool
from datetime import datetime
from telebot.types import (ReplyKeyboardMarkup, KeyboardButton,
                           InlineKeyboardMarkup, InlineKeyboardButton, InputFile,
                           InlineQueryResultArticle, InputTextMessageContent,
                           ReplyKeyboardRemove)

# ══════════════════════════════════════════════════════════════════════════════
#  RENDER KEEP-ALIVE SERVER
# ══════════════════════════════════════════════════════════════════════════════
from flask import Flask
from threading import Thread

app = Flask('')

@app.route('/')
def home():
    return "Bot is alive and running!"

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_web_server)
    t.daemon = True
    t.start()

# ══════════════════════════════════════════════════════════════════════════════
#  LOGGING SETUP
# ══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    filename='bot_activity.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.info("Bot script started/restarted.")

TOKEN    = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
BOT_USER = "SDiscussion_bot"
OWNER_ID = 863857194

bot     = telebot.TeleBot(TOKEN, parse_mode=None)
_wizard : dict = {}
_auto_timers: dict = {}
_LETTERS = "ABCDEFGHIJ"
_CORRECT = "\u2705"   # ✅

# ══════════════════════════════════════════════════════════════════════════════
#  SPEED FIX: User Memory Cache (DB call skip)
# ══════════════════════════════════════════════════════════════════════════════
_user_cache = set()
_ban_cache  = set()
_state_cache: dict = {}

# ══════════════════════════════════════════════════════════════════════════════
#  DATABASE CONNECTION POOL (SUPER FAST)
# ══════════════════════════════════════════════════════════════════════════════

try:
    db_pool = pool.ThreadedConnectionPool(1, 20, DATABASE_URL)
    logging.info("Database connection pool created successfully.")
except Exception as e:
    logging.error(f"Connection pool error: {e}")
    db_pool = None

class _PgWrapper:
    """psycopg2 connection ko sqlite3 jaisa banata hai aur speed ke liye pool use karta hai"""
    def __init__(self, conn):
        self._conn = conn

    def _fix(self, sql):
        return sql.replace("?", "%s")

    def execute(self, sql, params=()):
        cur = self._conn.cursor(cursor_factory=DictCursor)
        cur.execute(self._fix(sql), params)
        return cur

    def executemany(self, sql, params_list):
        cur = self._conn.cursor(cursor_factory=DictCursor)
        cur.executemany(self._fix(sql), params_list)
        return cur

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._conn.rollback()
        else:
            self._conn.commit()
        if db_pool and self._conn:
            db_pool.putconn(self._conn)
        else:
            if self._conn: self._conn.close()
        return False

def get_db():
    if db_pool:
        return _PgWrapper(db_pool.getconn())
    else:
        conn = psycopg2.connect(DATABASE_URL)
        return _PgWrapper(conn)

def init_db():
    with get_db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY, username TEXT, first_name TEXT,
            html_toggle INTEGER NOT NULL DEFAULT 0,
            state TEXT NOT NULL DEFAULT 'idle',
            created_at INTEGER NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::INTEGER
        )""")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS quizzes (
            quiz_id     SERIAL PRIMARY KEY,
            short_id    TEXT NOT NULL DEFAULT '',
            creator_id  BIGINT NOT NULL,
            title       TEXT NOT NULL,
            neg_marking TEXT NOT NULL DEFAULT '0',
            quiz_type   TEXT NOT NULL DEFAULT 'free',
            timer_seconds INTEGER NOT NULL DEFAULT 45,
            shuffle_q   INTEGER NOT NULL DEFAULT 0,
            shuffle_o   INTEGER NOT NULL DEFAULT 0,
            section_quiz INTEGER NOT NULL DEFAULT 0,
            created_at  INTEGER NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::INTEGER
        )""")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS questions (
            question_id SERIAL PRIMARY KEY,
            quiz_id     INTEGER NOT NULL REFERENCES quizzes(quiz_id) ON DELETE CASCADE,
            ref_text    TEXT NOT NULL DEFAULT '',
            q_text      TEXT NOT NULL,
            options     TEXT NOT NULL,
            correct_idx INTEGER NOT NULL,
            position    INTEGER NOT NULL DEFAULT 0
        )""")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS active_sessions (
            session_id    SERIAL PRIMARY KEY,
            user_id       BIGINT NOT NULL,
            quiz_id       INTEGER NOT NULL,
            chat_id       BIGINT NOT NULL,
            current_q_idx INTEGER NOT NULL DEFAULT 0,
            is_paused     INTEGER NOT NULL DEFAULT 0,
            is_completed  INTEGER NOT NULL DEFAULT 0,
            total_q       INTEGER NOT NULL DEFAULT 0,
            shuffled_order TEXT NOT NULL DEFAULT '[]',
            start_time    INTEGER NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::INTEGER,
            end_time      INTEGER
        )""")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS session_results (
            result_id    SERIAL PRIMARY KEY,
            session_id   INTEGER NOT NULL,
            user_id      BIGINT NOT NULL,
            participant_name TEXT NOT NULL DEFAULT '',
            question_id  INTEGER NOT NULL,
            selected_idx INTEGER,
            is_correct   INTEGER NOT NULL DEFAULT 0,
            answered_at  INTEGER NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::INTEGER
        )""")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS poll_map (
            poll_id     TEXT PRIMARY KEY,
            session_id  INTEGER NOT NULL,
            question_id INTEGER NOT NULL,
            correct_idx INTEGER NOT NULL,
            owner_id    BIGINT NOT NULL
        )""")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS banned_users (
            user_id   BIGINT PRIMARY KEY,
            banned_by BIGINT,
            reason    TEXT NOT NULL DEFAULT '',
            banned_at INTEGER NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::INTEGER
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_q  ON questions(quiz_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sr ON session_results(session_id, user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_as ON active_sessions(user_id, chat_id, is_completed)")

    migrations = [
        ("quizzes",          "timer_seconds",    "INTEGER NOT NULL DEFAULT 45"),
        ("quizzes",          "shuffle_q",         "INTEGER NOT NULL DEFAULT 0"),
        ("quizzes",          "shuffle_o",         "INTEGER NOT NULL DEFAULT 0"),
        ("quizzes",          "section_quiz",      "INTEGER NOT NULL DEFAULT 0"),
        ("quizzes",          "short_id",          "TEXT NOT NULL DEFAULT ''"),
        ("quizzes",          "neg_marking",       "TEXT NOT NULL DEFAULT '0'"),
        ("quizzes",          "quiz_type",         "TEXT NOT NULL DEFAULT 'free'"),
        ("questions",        "ref_text",          "TEXT NOT NULL DEFAULT ''"),
        ("questions",        "position",          "INTEGER NOT NULL DEFAULT 0"),
        ("session_results",  "participant_name",  "TEXT NOT NULL DEFAULT ''"),
        ("active_sessions",  "shuffled_order",    "TEXT NOT NULL DEFAULT '[]'"),
    ]
    with get_db() as conn:
        for table, col, defn in migrations:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {defn}")
            except Exception:
                pass

    try:
        with get_db() as conn:
            banned_rows = conn.execute("SELECT user_id FROM banned_users").fetchall()
            for r in banned_rows:
                _ban_cache.add(r["user_id"])
        logging.info(f"Ban cache loaded: {len(_ban_cache)} users")
    except Exception:
        pass

init_db()

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS — USERS / STATE (WITH CACHE)
# ══════════════════════════════════════════════════════════════════════════════

def is_owner(uid):
    return OWNER_ID is not None and uid == OWNER_ID

def is_banned(uid):
    return uid in _ban_cache

def notify_owner(text):
    if not OWNER_ID: return
    def _send():
        try: bot.send_message(OWNER_ID, text, parse_mode="HTML")
        except Exception: pass
    threading.Thread(target=_send, daemon=True).start()

def register_user(msg):
    u = msg.from_user
    if u.id in _user_cache:
        return
    with get_db() as conn:
        existing = conn.execute("SELECT user_id FROM users WHERE user_id=?", (u.id,)).fetchone()
        conn.execute("INSERT INTO users(user_id,username,first_name) VALUES(?,?,?) "
                     "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,first_name=excluded.first_name",
                     (u.id, u.username, u.first_name))
        if not existing:
            uname = f"@{u.username}" if u.username else "no username"
            chat_type = msg.chat.type if hasattr(msg, 'chat') else "unknown"
            notify_owner(
                f"🆕 <b>New User!</b>\n"
                f"👤 Name: <b>{html_mod.escape(u.first_name or '')}</b>\n"
                f"🔗 {uname}\n"
                f"🆔 ID: <code>{u.id}</code>\n"
                f"📍 Chat: {chat_type}\n"
                f"🕐 {datetime.now().strftime('%d %b %Y, %I:%M %p')}"
            )
    _user_cache.add(u.id)
    _state_cache[u.id] = "idle"

def get_user(uid):
    with get_db() as conn:
        return conn.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()

def set_state(uid, state):
    _state_cache[uid] = state
    with get_db() as conn:
        conn.execute("UPDATE users SET state=? WHERE user_id=?", (state, uid))

def get_state(uid):
    if uid in _state_cache:
        return _state_cache[uid]
    u = get_user(uid)
    state = u["state"] if u else "idle"
    _state_cache[uid] = state
    return state

def make_short_id(length=8):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))

def parse_neg_value(neg_str):
    try:
        if '/' in str(neg_str):
            p = str(neg_str).split('/')
            return float(p[0]) / float(p[1])
        return float(neg_str)
    except Exception:
        return 0.0

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS — UI / KEYBOARDS
# ══════════════════════════════════════════════════════════════════════════════

def main_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=3)
    kb.add(KeyboardButton("/create"),   KeyboardButton("/myquizzes"),  KeyboardButton("/startquiz"),
           KeyboardButton("/features"), KeyboardButton("/stats"),      KeyboardButton("/loadfile"),
           KeyboardButton("/result"),   KeyboardButton("/createhtml"), KeyboardButton("/quizpdf"),
           KeyboardButton("/testseries"), KeyboardButton("/practice"))
    return kb

def quiz_card_kb(quiz_id, short_id=""):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("     Start",  callback_data=f"qs_{quiz_id}"),
        InlineKeyboardButton("🚀 Group",
            url=f"https://t.me/{BOT_USER}?startgroup=quiz_{quiz_id}")
    )
    kb.add(
        InlineKeyboardButton("📋 Copy ID", callback_data=f"copyid_{short_id or quiz_id}"),
        InlineKeyboardButton("🔗 Share",   switch_inline_query=short_id or str(quiz_id))
    )
    return kb

def edit_panel_kb(quiz_id):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("⚙️ Settings",  callback_data=f"ep_settings_{quiz_id}"),
           InlineKeyboardButton("📖 Questions", callback_data=f"ep_questions_{quiz_id}"),
           InlineKeyboardButton("🔀 Shuffle",   callback_data=f"ep_shuffle_{quiz_id}"),
           InlineKeyboardButton("👥 Perms",     callback_data=f"ep_perms_{quiz_id}"),
           InlineKeyboardButton("📤 Export",    callback_data=f"ep_export_{quiz_id}"),
           InlineKeyboardButton("❌ Close",     callback_data=f"ep_close_{quiz_id}"))
    return kb

def shuffle_panel_kb(quiz_id, sq, so):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(f"🔀 Questions {'✅' if sq else '❌'}",
                                callback_data=f"sh_q_{quiz_id}"),
           InlineKeyboardButton(f"🔀 Options {'✅' if so else '❌'}",
                                callback_data=f"sh_o_{quiz_id}"),
           InlineKeyboardButton("⬅️ Back", callback_data=f"sh_back_{quiz_id}"))
    return kb

def safe_send(chat_id, text, **kw):
    for i in range(0, max(len(text), 1), 4096):
        try: bot.send_message(chat_id, text[i:i+4096], **kw)
        except Exception as e: 
            logging.error(f"safe_send failed: {e}")

def send_quiz_created_card(chat_id, uid, quiz_id, short_id, title, q_count, neg, quiz_type, timer):
    u = get_user(uid)
    creator = html_mod.escape((u["first_name"] or "User") if u else "User")
    safe_t = html_mod.escape(str(title))
    neg_display = neg if neg != "0" else "None"
    card = (
        f"<b>✅ Quiz Created!</b>\n\n"
        f"📚 <b>Name:</b> {safe_t}\n"
        f"#️⃣ <b>Questions:</b> {q_count}\n"
        f"⏱ <b>Timer:</b> {timer}s\n"
        f"🆔 <b>ID:</b> <code>{short_id}</code>\n"
        f"💵 <b>Type:</b> {quiz_type}\n"
        f"➖ <b>-ve:</b> {neg_display}\n"
        f"👤 <b>Creator:</b> {creator}"
    )
    try:
        bot.send_message(chat_id, card, parse_mode="HTML", reply_markup=quiz_card_kb(quiz_id, short_id))
    except Exception:
        bot.send_message(chat_id,
            f"Quiz Created!\nName: {title}\nQs: {q_count}\nTimer: {timer}s\nID: {short_id}",
            reply_markup=quiz_card_kb(quiz_id, short_id))

def send_edit_panel(chat_id, quiz, q_count, message_id=None):
    neg_val = parse_neg_value(quiz["neg_marking"])
    text = (f"🎯 *Quiz Editor*\n\n"
            f"📌 Name: {quiz['title'][:25]}\n"
            f"🔢 Questions: {q_count}\n"
            f"⏱ Timer: {quiz['timer_seconds']}s\n"
            f"💵 Type: {quiz['quiz_type'].capitalize()}\n"
            f"➖ Negative: {neg_val:.4f}\n"
            f"🔀 Shuffle Q: {'✅' if quiz['shuffle_q'] else '❌'}  "
            f"Opts: {'✅' if quiz['shuffle_o'] else '❌'}")
    kb = edit_panel_kb(quiz["quiz_id"])
    try:
        if message_id:
            bot.edit_message_text(text, chat_id, message_id, parse_mode="Markdown", reply_markup=kb)
        else:
            bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=kb)
    except Exception:
        bot.send_message(chat_id, text, reply_markup=kb)

# ══════════════════════════════════════════════════════════════════════════════
#  SMART PARSER FOR KATHAN / STATEMENTS
# ══════════════════════════════════════════════════════════════════════════════

_OPT_RE = re.compile(r"^[a-zA-Z0-9][).\-:]\s*(.*)", re.UNICODE)

def extract_q_and_ref(question_lines):
    if not question_lines:
        return "", ""
    if len(question_lines) == 1:
        q = re.sub(r"^[Qq]?\d+[).\-:\s]+", "", question_lines[0], flags=re.IGNORECASE).strip()
        if len(q) > 280: return q, q[:275] + "..."
        return "", q

    full_text = "\n".join(question_lines)
    has_list = bool(re.search(r"\n\s*([1-9][.:\)]|[IVX]+\.)", full_text))

    if len(full_text) <= 250 and not has_list:
        q = re.sub(r"^[Qq]?\d+[).\-:\s]+", "", full_text, flags=re.IGNORECASE).strip()
        return "", q

    clean_first = re.sub(r"^[Qq]?\d+[).\-:\s]+", "", question_lines[0], flags=re.IGNORECASE).strip()
    last_line = question_lines[-1].strip()

    if re.match(r"^([1-9][.:\)]|[IVX]+\.)", last_line):
        q_text = clean_first
        ref_text = "\n".join(question_lines[1:])
    else:
        q_text = last_line
        ref_lines = question_lines[:-1]
        ref_lines[0] = clean_first
        ref_text = "\n".join(ref_lines)

    if len(q_text) > 280:
        ref_text = q_text + "\n\n" + ref_text
        q_text = q_text[:275] + "..."

    return ref_text.strip(), q_text.strip()

def parse_manual_block(block):
    lines = [l.strip() for l in block.strip().splitlines() if l.strip()]
    if len(lines) < 3: raise ValueError(f"Too few lines ({len(lines)})")

    opt_start = -1
    for i, line in enumerate(lines):
        if _OPT_RE.match(line):
            opt_start = i
            break

    if opt_start == -1 or opt_start == 0:
        raise ValueError("Could not detect options. Use a) b) format.")

    ref_text, q_text = extract_q_and_ref(lines[:opt_start])

    opts, correct_idx = [], -1
    for line in lines[opt_start:]:
        m = _OPT_RE.match(line)
        if not m: continue
        opt = m.group(1).strip()
        if _CORRECT in opt:
            correct_idx = len(opts)
            opt = opt.replace(_CORRECT, "").strip()
        opts.append(opt[:100])

    if len(opts) < 2: raise ValueError(f"Only {len(opts)} option(s)")
    if len(opts) > 10: raise ValueError(f"{len(opts)} options > 10")
    return (ref_text, q_text, opts, correct_idx if correct_idx >= 0 else 0)

def bulk_parse_manual(raw):
    parsed, errors = [], []
    for i, block in enumerate(re.split(r"\n\s*\n", raw.strip()), 1):
        block = block.strip()
        if not block: continue
        try: parsed.append(parse_manual_block(block))
        except ValueError as e: errors.append(f"Block {i}: {e}")
    return parsed, errors

def _parse_bpsc_block(block):
    paras = [p.strip() for p in re.split(r"\n\s*\n", block.strip()) if p.strip()]
    if not paras: raise ValueError("Empty block")

    if len(paras) == 1:
        first_lines = paras[0].splitlines()
        question_lines = first_lines[:1]
        option_lines = [l.strip() for l in first_lines[1:] if l.strip()]
    else:
        question_lines = []
        for p in paras[:-1]:
            question_lines.extend(p.splitlines())
        option_lines = []
        for line in paras[-1].splitlines():
            line = line.strip()
            if line and not line.startswith("👉"):
                option_lines.append(line)

    ref_text, q_text = extract_q_and_ref(question_lines)

    opts, correct_idx = [], -1
    for line in option_lines:
        if not line: continue
        clean = line
        m = _OPT_RE.match(clean)
        if m: clean = m.group(1).strip()
        if _CORRECT in clean:
            correct_idx = len(opts)
            clean = clean.replace(_CORRECT, "").strip()
        if clean: opts.append(clean[:100])
        
    if len(opts) < 2: raise ValueError(f"Only {len(opts)} option(s)")
    if len(opts) > 10: opts = opts[:10]
    return (ref_text, q_text, opts, correct_idx if correct_idx >= 0 else 0)

def parse_bpsc_txt(content):
    raw_blocks = re.split(r"(?=^\s*Q\d+\.)", content, flags=re.MULTILINE)
    parsed, errors = [], []
    for i, block in enumerate(raw_blocks, 1):
        block = block.strip()
        if not block or not re.match(r"Q\d+\.", block, re.IGNORECASE): continue
        try: parsed.append(_parse_bpsc_block(block))
        except ValueError as e: errors.append(f"TXT Q{i}: {e}")
    return parsed, errors

def parse_json_schema_a(items):
    parsed, errors = [], []
    for i, item in enumerate(items, 1):
        try:
            q   = str(item.get("question","")).strip()
            ops = item.get("options", [])
            ci  = int(item.get("correct_index", 0))
            if not q: raise ValueError("'question' empty")
            if not isinstance(ops, list) or len(ops) < 2: raise ValueError("options < 2")
            if len(ops) > 10: ops = ops[:10]
            if not (0 <= ci < len(ops)): ci = 0
            parsed.append(("", q[:300], [str(o)[:100] for o in ops], ci))
        except Exception as e: errors.append(f"JSON A item {i}: {e}")
    return parsed, errors

def parse_json_schema_b(items):
    parsed, errors = [], []
    for i, item in enumerate(items, 1):
        try:
            ref_text = str(item.get("reference_text","")).strip()
            q_text   = str(item.get("question_text","")).strip()
            ops_raw  = item.get("options", [])
            corr_id  = str(item.get("correct_option_id","a")).strip().lower()
            if not q_text and not ref_text: raise ValueError("Both texts empty")
            if not isinstance(ops_raw, list) or len(ops_raw) < 2: raise ValueError(f"options must have >=2 items")
            opts, correct_idx = [], 0
            for j, opt in enumerate(ops_raw):
                if isinstance(opt, dict):
                    oid  = str(opt.get("id","")).strip().lower()
                    otxt = str(opt.get("text","")).strip()
                else:
                    oid, otxt = _LETTERS[j].lower() if j < 10 else str(j), str(opt).strip()
                if oid == corr_id: correct_idx = j
                opts.append(otxt[:100])
            if len(opts) > 10: opts = opts[:10]
            if len(q_text) > 300: q_text = q_text[:297] + "..."
            parsed.append((ref_text, q_text, opts, correct_idx))
        except Exception as e: errors.append(f"JSON B item {i}: {e}")
    return parsed, errors

def detect_and_parse(filename, content):
    fname = filename.lower()
    if fname.endswith(".json"):
        data = json.loads(content)
        if isinstance(data, dict) and "questions" in data: return parse_json_schema_b(data["questions"])
        elif isinstance(data, list):
            if data and isinstance(data[0], dict) and ("question_text" in data[0] or "reference_text" in data[0]):
                return parse_json_schema_b(data)
            return parse_json_schema_a(data if isinstance(data, list) else [])
        raise ValueError("JSON must be list or {questions:[...]}")
    if re.search(r"^\s*Q\d+\.", content, re.MULTILINE):
        r, e = parse_bpsc_txt(content)
        if r: return r, e
    return bulk_parse_manual(content)

# ══════════════════════════════════════════════════════════════════════════════
#  DATABASE WRITES (SPEED OPTIMIZED)
# ══════════════════════════════════════════════════════════════════════════════

def save_questions(quiz_id, questions):
    with get_db() as conn:
        count = conn.execute("SELECT COUNT(*) FROM questions WHERE quiz_id=?", (quiz_id,)).fetchone()[0]
        conn.executemany(
            "INSERT INTO questions(quiz_id,ref_text,q_text,options,correct_idx,position) VALUES(?,?,?,?,?,?)",
            [(quiz_id, r, q, json.dumps(o, ensure_ascii=False), c, count+i)
             for i, (r, q, o, c) in enumerate(questions)])

def create_quiz_and_save(uid, title, questions, neg="0", quiz_type="free", timer=45):
    short_id = make_short_id()
    with get_db() as conn:
        # SPEED FIX: Single query to get all existing short_ids instead of loop
        existing_ids = set()
        rows = conn.execute("SELECT short_id FROM quizzes").fetchall()
        for r in rows:
            existing_ids.add(r["short_id"])
        while short_id in existing_ids:
            short_id = make_short_id()

        cur = conn.execute(
            "INSERT INTO quizzes(creator_id,title,short_id,neg_marking,quiz_type,timer_seconds) "
            "VALUES(?,?,?,?,?,?) RETURNING quiz_id",
            (uid, title, short_id, neg, quiz_type, timer))
        quiz_id = cur.fetchone()[0]

        # Batch insert - all questions in one call
        if questions:
            conn.executemany(
                "INSERT INTO questions(quiz_id,ref_text,q_text,options,correct_idx,position) "
                "VALUES(?,?,?,?,?,?)",
                [(quiz_id, r, q, json.dumps(o, ensure_ascii=False), c, i)
                 for i, (r, q, o, c) in enumerate(questions)])

    return quiz_id, short_id, len(questions)

def find_quiz(uid, id_str):
    with get_db() as conn:
        if id_str.isdigit(): return conn.execute("SELECT * FROM quizzes WHERE quiz_id=? AND creator_id=?", (int(id_str), uid)).fetchone()
        return conn.execute("SELECT * FROM quizzes WHERE short_id=? AND creator_id=?", (id_str.upper(), uid)).fetchone()

# ══════════════════════════════════════════════════════════════════════════════
#  QUIZ SESSION ENGINE (SPEED OPTIMIZED)
# ══════════════════════════════════════════════════════════════════════════════

def _cancel_auto_timer(session_id):
    t = _auto_timers.pop(session_id, None)
    if t:
        try: t.cancel()
        except Exception: pass

def _auto_advance(session_id, expected_q_idx):
    """SPEED FIX: Direct call — skip extra DB check, send_next_poll handles it"""
    try:
        _auto_timers.pop(session_id, None)
        send_next_poll(session_id)
    except Exception as e:
        logging.error(f"Auto-advance error for session {session_id}: {e}")

def send_next_poll(session_id):
    """SPEED FIX: Combined DB calls — single read connection, single write connection"""
    _cancel_auto_timer(session_id)

    try:
        # ── SINGLE DB connection for ALL reads ──────────────────────────
        with get_db() as conn:
            sess = conn.execute(
                "SELECT * FROM active_sessions WHERE session_id=?",
                (session_id,)).fetchone()
            if not sess or sess["is_paused"] or sess["is_completed"]:
                return

            quiz = conn.execute(
                "SELECT * FROM quizzes WHERE quiz_id=?",
                (sess["quiz_id"],)).fetchone()

            all_qs = conn.execute(
                "SELECT * FROM questions WHERE quiz_id=? ORDER BY position, question_id",
                (sess["quiz_id"],)).fetchall()

        # ── Process questions from already-fetched data ──────────────────
        order = json.loads(sess["shuffled_order"] or "[]")
        if order:
            qmap = {q["question_id"]: q for q in all_qs}
            questions = [qmap[qid] for qid in order if qid in qmap]
        else:
            questions = list(all_qs)

        total = len(questions)
        q_idx = sess["current_q_idx"]

        if q_idx >= total:
            _finish_session(session_id)
            return

        q      = questions[q_idx]
        opts   = json.loads(q["options"])
        period = quiz["timer_seconds"] if quiz else 45
        correct_idx = q["correct_idx"]

        if quiz and quiz["shuffle_o"]:
            pairs = list(enumerate(opts))
            random.shuffle(pairs)
            orig_correct = correct_idx
            opts = []
            for new_i, (orig_i, txt) in enumerate(pairs):
                opts.append(txt)
                if orig_i == orig_correct:
                    correct_idx = new_i

        # ── Send reference if exists ─────────────────────────────────────
        ref = (q["ref_text"] or "").strip()
        if ref:
            ref_header = f"📖 *Reference* | Q{q_idx+1}/{total}"
            try:
                bot.send_message(sess["chat_id"],
                                 f"{ref_header}\n\n{ref}",
                                 parse_mode="Markdown")
            except Exception:
                bot.send_message(sess["chat_id"],
                                 f"Reference Q{q_idx+1}/{total}\n\n{ref}")

        # ── Build poll question ──────────────────────────────────────────
        prefix = f"[{q_idx+1}/{total}] "
        max_q  = 300 - len(prefix)
        q_body = q["q_text"][:max_q] if len(q["q_text"]) > max_q else q["q_text"]
        poll_q = prefix + q_body

        # ── Send poll ────────────────────────────────────────────────────
        msg = bot.send_poll(
            chat_id=sess["chat_id"], question=poll_q, options=opts,
            type="quiz", correct_option_id=correct_idx,
            is_anonymous=False, open_period=period)

        # ── SINGLE DB connection for ALL writes ─────────────────────────
        with get_db() as conn:
            conn.execute(
                "INSERT INTO poll_map(poll_id,session_id,question_id,correct_idx,owner_id) "
                "VALUES(?,?,?,?,?) "
                "ON CONFLICT (poll_id) DO UPDATE SET "
                "session_id=EXCLUDED.session_id, question_id=EXCLUDED.question_id, "
                "correct_idx=EXCLUDED.correct_idx, owner_id=EXCLUDED.owner_id",
                (msg.poll.id, session_id, q["question_id"],
                 correct_idx, sess["user_id"]))
            conn.execute(
                "UPDATE active_sessions SET current_q_idx=? WHERE session_id=?",
                (q_idx + 1, session_id))

        # ── Timer — SPEED FIX: reduced delay from 0.15s to 0.05s ────────
        t = threading.Timer(period + 0.05, _auto_advance,
                            args=[session_id, q_idx + 1])
        t.daemon = True
        t.start()
        _auto_timers[session_id] = t

    except telebot.apihelper.ApiTelegramException as exc:
        logging.error(f"Telegram API Exception: {exc}")
        try:
            bot.send_message(sess["chat_id"],
                             f"⚠️ Poll error #{q_idx+1}: {exc.description}\n"
                             f"Auto-skipping to next...")
        except Exception:
            pass
        try:
            with get_db() as conn:
                conn.execute(
                    "UPDATE active_sessions SET current_q_idx=? WHERE session_id=?",
                    (q_idx + 1, session_id))
            t = threading.Timer(1.5, _auto_advance,
                                args=[session_id, q_idx + 1])
            t.daemon = True
            t.start()
            _auto_timers[session_id] = t
        except Exception:
            pass
    except Exception as general_exc:
        logging.error(f"Poll send failed: {general_exc}")

def _finish_session(session_id):
    with get_db() as conn:
        sess = conn.execute("SELECT * FROM active_sessions WHERE session_id=?", (session_id,)).fetchone()
        if not sess: return
        conn.execute("UPDATE active_sessions SET is_completed=1,end_time=EXTRACT(EPOCH FROM NOW())::INTEGER WHERE session_id=?", (session_id,))
        quiz = conn.execute("SELECT * FROM quizzes WHERE quiz_id=?", (sess["quiz_id"],)).fetchone()
    quiz_title = quiz["title"] if quiz else "Quiz"
    neg_val    = parse_neg_value(quiz["neg_marking"]) if quiz else 0.0
    _send_leaderboard(session_id, sess["chat_id"], quiz_title, neg_val, sess["total_q"], sess["start_time"])

    if quiz:
        threading.Thread(
            target=_export_practice_html,
            args=(sess["chat_id"], sess["quiz_id"]),
            daemon=True
        ).start()

def _send_leaderboard(session_id, chat_id, quiz_title, neg_val, total_q, session_start):
    with get_db() as conn:
        rows = conn.execute("""
            SELECT user_id, MAX(participant_name) AS name, SUM(is_correct) AS correct,
                   COUNT(*) AS answered, MIN(answered_at) AS first_at, MAX(answered_at) AS last_at
            FROM session_results WHERE session_id=? GROUP BY user_id
            ORDER BY (SUM(is_correct) - (COUNT(*)-SUM(is_correct))*?) DESC, (MAX(answered_at)-MIN(answered_at)) ASC
        """, (session_id, neg_val)).fetchall()

    if not rows:
        bot.send_message(chat_id, f"🏁 Quiz '{quiz_title}' has ended!\n\nNo answers recorded.", parse_mode="HTML")
        return

    def short_name(raw):
        import re as _re
        n = (raw or "User").strip()
        orig_parts = n.split()
        first_token = orig_parts[0] if orig_parts else n
        name = _re.sub(r'^[^a-zA-Z0-9]+', '', first_token)
        name = _re.sub(r'[^a-zA-Z0-9]+$', '', name)
        if not name: name = first_token[:10]
        if name and not name.isupper() and not name.islower() and not name.isdigit():
            parts = _re.sub(r'([A-Z])', r' \1', name).split()
            if parts and parts[0].strip(): name = parts[0].strip()
        if len(name) > 10: name = name[:9] + "…"
        if len(orig_parts) > 1 and orig_parts[-1] and ord(orig_parts[-1][0]) > 127:
            name = name + " " + orig_parts[-1]
        return html_mod.escape(name)

    def _pad_center(text, width):
        tl = len(text)
        if tl >= width: return text
        left = (width - tl) // 2
        return " " * left + text

    def _pad_sides(left_text, right_text, width):
        half = width // 2
        return left_text.center(half) + right_text.center(half)

    safe_title = html_mod.escape(quiz_title)
    players = []
    for r in rows[:15]:
        correct = int(r["correct"] or 0)
        wrong   = int(r["answered"] or 0) - correct
        score   = round(correct - wrong * neg_val, 2)
        elapsed = int(r["last_at"] or 0) - int(r["first_at"] or 0)
        mins, sec = elapsed // 60, elapsed % 60
        if elapsed < 60:
            time_str = f"{elapsed}s"
        else:
            time_str = f"{mins}m {sec:02d}s"
        pct  = round((score / total_q * 100), 2) if total_q else 0.0
        pct  = max(0.0, pct)
        name = short_name(r["name"] or f"User{r['user_id']}")
        players.append({"name": name, "correct": correct, "wrong": wrong,
                        "score": score, "time_str": time_str, "pct": pct})

    SEP  = "━━━━━━━━━━━━━━━━━━━━━━━━"
    SEP2 = "─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─"
    W    = 32

    podium = ""
    if len(players) >= 1:
        r1 = players[0]
        podium += _pad_center("🥇", W) + "\n"
        podium += _pad_center(r1["name"], W) + "\n"
        podium += _pad_center(f"{r1['pct']}%", W) + "\n"
        podium += _pad_center(f"⏱ {r1['time_str']}", W) + "\n"
        podium += "\n"

    if len(players) >= 3:
        r2, r3 = players[1], players[2]
        podium += _pad_sides("🥈", "🥉", W) + "\n"
        podium += _pad_sides(r2["name"], r3["name"], W) + "\n"
        podium += _pad_sides(f"{r2['pct']}%", f"{r3['pct']}%", W) + "\n"
        podium += _pad_sides(f"⏱ {r2['time_str']}", f"⏱ {r3['time_str']}", W) + "\n"
    elif len(players) >= 2:
        r2 = players[1]
        podium += _pad_center("🥈", W) + "\n"
        podium += _pad_center(r2["name"], W) + "\n"
        podium += _pad_center(f"{r2['pct']}%", W) + "\n"
        podium += _pad_center(f"⏱ {r2['time_str']}", W) + "\n"

    rank_icons = {1: "👑", 2: "🥈", 3: "🥉"}
    rank_lines = []
    for i, p in enumerate(players):
        icon = rank_icons.get(i + 1, f"{i + 1}.")
        rank_lines.append(f"<b>{icon} {p['name']}</b>")
        rank_lines.append(f"✅ {p['correct']} | ❌ {p['wrong']} | 🎯 {p['score']} | {p['pct']}%")
        rank_lines.append(f"⏱ {p['time_str']}")
        if i < len(players) - 1:
            rank_lines.append(SEP2)

    max_time = players[-1]["time_str"] if players else ""

    msg = (
        f"🎯 Quiz '<b>{safe_title}</b>' — Results!\n\n"
        f"{SEP}\n"
        f"\n<pre>{podium}</pre>\n"
        f"{SEP}\n\n"
        + "\n".join(rank_lines)
        + f"\n\n{SEP}\n"
        f"👥 <i>Participants: {len(rows)}</i>  |  ⏱ <i>{max_time}</i>"
    )

    try:
        bot.send_message(chat_id, msg, parse_mode="HTML")
    except Exception:
        plain = [f"🎯 Quiz '{quiz_title}' — Results!\n", SEP, podium, SEP]
        for i, p in enumerate(players):
            icon = ["👑", "🥈", "🥉"][i] if i < 3 else f"{i+1}."
            plain.append(f"{icon} {p['name']}")
            plain.append(f"✅ {p['correct']} | ❌ {p['wrong']} | 🎯 {p['score']} | {p['pct']}%")
            plain.append(f"⏱ {p['time_str']}")
            if i < len(players) - 1:
                plain.append(SEP2)
        plain.extend([SEP, f"👥 Participants: {len(rows)}  |  ⏱ {max_time}"])
        bot.send_message(chat_id, "\n".join(plain))

def send_individual_result(chat_id, uid):
    with get_db() as conn:
        sess = conn.execute("SELECT * FROM active_sessions WHERE user_id=? ORDER BY session_id DESC LIMIT 1", (uid,)).fetchone()
        if not sess: return safe_send(chat_id, "No sessions found. Start one with /startquiz.")
        results = conn.execute("""SELECT sr.selected_idx, sr.is_correct, q.q_text, q.options, q.correct_idx 
            FROM session_results sr JOIN questions q ON sr.question_id=q.question_id 
            WHERE sr.session_id=? AND sr.user_id=? ORDER BY sr.result_id""", (sess["session_id"], uid)).fetchall()
        quiz = conn.execute("SELECT title,neg_marking FROM quizzes WHERE quiz_id=?", (sess["quiz_id"],)).fetchone()

    total   = sess["total_q"] or len(results)
    correct = sum(1 for r in results if r["is_correct"])
    neg_val = parse_neg_value(quiz["neg_marking"]) if quiz else 0.0
    wrong   = len(results) - correct
    score   = correct - wrong * neg_val
    pct     = (correct / total * 100) if total else 0.0
    title   = quiz["title"] if quiz else "Quiz"

    lines = [(f"📊 *Scorecard — {title}*\n\n🏆 Score: *{correct}/{total}* ({pct:.1f}%)\n🎯 Net score: *{score:.2f}*\n{'─'*36}\n")]
    for i, r in enumerate(results, 1):
        opts = json.loads(r["options"])
        sel  = opts[r["selected_idx"]] if r["selected_idx"] is not None and r["selected_idx"] < len(opts) else "—"
        ans  = opts[r["correct_idx"]]  if r["correct_idx"] < len(opts) else "?"
        lines.append(f"{'✅' if r['is_correct'] else '❌'} *Q{i}:* {r['q_text'][:120]}")
        if not r["is_correct"]: lines.append(f"   Your: _{sel}_\n   Correct: _{ans}_")
        lines.append("")

    full = "\n".join(lines)
    for i in range(0, max(len(full), 1), 4000): safe_send(chat_id, full[i:i+4000], parse_mode="Markdown")

# ══════════════════════════════════════════════════════════════════════════════
#  EXPORTS
# ══════════════════════════════════════════════════════════════════════════════

def _export_html(chat_id, quiz_id):
    with get_db() as conn:
        quiz = conn.execute("SELECT * FROM quizzes WHERE quiz_id=?", (quiz_id,)).fetchone()
        if not quiz: safe_send(chat_id, f"Quiz ID {quiz_id} not found."); return
        questions = conn.execute(
            "SELECT * FROM questions WHERE quiz_id=? ORDER BY position,question_id", (quiz_id,)).fetchall()
    if not questions: safe_send(chat_id, "No questions to export."); return
    q_blocks = []
    for i, q in enumerate(questions, 1):
        opts = json.loads(q["options"])
        ref  = html_mod.escape(q["ref_text"] or "")
        ref_html = f'<div class="ref">{ref}</div>' if ref else ""
        lis  = "".join(
            f'<li{"  class=\"correct\"" if j==q["correct_idx"] else ""}>'
            f'{_LETTERS[j]}) {html_mod.escape(o)}</li>'
            for j, o in enumerate(opts))
        q_blocks.append(f"""<div class="question">
  <p class="qnum">Q{i} <span>/ {len(questions)}</span></p>
  {ref_html}
  <p class="qtext">{html_mod.escape(q["q_text"])}</p>
  <ul class="opts">{lis}</ul></div>""")
    html_out = f"""<!DOCTYPE html><html lang="hi"><head>
<meta charset="UTF-8"/><title>{html_mod.escape(quiz["title"])}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{font-family:'Segoe UI',sans-serif;background:#f0f4f8;}}
header{{background:linear-gradient(135deg,#1a73e8,#0b3d91);color:#fff;padding:32px 48px;}}
h1{{font-size:26px;margin-bottom:4px;}}
.container{{max-width:820px;margin:24px auto;padding:0 20px 60px;}}
.question{{background:#fff;border-radius:10px;padding:22px 26px;margin-bottom:18px;
           box-shadow:0 2px 8px rgba(0,0,0,.08);border-left:4px solid #1a73e8;}}
.qnum{{font-size:11px;text-transform:uppercase;color:#1a73e8;font-weight:700;margin-bottom:4px;}}
.ref{{background:#f8f9fa;border-left:3px solid #aaa;padding:10px 14px;margin-bottom:12px;
      white-space:pre-wrap;font-size:13.5px;border-radius:4px;color:#333;}}
.qtext{{font-size:15px;font-weight:600;line-height:1.6;margin-bottom:14px;}}
.opts{{list-style:none;display:grid;gap:7px;}}
.opts li{{padding:8px 14px;border-radius:6px;background:#f8f9fa;border:1px solid #e0e0e0;font-size:14px;}}
.opts li.correct{{background:#e6f4ea;border-color:#34a853;color:#1b5e20;font-weight:600;}}
.opts li.correct::after{{content:" ✓";}}
footer{{text-align:center;padding:20px;color:#777;font-size:12px;}}
</style></head><body>
<header><h1>📚 {html_mod.escape(quiz["title"])}</h1>
<p>ID: {quiz_id} · {len(questions)} Questions · {datetime.now().strftime("%d %b %Y")}</p>
</header>
<div class="container">{"".join(q_blocks)}</div>
<footer>QuizBot Pro</footer></body></html>"""

    html_bytes = html_out.encode("utf-8")
    file_stream = io.BytesIO(html_bytes)
    file_stream.seek(0)
    try:
        bot.send_document(chat_id, InputFile(file_stream, file_name=f"quiz_{quiz_id}.html"),
            caption=f"HTML Export: {quiz['title']} ({len(questions)} Qs)")
    except Exception as e:
        safe_send(chat_id, f"Export failed: {e}")

def _export_txt(chat_id, quiz_id):
    with get_db() as conn:
        quiz = conn.execute("SELECT * FROM quizzes WHERE quiz_id=?", (quiz_id,)).fetchone()
        if not quiz: safe_send(chat_id, f"Quiz ID {quiz_id} not found."); return
        questions = conn.execute(
            "SELECT * FROM questions WHERE quiz_id=? ORDER BY position,question_id", (quiz_id,)).fetchall()
    if not questions: safe_send(chat_id, "No questions."); return
    sep   = "=" * 65
    lines = [sep, f"  {quiz['title'].upper()}", f"  {len(questions)} Qs | {datetime.now().strftime('%d %b %Y')}", sep, ""]
    akey  = ["", sep, "  ANSWER KEY", sep]
    for i, q in enumerate(questions, 1):
        opts = json.loads(q["options"])
        corr = q["correct_idx"]
        if q["ref_text"]:
            lines.append(f"Q{i}. [Reference] {q['ref_text'][:150]}")
        lines.append(f"Q{i}. {q['q_text']}")
        for j, o in enumerate(opts): lines.append(f"       {_LETTERS[j]}) {o}")
        lines.append("")
        akey.append(f"  Q{i:>3}.  [{_LETTERS[corr]}]  {opts[corr]}")
    akey.append(sep)

    txt_bytes = "\n".join(lines + akey).encode("utf-8")
    file_stream = io.BytesIO(txt_bytes)
    file_stream.seek(0)
    try:
        bot.send_document(chat_id, InputFile(file_stream, file_name=f"quiz_{quiz_id}_test.txt"),
            caption=f"Test Series: {quiz['title']} ({len(questions)} Qs + answer key)")
    except Exception as e:
        safe_send(chat_id, f"Export failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
#  PDF EXPORT — fpdf2 + uharfbuzz (Perfect Hindi/Devanagari Text Shaping)
# ══════════════════════════════════════════════════════════════════════════════

def _generate_quiz_pdf(quiz, questions):
    """
    fpdf2 + uharfbuzz se professional PDF — perfect Hindi/Devanagari rendering.
    Returns io.BytesIO with PDF bytes.
    """
    from fpdf import FPDF
    import re as _re

    title   = str(quiz.get("title", "Quiz"))
    qid     = str(quiz.get("short_id") or quiz.get("quiz_id", ""))
    neg_v   = parse_neg_value(quiz.get("neg_marking", "0"))
    neg_d   = f"{neg_v:.4f}".rstrip("0").rstrip(".") if neg_v else "0"
    total_q = len(questions)
    labels  = ["(A)","(B)","(C)","(D)","(E)","(F)","(G)","(H)","(I)","(J)"]

    class QPDF(FPDF):
        def header(self):
            if self.page_no() == 1:
                self.set_fill_color(26, 35, 126)
                self.rect(0, 0, 210, 26, "F")
                self.set_fill_color(229, 57, 53)
                self.rect(0, 26, 210, 1, "F")
                self.set_text_color(255, 255, 255)
                self.set_font("hindi", "B", 18)
                self.set_xy(10, 5)
                self.cell(190, 10, title, align="C")
                self.set_font("hindi", "", 9)
                self.set_text_color(144, 202, 249)
                self.set_xy(10, 16)
                self.cell(190, 6, f"Quiz ID: {qid}  |  {total_q} Questions", align="C")
                self.set_y(32)
            else:
                self.set_fill_color(26, 35, 126)
                self.rect(0, 0, 210, 13, "F")
                self.set_fill_color(229, 57, 53)
                self.rect(0, 13, 210, 0.7, "F")
                self.set_text_color(255, 255, 255)
                self.set_font("hindi", "B", 10)
                self.set_xy(15, 3)
                self.cell(90, 7, title[:40])
                self.set_font("hindi", "", 9)
                pg = f"Page {self.page_no()}"
                pw = self.get_string_width(pg)
                self.set_xy(195 - pw, 3)
                self.cell(pw, 7, pg)
                self.set_y(18)

        def footer(self):
            self.set_y(-15)
            self.set_draw_color(224, 224, 224)
            self.set_line_width(0.3)
            self.line(15, self.get_y(), 195, self.get_y())
            self.ln(3)
            self.set_text_color(150, 150, 150)
            self.set_font("hindi", "", 7)
            self.cell(60, 5, title[:25], align="L")
            self.cell(60, 5, f"Quiz ID: {qid}", align="C")
            self.cell(60, 5, f"Page {self.page_no()}", align="R")

    pdf = QPDF()
    pdf.set_auto_page_break(True, margin=20)

    font_reg  = os.path.join(".", "NotoSansDevanagari-Regular.ttf")
    font_bold = os.path.join(".", "NotoSansDevanagari-Bold.ttf")

    if os.path.exists(font_reg):
        pdf.add_font("hindi", "", font_reg)
        pdf.add_font("hindi", "B", font_bold if os.path.exists(font_bold) else font_reg)
        logging.info("PDF: NotoSansDevanagari font loaded (fpdf2)")
    else:
        logging.error("PDF: NotoSansDevanagari-Regular.ttf NOT FOUND!")
        raise FileNotFoundError(
            "NotoSansDevanagari-Regular.ttf not found in bot directory. "
            "Download from https://fonts.google.com/noto/specimen/Noto+Sans+Devanagari"
        )

    try:
        pdf.set_text_shaping(True)
        logging.info("PDF: HarfBuzz text shaping enabled")
    except Exception as e:
        logging.error(f"PDF: Text shaping FAILED: {e}. Install uharfbuzz: pip install uharfbuzz")

    pdf.add_page()

    # Instructions Box
    inst_lines = [
        f"• Total Questions / कुल प्रश्न: {total_q}",
        f"• Negative / ऋणात्मक: {neg_d}",
        f"• Timer / समय: {quiz.get('timer_seconds', 45)}s per question",
        "• प्रत्येक प्रश्न समान अंक का है",
        "• उत्तर हर प्रश्न के नीचे दिया गया है",
        f"• Date: {datetime.now().strftime('%d %b %Y')}",
    ]

    line_h, title_h, pad = 6, 8, 5
    box_y = pdf.get_y()
    box_h = pad + title_h + len(inst_lines) * line_h + pad

    pdf.set_fill_color(245, 245, 245)
    pdf.set_draw_color(224, 224, 224)
    pdf.rect(10, box_y, 190, box_h, "DF")

    pdf.set_xy(15, box_y + pad)
    pdf.set_font("hindi", "B", 11)
    pdf.set_text_color(26, 35, 126)
    pdf.cell(0, title_h, "Instructions / निर्देश:", new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("hindi", "", 9.5)
    pdf.set_text_color(51, 51, 51)
    for line in inst_lines:
        pdf.set_x(17)
        pdf.cell(0, line_h, line, new_x="LMARGIN", new_y="NEXT")

    pdf.set_y(box_y + box_h + 8)

    # Questions Loop
    for q in questions:
        qn       = q.get("position", 0) + 1
        qt       = str(q.get("q_text", ""))
        ref      = str(q.get("ref_text", "") or "").strip()
        opts_raw = q.get("options", "[]")
        opts     = json.loads(opts_raw) if isinstance(opts_raw, str) else (opts_raw or [])
        cor      = q.get("correct_idx", 0)

        est_h = 50 + len(opts) * 8
        if ref: est_h += max(18, (len(ref) // 75 + 1) * 6 + 12)
        if len(qt) > 80: est_h += (len(qt) // 75 + 1) * 7
        if pdf.get_y() + est_h > 265: pdf.add_page()

        card_top = pdf.get_y()

        if ref:
            pdf.set_fill_color(248, 249, 250)
            ref_y = pdf.get_y()
            pdf.set_x(17)
            pdf.set_font("hindi", "", 9)
            pdf.set_text_color(85, 85, 85)
            ref_fmt = _re.sub(r'(?<=[।\.\?])\s+(?=\d+[\.\)])', '\n', ref)
            pdf.multi_cell(176, 5.5, ref_fmt, fill=True, new_x="LMARGIN", new_y="NEXT")
            ref_end = pdf.get_y()
            pdf.set_draw_color(170, 170, 170)
            pdf.set_line_width(0.8)
            pdf.line(15.5, ref_y, 15.5, ref_end)
            pdf.set_line_width(0.2)
            pdf.ln(4)

        pdf.set_font("hindi", "B", 10.5)
        pdf.set_text_color(33, 33, 33)
        pdf.set_x(14)
        pdf.multi_cell(182, 6.5, f"Q{qn}. {qt}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        pdf.set_font("hindi", "", 10)
        pdf.set_text_color(66, 66, 66)
        for i, o in enumerate(opts):
            lb = labels[i] if i < len(labels) else f"({i+1})"
            pdf.set_x(22)
            pdf.multi_cell(170, 6, f"{lb}  {str(o)}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(3)

        al = labels[cor] if 0 <= cor < len(labels) else "?"
        at = str(opts[cor]) if 0 <= cor < len(opts) else "N/A"
        ans_text = f"  उत्तर (Answer): {al}  {at}"

        pdf.set_fill_color(232, 245, 233)
        pdf.set_draw_color(76, 175, 80)
        pdf.set_text_color(46, 125, 50)
        pdf.set_font("hindi", "B", 10)
        pdf.set_x(14)
        pdf.multi_cell(182, 7, ans_text, border=1, fill=True, new_x="LMARGIN", new_y="NEXT")

        card_bottom = pdf.get_y()
        pdf.set_draw_color(26, 115, 232)
        pdf.set_line_width(1.0)
        pdf.line(10.5, card_top, 10.5, card_bottom)
        pdf.set_line_width(0.2)
        pdf.ln(3)

        pdf.set_draw_color(224, 224, 224)
        pdf.line(22, pdf.get_y(), 190, pdf.get_y())
        pdf.ln(6)

    buf = io.BytesIO(pdf.output())
    buf.seek(0)
    return buf


def _export_pdf_quizpdf(chat_id, quiz_id):
    """Fetch quiz from DB and send professional fpdf2 PDF with perfect Hindi."""
    try:
        from fpdf import FPDF  # noqa
    except ImportError:
        safe_send(chat_id,
            "⚠️ *fpdf2* library nahi hai!\n\n"
            "Render ke `requirements.txt` mein ye add karo:\n"
            "`fpdf2>=2.7.6`\n`uharfbuzz`\n\n"
            "Tab tak TXT format bhej raha hun...",
            parse_mode="Markdown")
        _export_txt(chat_id, quiz_id)
        return

    with get_db() as conn:
        quiz = conn.execute("SELECT * FROM quizzes WHERE quiz_id=?", (quiz_id,)).fetchone()
        if not quiz:
            safe_send(chat_id, f"❌ Quiz ID `{quiz_id}` nahi mila.", parse_mode="Markdown")
            return
        questions = conn.execute(
            "SELECT * FROM questions WHERE quiz_id=? ORDER BY position, question_id",
            (quiz_id,)).fetchall()

    if not questions:
        safe_send(chat_id, "❌ Is quiz mein koi question nahi hai.")
        return

    safe_send(chat_id, "⏳ <b>PDF ban rahi hai...</b>", parse_mode="HTML")

    try:
        quiz_dict = dict(quiz)
        qs_list   = [dict(q) for q in questions]

        pdf_buf = _generate_quiz_pdf(quiz_dict, qs_list)
        title   = quiz["title"]
        sid     = quiz.get("short_id") or str(quiz_id)

        import re as _re
        clean = _re.sub(r"[^\w\s\-]", "", title)
        clean = _re.sub(r"\s+", "_", clean.strip())
        fname = f"{clean}_{sid}.pdf"

        caption = (
            f"📄 <b>{html_mod.escape(title)}</b>\n"
            f"📊 Questions: {len(questions)} | 🆔 <code>{sid}</code>\n"
            f"✅ Har question ke niche answer diya gaya hai\n"
            f"🔤 Hindi/Devanagari: Perfect rendering"
        )

        pdf_buf.seek(0)
        bot.send_document(chat_id, InputFile(pdf_buf, file_name=fname),
                          caption=caption, parse_mode="HTML")
    except FileNotFoundError as fnf:
        safe_send(chat_id,
                  f"❌ Font file missing!\n\n{fnf}\n\n"
                  "NotoSansDevanagari-Regular.ttf file bot directory mein rakho.",
                  parse_mode=None)
    except Exception as e:
        logging.error(f"quizpdf error quiz_id={quiz_id}: {e}", exc_info=True)
        safe_send(chat_id, f"❌ PDF error: {e}\nTXT format try kar raha hun...")
        _export_txt(chat_id, quiz_id)

_export_pdf_testseries = _export_pdf_quizpdf

# ══════════════════════════════════════════════════════════════════════════════
#  INTERACTIVE PRACTICE HTML EXPORT
# ══════════════════════════════════════════════════════════════════════════════

def _export_practice_html(chat_id, quiz_id):
    try:
        time.sleep(3)
        with get_db() as conn:
            quiz = conn.execute("SELECT * FROM quizzes WHERE quiz_id=?", (quiz_id,)).fetchone()
            if not quiz: return
            questions = conn.execute(
                "SELECT * FROM questions WHERE quiz_id=? ORDER BY position,question_id", (quiz_id,)).fetchall()
        if not questions: return

        neg_val = parse_neg_value(quiz["neg_marking"])
        neg_display = f"{neg_val:.6f}".rstrip('0').rstrip('.') if neg_val else "0"
        total_q = len(questions)
        timer_minutes = max(10, (total_q + 2) // 3)
        timer_seconds_total = timer_minutes * 60

        js_items = []
        for q in questions:
            opts = json.loads(q["options"])
            js_items.append({"q": q["q_text"], "ref": q["ref_text"] or "", "opts": opts, "ans": q["correct_idx"]})
        js_questions = json.dumps(js_items, ensure_ascii=False)
        safe_title = html_mod.escape(quiz["title"])

        html_out = f"""<!DOCTYPE html>
<html lang="hi"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{safe_title}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{font-family:'Segoe UI',sans-serif;background:#f0f2f5;color:#222;}}
#start-screen{{max-width:480px;margin:60px auto;background:#fff;border-radius:16px;padding:32px 24px;text-align:center;box-shadow:0 4px 20px rgba(0,0,0,.1);}}
#start-screen h1{{font-size:26px;color:#3b3f9e;margin-bottom:6px;}}
#start-screen p{{color:#888;font-size:14px;margin-bottom:20px;}}
.info-row{{display:flex;justify-content:space-between;padding:11px 0;border-bottom:1px solid #eee;font-size:15px;}}
.info-label{{color:#888;}}.info-value{{font-weight:600;}}
#start-btn{{margin-top:22px;width:100%;padding:14px;background:#3b3f9e;color:#fff;border:none;border-radius:10px;font-size:17px;cursor:pointer;}}
#start-btn:hover{{background:#2d3180;}}
#hdr{{background:#3b3f9e;color:#fff;padding:10px 16px;display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:100;}}
#hdr h2{{font-size:15px;}}#timer{{font-size:20px;font-weight:700;color:#ffd700;}}
#quiz-screen{{display:none;max-width:680px;margin:16px auto;padding:0 12px 80px;}}
.qcard{{background:#fff;border-radius:12px;padding:20px;box-shadow:0 2px 10px rgba(0,0,0,.08);}}
.qmeta{{font-size:11px;color:#3b3f9e;font-weight:700;margin-bottom:4px;}}
.qref{{background:#f8f9fa;border-left:3px solid #aaa;padding:9px 13px;margin-bottom:10px;font-size:13px;color:#444;border-radius:4px;white-space:pre-wrap;}}
.qtext{{font-size:15.5px;font-weight:600;line-height:1.65;margin-bottom:14px;}}
.qmarks{{font-size:12px;color:#3b3f9e;margin-bottom:10px;}}
.opt{{display:block;width:100%;text-align:left;padding:11px 15px;margin:7px 0;border:1.5px solid #ddd;border-radius:8px;background:#fff;font-size:14.5px;cursor:pointer;transition:all .15s;}}
.opt:hover{{background:#f0f0ff;border-color:#3b3f9e;}}
.opt.correct{{background:#e6f9ee;border-color:#27ae60;color:#1a7a42;font-weight:600;}}
.opt.wrong{{background:#fdecea;border-color:#e74c3c;color:#c0392b;}}
#nav{{position:fixed;bottom:0;left:0;right:0;background:#fff;padding:10px 14px;display:none;justify-content:space-between;gap:8px;box-shadow:0 -2px 10px rgba(0,0,0,.1);}}
.nbtn{{flex:1;padding:12px;border:none;border-radius:8px;font-size:14px;cursor:pointer;font-weight:600;}}
#pbtn{{background:#eee;color:#444;}}#rbtn{{background:#fff3cd;color:#856404;border:1.5px solid #ffc107;}}#nbtn{{background:#3b3f9e;color:#fff;}}
#result-screen{{display:none;max-width:460px;margin:40px auto;background:#fff;border-radius:16px;padding:26px 20px;text-align:center;box-shadow:0 4px 20px rgba(0,0,0,.1);}}
.rtitle{{font-size:22px;font-weight:700;color:#3b3f9e;margin-bottom:18px;}}
.rgrid{{display:grid;grid-template-columns:1fr 1fr;gap:11px;margin-bottom:18px;}}
.rbox{{background:#f0f2f5;border-radius:10px;padding:13px;}}.rval{{font-size:21px;font-weight:700;}}.rlbl{{font-size:12px;color:#777;margin-top:3px;}}
.gc{{color:#27ae60;}}.rc{{color:#e74c3c;}}.bc{{color:#3b3f9e;}}
.abtn{{width:100%;padding:12px;border:none;border-radius:9px;font-size:15px;cursor:pointer;color:#fff;margin-top:7px;font-weight:600;}}
</style></head><body>
<div id="start-screen">
  <h1>📋 Practice Quiz</h1><p>Bihar Special BPSC PYQs</p>
  <div class="info-row"><span class="info-label">Topic</span><span class="info-value">{safe_title}</span></div>
  <div class="info-row"><span class="info-label">Questions</span><span class="info-value">{total_q}</span></div>
  <div class="info-row"><span class="info-label">Timer</span><span class="info-value">{timer_minutes} Minutes</span></div>
  <div class="info-row"><span class="info-label">N.Mark</span><span class="info-value">-{neg_display} per wrong answer</span></div>
  <button id="start-btn" onclick="startQuiz()">Start</button>
</div>
<div id="quiz-screen">
  <div id="hdr"><h2 id="qctr">Q 1 / {total_q}</h2><div id="timer">--:--</div></div>
  <div class="qcard" style="margin-top:14px;">
    <div class="qmeta" id="qmeta"></div>
    <div class="qref" id="qref" style="display:none"></div>
    <div class="qtext" id="qtext"></div>
    <div class="qmarks">+1 / -{neg_display}</div>
    <div id="opts"></div>
  </div>
</div>
<div id="nav">
  <button class="nbtn" id="pbtn" onclick="go(-1)">◀ Prev</button>
  <button class="nbtn" id="rbtn" onclick="markRev()">🔖 Review</button>
  <button class="nbtn" id="nbtn" onclick="go(1)">Next ▶</button>
</div>
<div id="result-screen">
  <div class="rtitle">🏆 Result</div>
  <div class="rgrid">
    <div class="rbox"><div class="rval bc" id="rs"></div><div class="rlbl">Score</div></div>
    <div class="rbox"><div class="rval" id="ra"></div><div class="rlbl">Accuracy</div></div>
    <div class="rbox"><div class="rval gc" id="rc"></div><div class="rlbl">✅ Correct</div></div>
    <div class="rbox"><div class="rval rc" id="rw"></div><div class="rlbl">❌ Wrong</div></div>
    <div class="rbox"><div class="rval" id="rsk"></div><div class="rlbl">⏭ Skipped</div></div>
    <div class="rbox"><div class="rval" id="rt"></div><div class="rlbl">⏱ Time</div></div>
  </div>
  <button class="abtn" style="background:#3b3f9e;" onclick="retryAll()">🔁 Play Again</button>
  <button class="abtn" style="background:#e74c3c;" onclick="retryWrong()">📝 Review Mistakes</button>
</div>
<script>
const ALL_QS={js_questions};
const NEG={neg_val};
const TOTAL_SECS={timer_seconds_total};
let QS=ALL_QS.slice(),cur=0,ans={{}},rev=new Set(),tLeft=TOTAL_SECS,tiv=null,t0=null;
function startQuiz(){{document.getElementById('start-screen').style.display='none';document.getElementById('quiz-screen').style.display='block';document.getElementById('nav').style.display='flex';t0=Date.now();startTimer();showQ(0);}}
function startTimer(){{tiv=setInterval(()=>{{tLeft--;const m=String(Math.floor(tLeft/60)).padStart(2,'0'),s=String(tLeft%60).padStart(2,'0');document.getElementById('timer').textContent=m+':'+s;if(tLeft<=0){{clearInterval(tiv);showResult();}}}},1000);}}
function showQ(i){{cur=i;const q=QS[i];document.getElementById('qctr').textContent='Q '+(i+1)+' / '+QS.length;document.getElementById('qmeta').textContent='Question '+(i+1);const refEl=document.getElementById('qref');if(q.ref){{refEl.style.display='block';refEl.textContent=q.ref;}}else{{refEl.style.display='none';}}document.getElementById('qtext').textContent='Q'+(i+1)+'. '+q.q;const cont=document.getElementById('opts');cont.innerHTML='';q.opts.forEach((o,j)=>{{const b=document.createElement('button');b.className='opt';b.textContent=o;if(ans[i]!==undefined){{if(j===ans[i])b.classList.add(j===q.ans?'correct':'wrong');else if(j===q.ans)b.classList.add('correct');b.disabled=true;}}else{{b.onclick=()=>pick(i,j);}}cont.appendChild(b);}});document.getElementById('pbtn').disabled=i===0;document.getElementById('nbtn').textContent=i===QS.length-1?'Submit ✓':'Next ▶';document.getElementById('rbtn').style.background=rev.has(i)?'#ffc107':'';}}
function pick(qi,oi){{ans[qi]=oi;const q=QS[qi];document.querySelectorAll('.opt').forEach((b,j)=>{{b.disabled=true;if(j===oi)b.classList.add(j===q.ans?'correct':'wrong');else if(j===q.ans)b.classList.add('correct');}});setTimeout(()=>go(1),700);}}
function go(d){{const nx=cur+d;if(nx<0)return;if(nx>=QS.length){{clearInterval(tiv);showResult();return;}}showQ(nx);}}
function markRev(){{rev.has(cur)?rev.delete(cur):rev.add(cur);showQ(cur);}}
function showResult(){{document.getElementById('quiz-screen').style.display='none';document.getElementById('nav').style.display='none';document.getElementById('result-screen').style.display='block';let c=0,w=0,sk=0;QS.forEach((q,i)=>{{if(ans[i]===undefined)sk++;else if(ans[i]===q.ans)c++;else w++;}});const score=c-w*NEG,acc=(c+w)>0?((c/(c+w))*100).toFixed(1):'0.0';const el=Math.floor((Date.now()-t0)/1000);document.getElementById('rs').textContent=score.toFixed(2);document.getElementById('ra').textContent=acc+'%';document.getElementById('rc').textContent=c;document.getElementById('rw').textContent=w;document.getElementById('rsk').textContent=sk;document.getElementById('rt').textContent=Math.floor(el/60)+'m '+String(el%60).padStart(2,'0')+'s';}}
function retryAll(){{QS=ALL_QS.slice();ans={{}};rev=new Set();tLeft=TOTAL_SECS;cur=0;document.getElementById('result-screen').style.display='none';document.getElementById('quiz-screen').style.display='block';document.getElementById('nav').style.display='flex';t0=Date.now();clearInterval(tiv);startTimer();showQ(0);}}
function retryWrong(){{const wrong=ALL_QS.filter((q,i)=>ans[i]!==undefined&&ans[i]!==q.ans);if(!wrong.length){{alert('Koi wrong answer nahi!');return;}}QS=wrong;ans={{}};rev=new Set();tLeft=Math.max(300,wrong.length*20);cur=0;document.getElementById('result-screen').style.display='none';document.getElementById('quiz-screen').style.display='block';document.getElementById('nav').style.display='flex';t0=Date.now();clearInterval(tiv);startTimer();showQ(0);}}
</script></body></html>"""

        html_bytes = html_out.encode("utf-8")
        file_stream = io.BytesIO(html_bytes)
        file_stream.seek(0)
        caption_text = (f"📋 Quiz: {safe_title}\n\n📥 Download → Open in any browser (Chrome recommended)\n\n❓ Questions: {total_q} | ⏱ Timer: {timer_minutes} min | ➖ N.Mark: -{neg_display}")
        bot.send_document(chat_id, InputFile(file_stream, file_name=f"practice_quiz_{quiz_id}.html"), caption=caption_text, parse_mode=None)
        logging.info(f"Practice HTML sent for quiz {quiz_id}")
    except Exception as e:
        logging.error(f"Practice HTML export failed for quiz {quiz_id}: {e}")
        try: bot.send_message(chat_id, f"⚠️ Practice HTML generate nahi ho payi: {e}")
        except Exception: pass

# ══════════════════════════════════════════════════════════════════════════════
#  COMMANDS
# ══════════════════════════════════════════════════════════════════════════════

def is_group(msg_or_chat_id):
    if isinstance(msg_or_chat_id, int): return msg_or_chat_id < 0
    try: return msg_or_chat_id.chat.type in ("group", "supergroup", "channel")
    except AttributeError: return False

def group_only_reply(msg):
    if is_group(msg):
        bot.send_message(msg.chat.id, "ℹ️ Quiz creation aur settings ke liye mujhe *private chat* mein message karo!", parse_mode="Markdown", reply_markup=ReplyKeyboardRemove())
        return True
    return False

@bot.message_handler(commands=["logs"])
def cmd_logs(msg):
    if not is_owner(msg.from_user.id):
        return bot.send_message(msg.chat.id, "🚫 Sirf owner server logs dekh sakta hai.")
    try:
        if os.path.exists('bot_activity.log'):
            with open('bot_activity.log', 'rb') as f:
                bot.send_document(msg.chat.id, f, caption="📂 Yeh rahi aapki server log file!")
        else:
            bot.send_message(msg.chat.id, "⚠️ Log file abhi tak nahi bani hai.")
    except Exception as e:
        bot.send_message(msg.chat.id, f"⚠️ Error: {e}")

@bot.message_handler(commands=["start"])
def cmd_start(msg):
    register_user(msg)
    uid, parts = msg.from_user.id, msg.text.split(maxsplit=1)
    if is_banned(uid):
        return bot.send_message(msg.chat.id, "🚫 Aapko is bot se ban kar diya gaya hai.")
    if is_group(msg):
        if len(parts) > 1 and parts[1].startswith("quiz_"):
            try:
                set_state(uid, "idle")
                _do_start_quiz(msg.chat.id, uid, int(parts[1][5:]))
            except Exception as e: safe_send(msg.chat.id, f"⚠️ Could not start quiz: {e}")
        else:
            with get_db() as conn: last_sess = conn.execute("SELECT quiz_id FROM active_sessions WHERE chat_id=? ORDER BY session_id DESC LIMIT 1", (msg.chat.id,)).fetchone()
            if last_sess: _do_start_quiz(msg.chat.id, uid, last_sess["quiz_id"])
            else: bot.send_message(msg.chat.id, "👋 Is group mein abhi tak koi quiz nahi hua.", parse_mode="Markdown", reply_markup=ReplyKeyboardRemove())
        return

    if len(parts) > 1 and parts[1].startswith("quiz_"):
        try:
            set_state(uid, "idle")
            _do_start_quiz(msg.chat.id, uid, int(parts[1][5:]))
            return
        except Exception: pass

    set_state(uid, "idle")
    _wizard.pop(uid, None)
    safe_send(msg.chat.id, f"Hello, *{msg.from_user.first_name or 'there'}*! Welcome to *QuizBot Pro*.\n\nUse /create to make a quiz, /features for all commands.", parse_mode="Markdown", reply_markup=ReplyKeyboardRemove())

@bot.message_handler(commands=["features"])
def cmd_features(msg):
    if group_only_reply(msg): return
    safe_send(msg.chat.id, "*QuizBot Pro — Commands*\n\n*Creation:* /create /done /edit /stopedit /cancel /myquizzes\n*Import:* /loadfile — upload .txt or .json directly\n*Session:* /startquiz /pause /resume /stop\n*Result:* /result — your personal scorecard\n*Export:* /createhtml /quizpdf /testseries\n*Settings:* /html /stats\n\n📄 `/quizpdf <id>` — Professional PDF with answers below each question", parse_mode="Markdown")

@bot.message_handler(commands=["stats"])
def cmd_stats(msg):
    if group_only_reply(msg): return
    with get_db() as conn:
        u = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        q = conn.execute("SELECT COUNT(*) FROM quizzes").fetchone()[0]
        n = conn.execute("SELECT COUNT(*) FROM questions").fetchone()[0]
        s = conn.execute("SELECT COUNT(*) FROM active_sessions WHERE is_completed=1").fetchone()[0]
    safe_send(msg.chat.id, f"*Stats*\nUsers: `{u}` | Quizzes: `{q}` | Questions: `{n}` | Sessions: `{s}`", parse_mode="Markdown")

@bot.message_handler(commands=["create"])
def cmd_create(msg):
    if group_only_reply(msg): return
    register_user(msg)
    uid = msg.from_user.id
    set_state(uid, "awaiting_quiz_title")
    _wizard[uid] = {"title": None, "questions": [], "neg": "0", "quiz_type": "free", "timer": 45, "section": 0}
    safe_send(msg.chat.id, "✅ *New Quiz*\n\nSend me the *quiz name*.\n_(Type /cancel to abort)_", parse_mode="Markdown")

@bot.message_handler(commands=["cancel"])
def cmd_cancel(msg):
    if group_only_reply(msg): return
    uid = msg.from_user.id
    set_state(uid, "idle"); _wizard.pop(uid, None)
    safe_send(msg.chat.id, "Cancelled.", reply_markup=ReplyKeyboardRemove())

@bot.message_handler(commands=["done"])
def cmd_done(msg):
    if group_only_reply(msg): return
    uid = msg.from_user.id
    state = get_state(uid)
    if state not in ("adding_questions", "awaiting_quiz_title"): return safe_send(msg.chat.id, "No active quiz creation. Use /create.")
    store = _wizard.get(uid, {})
    if not store.get("questions"): return safe_send(msg.chat.id, "No questions buffered yet.")
    n = len(store["questions"])
    set_state(uid, "awaiting_section_quiz")
    safe_send(msg.chat.id, f"✅ *{n} question(s) ready!*\n\n📋 *Section quiz?* yes / no", parse_mode="Markdown")

@bot.message_handler(commands=["edit"])
def cmd_edit(msg):
    if group_only_reply(msg): return
    register_user(msg)
    uid, parts = msg.from_user.id, msg.text.split(maxsplit=1)
    if len(parts) > 1:
        id_str = parts[1].strip()
        quiz = find_quiz(uid, id_str)
        if quiz:
            with get_db() as conn: q_count = conn.execute("SELECT COUNT(*) FROM questions WHERE quiz_id=?", (quiz["quiz_id"],)).fetchone()[0]
            send_edit_panel(msg.chat.id, quiz, q_count)
            return
        return safe_send(msg.chat.id, f"Quiz '{id_str}' not found or you are not the creator.")
    set_state(uid, "awaiting_edit_quiz_id")
    _wizard[uid] = {"edit_mode": True, "questions": [], "quiz_id": None}
    safe_send(msg.chat.id, "Send the *Quiz ID or Short ID* to edit.", parse_mode="Markdown")

@bot.message_handler(commands=["stopedit"])
def cmd_stopedit(msg):
    if group_only_reply(msg): return
    uid = msg.from_user.id
    if get_state(uid) != "editing_questions": return safe_send(msg.chat.id, "Not in edit mode.")
    store = _wizard.get(uid, {})
    qs, qid = store.get("questions", []), store.get("quiz_id")
    if qs and qid:
        save_questions(qid, qs)
        safe_send(msg.chat.id, f"Saved *{len(qs)}* question(s) to Quiz `{qid}`.", parse_mode="Markdown", reply_markup=ReplyKeyboardRemove())
    else: safe_send(msg.chat.id, "Nothing to save.", reply_markup=ReplyKeyboardRemove())
    set_state(uid, "idle"); _wizard.pop(uid, None)

@bot.message_handler(commands=["myquizzes"])
def cmd_myquizzes(msg):
    if group_only_reply(msg): return
    register_user(msg)
    uid = msg.from_user.id
    with get_db() as conn:
        rows = conn.execute("SELECT z.quiz_id,z.short_id,z.title,z.quiz_type,z.timer_seconds,COUNT(q.question_id) AS cnt "
                            "FROM quizzes z LEFT JOIN questions q ON z.quiz_id=q.quiz_id "
                            "WHERE z.creator_id=? GROUP BY z.quiz_id ORDER BY z.created_at DESC", (uid,)).fetchall()
    if not rows: return safe_send(msg.chat.id, "No quizzes yet. Use /create!")
    lines = ["*Your Quizzes:*\n"]
    for r in rows:
        sid = r["short_id"] or str(r["quiz_id"])
        lines.append(f"`{sid}` (#{r['quiz_id']}) — *{r['title']}* ({r['cnt']} Qs · {r['timer_seconds']}s · {r['quiz_type']})")
    safe_send(msg.chat.id, "\n".join(lines), parse_mode="Markdown")

@bot.message_handler(regexp=r"^/startquiz_(\d+)(@\S+)?$")
def cmd_startquiz_direct(msg):
    register_user(msg)
    if m := re.match(r"^/startquiz_(\d+)", msg.text.strip()): _do_start_quiz(msg.chat.id, msg.from_user.id, int(m.group(1)))

@bot.message_handler(commands=["startquiz"])
def cmd_startquiz(msg):
    register_user(msg)
    uid, cmd_text = msg.from_user.id, re.sub(r'^/startquiz@\S+', '/startquiz', msg.text.strip())
    parts = cmd_text.split(maxsplit=1)
    if len(parts) > 1 and parts[1].strip().isdigit(): _do_start_quiz(msg.chat.id, uid, int(parts[1].strip()))
    elif is_group(msg): bot.send_message(msg.chat.id, "Usage: `/startquiz <quiz_id>`", parse_mode="Markdown", reply_markup=ReplyKeyboardRemove())
    else:
        set_state(uid, "awaiting_start_quiz_id")
        safe_send(msg.chat.id, "Send me the *Quiz ID* to start.", parse_mode="Markdown")

def _countdown_and_start(chat_id, uid, quiz_id, show_countdown=True):
    with get_db() as conn:
        quiz = conn.execute("SELECT * FROM quizzes WHERE quiz_id=?", (quiz_id,)).fetchone()
        if not quiz: return safe_send(chat_id, f"⚠️ Quiz ID {quiz_id} not found.")
        qs = conn.execute("SELECT question_id FROM questions WHERE quiz_id=? ORDER BY position,question_id", (quiz_id,)).fetchall()
        if not qs: return safe_send(chat_id, "⚠️ This quiz has no questions.")
        existing = conn.execute("SELECT session_id FROM active_sessions WHERE user_id=? AND chat_id=? AND is_completed=0", (uid, chat_id)).fetchone()
        if existing: return safe_send(chat_id, "⚠️ Active session exists. Use /stop first.")

        q_ids = [q["question_id"] for q in qs]
        if quiz["shuffle_q"]: random.shuffle(q_ids)
        cur = conn.execute("INSERT INTO active_sessions(user_id,quiz_id,chat_id,total_q,shuffled_order) VALUES(?,?,?,?,?) RETURNING session_id", (uid, quiz_id, chat_id, len(qs), json.dumps(q_ids)))
        sid = cur.fetchone()[0]

    if show_countdown and is_group(chat_id):
        try:
            bot.send_message(chat_id, f"🏁 *{quiz['title']}*\n📊 {len(qs)} questions · ⏱ {quiz['timer_seconds']}s each", parse_mode="Markdown")
            time.sleep(0.5); cd = bot.send_message(chat_id, "🟡 *Ready...*", parse_mode="Markdown")
            time.sleep(1.2); bot.edit_message_text("🟠 *Steady...*", chat_id, cd.message_id, parse_mode="Markdown")
            time.sleep(1.2); bot.edit_message_text("🟢 *GO! 🚀*", chat_id, cd.message_id, parse_mode="Markdown")
            time.sleep(0.5)
        except Exception: pass
    else:
        try: bot.send_message(chat_id, f"🏁 *{quiz['title']} starting!*\n📊 {len(qs)} questions · ⏱ {quiz['timer_seconds']}s each", parse_mode="Markdown")
        except Exception: pass

    send_next_poll(sid)

def _do_start_quiz(chat_id, uid, quiz_id):
    _countdown_and_start(chat_id, uid, quiz_id, show_countdown=True)

@bot.message_handler(commands=["pause"])
def cmd_pause(msg):
    uid = msg.from_user.id
    with get_db() as conn:
        s = conn.execute("SELECT session_id FROM active_sessions WHERE user_id=? AND chat_id=? AND is_completed=0", (uid, msg.chat.id)).fetchone()
        if not s: return safe_send(msg.chat.id, "No active session.")
        conn.execute("UPDATE active_sessions SET is_paused=1 WHERE session_id=?", (s["session_id"],))
    _cancel_auto_timer(s["session_id"])
    safe_send(msg.chat.id, "⏸ Paused. Use /resume to continue.")

@bot.message_handler(commands=["resume"])
def cmd_resume(msg):
    uid = msg.from_user.id
    with get_db() as conn:
        s = conn.execute("SELECT session_id FROM active_sessions WHERE user_id=? AND chat_id=? AND is_completed=0", (uid, msg.chat.id)).fetchone()
        if not s: return safe_send(msg.chat.id, "No active session.")
        conn.execute("UPDATE active_sessions SET is_paused=0 WHERE session_id=?", (s["session_id"],))
        sid = s["session_id"]
    safe_send(msg.chat.id, "▶️ Resumed!")
    send_next_poll(sid)

@bot.message_handler(commands=["stop"])
def cmd_stop(msg):
    uid = msg.from_user.id
    with get_db() as conn:
        s = conn.execute("SELECT session_id FROM active_sessions WHERE user_id=? AND chat_id=? AND is_completed=0", (uid, msg.chat.id)).fetchone()
        if not s: return safe_send(msg.chat.id, "No active session.")
        sess = conn.execute("SELECT * FROM active_sessions WHERE session_id=?", (s["session_id"],)).fetchone()
        quiz = conn.execute("SELECT * FROM quizzes WHERE quiz_id=?", (sess["quiz_id"],)).fetchone()
        conn.execute("UPDATE active_sessions SET is_completed=1,end_time=EXTRACT(EPOCH FROM NOW())::INTEGER WHERE session_id=?", (s["session_id"],))
    _cancel_auto_timer(s["session_id"])
    safe_send(msg.chat.id, "🛑 *Session stopped.*", parse_mode="Markdown")
    neg_val = parse_neg_value(quiz["neg_marking"]) if quiz else 0.0
    _send_leaderboard(s["session_id"], msg.chat.id, quiz["title"] if quiz else "Quiz", neg_val, sess["total_q"], sess["start_time"])

@bot.message_handler(commands=["fast"])
def cmd_fast(msg):
    uid, parts = msg.from_user.id, msg.text.split()
    delta = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 5
    with get_db() as conn:
        s = conn.execute("SELECT session_id, quiz_id FROM active_sessions WHERE user_id=? AND chat_id=? AND is_completed=0", (uid, msg.chat.id)).fetchone()
        if not s: return safe_send(msg.chat.id, "⚠️ Koi active quiz nahi hai.")
        quiz = conn.execute("SELECT timer_seconds FROM quizzes WHERE quiz_id=?", (s["quiz_id"],)).fetchone()
        new_timer = max(10, int(quiz["timer_seconds"]) - delta)
        conn.execute("UPDATE quizzes SET timer_seconds=? WHERE quiz_id=?", (new_timer, s["quiz_id"]))
    safe_send(msg.chat.id, f"⚡ Timer fast! Ab <b>{new_timer}s</b> per question (-{delta}s)", parse_mode="HTML")

@bot.message_handler(commands=["slow"])
def cmd_slow(msg):
    uid, parts = msg.from_user.id, msg.text.split()
    delta = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 5
    with get_db() as conn:
        s = conn.execute("SELECT session_id, quiz_id FROM active_sessions WHERE user_id=? AND chat_id=? AND is_completed=0", (uid, msg.chat.id)).fetchone()
        if not s: return safe_send(msg.chat.id, "⚠️ Koi active quiz nahi hai.")
        quiz = conn.execute("SELECT timer_seconds FROM quizzes WHERE quiz_id=?", (s["quiz_id"],)).fetchone()
        new_timer = min(300, int(quiz["timer_seconds"]) + delta)
        conn.execute("UPDATE quizzes SET timer_seconds=? WHERE quiz_id=?", (new_timer, s["quiz_id"]))
    safe_send(msg.chat.id, f"🐢 Timer slow! Ab <b>{new_timer}s</b> per question (+{delta}s)", parse_mode="HTML")

@bot.message_handler(commands=["result"])
def cmd_result(msg):
    if group_only_reply(msg): return
    register_user(msg)
    send_individual_result(msg.chat.id, msg.from_user.id)

@bot.message_handler(commands=["html"])
def cmd_html(msg):
    if group_only_reply(msg): return
    register_user(msg)
    uid = msg.from_user.id
    with get_db() as conn:
        old = conn.execute("SELECT html_toggle FROM users WHERE user_id=?", (uid,)).fetchone()
        new = 1 - int(old["html_toggle"] or 0)
        conn.execute("UPDATE users SET html_toggle=? WHERE user_id=?", (new, uid))
    safe_send(msg.chat.id, f"HTML reports: *{'ON' if new else 'OFF'}*.", parse_mode="Markdown")

@bot.message_handler(commands=["practice"])
def cmd_practice(msg):
    if group_only_reply(msg): return
    register_user(msg)
    uid, parts = msg.from_user.id, msg.text.split(maxsplit=1)
    if len(parts) > 1 and parts[1].strip().isdigit():
        safe_send(msg.chat.id, "⏳ Practice HTML generate ho rahi hai...")
        threading.Thread(target=_export_practice_html, args=(msg.chat.id, int(parts[1].strip())), daemon=True).start()
    else:
        with get_db() as conn:
            last = conn.execute("SELECT quiz_id FROM active_sessions WHERE user_id=? ORDER BY session_id DESC LIMIT 1", (uid,)).fetchone()
        if last:
            safe_send(msg.chat.id, "⏳ Practice HTML generate ho rahi hai...")
            threading.Thread(target=_export_practice_html, args=(msg.chat.id, last["quiz_id"]), daemon=True).start()
        else:
            safe_send(msg.chat.id, "Usage: `/practice <quiz_id>`", parse_mode="Markdown")

@bot.message_handler(commands=["createhtml"])
def cmd_createhtml(msg):
    if group_only_reply(msg): return
    register_user(msg)
    uid, parts = msg.from_user.id, msg.text.split(maxsplit=1)
    if len(parts) > 1 and parts[1].strip().isdigit(): _export_html(msg.chat.id, int(parts[1].strip()))
    else:
        set_state(uid, "awaiting_html_id")
        safe_send(msg.chat.id, "Send the *Quiz ID* to export as HTML.", parse_mode="Markdown")

@bot.message_handler(commands=["testseries"])
def cmd_testseries(msg):
    if group_only_reply(msg): return
    register_user(msg)
    uid, parts = msg.from_user.id, msg.text.split(maxsplit=1)
    if len(parts) > 1 and parts[1].strip().isdigit():
        threading.Thread(target=_export_pdf_quizpdf, args=(msg.chat.id, int(parts[1].strip())), daemon=True).start()
    else:
        set_state(uid, "awaiting_txt_id")
        safe_send(msg.chat.id, "📄 *Quiz PDF*\n\nQuiz ID bhejo.", parse_mode="Markdown")

@bot.message_handler(commands=["quizpdf"])
def cmd_quizpdf(msg):
    if group_only_reply(msg): return
    register_user(msg)
    uid, parts = msg.from_user.id, msg.text.split(maxsplit=1)
    if len(parts) > 1:
        id_str = parts[1].strip()
        quiz = find_quiz(uid, id_str)
        if quiz:
            threading.Thread(target=_export_pdf_quizpdf, args=(msg.chat.id, quiz['quiz_id']), daemon=True).start()
        else:
            safe_send(msg.chat.id, f"❌ Quiz ID `{id_str}` nahi mila ya ye aapka nahi hai.")
    else:
        set_state(uid, "awaiting_txt_id")
        safe_send(msg.chat.id, "📄 *Quiz PDF*\n\nQuiz ID ya Short ID bhejo.\n\nUsage: `/quizpdf <quiz_id>`", parse_mode="Markdown")

@bot.message_handler(commands=["loadfile"])
def cmd_loadfile(msg):
    if group_only_reply(msg): return
    register_user(msg)
    set_state(msg.from_user.id, "awaiting_loadfile")
    safe_send(msg.chat.id, "*Load Quiz from File*\n\nUpload your file now.\n\n*Supported:*\n📄 `.txt` — PYQ format\n🔵 `.json` — Standard\n\n_(/cancel to abort.)_", parse_mode="Markdown")

@bot.message_handler(commands=["settimer"])
def cmd_settimer(msg):
    if group_only_reply(msg): return
    register_user(msg)
    uid, parts = msg.from_user.id, msg.text.split()
    if len(parts) < 3 or not parts[1].isdigit() or not parts[2].isdigit(): return safe_send(msg.chat.id, "Usage: `/settimer <quiz_id> <seconds>`", parse_mode="Markdown")
    qid, sec = int(parts[1]), int(parts[2])
    if sec < 10: return safe_send(msg.chat.id, "❌ Timer must be ≥ 10 seconds.")
    with get_db() as conn:
        q = conn.execute("SELECT creator_id FROM quizzes WHERE quiz_id=?", (qid,)).fetchone()
        if not q or q["creator_id"] != uid: return safe_send(msg.chat.id, "Quiz not found or not yours.")
        conn.execute("UPDATE quizzes SET timer_seconds=? WHERE quiz_id=?", (sec, qid))
    safe_send(msg.chat.id, f"✅ Timer set to *{sec}s* for Quiz `{qid}`.", parse_mode="Markdown")

@bot.message_handler(commands=["setneg"])
def cmd_setneg(msg):
    if group_only_reply(msg): return
    register_user(msg)
    uid, parts = msg.from_user.id, msg.text.split()
    if len(parts) < 3 or not parts[1].isdigit(): return safe_send(msg.chat.id, "Usage: `/setneg <quiz_id> <0 or 1/3>`", parse_mode="Markdown")
    qid, neg = int(parts[1]), parts[2]
    with get_db() as conn:
        q = conn.execute("SELECT creator_id FROM quizzes WHERE quiz_id=?", (qid,)).fetchone()
        if not q or q["creator_id"] != uid: return safe_send(msg.chat.id, "Quiz not found or not yours.")
        conn.execute("UPDATE quizzes SET neg_marking=? WHERE quiz_id=?", (neg, qid))
    safe_send(msg.chat.id, f"✅ Negative marking set to *{neg}* for Quiz `{qid}`.", parse_mode="Markdown")

@bot.message_handler(commands=["settype"])
def cmd_settype(msg):
    if group_only_reply(msg): return
    register_user(msg)
    uid, parts = msg.from_user.id, msg.text.split()
    if len(parts) < 3 or parts[2].lower() not in ("free","paid"): return safe_send(msg.chat.id, "Usage: `/settype <quiz_id> <free/paid>`", parse_mode="Markdown")
    qid, qt = int(parts[1]), parts[2].lower()
    with get_db() as conn:
        q = conn.execute("SELECT creator_id FROM quizzes WHERE quiz_id=?", (qid,)).fetchone()
        if not q or q["creator_id"] != uid: return safe_send(msg.chat.id, "Quiz not found or not yours.")
        conn.execute("UPDATE quizzes SET quiz_type=? WHERE quiz_id=?", (qt, qid))
    safe_send(msg.chat.id, f"✅ Type set to *{qt}* for Quiz `{qid}`.", parse_mode="Markdown")

@bot.message_handler(content_types=["document"])
def handle_document(msg):
    register_user(msg)
    if is_group(msg): return
    uid, state = msg.from_user.id, get_state(msg.from_user.id)
    if state not in ("awaiting_loadfile", "adding_questions", "awaiting_quiz_title"): return safe_send(msg.chat.id, "Use /loadfile or /create first, then send your file.")
    doc, fname = msg.document, msg.document.file_name or "upload"
    if doc.file_size and doc.file_size > 10 * 1024 * 1024: return safe_send(msg.chat.id, "File too large (max 10 MB).")

    safe_send(msg.chat.id, f"⏳ Processing `{fname}`...", parse_mode="Markdown")
    try:
        raw = bot.download_file(bot.get_file(doc.file_id).file_path)
        content = raw.decode("utf-8", errors="replace")
    except Exception as exc: return safe_send(msg.chat.id, f"Download failed: {exc}")

    try: parsed, errors = detect_and_parse(fname, content)
    except Exception as exc: return safe_send(msg.chat.id, f"Parse error: {exc}")

    seen, unique = set(), []
    for q in parsed:
        key = (q[0][:80] + "|" + q[1][:80] + "|" + str(q[2])[:60])
        if key not in seen: seen.add(key); unique.append(q)

    if not unique: return safe_send(msg.chat.id, f"No valid questions in `{fname}`.\n" + "\n".join(errors[:6]), parse_mode="Markdown")

    if state == "awaiting_loadfile":
        title = f"Import: {fname[:40]} — {datetime.now().strftime('%d %b %Y')}"
        quiz_id, short_id, q_count = create_quiz_and_save(uid, title[:100], unique)
        set_state(uid, "idle")
        send_quiz_created_card(msg.chat.id, uid, quiz_id, short_id, title[:50], q_count, "0", "free", 45)
    else:
        if state == "awaiting_quiz_title":
            store = _wizard.setdefault(uid, {"title": fname[:50], "questions": [], "neg": "0", "quiz_type": "free", "timer": 45, "section": 0})
            if not store.get("title"): store["title"] = fname[:50]
        store = _wizard.setdefault(uid, {"questions": []})
        store.setdefault("questions", []).extend(unique)
        total = len(store["questions"])
        set_state(uid, "adding_questions")
        safe_send(msg.chat.id, f"✅ *{len(unique)}* processed! Total: *{total}*\nSend more or /done to finish.", parse_mode="Markdown")

    if errors: safe_send(msg.chat.id, f"*{len(errors)} skipped:*\n" + "\n".join(f"• {e}" for e in errors[:6]), parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: True)
def handle_callback(call):
    uid, data, cid, mid = call.from_user.id, call.data or "", call.message.chat.id, call.message.message_id

    if data.startswith("qs_"):
        bot.answer_callback_query(call.id, "Starting...")
        _do_start_quiz(cid, uid, int(data[3:]))

    elif data.startswith("ep_"):
        parts = data.split("_")
        action, quiz_id = parts[1], int(parts[2])
        with get_db() as conn:
            quiz = conn.execute("SELECT * FROM quizzes WHERE quiz_id=?", (quiz_id,)).fetchone()
            q_count = conn.execute("SELECT COUNT(*) FROM questions WHERE quiz_id=?", (quiz_id,)).fetchone()[0]

        if action == "close":
            bot.answer_callback_query(call.id, "Closed.")
            try: bot.delete_message(cid, mid)
            except Exception: pass
        elif action == "shuffle":
            bot.answer_callback_query(call.id)
            if not quiz: return
            kb = shuffle_panel_kb(quiz_id, quiz["shuffle_q"], quiz["shuffle_o"])
            text = f"🔀 *Shuffle Settings*\n\nQuestions: {'✅' if quiz['shuffle_q'] else '❌'}\nOptions:   {'✅' if quiz['shuffle_o'] else '❌'}"
            try: bot.edit_message_text(text, cid, mid, parse_mode="Markdown", reply_markup=kb)
            except Exception: bot.send_message(cid, text, parse_mode="Markdown", reply_markup=kb)
        elif action == "export":
            bot.answer_callback_query(call.id, "Exporting...")
            _export_html(cid, quiz_id); _export_txt(cid, quiz_id)
        elif action == "questions":
            bot.answer_callback_query(call.id)
            with get_db() as conn: quiz_row = conn.execute("SELECT creator_id FROM quizzes WHERE quiz_id=?", (quiz_id,)).fetchone()
            if quiz_row and quiz_row["creator_id"] == uid:
                set_state(uid, "editing_questions")
                _wizard[uid] = {"edit_mode": True, "quiz_id": quiz_id, "questions": []}
                safe_send(cid, f"Paste new questions to append to Quiz `{quiz_id}`.\nUse /stopedit when done.", parse_mode="Markdown")
            else: bot.answer_callback_query(call.id, "Not your quiz.", show_alert=True)
        elif action == "settings":
            bot.answer_callback_query(call.id)
            if not quiz: return
            safe_send(cid, f"*Settings — {quiz['title']}*\n\nTo change, use:\n`/settimer {quiz_id} <seconds>`\n`/setneg {quiz_id} <0 or 1/3>`\n`/settype {quiz_id} <free/paid>`", parse_mode="Markdown")
        elif action == "perms": bot.answer_callback_query(call.id, "Perms feature coming soon!", show_alert=True)
        elif action == "back":
            if not quiz: return
            bot.answer_callback_query(call.id)
            send_edit_panel(cid, quiz, q_count, mid)

    elif data.startswith("sh_"):
        parts = data.split("_")
        action, quiz_id = parts[1], int(parts[2])
        if action == "back":
            bot.answer_callback_query(call.id)
            with get_db() as conn:
                quiz = conn.execute("SELECT * FROM quizzes WHERE quiz_id=?", (quiz_id,)).fetchone()
                q_count = conn.execute("SELECT COUNT(*) FROM questions WHERE quiz_id=?", (quiz_id,)).fetchone()[0]
            send_edit_panel(cid, quiz, q_count, mid)
            return
        with get_db() as conn:
            quiz = conn.execute("SELECT * FROM quizzes WHERE quiz_id=?", (quiz_id,)).fetchone()
            if not quiz: return bot.answer_callback_query(call.id, "Quiz not found.")
            if action == "q": conn.execute("UPDATE quizzes SET shuffle_q=? WHERE quiz_id=?", (1 - quiz["shuffle_q"], quiz_id))
            elif action == "o": conn.execute("UPDATE quizzes SET shuffle_o=? WHERE quiz_id=?", (1 - quiz["shuffle_o"], quiz_id))
            quiz = conn.execute("SELECT * FROM quizzes WHERE quiz_id=?", (quiz_id,)).fetchone()
        bot.answer_callback_query(call.id, "✅ Updated!")
        kb = shuffle_panel_kb(quiz_id, quiz["shuffle_q"], quiz["shuffle_o"])
        text = f"🔀 *Shuffle Settings*\n\nQuestions: {'✅' if quiz['shuffle_q'] else '❌'}\nOptions:   {'✅' if quiz['shuffle_o'] else '❌'}"
        try: bot.edit_message_text(text, cid, mid, parse_mode="Markdown", reply_markup=kb)
        except Exception: pass
    elif data.startswith("copyid_"):
        bot.answer_callback_query(call.id, f"ID: {data[7:]} — copy kar lo!", show_alert=True)
    else: bot.answer_callback_query(call.id)

@bot.message_handler(commands=["ban"])
def cmd_ban(msg):
    if not is_owner(msg.from_user.id): return bot.send_message(msg.chat.id, "🚫 Sirf owner kar sakta hai.")
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit(): return bot.send_message(msg.chat.id, "Usage: <code>/ban USER_ID reason</code>", parse_mode="HTML")
    target_id = int(parts[1])
    reason = parts[2] if len(parts) > 2 else "No reason given"
    with get_db() as conn:
        conn.execute("INSERT INTO banned_users(user_id, banned_by, reason) VALUES(?,?,?) ON CONFLICT (user_id) DO UPDATE SET banned_by=EXCLUDED.banned_by, reason=EXCLUDED.reason", (target_id, msg.from_user.id, reason))
        user = conn.execute("SELECT first_name, username FROM users WHERE user_id=?", (target_id,)).fetchone()
    _ban_cache.add(target_id)
    name = (user["first_name"] or str(target_id)) if user else str(target_id)
    uname = (f"@{user['username']}" if user and user["username"] else "") if user else ""
    try: bot.send_message(target_id, f"🚫 Aapko bot se ban kar diya gaya hai.\nKaran: {html_mod.escape(reason)}", parse_mode="HTML")
    except Exception: pass
    bot.send_message(msg.chat.id, f"✅ <b>{html_mod.escape(name)}</b> {uname} (<code>{target_id}</code>) ko ban kar diya.\nKaran: {html_mod.escape(reason)}", parse_mode="HTML")

@bot.message_handler(commands=["unban"])
def cmd_unban(msg):
    if not is_owner(msg.from_user.id): return bot.send_message(msg.chat.id, "🚫 Sirf owner kar sakta hai.")
    parts = msg.text.split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit(): return bot.send_message(msg.chat.id, "Usage: <code>/unban USER_ID</code>", parse_mode="HTML")
    target_id = int(parts[1])
    with get_db() as conn: deleted = conn.execute("DELETE FROM banned_users WHERE user_id=?", (target_id,)).rowcount
    _ban_cache.discard(target_id)
    if deleted:
        try: bot.send_message(target_id, "✅ Aapka ban hata diya gaya hai.")
        except Exception: pass
        bot.send_message(msg.chat.id, f"✅ <code>{target_id}</code> ko unban kar diya.", parse_mode="HTML")
    else: bot.send_message(msg.chat.id, f"⚠️ <code>{target_id}</code> banned nahi tha.", parse_mode="HTML")

@bot.message_handler(commands=["users"])
def cmd_users(msg):
    if not is_owner(msg.from_user.id): return bot.send_message(msg.chat.id, "🚫 Sirf owner dekh sakta hai.")
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        banned = conn.execute("SELECT COUNT(*) FROM banned_users").fetchone()[0]
        recent = conn.execute("SELECT user_id, first_name, username, created_at FROM users ORDER BY created_at DESC LIMIT 10").fetchall()
    lines = [f"👥 <b>Total Users:</b> {total}  |  🚫 Banned: {banned}\n\n<b>Recent 10:</b>"]
    for u in recent:
        uname = f"@{u['username']}" if u["username"] else "—"
        dt = datetime.fromtimestamp(u["created_at"]).strftime("%d %b %I:%M %p")
        lines.append(f"• <b>{html_mod.escape(u['first_name'] or 'User')}</b> {uname} | <code>{u['user_id']}</code> | {dt}")
    bot.send_message(msg.chat.id, "\n".join(lines), parse_mode="HTML")

@bot.message_handler(commands=["banlist"])
def cmd_banlist(msg):
    if not is_owner(msg.from_user.id): return bot.send_message(msg.chat.id, "🚫 Sirf owner dekh sakta hai.")
    with get_db() as conn:
        rows = conn.execute("SELECT b.user_id, b.reason, b.banned_at, u.first_name, u.username FROM banned_users b LEFT JOIN users u ON b.user_id=u.user_id ORDER BY b.banned_at DESC").fetchall()
    if not rows: return bot.send_message(msg.chat.id, "✅ Koi banned user nahi hai.")
    lines = [f"🚫 <b>Banned Users ({len(rows)}):</b>\n"]
    for r in rows:
        name = html_mod.escape(r["first_name"] or "Unknown")
        uname = f"@{r['username']}" if r["username"] else "—"
        dt = datetime.fromtimestamp(r["banned_at"]).strftime("%d %b %Y")
        lines.append(f"• <b>{name}</b> {uname}\n  🆔 <code>{r['user_id']}</code> | 📅 {dt}\n  Karan: {html_mod.escape(r['reason'])}")
    bot.send_message(msg.chat.id, "\n".join(lines), parse_mode="HTML")

@bot.message_handler(content_types=["text"])
def handle_text(msg):
    register_user(msg)
    if is_group(msg): return
    uid, text = msg.from_user.id, msg.text.strip()
    state = get_state(uid)
    if is_banned(uid): return bot.send_message(msg.chat.id, "🚫 Aap banned hain.")

    if state == "awaiting_quiz_title":
        title = text[:100]
        _wizard.setdefault(uid, {})["title"] = title
        _wizard[uid].setdefault("questions", [])
        set_state(uid, "adding_questions")
        safe_send(msg.chat.id, f"✅ *Name: {title}*\n\nSend questions, or upload a .txt / .json file, or /cancel\n\nPaste format:\n```\nQuestion text\na) Option A ✅\nb) Option B\n```\nSeparate questions with a blank line. When done, type /done.", parse_mode="Markdown")
    elif state == "adding_questions":
        store = _wizard.setdefault(uid, {"title": "Untitled", "questions": []})
        parsed, errors = bulk_parse_manual(text)
        store.setdefault("questions", []).extend(parsed)
        total = len(store["questions"])
        reply = f"✅ *{len(parsed)}* processed! Total: *{total}*\n\n"
        if errors: reply += "Skipped:\n" + "\n".join(f"• {e}" for e in errors[:5]) + "\n\n"
        reply += "Send more or /done to finish."
        safe_send(msg.chat.id, reply, parse_mode="Markdown")
    elif state == "awaiting_section_quiz":
        _wizard.setdefault(uid, {})["section"] = 1 if text.lower().startswith("y") else 0
        set_state(uid, "awaiting_timer")
        safe_send(msg.chat.id, "⏳ *Timer in seconds* (>10)\n\nE.g. `20` for 20 seconds per question:", parse_mode="Markdown")
    elif state == "awaiting_timer":
        if not text.isdigit() or int(text) <= 9: return safe_send(msg.chat.id, "❌ >9\n\nPlease enter a number greater than 9.", parse_mode="Markdown")
        _wizard.setdefault(uid, {})["timer"] = int(text)
        set_state(uid, "awaiting_neg_marking")
        safe_send(msg.chat.id, "📝 *Negative marking* (0 or 1/3)\n\nEnter `0` for none, `1/3`, `1/4`, etc.:", parse_mode="Markdown")
    elif state == "awaiting_neg_marking":
        clean = text.strip().lower()
        if clean in ("0","none","n"): neg = "0"
        elif "/" in clean: neg = clean
        else:
            try: neg = str(float(clean))
            except ValueError: return safe_send(msg.chat.id, "Enter `0`, `1/3`, `1/4`, or `0.33`", parse_mode="Markdown")
        _wizard.setdefault(uid, {})["neg"] = neg
        set_state(uid, "awaiting_quiz_type")
        safe_send(msg.chat.id, f"📝 *Type* (free/paid)", parse_mode="Markdown")
    elif state == "awaiting_quiz_type":
        # ── SPEED FIX: Instant feedback before DB save ───────────────────
        clean = text.strip().lower()
        if clean not in ("free", "paid"):
            return safe_send(msg.chat.id, "Send `free` or `paid`.", parse_mode="Markdown")
        store = _wizard.get(uid, {})
        qs    = store.get("questions", [])
        title = store.get("title", "Untitled Quiz")
        neg   = store.get("neg", "0")
        timer = store.get("timer", 45)
        if not qs:
            set_state(uid, "adding_questions")
            return safe_send(msg.chat.id, "No questions buffered.")
        # Instant reply FIRST — user feels no delay
        safe_send(msg.chat.id, "⏳ *Saving quiz...*", parse_mode="Markdown")
        set_state(uid, "idle")
        _wizard.pop(uid, None)
        # Now save (user already got feedback)
        quiz_id, short_id, q_count = create_quiz_and_save(uid, title, qs, neg, clean, timer)
        send_quiz_created_card(msg.chat.id, uid, quiz_id, short_id, title, q_count, neg, clean, timer)
    elif state == "awaiting_edit_quiz_id":
        quiz = find_quiz(uid, text.strip())
        if not quiz: return safe_send(msg.chat.id, f"Quiz '{text}' not found or not yours.")
        with get_db() as conn: q_count = conn.execute("SELECT COUNT(*) FROM questions WHERE quiz_id=?", (quiz["quiz_id"],)).fetchone()[0]
        _wizard[uid] = {"edit_mode": True, "quiz_id": quiz["quiz_id"], "questions": []}
        set_state(uid, "editing_questions")
        send_edit_panel(msg.chat.id, quiz, q_count)
    elif state == "editing_questions":
        store = _wizard.setdefault(uid, {"questions": []})
        parsed, errors = bulk_parse_manual(text)
        store.setdefault("questions", []).extend(parsed)
        total = len(store["questions"])
        reply = f"Queued *{len(parsed)}* Q(s). Buffered: *{total}*.\n\n"
        if errors: reply += "\n".join(f"• {e}" for e in errors[:5]) + "\n\n"
        reply += "More or /stopedit to save."
        safe_send(msg.chat.id, reply, parse_mode="Markdown")
    elif state == "awaiting_html_id":
        if not text.isdigit(): return safe_send(msg.chat.id, "Enter a valid numeric Quiz ID.")
        set_state(uid, "idle"); _export_html(msg.chat.id, int(text))
    elif state == "awaiting_txt_id":
        if not text.isdigit(): return safe_send(msg.chat.id, "Enter a valid numeric Quiz ID.")
        set_state(uid, "idle")
        threading.Thread(target=_export_pdf_testseries, args=(msg.chat.id, int(text)), daemon=True).start()
    elif state == "awaiting_start_quiz_id":
        if not text.isdigit(): return safe_send(msg.chat.id, "Enter a valid numeric Quiz ID.")
        set_state(uid, "idle"); _wizard.pop(uid, None); _do_start_quiz(msg.chat.id, uid, int(text))
    elif state == "awaiting_loadfile":
        safe_send(msg.chat.id, "Please *upload a file* (attach .txt or .json as document).", parse_mode="Markdown")
    else: safe_send(msg.chat.id, "Use the keyboard or /features for commands.", reply_markup=ReplyKeyboardRemove())

# ══════════════════════════════════════════════════════════════════════════════
#  POLL ANSWER HANDLER
# ══════════════════════════════════════════════════════════════════════════════

@bot.poll_answer_handler()
def handle_poll_answer(poll_answer):
    uid      = poll_answer.user.id
    name     = (poll_answer.user.first_name or "") + (" " + (poll_answer.user.last_name or "")).strip()
    name     = name.strip() or f"User{uid}"
    selected = poll_answer.option_ids[0] if poll_answer.option_ids else None

    with get_db() as conn:
        pm = conn.execute("SELECT * FROM poll_map WHERE poll_id=?", (poll_answer.poll_id,)).fetchone()
        if not pm: return
        is_correct = int(selected is not None and selected == pm["correct_idx"])
        already = conn.execute("SELECT result_id FROM session_results WHERE session_id=? AND user_id=? AND question_id=?", (pm["session_id"], uid, pm["question_id"])).fetchone()
        if not already:
            conn.execute("INSERT INTO session_results(session_id,user_id,participant_name,question_id,selected_idx,is_correct) VALUES(?,?,?,?,?,?)", (pm["session_id"], uid, name, pm["question_id"], selected, is_correct))

@bot.inline_handler(func=lambda query: True)
def handle_inline_query(inline_query):
    uid = inline_query.from_user.id
    q_text = inline_query.query.strip().lower()
    with get_db() as conn:
        quizzes = conn.execute("SELECT z.*, COUNT(q.question_id) AS q_count FROM quizzes z LEFT JOIN questions q ON z.quiz_id=q.quiz_id WHERE z.creator_id=? GROUP BY z.quiz_id ORDER BY z.created_at DESC LIMIT 50", (uid,)).fetchall()
    results = []
    for quiz in quizzes:
        short_id = str(quiz["short_id"]).lower()
        title = str(quiz["title"]).lower()
        if q_text and q_text not in title and q_text not in short_id: continue
        neg_val = parse_neg_value(quiz["neg_marking"])
        neg_display = f"{neg_val:.6f}".rstrip('0').rstrip('.') if neg_val else "None"
        safe_title = html_mod.escape(quiz['title'])
        card_text = (f"💳 <b>Quiz Name:</b> {safe_title}\n#️⃣ <b>Questions:</b> {quiz['q_count']}\n⏰ <b>Timer:</b> {quiz['timer_seconds']}s\n🆔 <b>Quiz ID:</b> <code>{quiz['short_id']}</code>\n🏴‍☠️ <b>-ve:</b> {neg_display}\n💰 <b>Type:</b> {quiz['quiz_type']}")
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton("🎯 Start", url=f"https://t.me/{BOT_USER}?start=quiz_{quiz['quiz_id']}"))
        kb.add(InlineKeyboardButton("🚀 Group", url=f"https://t.me/{BOT_USER}?startgroup=quiz_{quiz['quiz_id']}"))
        kb.add(InlineKeyboardButton("🔗 Share", switch_inline_query=quiz["short_id"]))
        res = InlineQueryResultArticle(id=str(quiz["quiz_id"]), title=quiz["title"], description=f"{quiz['q_count']} Qs | {quiz['timer_seconds']}s timer | ID: {quiz['short_id']}", input_message_content=InputTextMessageContent(card_text, parse_mode="HTML"), reply_markup=kb)
        results.append(res)
    bot.answer_inline_query(inline_query.id, results, cache_time=1, is_personal=True)

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN EXECUTION
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("  QuizBot Pro v5.1 (SPEED + fpdf2 Hindi Perfect)")
    print(f"  DB: Supabase PostgreSQL | Bot: @{BOT_USER}")
    print("=" * 60)
    
    keep_alive()
    
    def start_bot():
        while True:
            try:
                logging.info("Bot is starting/connecting to Telegram...")
                print("Bot is running... Faster Polling Enabled.")
                bot.infinity_polling(
                    timeout=10, long_polling_timeout=5, skip_pending=True,
                    allowed_updates=["message", "poll_answer", "callback_query", "inline_query"],
                    logger_level=20)
            except KeyboardInterrupt:
                print("\nBot stopped manually.")
                break
            except Exception as e:
                logging.error(f"Bot crashed: {e}")
                print(f"⚠️ Error: {e}. Restarting in 5s...")
                time.sleep(5)

    start_bot()