# ================================================================
# 📄 QUIZ BOT — PDF + RESULTS | COMPLETE FINAL CODE
# ================================================================
# pip install python-telegram-bot[ext] pymongo reportlab
#
# Hindi Font (same folder mein rakhein):
#   NotoSansDevanagari-Regular.ttf
#   NotoSansDevanagari-Bold.ttf
#
# Run: python bot.py
# ================================================================

import os
import io
import re
import logging
from datetime import datetime
from xml.sax.saxutils import escape as xml_escape

from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters, PollAnswerHandler,
)
from pymongo import MongoClient

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib.colors import HexColor, black, white
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer,
    Table, TableStyle, KeepTogether, HRFlowable,
)

# =========================================
# ⚙️ CONFIGURATION
# =========================================
BOT_TOKEN = "8599439624:AAEMj-en21FpmUk7_Pe7PmbPQ_3rgkg_8bU"
MONGO_URI = "mongodb+srv://ipsksharma_db_user:Pej9IcC06zRcsKZj@cluster0.3spospr.mongodb.net/?appName=Cluster0"
DB_NAME = "SDiscussion_bot"
FONT_DIR = "."
ADMIN_IDS = [863857194]

# =========================================
# LOGGING
# =========================================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =========================================
# DATABASE
# =========================================
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    quizzes_col = db["quizzes"]
    questions_col = db["questions"]
    sessions_col = db["sessions"]
    poll_map_col = db["poll_mappings"]
    answers_col = db["user_answers"]

    quizzes_col.create_index("quiz_id", unique=True)
    questions_col.create_index([("quiz_id", 1), ("question_number", 1)])
    sessions_col.create_index("quiz_id")
    poll_map_col.create_index("poll_id", unique=True)
    answers_col.create_index([("quiz_id", 1), ("user_id", 1), ("question_number", 1)])
    logger.info("✅ Database connected")
except Exception as e:
    logger.error(f"❌ DB error: {e}")
    raise

# =========================================
# FONTS
# =========================================
FONT_REGULAR = "Helvetica"
FONT_BOLD = "Helvetica-Bold"


def setup_fonts():
    global FONT_REGULAR, FONT_BOLD
    reg = os.path.join(FONT_DIR, "NotoSansDevanagari-Regular.ttf")
    bold = os.path.join(FONT_DIR, "NotoSansDevanagari-Bold.ttf")
    try:
        if os.path.exists(reg):
            pdfmetrics.registerFont(TTFont("HindiFont", reg))
            FONT_REGULAR = "HindiFont"
            logger.info("✅ Hindi regular font loaded")
        if os.path.exists(bold):
            pdfmetrics.registerFont(TTFont("HindiFontBold", bold))
            FONT_BOLD = "HindiFontBold"
            logger.info("✅ Hindi bold font loaded")
    except Exception as e:
        logger.error(f"Font error: {e}")
        FONT_REGULAR = "Helvetica"
        FONT_BOLD = "Helvetica-Bold"


setup_fonts()

# =========================================
# DATABASE HELPERS
# =========================================


def get_quiz(quiz_id):
    return quizzes_col.find_one({"quiz_id": quiz_id.strip().upper()})


def get_quiz_questions(quiz_id):
    return list(
        questions_col.find({"quiz_id": quiz_id.strip().upper()}).sort("question_number", 1)
    )


def get_full_quiz(quiz_id):
    quiz = get_quiz(quiz_id)
    if not quiz:
        return None
    quiz["questions"] = get_quiz_questions(quiz_id)
    return quiz


def save_quiz_meta(data):
    quizzes_col.update_one({"quiz_id": data["quiz_id"]}, {"$set": data}, upsert=True)


def save_question(data):
    questions_col.update_one(
        {"quiz_id": data["quiz_id"], "question_number": data["question_number"]},
        {"$set": data}, upsert=True,
    )


def delete_quiz_data(quiz_id):
    qid = quiz_id.upper()
    quizzes_col.delete_many({"quiz_id": qid})
    questions_col.delete_many({"quiz_id": qid})
    sessions_col.delete_many({"quiz_id": qid})
    poll_map_col.delete_many({"quiz_id": qid})
    answers_col.delete_many({"quiz_id": qid})


def count_questions(quiz_id):
    return questions_col.count_documents({"quiz_id": quiz_id.upper()})


def list_all_quizzes():
    return list(quizzes_col.find().sort("created_at", -1))


def safe(text):
    if not text:
        return ""
    return xml_escape(str(text))


# =========================================
# TIME HELPER
# =========================================


def format_time(seconds):
    if seconds is None or seconds < 0:
        return "0s"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    m = seconds // 60
    s = seconds % 60
    if m < 60:
        return f"{m}m {s:02d}s"
    h = m // 60
    m = m % 60
    return f"{h}h {m:02d}m"


# =========================================
# 🏆 RESULT CALCULATION & FORMATTING
# =========================================


def calculate_results(quiz_id):
    quiz = get_quiz(quiz_id)
    if not quiz:
        return None, []

    total_q = count_questions(quiz_id)
    neg = quiz.get("negative_marking", 0)

    pipeline = [
        {"$match": {"quiz_id": quiz_id.upper()}},
        {"$group": {
            "_id": "$user_id",
            "user_name": {"$last": "$user_name"},
            "correct": {"$sum": {"$cond": ["$is_correct", 1, 0]}},
            "wrong": {"$sum": {"$cond": [{"$eq": ["$is_correct", False]}, 1, 0]}},
            "total_answered": {"$sum": 1},
            "first_answer": {"$min": "$answered_at"},
            "last_answer": {"$max": "$answered_at"},
        }},
    ]

    raw = list(answers_col.aggregate(pipeline))
    results = []

    for r in raw:
        c = r["correct"]
        w = r["wrong"]
        score = round(c - (w * neg), 2)
        pct = round((score / total_q * 100), 2) if total_q > 0 else 0
        pct = max(0, pct)

        if r.get("first_answer") and r.get("last_answer"):
            td = (r["last_answer"] - r["first_answer"]).total_seconds()
        else:
            td = 0

        results.append({
            "user_id": r["_id"],
            "name": r.get("user_name", "Unknown") or "Unknown",
            "correct": c,
            "wrong": w,
            "score": score,
            "pct": pct,
            "time_sec": td,
            "time_str": format_time(td),
        })

    results.sort(key=lambda x: (-x["score"], x["time_sec"]))
    for i, r in enumerate(results):
        r["rank"] = i + 1

    return quiz, results


def _pad_center(text, width):
    tl = len(text)
    if tl >= width:
        return text
    left = (width - tl) // 2
    return " " * left + text


def _pad_sides(left_text, right_text, width):
    half = width // 2
    lt = left_text.center(half)
    rt = right_text.center(half)
    return lt + rt


def format_result_message(quiz, results):
    title = quiz.get("title", "Quiz")

    if not results:
        return f"🎯 Quiz '{safe(title)}' — No results yet!"

    msg = f"🎯 Quiz '<b>{safe(title)}</b>' — Results!\n\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━\n"

    # ── PODIUM (monospace <pre>) ──
    W = 32
    podium = ""

    if len(results) >= 1:
        r1 = results[0]
        podium += _pad_center("🥇", W) + "\n"
        podium += _pad_center(r1["name"], W) + "\n"
        podium += _pad_center(f"{r1['pct']}%", W) + "\n"
        podium += _pad_center(f"⏱ {r1['time_str']}", W) + "\n"
        podium += "\n"

    if len(results) >= 3:
        r2, r3 = results[1], results[2]
        podium += _pad_sides("🥈", "🥉", W) + "\n"
        podium += _pad_sides(r2["name"], r3["name"], W) + "\n"
        podium += _pad_sides(f"{r2['pct']}%", f"{r3['pct']}%", W) + "\n"
        podium += _pad_sides(f"⏱ {r2['time_str']}", f"⏱ {r3['time_str']}", W) + "\n"
    elif len(results) >= 2:
        r2 = results[1]
        podium += _pad_center("🥈", W) + "\n"
        podium += _pad_center(r2["name"], W) + "\n"
        podium += _pad_center(f"{r2['pct']}%", W) + "\n"
        podium += _pad_center(f"⏱ {r2['time_str']}", W) + "\n"

    msg += f"\n<pre>{podium}</pre>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    # ── LEADERBOARD ──
    rank_icons = {1: "👑", 2: "🥈", 3: "🥉"}

    for i, r in enumerate(results):
        icon = rank_icons.get(r["rank"], f"{r['rank']}.")
        msg += f"<b>{icon} {safe(r['name'])}</b>\n"
        msg += f"✅ {r['correct']} | ❌ {r['wrong']} | "
        msg += f"🎯 {r['score']} | {r['pct']}%\n"
        msg += f"⏱ {r['time_str']}\n"
        if i < len(results) - 1:
            msg += "─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─\n"

    msg += "\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
    max_t = max(r["time_sec"] for r in results) if results else 0
    msg += f"👥 Participants: {len(results)} | ⏱ {format_time(max_t)}\n"

    return msg


# =========================================
# 📄 PDF GENERATION
# =========================================
PAGE_W, PAGE_H = A4
C_HEADER = HexColor("#1a237e")
C_ACCENT = HexColor("#e53935")
C_GREEN = HexColor("#2e7d32")
C_LIGHT_GREEN = HexColor("#e8f5e9")
C_GREEN_BORDER = HexColor("#4caf50")
C_GREY = HexColor("#757575")
C_LIGHT_GREY = HexColor("#f5f5f5")
C_BORDER = HexColor("#e0e0e0")
C_OPTION = HexColor("#424242")


def _pdf_styles():
    return {
        "inst_title": ParagraphStyle(
            "IT", fontName=FONT_BOLD, fontSize=11,
            textColor=C_HEADER, spaceBefore=5, spaceAfter=3,
        ),
        "inst": ParagraphStyle(
            "IN", fontName=FONT_REGULAR, fontSize=9.5,
            textColor=black, leftIndent=10, spaceBefore=2,
            spaceAfter=2, leading=13,
        ),
        "question": ParagraphStyle(
            "Q", fontName=FONT_BOLD, fontSize=10.5,
            textColor=HexColor("#212121"), spaceBefore=3,
            spaceAfter=4, leading=15,
        ),
        "option": ParagraphStyle(
            "O", fontName=FONT_REGULAR, fontSize=10,
            textColor=C_OPTION, leftIndent=20, spaceBefore=1,
            spaceAfter=1, leading=13,
        ),
        "answer": ParagraphStyle(
            "A", fontName=FONT_BOLD, fontSize=10,
            textColor=C_GREEN, leftIndent=5, spaceBefore=2,
            spaceAfter=2, leading=14,
        ),
    }


def _first_page(c, doc):
    c.saveState()
    h = 2.2 * cm
    c.setFillColor(C_HEADER)
    c.rect(0, PAGE_H - h, PAGE_W, h, fill=1, stroke=0)
    c.setFillColor(C_ACCENT)
    c.rect(0, PAGE_H - h, PAGE_W, 3, fill=1, stroke=0)

    t = getattr(doc, "quiz_title", "Quiz")
    c.setFillColor(white)
    c.setFont(FONT_BOLD, 18)
    tw = c.stringWidth(t, FONT_BOLD, 18)
    c.drawString((PAGE_W - tw) / 2, PAGE_H - 1.0 * cm, t)

    qid = getattr(doc, "quiz_id", "")
    c.setFont(FONT_REGULAR, 9)
    c.setFillColor(HexColor("#90caf9"))
    it = f"Quiz ID: {qid}"
    tw = c.stringWidth(it, FONT_REGULAR, 9)
    c.drawString((PAGE_W - tw) / 2, PAGE_H - 1.6 * cm, it)

    _footer(c, doc)
    c.restoreState()


def _later_pages(c, doc):
    c.saveState()
    h = 1.0 * cm
    c.setFillColor(C_HEADER)
    c.rect(0, PAGE_H - h, PAGE_W, h, fill=1, stroke=0)
    c.setFillColor(C_ACCENT)
    c.rect(0, PAGE_H - h, PAGE_W, 2, fill=1, stroke=0)

    t = getattr(doc, "quiz_title", "Quiz")
    c.setFillColor(white)
    c.setFont(FONT_BOLD, 10)
    c.drawString(1.5 * cm, PAGE_H - 0.65 * cm, t)

    c.setFont(FONT_REGULAR, 9)
    pg = f"Page {doc.page}"
    tw = c.stringWidth(pg, FONT_REGULAR, 9)
    c.drawString(PAGE_W - 1.5 * cm - tw, PAGE_H - 0.65 * cm, pg)

    _footer(c, doc)
    c.restoreState()


def _footer(c, doc):
    c.setStrokeColor(C_BORDER)
    c.setLineWidth(0.5)
    c.line(1.5 * cm, 1.2 * cm, PAGE_W - 1.5 * cm, 1.2 * cm)

    c.setFillColor(C_GREY)
    c.setFont(FONT_REGULAR, 7.5)

    t = getattr(doc, "quiz_title", "")
    c.drawString(1.5 * cm, 0.7 * cm, t)

    qid = getattr(doc, "quiz_id", "")
    it = f"Quiz ID: {qid}"
    tw = c.stringWidth(it, FONT_REGULAR, 7.5)
    c.drawString((PAGE_W - tw) / 2, 0.7 * cm, it)

    pg = f"Page {doc.page}"
    tw = c.stringWidth(pg, FONT_REGULAR, 7.5)
    c.drawString(PAGE_W - 1.5 * cm - tw, 0.7 * cm, pg)


def generate_quiz_pdf(quiz_data):
    buf = io.BytesIO()
    title = quiz_data.get("title", "Quiz")
    quiz_id = quiz_data.get("quiz_id", "N/A")
    questions = quiz_data.get("questions", [])
    neg = quiz_data.get("negative_marking", 0)

    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=1.5 * cm, rightMargin=1.5 * cm,
        topMargin=2.5 * cm, bottomMargin=1.8 * cm,
    )
    doc.quiz_title = title
    doc.quiz_id = quiz_id

    st = _pdf_styles()
    aw = PAGE_W - 3.0 * cm
    story = [Spacer(1, 0.3 * cm)]

    # Instructions
    inst = [
        [Paragraph("<b>Instructions / निर्देश:</b>", st["inst_title"])],
        [Paragraph(f"• Total Questions / कुल प्रश्न: {len(questions)}", st["inst"])],
        [Paragraph(f"• Negative / ऋणात्मक: {neg}", st["inst"])],
        [Paragraph("• Each question carries equal marks / प्रत्येक प्रश्न समान अंक का है", st["inst"])],
        [Paragraph("• Answer below each question / उत्तर हर प्रश्न के नीचे दिया गया है", st["inst"])],
    ]
    it = Table(inst, colWidths=[aw])
    it.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), C_LIGHT_GREY),
        ("BOX", (0, 0), (-1, -1), 1, C_BORDER),
        ("TOPPADDING", (0, 0), (-1, 0), 8),
        ("BOTTOMPADDING", (0, -1), (-1, -1), 8),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
        ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING", (0, 1), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 1), (-1, -2), 2),
    ]))
    story.extend([it, Spacer(1, 0.5 * cm)])

    # Questions with answers
    labels = ["(A)", "(B)", "(C)", "(D)", "(E)", "(F)"]

    for q in questions:
        qn = q.get("question_number", 0)
        qt = q.get("question", "")
        opts = q.get("options", [])
        cor = q.get("correct_option", 0)

        block = [Paragraph(
            f'<font color="#1565c0"><b>Q{qn}.</b></font> {safe(qt)}',
            st["question"],
        )]

        for i, o in enumerate(opts):
            lb = labels[i] if i < len(labels) else f"({i + 1})"
            block.append(Paragraph(f"{lb}  {safe(o)}", st["option"]))

        block.append(Spacer(1, 4))

        if 0 <= cor < len(opts):
            al = labels[cor] if cor < len(labels) else f"({cor + 1})"
            at = opts[cor]
        else:
            al, at = "?", "N/A"

        ap = Paragraph(
            f"<b>✓ उत्तर (Answer): {al} {safe(at)}</b>", st["answer"]
        )
        atbl = Table([[ap]], colWidths=[aw - 0.6 * cm])
        atbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), C_LIGHT_GREEN),
            ("BOX", (0, 0), (-1, -1), 1, C_GREEN_BORDER),
            ("LEFTPADDING", (0, 0), (-1, -1), 10),
            ("RIGHTPADDING", (0, 0), (-1, -1), 10),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ]))
        wr = Table([[atbl]], colWidths=[aw])
        wr.setStyle(TableStyle([
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("LEFTPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ]))
        block.append(wr)
        block.append(Spacer(1, 4))
        block.append(HRFlowable(
            width="90%", thickness=0.5, color=C_BORDER,
            spaceBefore=3, spaceAfter=8,
        ))
        story.append(KeepTogether(block))

    doc.build(story, onFirstPage=_first_page, onLaterPages=_later_pages)
    buf.seek(0)
    return buf


# =========================================
# 🤖 COMMAND HANDLERS
# =========================================


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (
        "🎓 <b>Quiz Bot — PDF + Results</b>\n\n"
        "📄 /quizpdf <code>ID</code> — Quiz PDF (answer niche)\n"
        "🏆 /results <code>ID</code> — Result dekhein\n"
        "📊 /myquizzes — Quiz list\n"
        "ℹ️ /help — Help\n\n"
        "<b>Admin:</b>\n"
        "➕ /createquiz <code>ID | Title</code>\n"
        "📝 /addq <code>ID NUM</code> (poll reply)\n"
        "▶️ /startquiz <code>ID</code> — Quiz shuru\n"
        "🛑 /stopquiz <code>ID</code> — Quiz band\n"
        "⚙️ /setneg <code>ID VALUE</code>\n"
        "🗑️ /deletequiz <code>ID</code>\n"
    )
    await update.message.reply_text(t, parse_mode="HTML")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (
        "📖 <b>How to use:</b>\n\n"
        "<b>1.</b> <code>/createquiz JAN25 | Jan 2025 CA</code>\n"
        "<b>2.</b> Quiz poll forward karo, reply mein:\n"
        "    <code>/addq JAN25 1</code>\n"
        "    <code>/addq JAN25 2</code> ...\n"
        "<b>3.</b> Group mein: <code>/startquiz JAN25</code>\n"
        "<b>4.</b> Users answer karein\n"
        "<b>5.</b> <code>/results JAN25</code> — Result\n"
        "<b>6.</b> <code>/quizpdf JAN25</code> — PDF\n\n"
        "✅ Result mein timing + separator lines\n"
        "✅ Podium — 1st center, 2nd-3rd sides\n"
        "✅ PDF mein har question ke niche answer\n"
    )
    await update.message.reply_text(t, parse_mode="HTML")


async def cmd_createquiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return await update.message.reply_text("❌ Unauthorized")

    parts = update.message.text.replace("/createquiz", "").strip()
    if "|" not in parts:
        return await update.message.reply_text(
            "Usage: <code>/createquiz ID | Title</code>\n"
            "Example: <code>/createquiz JAN25 | January 2025 CA</code>",
            parse_mode="HTML",
        )

    qid, title = parts.split("|", 1)
    qid, title = qid.strip().upper(), title.strip()

    if not qid or not title:
        return await update.message.reply_text("❌ ID aur Title dono dein!")

    save_quiz_meta({
        "quiz_id": qid, "title": title,
        "created_by": update.effective_user.id,
        "created_at": datetime.now(), "negative_marking": 0,
    })

    await update.message.reply_text(
        f"✅ <b>Quiz Created!</b>\n\n"
        f"🆔 ID: <code>{qid}</code>\n"
        f"📝 Title: {safe(title)}\n\n"
        f"Ab polls reply karke add karein:\n"
        f"<code>/addq {qid} 1</code>",
        parse_mode="HTML",
    )


async def cmd_addq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return await update.message.reply_text("❌ Unauthorized")

    reply = update.message.reply_to_message
    if not reply or not reply.poll:
        return await update.message.reply_text(
            "❌ Quiz poll ko reply karke use karein!\n"
            "<code>/addq QUIZ_ID NUM</code>", parse_mode="HTML",
        )

    args = update.message.text.strip().split()
    if len(args) < 3:
        return await update.message.reply_text(
            "Usage: <code>/addq ID NUM</code>", parse_mode="HTML"
        )

    qid = args[1].upper()
    try:
        qn = int(args[2])
    except ValueError:
        return await update.message.reply_text("❌ Number dein!")

    if not get_quiz(qid):
        return await update.message.reply_text(
            f"❌ Quiz '<code>{qid}</code>' nahi mila! Pehle /createquiz se banayein.",
            parse_mode="HTML",
        )

    poll = reply.poll
    if poll.type != "quiz":
        return await update.message.reply_text("❌ Quiz-type poll chahiye!")

    save_question({
        "quiz_id": qid, "question_number": qn,
        "question": poll.question,
        "options": [o.text for o in poll.options],
        "correct_option": poll.correct_option_id,
        "explanation": poll.explanation or "",
    })

    total = count_questions(qid)
    await update.message.reply_text(
        f"✅ <b>Q#{qn} added!</b>\n"
        f"Quiz: <code>{qid}</code> | Total: {total}\n\n"
        f"PDF: <code>/quizpdf {qid}</code>", parse_mode="HTML",
    )


async def cmd_startquiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return await update.message.reply_text("❌ Unauthorized")

    args = update.message.text.strip().split()
    if len(args) < 2:
        return await update.message.reply_text(
            "Usage: <code>/startquiz QUIZ_ID</code>", parse_mode="HTML"
        )

    qid = args[1].upper()
    quiz = get_quiz(qid)
    if not quiz:
        return await update.message.reply_text(f"❌ Quiz '{qid}' nahi mila!")

    questions = get_quiz_questions(qid)
    if not questions:
        return await update.message.reply_text("❌ Koi question nahi hai!")

    chat_id = update.effective_chat.id

    # Clear old data
    answers_col.delete_many({"quiz_id": qid})
    poll_map_col.delete_many({"quiz_id": qid})

    # Save session
    sessions_col.update_one(
        {"quiz_id": qid},
        {"$set": {
            "quiz_id": qid, "chat_id": chat_id,
            "started_at": datetime.now(),
            "started_by": update.effective_user.id,
            "status": "active",
        }},
        upsert=True,
    )

    await update.message.reply_text(
        f"🎯 <b>Quiz Starting: {safe(quiz.get('title', qid))}</b>\n"
        f"📊 Total: {len(questions)} questions\n⏳ Get ready...",
        parse_mode="HTML",
    )

    # Send polls
    for q in questions:
        try:
            sent = await context.bot.send_poll(
                chat_id=chat_id,
                question=q["question"],
                options=q["options"],
                type="quiz",
                correct_option_id=q["correct_option"],
                explanation=q.get("explanation", ""),
                is_anonymous=False,
            )
            poll_map_col.insert_one({
                "poll_id": sent.poll.id,
                "quiz_id": qid,
                "question_number": q["question_number"],
                "correct_option": q["correct_option"],
                "chat_id": chat_id,
            })
        except Exception as e:
            logger.error(f"Poll send error Q{q['question_number']}: {e}")

    await context.bot.send_message(
        chat_id=chat_id,
        text=f"✅ <b>All {len(questions)} questions sent!</b>\n\n"
             f"Results: <code>/results {qid}</code>",
        parse_mode="HTML",
    )


async def cmd_stopquiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return await update.message.reply_text("❌ Unauthorized")

    args = update.message.text.strip().split()
    if len(args) < 2:
        return await update.message.reply_text(
            "Usage: <code>/stopquiz ID</code>", parse_mode="HTML"
        )

    qid = args[1].upper()
    sessions_col.update_one({"quiz_id": qid}, {"$set": {"status": "stopped"}})
    await update.message.reply_text(
        f"🛑 Quiz '<code>{qid}</code>' stopped!\n"
        f"Results: <code>/results {qid}</code>", parse_mode="HTML",
    )


async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    answer = update.poll_answer
    if not answer or not answer.option_ids:
        return

    poll_id = answer.poll_id
    user = answer.user
    selected = answer.option_ids[0]

    mapping = poll_map_col.find_one({"poll_id": poll_id})
    if not mapping:
        return

    qid = mapping["quiz_id"]
    qnum = mapping["question_number"]
    correct = mapping["correct_option"]

    name = user.first_name or ""
    if user.last_name:
        name += f" {user.last_name}"

    answers_col.update_one(
        {"quiz_id": qid, "user_id": user.id, "question_number": qnum},
        {"$set": {
            "quiz_id": qid,
            "user_id": user.id,
            "user_name": name.strip(),
            "question_number": qnum,
            "selected_option": selected,
            "correct_option": correct,
            "is_correct": selected == correct,
            "answered_at": datetime.now(),
        }},
        upsert=True,
    )


async def cmd_results(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.strip().split()
    if len(args) < 2:
        return await update.message.reply_text(
            "Usage: <code>/results QUIZ_ID</code>", parse_mode="HTML"
        )

    qid = args[1].upper()
    quiz, results = calculate_results(qid)

    if not quiz:
        return await update.message.reply_text(f"❌ Quiz '{qid}' nahi mila!")
    if not results:
        return await update.message.reply_text("❌ Koi result nahi hai abhi!")

    msg = format_result_message(quiz, results)

    if len(msg) > 4000:
        for i in range(0, len(msg), 4000):
            await update.message.reply_text(msg[i:i + 4000], parse_mode="HTML")
    else:
        await update.message.reply_text(msg, parse_mode="HTML")


async def cmd_quizpdf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    match = re.match(r"^/quizpdf(?:@\w+)?\s*(.*)", text, re.IGNORECASE)
    qid = match.group(1).strip().upper() if match else ""

    if not qid:
        return await update.message.reply_text(
            "❌ Quiz ID dein!\n\nUsage: <code>/quizpdf QUIZ_ID</code>\n"
            "List: /myquizzes", parse_mode="HTML",
        )

    wait = await update.message.reply_text(
        "⏳ <b>PDF bana raha hoon...</b>", parse_mode="HTML"
    )

    try:
        qdata = get_full_quiz(qid)
        if not qdata:
            return await wait.edit_text(
                f"❌ Quiz '<code>{qid}</code>' nahi mila!", parse_mode="HTML"
            )

        qs = qdata.get("questions", [])
        if not qs:
            return await wait.edit_text(
                f"❌ Quiz mein koi question nahi!", parse_mode="HTML"
            )

        pdf = generate_quiz_pdf(qdata)

        st = re.sub(r"[^\w\s\-]", "", qdata.get("title", "Quiz"))
        st = re.sub(r"\s+", "_", st.strip())
        filename = f"{st}_{qid}.pdf"

        caption = (
            f"📄 <b>{safe(qdata.get('title', 'Quiz'))}</b>\n"
            f"📊 Questions: {len(qs)} | 🆔 <code>{qid}</code>\n"
            f"✅ Har question ke niche answer diya gaya hai"
        )

        await update.message.reply_document(
            document=pdf, filename=filename,
            caption=caption, parse_mode="HTML",
        )
        await wait.delete()

    except Exception as e:
        logger.error(f"PDF error: {e}", exc_info=True)
        await wait.edit_text(f"❌ Error: {str(e)}")


async def cmd_myquizzes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    quizzes = list_all_quizzes()
    if not quizzes:
        return await update.message.reply_text(
            "📭 Koi quiz nahi.\nNaya banane ke liye: /createquiz"
        )

    t = "📊 <b>Available Quizzes:</b>\n\n"
    for q in quizzes[:25]:
        cnt = count_questions(q["quiz_id"])
        t += (
            f"🆔 <code>{q['quiz_id']}</code> — "
            f"{safe(q.get('title', 'Untitled'))} ({cnt}Q)\n"
        )
    t += "\n📄 <code>/quizpdf ID</code> | 🏆 <code>/results ID</code>"
    await update.message.reply_text(t, parse_mode="HTML")


async def cmd_deletequiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return await update.message.reply_text("❌ Unauthorized")

    args = update.message.text.strip().split()
    if len(args) < 2:
        return await update.message.reply_text(
            "Usage: <code>/deletequiz ID</code>", parse_mode="HTML"
        )

    qid = args[1].upper()
    if get_quiz(qid):
        delete_quiz_data(qid)
        await update.message.reply_text(
            f"✅ Quiz '<code>{qid}</code>' deleted!", parse_mode="HTML"
        )
    else:
        await update.message.reply_text(
            f"❌ Quiz '<code>{qid}</code>' nahi mila!", parse_mode="HTML"
        )


async def cmd_setneg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return await update.message.reply_text("❌ Unauthorized")

    args = update.message.text.strip().split()
    if len(args) < 3:
        return await update.message.reply_text(
            "Usage: <code>/setneg ID 0.33</code>", parse_mode="HTML"
        )

    qid = args[1].upper()
    try:
        val = float(args[2])
    except ValueError:
        return await update.message.reply_text("❌ Number dein!")

    if not get_quiz(qid):
        return await update.message.reply_text("❌ Quiz nahi mila!")

    quizzes_col.update_one({"quiz_id": qid}, {"$set": {"negative_marking": val}})
    await update.message.reply_text(
        f"✅ Negative marking = {val} for <code>{qid}</code>", parse_mode="HTML"
    )


async def handle_poll_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not update.message or not update.message.poll:
        return

    poll = update.message.poll
    if poll.type != "quiz":
        return await update.message.reply_text("⚠️ Quiz poll nahi hai!")

    await update.message.reply_text(
        f"📝 <b>Quiz Poll Received!</b>\n\n"
        f"Q: {safe(poll.question[:80])}...\n"
        f"Options: {len(poll.options)}\n"
        f"Correct: Option {poll.correct_option_id + 1}\n\n"
        f"Reply karke add karein:\n"
        f"<code>/addq QUIZ_ID NUM</code>", parse_mode="HTML",
    )


async def error_handler(update, context):
    logger.error(f"Error: {context.error}", exc_info=True)


# =========================================
# SAMPLE DATA
# =========================================


def insert_sample():
    save_quiz_meta({
        "quiz_id": "TEST01", "title": "Sample Test Quiz",
        "created_by": 0, "created_at": datetime.now(),
        "negative_marking": 0.33,
    })
    qs = [
        ("भारत की राजधानी क्या है?", ["मुंबई", "नई दिल्ली", "कोलकाता", "चेन्नई"], 1),
        ("Largest planet?", ["Mars", "Venus", "Jupiter", "Saturn"], 2),
        ("सबसे बड़ा राज्य (क्षेत्रफल)?", ["मध्य प्रदेश", "उत्तर प्रदेश", "राजस्थान", "महाराष्ट्र"], 2),
        ("National anthem author?", ["Bankim Chandra", "Rabindranath Tagore", "Sarojini Naidu", "Mahatma Gandhi"], 1),
        ("गंगा नदी कहाँ से निकलती है?", ["गंगोत्री", "यमुनोत्री", "केदारनाथ", "बद्रीनाथ"], 0),
    ]
    for i, (q, o, c) in enumerate(qs, 1):
        save_question({
            "quiz_id": "TEST01", "question_number": i,
            "question": q, "options": o, "correct_option": c,
        })
    logger.info("✅ Sample data inserted! Use /quizpdf TEST01")


# =========================================
# 🚀 MAIN
# =========================================


def main():
    # Pehli baar test ke liye uncomment karein:
    # insert_sample()

    app = Application.builder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("createquiz", cmd_createquiz))
    app.add_handler(CommandHandler("addq", cmd_addq))
    app.add_handler(CommandHandler("startquiz", cmd_startquiz))
    app.add_handler(CommandHandler("stopquiz", cmd_stopquiz))
    app.add_handler(CommandHandler("results", cmd_results))
    app.add_handler(CommandHandler("myquizzes", cmd_myquizzes))
    app.add_handler(CommandHandler("deletequiz", cmd_deletequiz))
    app.add_handler(CommandHandler("setneg", cmd_setneg))

    # /quizpdf — with or without space
    app.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r"^/quizpdf"), cmd_quizpdf
    ))

    # Poll answer tracking
    app.add_handler(PollAnswerHandler(handle_poll_answer))

    # Forwarded poll detection
    app.add_handler(MessageHandler(filters.POLL, handle_poll_msg))

    # Error handler
    app.add_error_handler(error_handler)

    logger.info("🤖 Bot started! All commands ready...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

# ================================================================
# SETUP GUIDE:
#
# 1. pip install python-telegram-bot[ext] pymongo reportlab
#
# 2. Hindi font download karein (Google Fonts):
#    NotoSansDevanagari-Regular.ttf
#    NotoSansDevanagari-Bold.ttf
#    → Bot ke same folder mein rakhein
#
# 3. Edit karein:
#    BOT_TOKEN = "apna token"
#    MONGO_URI = "apna MongoDB URI"
#    ADMIN_IDS = [apna_telegram_id]
#
# 4. Test ke liye pehli baar:
#    main() mein insert_sample() uncomment karein
#
# 5. python bot.py
#
# 6. Bot mein test:
#    /quizpdf TEST01  → PDF with answers
#    /startquiz TEST01 → Group mein quiz
#    /results TEST01   → Beautiful results
#
# ================================================================