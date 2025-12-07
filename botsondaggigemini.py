import datetime
import requests
import pandas as pd
import asyncio
import os
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, PollAnswerHandler, ContextTypes

# ---------------- CONFIG ----------------

BOT_TOKEN = "INSERISCI_IL_TUO_TOKEN"
GROUP_ID = -1001234567890  # ID del tuo canale/gruppo
ADMIN_ID = 123456789       # <-- METTI IL TUO USER ID

EXCEL_FILE = "serieA_full_data.xlsx"
TEMP_EXCEL_FILE = "serieA_full_data_temp.xlsx"

API_KEY = "INSERISCI_API_KEY"
LEAGUE_CODE = "SA"

# ----------------------------------------

file_lock = asyncio.Lock()

# ---------------- EXCEL ----------------

def load_all_data():
    cols = ["timestamp", "poll_id", "user_id", "username",
            "first_name", "last_name", "option_id", "match"]

    try:
        df = pd.read_excel(EXCEL_FILE, sheet_name="Log Completo", dtype=str)
        df = df.fillna("")
        return df
    except:
        return pd.DataFrame(columns=cols)

def create_summary_table(df):
    df_votes = df[(df['user_id'] != "") & (df['option_id'] != "")]
    if df_votes.empty:
        return pd.DataFrame(columns=["Partita", "Utente", "Voto", "Orario"])

    map_vote = {"0":"1", "1":"X", "2":"2"}
    df_votes["Voto"] = df_votes["option_id"].map(map_vote)
    df_votes["Utente"] = (
        df_votes["username"].replace("", pd.NA)
        .fillna(df_votes["first_name"])
        .fillna("User") + " (" + df_votes["user_id"] + ")"
    )

    df_votes = df_votes.sort_values("timestamp", ascending=False)
    df_votes = df_votes.drop_duplicates(subset=["match","user_id"])

    return df_votes[["match","Utente","Voto","timestamp"]].rename(columns={
        "match":"Partita",
        "timestamp":"Orario"
    })

def save_to_excel(df_log, df_summary):
    with pd.ExcelWriter(EXCEL_FILE, engine="openpyxl") as writer:
        df_log.to_excel(writer, sheet_name="Log Completo", index=False)
        df_summary.to_excel(writer, sheet_name="Riepilogo Voti", index=False)

# ---------------- API FOOTBALL ----------------

def fetch_matches():
    headers = {"X-Auth-Token": API_KEY}
    url = f"https://api.football-data.org/v4/competitions/{LEAGUE_CODE}/matches"
    try:
        res = requests.get(url, headers=headers, timeout=10)
        return res.json().get("matches", [])
    except:
        return []

# ---------------- ADMIN ----------------

async def send_polls(context):
    matches = await asyncio.to_thread(fetch_matches)

    for m in matches[:10]:
        home = m["homeTeam"]["name"]
        away = m["awayTeam"]["name"]

        q = f"{home} vs {away}"
        opts = ["1", "X", "2"]

        poll = await context.bot.send_poll(
            chat_id=GROUP_ID,
            question=q,
            options=opts,
            is_anonymous=False
        )

        row = {
            "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "poll_id": poll.poll.id,
            "user_id": "",
            "username": "",
            "first_name": "",
            "last_name": "",
            "option_id": "",
            "match": q
        }

        async with file_lock:
            df = await asyncio.to_thread(load_all_data)
            df = pd.concat([df, pd.DataFrame([row])])
            summary = await asyncio.to_thread(create_summary_table, df)
            await asyncio.to_thread(save_to_excel, df, summary)

async def polls_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await send_polls(context)

async def get_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if os.path.exists(EXCEL_FILE):
        await update.message.reply_document(open(EXCEL_FILE, "rb"))

# ---------------- UTENTI ----------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid == ADMIN_ID:
        await update.message.reply_text("👑 Admin attivo")
    else:
        await update.message.reply_text("✅ Puoi votare i sondaggi nel canale.\nUsa /score per vedere il tuo punteggio.")

async def score(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)

    async with file_lock:
        df = await asyncio.to_thread(load_all_data)

    votes = df[(df["user_id"] == uid) & (df["option_id"] != "")]
    total = len(votes)

    await update.message.reply_text(f"📊 Il tuo punteggio:\n✅ Voti validi: {total}")

# ---------------- REGISTRA VOTI ----------------

async def poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pa = update.poll_answer

    poll_id = str(pa.poll_id)
    uid = str(pa.user.id)

    row = {
        "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "poll_id": poll_id,
        "user_id": uid,
        "username": pa.user.username or "",
        "first_name": pa.user.first_name or "",
        "last_name": pa.user.last_name or "",
        "option_id": str(pa.option_ids[0]),
        "match": ""
    }

    async with file_lock:
        df = await asyncio.to_thread(load_all_data)

        match_row = df[(df["poll_id"] == poll_id) & (df["user_id"] == "")]
        if match_row.empty:
            return

        row["match"] = match_row.iloc[0]["match"]
        df = pd.concat([df, pd.DataFrame([row])])
        summary = await asyncio.to_thread(create_summary_table, df)
        await asyncio.to_thread(save_to_excel, df, summary)

# ---------------- MAIN ----------------

if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("polls", polls_cmd))
    app.add_handler(CommandHandler("getexcel", get_excel))
    app.add_handler(CommandHandler("score", score))
    app.add_handler(PollAnswerHandler(poll_answer))

    print("✅ Bot avviato")
    app.run_polling()
