import datetime
import requests
import pandas as pd
import asyncio
import os
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, PollAnswerHandler, ContextTypes

# ---------------- CONFIG ----------------
BOT_TOKEN = "8338334264:AAHAKlWSfMRl3NNV67onP3-FXbOvFgaXo4Q"
GROUP_ID = -1003215176643
EXCEL_FILE = "serieA_full_data.xlsx"
API_KEY = "7fc5a86f88214b3098d25d8e4a2ffc5b"
LEAGUE_CODE = "SA"
TEMP_EXCEL_FILE = "serieA_full_data_temp.xlsx"
# ----------------------------------------

file_lock = asyncio.Lock()

def load_all_data():
    required_cols = ["timestamp", "poll_id", "user_id", "username", "first_name", "last_name", "option_id", "match"]
    dtype_spec = {'poll_id': str, 'user_id': str, 'option_id': str}
    try:
        df = pd.read_excel(EXCEL_FILE, sheet_name="Log Completo", dtype=dtype_spec)
        df = df.fillna("")
        if not all(col in df.columns for col in required_cols):
            print("⚠️ Colonne mancanti nel Log Completo. Reinizializzo il DataFrame.")
            df = pd.DataFrame(columns=required_cols)
        return df
    except FileNotFoundError:
        print(f"ℹ️ File Excel '{EXCEL_FILE}' non trovato. Creazione nuovo DataFrame.")
        return pd.DataFrame(columns=required_cols)
    except Exception as e:
        print(f"⚠️ Errore caricamento '{EXCEL_FILE}': {e}")
        return pd.DataFrame(columns=required_cols)

def create_summary_table(df_log):
    df_votes = df_log[(df_log['user_id'] != "") & (df_log['option_id'] != "")].copy()
    if df_votes.empty:
        return pd.DataFrame(columns=['Partita', 'Utente (Username/ID)', 'Voto (1, X, 2)', 'Orario Voto'])

    vote_map = {'0': '1', '1': 'X', '2': '2'}
    df_votes['Voto Testo'] = df_votes['option_id'].astype(str).map(vote_map)

    df_votes['Username Pulito'] = df_votes['username'].astype(str).str.replace('@', '', regex=False).str.replace('nan', '', regex=False)
    df_votes['Nome Completo'] = (
        df_votes['first_name'].astype(str).str.replace('nan', '', regex=False) + ' ' +
        df_votes['last_name'].astype(str).str.replace('nan', '', regex=False)
    ).str.strip()

    def crea_nome_utente(r):
        if r['Username Pulito']:
            return f"{r['Username Pulito']} ({r['user_id']})"
        if r['Nome Completo']:
            return f"{r['Nome Completo']} ({r['user_id']})"
        return f"ID {r['user_id']}"
    
    df_votes['Utente (ID)'] = df_votes.apply(crea_nome_utente, axis=1)
    df_summary = df_votes.sort_values(by=['match', 'timestamp'], ascending=[True, False]).drop_duplicates(subset=['match', 'user_id'], keep='first')
    df_summary = df_summary[['match', 'Utente (ID)', 'Voto Testo', 'timestamp']]
    df_summary.columns = ['Partita', 'Utente (Username/ID)', 'Voto (1, X, 2)', 'Orario Voto']
    return df_summary.sort_values(by=['Partita', 'Utente (Username/ID)'], ascending=True)

def save_data_to_excel(df_log, df_summary):
    data_to_write = {"Log Completo": df_log, "Riepilogo Voti": df_summary}
    try:
        with pd.ExcelWriter(EXCEL_FILE, engine='openpyxl') as writer:
            for sheet_name, df in data_to_write.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"✅ Dati salvati in '{EXCEL_FILE}'.")
        return True
    except PermissionError:
        print(f"🛑 File aperto: '{EXCEL_FILE}'")
        try:
            with pd.ExcelWriter(TEMP_EXCEL_FILE, engine='openpyxl') as writer:
                for sheet_name, df in data_to_write.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            os.replace(TEMP_EXCEL_FILE, EXCEL_FILE)
            print(f"✅ Dati salvati tramite file temporaneo.")
            return True
        except Exception as e:
            if os.path.exists(TEMP_EXCEL_FILE):
                os.remove(TEMP_EXCEL_FILE)
            print(f"🛑 Errore persistente su {EXCEL_FILE}: {e}")
            return False
    except Exception as e:
        print(f"⚠️ Errore generico salvataggio {EXCEL_FILE}: {e}")
        return False

# ---------------- API Football-Data ----------------
def fetch_matches_from_api(season_year, matchday):
    headers = {"X-Auth-Token": API_KEY}
    url = f"https://api.football-data.org/v4/competitions/{LEAGUE_CODE}/matches"
    params = {"season": season_year, "matchday": matchday}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json().get("matches", [])
    except Exception as e:
        print(f"Errore API fetch partite: {e}")
        return []

def get_next_round_matches():
    headers = {"X-Auth-Token": API_KEY}
    url_competition = f"https://api.football-data.org/v4/competitions/{LEAGUE_CODE}"
    try:
        resp_comp = requests.get(url_competition, headers=headers, timeout=10)
        resp_comp.raise_for_status()
        data_comp = resp_comp.json()
        current_season = data_comp.get("currentSeason")
        if not current_season:
            return []
        season_year = current_season.get("startDate")[:4]
        current_matchday = current_season.get("currentMatchday")
        if not season_year or not current_matchday:
            return []
    except Exception as e:
        print(f"Errore API competizione: {e}")
        return []

    matches_current_md = fetch_matches_from_api(season_year, current_matchday)
    now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    future_matches = [m for m in matches_current_md if datetime.datetime.fromisoformat(m["utcDate"].replace("Z","+00:00")) >= now]

    if future_matches:
        return future_matches
    else:
        next_matchday = current_matchday + 1
        matches_next_md = fetch_matches_from_api(season_year, next_matchday)
        future_matches_next_md = [m for m in matches_next_md if datetime.datetime.fromisoformat(m["utcDate"].replace("Z","+00:00")) >= now]
        return future_matches_next_md

# ---------------- BOT ----------------
async def send_matches_poll(context: ContextTypes.DEFAULT_TYPE):
    matches = await asyncio.to_thread(get_next_round_matches)
    if not matches:
        await context.bot.send_message(chat_id=GROUP_ID, text="⚠️ Nessuna partita trovata.")
        return

    for match in matches:
        home = match["homeTeam"]["name"]
        away = match["awayTeam"]["name"]
        question = f"{home} vs {away} – pronostico giornata"
        options = ["1", "X", "2"]
        poll = await context.bot.send_poll(chat_id=GROUP_ID, question=question, options=options, is_anonymous=False)

        row = {
            "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "poll_id": poll.poll.id,
            "user_id": "",
            "username": "",
            "first_name": "",
            "last_name": "",
            "option_id": "",
            "match": f"{home} vs {away}"
        }

        async with file_lock:
            df_log = await asyncio.to_thread(load_all_data)
            df_log = pd.concat([df_log, pd.DataFrame([row])], ignore_index=True)
            df_summary = await asyncio.to_thread(create_summary_table, df_log)
            await asyncio.to_thread(save_data_to_excel, df_log, df_summary)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Bot pronto! Usa /polls per creare sondaggi.")

async def polls_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_matches_poll(context)
    await update.message.reply_text("Sondaggi inviati!")

async def poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pa = update.poll_answer
    vote_timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    vote_poll_id = str(pa.poll_id)
    vote_user_id = str(pa.user.id)
    vote_username = pa.user.username or ""
    vote_first_name = pa.user.first_name or ""
    vote_last_name = pa.user.last_name or ""
    vote_option_id = "|".join(map(str, pa.option_ids))

    async with file_lock:
        df_log = await asyncio.to_thread(load_all_data)
        match_row = df_log[(df_log['poll_id'] == vote_poll_id) & (df_log['user_id'] == "")]
        if match_row.empty:
            print(f"⚠️ Voto sconosciuto poll_id {vote_poll_id}")
            return
        match_name = match_row['match'].iloc[0]

        row = {
            "timestamp": vote_timestamp,
            "poll_id": vote_poll_id,
            "user_id": vote_user_id,
            "username": vote_username,
            "first_name": vote_first_name,
            "last_name": vote_last_name,
            "option_id": vote_option_id,
            "match": match_name
        }

        df_log = pd.concat([df_log, pd.DataFrame([row])], ignore_index=True)
        df_summary = await asyncio.to_thread(create_summary_table, df_log)
        await asyncio.to_thread(save_data_to_excel, df_log, df_summary)
        print(f"Voto registrato: {vote_user_id} ha votato {vote_option_id} per {match_name}")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("polls", polls_command))
    app.add_handler(PollAnswerHandler(poll_answer))
    print("Bot avviato!")
    app.run_polling()
