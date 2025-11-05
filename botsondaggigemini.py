import datetime
import requests
import pandas as pd
import asyncio  # Aggiunto per Lock e to_thread
import os       # Aggiunto per gestione file e rinomina
import json     # Mantenuto ma non usato nella logica di salvataggio
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, PollAnswerHandler, ContextTypes

# ---------------- CONFIG ----------------
BOT_TOKEN = "8338334264:AAHAKlWSfMRl3NNV67onP3-FXbOvFgaXo4Q"
GROUP_ID = -1003215176643  # chat_id numerico del gruppo
EXCEL_FILE = "serieA_full_data.xlsx"   # File Excel contenente Log Completo e Riepilogo Voti
API_KEY = "7fc5a86f88214b3098d25d8e4a2ffc5b"
LEAGUE_CODE = "SA"  # Serie A
TEMP_EXCEL_FILE = "serieA_full_data_temp.xlsx" # File temporaneo per scrittura resiliente EXCEL
# ----------------------------------------

# Lock per evitare race condition sui file
file_lock = asyncio.Lock()

def load_all_data():
    """Carica il log completo dal foglio 'Log Completo' del file Excel, o inizializza un DataFrame vuoto."""
    # Aggiunte colonne first_name e last_name
    required_cols = ["timestamp", "poll_id", "user_id", "username", "first_name", "last_name", "option_id", "match"]
    
    # FIX: Specifica i tipi di dato per le colonne ID per evitare errori di tipo misto (numeri e stringhe)
    # Questo forza Pandas a trattare gli ID come testo, che è più sicuro.
    dtype_spec = {
        'poll_id': str,
        'user_id': str,
        'option_id': str
    }

    try:
        # Tenta di leggere il foglio di log dal file Excel, forzando i tipi di dato
        df = pd.read_excel(EXCEL_FILE, sheet_name="Log Completo", dtype=dtype_spec)
        
        # FIX: Sostituisce i valori NaN (celle vuote lette da Excel) con stringhe vuote
        # Questo è cruciale per far funzionare i filtri (es. user_id == "")
        df = df.fillna("")

        if not all(col in df.columns for col in required_cols):
             print(f"⚠️ Colonne mancanti nel Log Completo. Reinizializzo il DataFrame.")
             df = pd.DataFrame(columns=required_cols)
        return df
    except FileNotFoundError:
        print(f"ℹ️ File Excel '{EXCEL_FILE}' non trovato. Creazione nuovo DataFrame.")
        return pd.DataFrame(columns=required_cols)
    except Exception as e:
        print(f"⚠️ Errore durante il caricamento di '{EXCEL_FILE}': {e}. Inizializzo un DataFrame vuoto.")
        return pd.DataFrame(columns=required_cols)

def create_summary_table(df_log):
    """Crea la tabella riassuntiva globale, ordinata per Partita e mostrando l'ultimo voto per utente."""
    
    # Filtra solo i voti validi (escludendo le righe master)
    df_votes = df_log[(df_log['user_id'] != "") & (df_log['option_id'] != "")].copy()
    if df_votes.empty:
        # Colonne aggiornate per il riepilogo
        return pd.DataFrame(columns=['Partita', 'Utente (Username/ID)', 'Voto (1, X, 2)', 'Orario Voto'])

    # Mappa le opzioni da ID numerico a testo '1', 'X', '2'
    vote_map = {'0': '1', '1': 'X', '2': '2'}

    # Formatta le colonne necessarie
    df_votes['Voto Testo'] = df_votes['option_id'].astype(str).map(vote_map)
    
    # FIX: Gestione robusta dell'username E first/last name
    # 1. Converte in stringa, gestendo i NaN
    df_votes['Username Pulito'] = df_votes['username'].astype(str).str.replace('@', '', regex=False).str.replace('nan', '', regex=False)
    # 2. Concatena nome e cognome, gestendo NaN e spazi extra
    df_votes['Nome Completo'] = (
        df_votes['first_name'].astype(str).str.replace('nan', '', regex=False) + ' ' + 
        df_votes['last_name'].astype(str).str.replace('nan', '', regex=False)
    ).str.strip()

    # Crea la colonna Utente (ID)
    def crea_nome_utente(r):
        if r['Username Pulito']:
            return f"{r['Username Pulito']} ({r['user_id']})"
        if r['Nome Completo']:
            return f"{r['Nome Completo']} ({r['user_id']})"
        return f"ID {r['user_id']}"
        
    df_votes['Utente (ID)'] = df_votes.apply(crea_nome_utente, axis=1)


    # Prende solo l'ultimo voto per ogni utente in ogni partita
    # Ordina per timestamp (più recenti in alto), poi elimina i duplicati utente/match
    df_summary = df_votes.sort_values(by=['match', 'timestamp'], ascending=[True, False]).drop_duplicates(subset=['match', 'user_id'], keep='first')

    # Seleziona le colonne per l'Excel di riepilogo (aggiunto timestamp)
    df_summary = df_summary[['match', 'Utente (ID)', 'Voto Testo', 'timestamp']]
    df_summary.columns = ['Partita', 'Utente (Username/ID)', 'Voto (1, X, 2)', 'Orario Voto']
    
    # Ordina per Partita e Utente per la formattazione finale
    return df_summary.sort_values(by=['Partita', 'Utente (Username/ID)'], ascending=True)

def save_data_to_excel(df_log, df_summary):
    """Salva il log completo e il riepilogo nel file Excel in due fogli distinti in modo resiliente."""
    
    data_to_write = {
        "Log Completo": df_log,
        "Riepilogo Voti": df_summary
    }

    try:
        # Tentativo 1: Scrittura diretta
        with pd.ExcelWriter(EXCEL_FILE, engine='openpyxl') as writer:
            for sheet_name, df in data_to_write.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"✅ Dati (Log e Riepilogo) salvati/aggiornati in '{EXCEL_FILE}'.")
        return True
    
    except PermissionError:
        print(f"🛑 ERRORE PERMESSO EXCEL: Impossibile salvare il file '{EXCEL_FILE}'. Potrebbe essere aperto.")
        try:
            # Tentativo 2: Scrivi su file temporaneo e poi rinomina/sostituisci
            with pd.ExcelWriter(TEMP_EXCEL_FILE, engine='openpyxl') as writer:
                 for sheet_name, df in data_to_write.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            os.replace(TEMP_EXCEL_FILE, EXCEL_FILE)
            print(f"✅ Dati salvati in '{EXCEL_FILE}' tramite file temporaneo.")
            return True
        except Exception as e:
            if os.path.exists(TEMP_EXCEL_FILE):
                os.remove(TEMP_EXCEL_FILE)
            print(f"🛑 Blocco file persistente su {EXCEL_FILE}. Impossibile salvare il file: {e}")
            return False
    except Exception as e:
        print(f"⚠️ Errore generico durante il salvataggio di {EXCEL_FILE}: {e}. (Verifica l'installazione di 'openpyxl')")
        return False

# ---------------- FUNZIONI BOT (API Football-Data) ----------------

def fetch_matches_from_api(season_year, matchday):
    """Recupera le partite per una specifica stagione e giornata."""
    headers = {"X-Auth-Token": API_KEY}
    url = f"https://api.football-data.org/v4/competitions/{LEAGUE_CODE}/matches"
    params = {
        "season": season_year,
        "matchday": matchday
    }
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json().get("matches", [])
    except Exception as e:
        print(f"Errore API durante fetch partite (season: {season_year}, matchday: {matchday}): {e}")
        return []

def get_next_round_matches():
    """Determina la stagione e la giornata correnti, poi recupera le partite future."""
    headers = {"X-Auth-Token": API_KEY}
    url_competition = f"https://api.football-data.org/v4/competitions/{LEAGUE_CODE}"

    # 1. Trova la stagione e la giornata correnti
    try:
        resp_comp = requests.get(url_competition, headers=headers, timeout=10)
        resp_comp.raise_for_status()
        data_comp = resp_comp.json()
        
        current_season = data_comp.get("currentSeason")
        if not current_season:
            print("⚠️ Impossibile trovare 'currentSeason' nei dati della competizione.")
            return []
            
        season_year = current_season.get("startDate")[:4] # Es. "2025" da "2025-08-10"
        current_matchday = current_season.get("currentMatchday")
        
        if not season_year or not current_matchday:
            print(f"⚠️ Dati stagione incompleti: year={season_year}, matchday={current_matchday}")
            return []
        
        print(f"✅ Stagione corrente: {season_year}, Giornata corrente: {current_matchday}")

    except Exception as e:
        print(f"Errore API nel recuperare i dati della competizione: {e}")
        return []

    # 2. Scarica le partite della giornata CORRENTE
    matches_current_md = fetch_matches_from_api(season_year, current_matchday)
    if not matches_current_md:
        print(f"⚠️ Nessuna partita trovata per la giornata {current_matchday}.")
    
    # 3. Filtra le partite future dalla giornata corrente
    future_matches = []
    now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    for match in matches_current_md:
        match_date = datetime.datetime.fromisoformat(match["utcDate"].replace("Z", "+00:00"))
        if match_date >= now:
            future_matches.append(match)

    # 4. Decide se usare la giornata corrente o la prossima
    if future_matches:
        print(f"✅ Trovate {len(future_matches)} partite future per la giornata {current_matchday}.")
        return future_matches
    else:
        print(f"ℹ️ Nessuna partita futura trovata per la giornata {current_matchday}. Provo con la giornata successiva...")
        next_matchday = current_matchday + 1
        matches_next_md = fetch_matches_from_api(season_year, next_matchday)
        
        if not matches_next_md:
            print(f"⚠️ Nessuna partita trovata per la giornata {next_matchday}.")
            return []
            
        # Filtriamo anche queste per sicurezza, anche se dovrebbero essere tutte future
        future_matches_next_md = []
        for match in matches_next_md:
            match_date = datetime.datetime.fromisoformat(match["utcDate"].replace("Z", "+00:00"))
            if match_date >= now:
                future_matches_next_md.append(match)
        
        print(f"✅ Trovate {len(future_matches_next_md)} partite per la giornata {next_matchday}.")
        return future_matches_next_md


async def send_matches_poll(context: ContextTypes.DEFAULT_TYPE):
    """Recupera le partite e invia un sondaggio per ciascuna, registrando la riga master su Excel."""
    matches = await asyncio.to_thread(get_next_round_matches)
    
    if not matches:
        await context.bot.send_message(chat_id=GROUP_ID, text="⚠️ Nessuna partita trovata per la prossima giornata.")
        return

    for match in matches:
        home = match["homeTeam"]["name"]
        away = match["awayTeam"]["name"]
        question = f"{home} vs {away} – pronostico giornata"
        options = ["1", "X", "2"]
        poll = await context.bot.send_poll(chat_id=GROUP_ID, question=question, options=options, is_anonymous=False)
        
        # Riga master per identificare il sondaggio
        row = {
            "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "poll_id": poll.poll.id,
            "user_id": "", # Riga master ha user_id vuoto
            "username": "",
            "first_name": "", # Aggiunto
            "last_name": "",  # Aggiunto
            "option_id": "",
            "match": f"{home} vs {away}"
        }
        
        # Usiamo il lock per la scrittura su Excel
        async with file_lock:
            try:
                # 1. Carica il log esistente dall'Excel
                df_log = await asyncio.to_thread(load_all_data)
                
                # 2. Aggiungi la riga master
                df_log = pd.concat([df_log, pd.DataFrame([row])], ignore_index=True)
                
                # 3. Ricalcola il riepilogo
                df_summary = await asyncio.to_thread(create_summary_table, df_log)
                
                # 4. Scrivi entrambi i fogli nel file Excel
                await asyncio.to_thread(save_data_to_excel, df_log, df_summary)

            except Exception as e:
                print(f"🛑 ERRORE scrittura Excel in send_matches_poll: {e}")
                await context.bot.send_message(
                    chat_id=GROUP_ID, 
                    text=f"⚠️ Errore di scrittura: Impossibile salvare il sondaggio per {home} vs {away}. Il file Excel è bloccato. Chiudi il file `{EXCEL_FILE}` se è aperto."
                )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Bot pronto! Usa /polls per creare i sondaggi della prossima giornata di Serie A.")

async def polls_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_matches_poll(context)
    await update.message.reply_text("Sondaggi inviati!")

async def poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pa = update.poll_answer
    
    # Dati del voto
    vote_timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    vote_poll_id = str(pa.poll_id) # FIX CORRETTO: l'ID è direttamente in pa.poll_id
    vote_user_id = str(pa.user.id) # FIX: Converti in stringa per coerenza
    # Acquisizione migliorata nome utente
    vote_username = pa.user.username if pa.user.username else ""
    vote_first_name = pa.user.first_name if pa.user.first_name else ""
    vote_last_name = pa.user.last_name if pa.user.last_name else ""
    vote_option_id = "|".join(map(str, pa.option_ids)) # Opzione votata (es. '0', '1', '2')

    # Usiamo il lock per leggere e scrivere su Excel
    async with file_lock:
        df_log = None 
        match_name = None
        try:
            # 1. Carica il log esistente (con i tipi di dato corretti)
            df_log = await asyncio.to_thread(load_all_data)
            
            # Cerca il nome del match dalla riga "master"
            # ORA il confronto è stringa vs stringa (vote_poll_id e df_log['poll_id'])
            match_row = df_log[(df_log['poll_id'] == vote_poll_id) & (df_log['user_id'] == "")]
            
            if match_row.empty:
                print(f"⚠️ Ricevuto voto per poll_id sconosciuto: {vote_poll_id}")
                return
            
            match_name = match_row['match'].iloc[0]

            # Crea la riga del voto
            row = {
                "timestamp": vote_timestamp,
                "poll_id": vote_poll_id,
                "user_id": vote_user_id,
                "username": vote_username,
                "first_name": vote_first_name, # Aggiunto
                "last_name": vote_last_name,   # Aggiunto
                "option_id": vote_option_id,
                "match": match_name
            }
            
            # 2. Aggiunge il nuovo voto al log completo
            df_log = pd.concat([df_log, pd.DataFrame([row])], ignore_index=True)
            
            # 3. Crea la tabella di riepilogo globale
            df_summary = await asyncio.to_thread(create_summary_table, df_log)
            
            # 4. Scrive il log completo e il riepilogo nel file Excel
            await asyncio.to_thread(save_data_to_excel, df_log, df_summary)
            
        except Exception as e:
            print(f"⚠️ Errore generico nella gestione del voto (Excel): {e}")
            return
        
        print(f"Voto registrato e riepilogo Excel aggiornato: user {vote_user_id} ha votato opzione {vote_option_id} per {match_name}")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("polls", polls_command))
    app.add_handler(PollAnswerHandler(poll_answer))

    print("Bot avviato, pronto a creare sondaggi e registrare voti.")
    app.run_polling()

