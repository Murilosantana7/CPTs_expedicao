import pandas as pd
import gspread
import requests
import base64
import json
import os
from datetime import datetime, timedelta
from pytz import timezone
# Esta é a biblioteca que resolve o erro das suas imagens:
from google.oauth2.service_account import Credentials

# --- CONFIGURAÇÕES ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
NOME_ABA = 'Base Pending Tratado'
INTERVALO = 'A:F'

def autenticar_google():
    """Resolve o erro 'No access token' forçando o uso de Credentials oficial."""
    creds_var = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    if not creds_var: return None

    try:
        # Tenta carregar JSON puro ou Base64
        try:
            creds_dict = json.loads(creds_var)
        except json.JSONDecodeError:
            creds_dict = json.loads(base64.b64decode(creds_var).decode("utf-8"))

        # MÉTODO ROBUSTO: Cria a credencial com escopos explícitos
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        return gspread.authorize(creds)
    except Exception as e:
        print(f"Erro Auth: {e}")
        return None

def formatar_doca(doca):
    nums = ''.join(filter(str.isdigit, str(doca)))
    return nums if nums else "--"

def montar_mensagem(df):
    agora = datetime.now(timezone('America/Sao_Paulo')).replace(tzinfo=None)
    limite_2h = agora + timedelta(hours=2)
    
    df_2h = df[(df['CPT'] >= agora) & (df['CPT'] < limite_2h)].copy()
    saida = ["🚛 **LTs pendentes:**\n"]
    
    if df_2h.empty:
        saida.append("✅ Sem pendências próximas.")
    else:
        # Bloco de código puro (sem a palavra 'text') para o estilo escuro
        bloco = ["```"]
        w_lt, w_doca, w_cpt = 15, 6, 7
        
        bloco.append(f"{'LT':<{w_lt}} {'DOCA':<{w_doca}} {'CPT':<{w_cpt}} DESTINO")
        bloco.append("-" * 48)

        for hora, grupo in df_2h.groupby(df_2h['CPT'].dt.hour):
            bloco.append(f"\n[{len(grupo)} LHs às {hora:02d}h]")
            for _, row in grupo.iterrows():
                lt = row['LH Trip Number'].strip()[:w_lt-1]
                doca = formatar_doca(row['Doca'])
                cpt = row['CPT'].strftime('%H:%M')
                destino = row['Station Name'].strip()[:20]
                bloco.append(f"{lt:<{w_lt}} {doca:<{w_doca}} {cpt:<{w_cpt}} {destino}")
        
        bloco.append("```")
        saida.append("\n".join(bloco))

    saida.append("\n**Resumo Turnos:**")
    totais = df['Turno'].value_counts().to_dict()
    for t in ['Turno 1', 'Turno 2', 'Turno 3']:
        if t in totais: saida.append(f"• {t}: {totais[t]} pendentes")
    
    return "\n".join(saida)

def main():
    webhook = os.environ.get('SEATALK_WEBHOOK_URL')
    sheet_id = os.environ.get('SPREADSHEET_ID')
    
    cliente = autenticar_google()
    if not cliente: return

    try:
        dados = cliente.open_by_key(sheet_id).worksheet(NOME_ABA).get(INTERVALO)
        df = pd.DataFrame(dados[1:], columns=[c.strip() for c in dados[0]])
        df['CPT'] = pd.to_datetime(df['CPT'], dayfirst=True, errors='coerce')
        df = df.dropna(subset=['CPT'])
        
        def get_turno(h):
            if 6 <= h < 14: return "Turno 1"
            if 14 <= h < 22: return "Turno 2"
            return "Turno 3"
        df['Turno'] = df['CPT'].dt.hour.apply(get_turno)
        
        msg = montar_mensagem(df)
        requests.post(webhook, json={"tag": "text", "text": {"format": 1, "content": msg}})
        print("✅ Sucesso!")
    except Exception as e:
        print(f"Erro na execução: {e}")

if __name__ == "__main__":
    main()
