import pandas as pd
import gspread
import requests
import base64
import json
import os
from datetime import datetime, timedelta
from pytz import timezone
from google.oauth2.service_account import Credentials

# --- CONFIGURAÇÕES FIXAS (Preservadas) ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
NOME_ABA = 'Base Pending Tratado'
INTERVALO = 'A:F'

def autenticar_google():
    """Autenticação preservada conforme solicitado."""
    creds_var = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    if not creds_var: return None
    try:
        try:
            creds_dict = json.loads(creds_var)
        except json.JSONDecodeError:
            creds_dict = json.loads(base64.b64decode(creds_var).decode("utf-8"))
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        return gspread.authorize(creds)
    except Exception:
        return None

def formatar_doca(doca):
    nums = ''.join(filter(str.isdigit, str(doca)))
    return nums if nums else "--"

def montar_mensagem(df):
    agora = datetime.now(timezone('America/Sao_Paulo')).replace(tzinfo=None)
    limite_2h = agora + timedelta(hours=2)
    
    df_2h = df[(df['CPT'] >= agora) & (df['CPT'] < limite_2h)].copy()
    
    # Início do Bloco Único
    saida = ["```"]
    saida.append("🚛 LTs pendentes:\n")
    
    if df_2h.empty:
        saida.append("✅ Sem pendências para as próximas 2h.")
    else:
        df_2h = df_2h.sort_values('CPT')
        df_2h['H_Grupo'] = df_2h['CPT'].dt.hour
        
        # Larguras para o cabeçalho interno
        w_lt, w_doca, w_cpt = 14, 8, 8
        
        for hora, grupo in df_2h.groupby('H_Grupo', sort=False):
            # Título do grupo
            qtd = len(grupo)
            saida.append(f"{qtd} LH{'s' if qtd > 1 else ''} pendente{'s' if qtd > 1 else ''} às {hora:02d}h\n")
            
            # Cabeçalho para cada horário
            sub_header = f"{'LT':^{w_lt}} | {'Doca':^{w_doca}} | {'CPT:':^{w_cpt}} | Destino"
            saida.append(sub_header)
            
            for _, row in grupo.iterrows():
                lt = row['LH Trip Number'].strip()
                doca = formatar_doca(row['Doca'])
                cpt = row['CPT'].strftime('%H:%M')
                destino = row['Station Name'].strip()
                
                # Linha com CPT antes do Destino
                linha = f"{lt:<{w_lt}} | {doca:^{w_doca}} | {cpt:^{w_cpt}} | {destino}"
                saida.append(linha)
            
            # Linha separadora sólida
            saida.append("\n" + "—"*45 + "\n")

    # Rodapé de Turnos
    saida.append("LH´s pendentes para os próximos turnos:\n")
    
    def get_turno_atual(h):
        if 6 <= h < 14: return "Turno 1"
        if 14 <= h < 22: return "Turno 2"
        return "Turno 3"
    
    turno_atual = get_turno_atual(agora.hour)
    totais = df['Turno'].value_counts().to_dict()
    
    ordem_resumo = {
        'Turno 1': ['Turno 2', 'Turno 3'],
        'Turno 2': ['Turno 3', 'Turno 1'],
        'Turno 3': ['Turno 1', 'Turno 2']
    }
    
    for t in ordem_resumo.get(turno_atual, []):
        qtd = totais.get(t, 0)
        saida.append(f"⚠️ {qtd} LHs pendentes no {t}")

    saida.append("```")
    return "\n".join(saida)

def main():
    webhook = os.environ.get('SEATALK_WEBHOOK_URL')
    sheet_id = os.environ.get('SPREADSHEET_ID')
    
    cliente = autenticar_google()
    if not cliente: return

    try:
        planilha = cliente.open_by_key(sheet_id)
        dados = planilha.worksheet(NOME_ABA).get(INTERVALO)
        
        df_raw = pd.DataFrame(dados)
        df = df_raw[1:].copy()
        df.columns = df_raw.iloc[0].str.strip()
        
        df['CPT'] = pd.to_datetime(df['CPT'], dayfirst=True, errors='coerce')
        df = df.dropna(subset=['CPT'])
        
        def get_turno(h):
            if 6 <= h < 14: return "Turno 1"
            if 14 <= h < 22: return "Turno 2"
            return "Turno 3"
        df['Turno'] = df['CPT'].dt.hour.apply(get_turno)
        
        mensagem = montar_mensagem(df)
        
        # Envio em bloco único
        requests.post(webhook, json={"tag": "text", "text": {"content": mensagem}})
        print("✅ Padrão com cabeçalhos por horário aplicado!")
        
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    main()
