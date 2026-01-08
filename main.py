import pandas as pd
import gspread
import requests
import base64
import json
import os
from datetime import datetime, timedelta
from pytz import timezone
from google.oauth2.service_account import Credentials

# --- CONFIGURAÇÕES (MANTIDAS) ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
NOME_ABA = 'Base Pending Tratado'
INTERVALO = 'A:F'

def autenticar_google():
    """Lógica de autenticação preservada conforme solicitado."""
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
    
    # Filtro das próximas 2 horas
    df_2h = df[(df['CPT'] >= agora) & (df['CPT'] < limite_2h)].copy()
    
    saida = ["🚛 **LTs pendentes:**\n"]
    
    if df_2h.empty:
        saida.append("✅ Sem pendências próximas.")
    else:
        # --- AJUSTE DE FORMATAÇÃO DO BLOCO ---
        bloco = ["```"]
        
        # Larguras otimizadas para evitar quebra de linha no celular
        w_lt = 14   # Coluna LT
        w_doca = 5  # Coluna DOCA
        w_cpt = 6   # Coluna CPT
        
        # Cabeçalho da Tabela
        header = f"{'LT':<{w_lt}} {'DOCA':<{w_doca}} {'CPT':<{w_cpt}} DESTINO"
        bloco.append(header)
        bloco.append("-" * 42) # Linha divisória proporcional

        df_2h['H_Grupo'] = df_2h['CPT'].dt.hour
        
        for hora, grupo in df_2h.groupby('H_Grupo'):
            # Identificador de Horário dentro do bloco
            bloco.append(f"\n[{len(grupo)} LHs às {hora:02d}h]")
            
            for _, row in grupo.iterrows():
                lt = row['LH Trip Number'].strip()[:w_lt-1]
                doca = formatar_doca(row['Doca'])
                cpt = row['CPT'].strftime('%H:%M')
                
                # Destino com limite de caracteres para manter o alinhamento reto
                destino = row['Station Name'].strip()[:20]
                
                linha = f"{lt:<{w_lt}} {doca:<{w_doca}} {cpt:<{w_cpt}} {destino}"
                bloco.append(linha)
        
        bloco.append("```")
        saida.append("\n".join(bloco))

    # Resumo de Turnos (Fora do bloco de código)
    saida.append("\n**Resumo Turnos:**")
    totais = df['Turno'].value_counts().to_dict()
    for t in ['Turno 1', 'Turno 2', 'Turno 3']:
        if t in totais:
            saida.append(f"• {t}: {totais[t]} pendentes")

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
        
        # Envio final com formatação Markdown (format: 1)
        requests.post(webhook, json={"tag": "text", "text": {"format": 1, "content": mensagem}})
        print("✅ Mensagem enviada com layout atualizado!")
        
    except Exception as e:
        print(f"Erro na execução: {e}")

if __name__ == "__main__":
    main()
