import pandas as pd
import gspread
import requests
import base64
import json
import os
from datetime import datetime, timedelta
from pytz import timezone
from google.oauth2.service_account import Credentials

# --- CONFIGURAÇÕES ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
NOME_ABA = 'Base Pending Tratado'
INTERVALO = 'A:F'

def autenticar_google():
    """Lógica de autenticação mantida intacta."""
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
    
    saida = ["🚛 **LTs pendentes:**\n"]
    
    if df_2h.empty:
        saida.append("✅ Sem pendências próximas.")
    else:
        # --- BLOCO DE CÓDIGO PADRÃO ---
        bloco = ["```"]
        
        # Larguras ajustadas para o layout das imagens enviadas
        w_lt = 15
        w_doca = 6
        w_cpt = 7
        
        # Cabeçalho idêntico ao da imagem
        header = f"{'LT':<{w_lt}} {'DOCA':<{w_doca}} {'CPT':<{w_cpt}} DESTINO"
        bloco.append(header)
        bloco.append("-" * 48)

        df_2h['H_Grupo'] = df_2h['CPT'].dt.hour
        
        # Ordenar por CPT para garantir a sequência correta
        df_2h = df_2h.sort_values('CPT')
        
        for hora, grupo in df_2h.groupby('H_Grupo', sort=False):
            # Título do grupo (ex: [3 LHs às 18h])
            bloco.append(f"\n[{len(grupo)} LHs às {hora:02d}h]")
            
            for _, row in grupo.iterrows():
                lt = row['LH Trip Number'].strip()[:w_lt-1]
                doca = formatar_doca(row['Doca'])
                cpt = row['CPT'].strftime('%H:%M')
                
                # Destino: Limita o tamanho para não quebrar a linha lateralmente
                destino = row['Station Name'].strip()[:20]
                
                linha = f"{lt:<{w_lt}} {doca:<{w_doca}} {cpt:<{w_cpt}} {destino}"
                bloco.append(linha)
        
        bloco.append("```")
        saida.append("\n".join(bloco))

    # Resumo de Turnos conforme image_b9b434.png
    saida.append("\n**Resumo Turnos:**")
    totais = df['Turno'].value_counts().to_dict()
    # Garante a ordem Turno 1, 2, 3 no resumo
    for t in ['Turno 1', 'Turno 2', 'Turno 3']:
        qtd = totais.get(t, 0)
        saida.append(f"• {t}: {qtd} pendentes")

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
        
        # Converte CPT e remove valores inválidos
        df['CPT'] = pd.to_datetime(df['CPT'], dayfirst=True, errors='coerce')
        df = df.dropna(subset=['CPT'])
        
        # Lógica de turnos
        def get_turno(h):
            if 6 <= h < 14: return "Turno 1"
            if 14 <= h < 22: return "Turno 2"
            return "Turno 3"
        df['Turno'] = df['CPT'].dt.hour.apply(get_turno)
        
        mensagem = montar_mensagem(df)
        
        # Envio para o Webhook do Seatalk
        payload = {
            "tag": "text",
            "text": {
                "format": 1, # Markdown habilitado
                "content": mensagem
            }
        }
        requests.post(webhook, json=payload).raise_for_status()
        print("✅ Script finalizado com sucesso no novo padrão.")
        
    except Exception as e:
        print(f"Erro na execução: {e}")

if __name__ == "__main__":
    main()
