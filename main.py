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
SCOPES = [
    '[https://www.googleapis.com/auth/spreadsheets](https://www.googleapis.com/auth/spreadsheets)',
    '[https://www.googleapis.com/auth/drive](https://www.googleapis.com/auth/drive)'
]
NOME_ABA = 'Base Pending Tratado'
INTERVALO = 'A:F'

def autenticar_google():
    """Autenticação robusta para evitar erro de 'No access token'."""
    creds_var = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    if not creds_var:
        return None

    creds_dict = None
    try:
        creds_dict = json.loads(creds_var)
    except json.JSONDecodeError:
        try:
            decoded_bytes = base64.b64decode(creds_var, validate=True)
            creds_dict = json.loads(decoded_bytes.decode("utf-8"))
        except Exception:
            return None

    try:
        # Resolve o erro de token exibido na sua imagem de log
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        return gspread.authorize(creds)
    except Exception as e:
        print(f"Erro Auth: {e}")
        return None

def identificar_turno(hora):
    if 6 <= hora < 14: return "Turno 1"
    elif 14 <= hora < 22: return "Turno 2"
    else: return "Turno 3"

def formatar_doca(doca):
    doca = str(doca).strip()
    nums = ''.join(filter(str.isdigit, doca))
    return nums if nums else "--"

def montar_mensagem(df):
    agora = datetime.now(timezone('America/Sao_Paulo')).replace(tzinfo=None)
    limite_2h = agora + timedelta(hours=2)
    turno_atual = identificar_turno(agora.hour)
    
    df_2h = df[(df['CPT'] >= agora) & (df['CPT'] < limite_2h)].copy()
    
    # Cabeçalho principal
    saida = ["🚛 **LTs pendentes:**\n"]
    
    if df_2h.empty:
        saida.append("✅ Sem pendências para as próximas 2h.")
    else:
        # --- INÍCIO DO BLOCO DE TABELA ---
        # Removido o 'text' após as crases para usar o bloco padrão do Seatalk
        bloco = ["```"]
        
        # Definição de larguras para alinhamento das colunas
        w_lt = 15
        w_doca = 6
        w_cpt = 7
        
        header = f"{'LT':<{w_lt}} {'DOCA':<{w_doca}} {'CPT':<{w_cpt}} DESTINO"
        bloco.append(header)
        bloco.append("-" * 50)

        df_2h['Hora_Grupo'] = df_2h['CPT'].dt.hour
        
        for hora, grupo in df_2h.groupby('Hora_Grupo'):
            # Linha de separação de hora dentro da tabela
            bloco.append(f"\n[{len(grupo)} LHs às {hora:02d}h]")
            
            for _, row in grupo.iterrows():
                lt = row['LH Trip Number'].strip()[:w_lt-1]
                doca = formatar_doca(row['Doca'])
                cpt = row['CPT'].strftime('%H:%M')
                
                # Destino: Limita o tamanho para não quebrar a linha no mobile
                destino = row['Station Name'].strip()
                if len(destino) > 22:
                    destino = destino[:21] + ".."
                
                linha = f"{lt:<{w_lt}} {doca:<{w_doca}} {cpt:<{w_cpt}} {destino}"
                bloco.append(linha)
        
        bloco.append("```")
        saida.append("\n".join(bloco))

    # Resumo de Turnos (fora do bloco de código)
    saida.append("\n**Resumo Próximos Turnos:**")
    totais = df['Turno'].value_counts()
    ordem = {'Turno 1': ['Turno 2', 'Turno 3'], 'Turno 2': ['Turno 3', 'Turno 1'], 'Turno 3': ['Turno 1', 'Turno 2']}
    
    for t in ordem.get(turno_atual, []):
        qtd = totais.get(t, 0)
        saida.append(f"• {t}: {qtd} pendente{'s' if qtd != 1 else ''}")

    return "\n".join(saida)

def main():
    webhook = os.environ.get('SEATALK_WEBHOOK_URL')
    sheet_id = os.environ.get('SPREADSHEET_ID')
    
    if not webhook or not sheet_id:
        print("Faltam variáveis de ambiente.")
        return

    cliente = autenticar_google()
    if not cliente:
        print("Falha na autenticação.")
        return

    try:
        planilha = cliente.open_by_key(sheet_id)
        aba = planilha.worksheet(NOME_ABA)
        dados = aba.get(INTERVALO)
        
        df_raw = pd.DataFrame(dados)
        df = df_raw[1:].copy()
        df.columns = df_raw.iloc[0].str.strip()
        
        # Tratamento de datas
        df['CPT'] = pd.to_datetime(df['CPT'], dayfirst=True, errors='coerce')
        df = df.dropna(subset=['CPT'])
        df['Turno'] = df['CPT'].dt.hour.apply(identificar_turno)
        
        mensagem = montar_mensagem(df)
        
        # Envio para o Webhook
        payload = {"tag": "text", "text": {"format": 1, "content": mensagem}}
        requests.post(webhook, json=payload).raise_for_status()
        print("✅ Script executado e mensagem enviada.")
        
    except Exception as e:
        print(f"Erro durante a execução: {e}")

if __name__ == "__main__":
    main()
