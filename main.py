import pandas as pd
import gspread
import requests
import time
import base64
import binascii
from datetime import datetime, timedelta
from pytz import timezone
import os
import json

# --- CONSTANTES GLOBAIS ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
NOME_ABA = 'Base Pending Tratado'
INTERVALO = 'A:F'

# --- AUTENTICAÇÃO ---
def autenticar_google():
    creds_var = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    
    if not creds_var:
        print("❌ Erro: Variável 'GOOGLE_SERVICE_ACCOUNT_JSON' não definida.")
        return None

    creds_dict = None
    try:
        creds_dict = json.loads(creds_var)
        print("✅ Credenciais carregadas via JSON puro.")
    except json.JSONDecodeError:
        try:
            print("⚠️ Tentando decodificar Base64...")
            decoded_bytes = base64.b64decode(creds_var, validate=True)
            decoded_str = decoded_bytes.decode("utf-8")
            creds_dict = json.loads(decoded_str)
            print("✅ Credenciais decodificadas de Base64.")
        except Exception as e:
            print(f"❌ Erro Crítico na leitura das credenciais: {e}")
            return None

    if not creds_dict:
        return None

    try:
        return gspread.service_account_from_dict(creds_dict, scopes=SCOPES)
    except Exception as e:
        print(f"❌ Erro ao conectar com gspread: {e}")
        return None

# --- LÓGICA DE NEGÓCIO ---
def identificar_turno(hora):
    if 6 <= hora < 14:
        return "Turno 1"
    elif 14 <= hora < 22:
        return "Turno 2"
    else:
        return "Turno 3"

def obter_dados_expedicao(cliente, spreadsheet_id):
    if not cliente:
        return None, "⚠️ Cliente não autenticado."

    try:
        planilha = cliente.open_by_key(spreadsheet_id)
        aba = planilha.worksheet(NOME_ABA)
        dados = aba.get(INTERVALO)
    except Exception as e:
        return None, f"⚠️ Erro ao acessar planilha: {e}"

    if not dados or len(dados) < 2:
        return None, "⚠️ Nenhum dado encontrado."

    df = pd.DataFrame(dados[1:], columns=dados[0])
    df.columns = df.columns.str.strip()

    required_cols = ['Doca', 'LH Trip Number', 'Station Name', 'CPT']
    for col in required_cols:
        if col not in df.columns:
            return None, f"⚠️ Coluna '{col}' não encontrada."

    df = df[df['LH Trip Number'].str.strip() != '']
    df['CPT'] = pd.to_datetime(df['CPT'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['CPT'])
    df['Turno'] = df['CPT'].dt.hour.apply(identificar_turno)

    return df, None

def formatar_doca(doca):
    doca = doca.strip()
    if not doca or doca == '-':
        return "Doca --"
    elif doca.startswith("EXT.OUT"):
        numeros = ''.join(filter(str.isdigit, doca))
        return f"Doca {numeros}"
    elif not doca.startswith("Doca"):
        return f"Doca {doca}"
    else:
        return doca

# --- MONTAGEM DA MENSAGEM (FORMATO DA IMAGEM) ---
def montar_mensagem(df):
    agora = datetime.now(timezone('America/Sao_Paulo')).replace(tzinfo=None)
    limite_2h = agora + timedelta(hours=2)
    turno_atual = identificar_turno(agora.hour)

    mensagens = []
    totais = df['Turno'].value_counts().to_dict()

    df_2h = df[(df['CPT'] >= agora) & (df['CPT'] < limite_2h)].copy()
    
    mensagens.append("🚛 LTs pendentes:\n")

    if df_2h.empty:
        mensagens.append("✅ Sem LT pendente para as próximas 2h.\n")
    else:
        df_2h['Hora'] = df_2h['CPT'].dt.hour

        for hora, grupo in df_2h.groupby('Hora', sort=True):
            qtd_lhs = len(grupo)
            # Título fora do bloco de código (Texto normal)
            mensagens.append(f"{qtd_lhs} LH{'s' if qtd_lhs > 1 else ''} pendente{'s' if qtd_lhs > 1 else ''} às {hora:02d}h\n")
            
            # Bloco de código APENAS para a tabela
            mensagens.append("```") 
            
            # Cabeçalho: LT | Doca | CPT | Destino
            mensagens.append(f"{'LT':<13} | {'Doca':^6} | {'CPT':^5} | Destino")
            
            for _, row in grupo.iterrows():
                lt = row['LH Trip Number'].strip()
                destino = row['Station Name'].strip()
                
                # Formata Doca (Remove "Doca " para caber melhor ou usa --)
                doca_full = formatar_doca(row['Doca'])
                if "Doca --" in doca_full:
                    doca = "--"
                else:
                    doca = doca_full.replace("Doca ", "")

                cpt = row['CPT'].strftime('%H:%M')
                
                # Linha formatada
                linha = f"{lt:<13} | {doca:^6} | {cpt:^5} | {destino}"
                mensagens.append(linha)
            
            mensagens.append("```") # Fecha bloco da tabela
            mensagens.append("") # Linha em branco entre horários

    mensagens.append("─" * 40)
    mensagens.append("LH´s pendentes para os próximos turnos:\n")

    prioridades_turno = {
        'Turno 1': ['Turno 2', 'Turno 3'],
        'Turno 2': ['Turno 3', 'Turno 1'],
        'Turno 3': ['Turno 1', 'Turno 2']
    }

    for turno in prioridades_turno.get(turno_atual, []):
        qtd = totais.get(turno, 0)
        if qtd > 0:
            mensagens.append(f"⚠️ {qtd} LH{'s' if qtd != 1 else ''} pendente{'s' if qtd != 1 else ''} no {turno}")

    return "\n".join(mensagens)

# --- ENVIO (CORRIGIDO: SEM BLOCOS EXTRAS) ---
def enviar_webhook(mensagem, webhook_url):
    if not webhook_url:
        print("❌ Erro: WEBHOOK_URL ausente.")
        return
    try:
        payload = {
            "tag": "text",
            "text": {
                "format": 1,
                "content": mensagem
            }
        }
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("✅ Enviado.")
    except Exception as e:
        print(f"❌ Erro envio: {e}")

def enviar_em_blocos(mensagem, webhook_url, limite=3000):
    linhas = mensagem.split('\n')
    bloco = []
    
    for linha in linhas:
        bloco.append(linha)
        # Se o bloco atual passar do limite, envia o que tem
        if len("\n".join(bloco)) > limite:
            bloco.pop() # Remove a última linha que estourou
            enviar_webhook("\n".join(bloco), webhook_url) # Envia SEM adicionar ``` extras
            time.sleep(1)
            bloco = [linha] # Começa novo bloco
            
    if bloco:
        enviar_webhook("\n".join(bloco), webhook_url) # Envia o resto SEM adicionar ``` extras

# --- MAIN ---
def main():
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL')
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')

    if not webhook_url or not spreadsheet_id:
        print("❌ Configuração incompleta (Secrets).")
        return

    cliente = autenticar_google()
    if not cliente:
        return

    df, erro = obter_dados_expedicao(cliente, spreadsheet_id)
    if erro:
        print(erro)
        return

    mensagem = montar_mensagem(df)
    enviar_em_blocos(mensagem, webhook_url)

if __name__ == "__main__":
    main()
