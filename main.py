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
        print("❌ Erro: Variável de ambiente 'GOOGLE_SERVICE_ACCOUNT_JSON' não definida.")
        return None

    creds_dict = None

    try:
        creds_dict = json.loads(creds_var)
        print("✅ Credenciais carregadas via JSON puro.")
    except json.JSONDecodeError:
        try:
            print("⚠️ JSON direto inválido, tentando decodificar Base64...")
            decoded_bytes = base64.b64decode(creds_var, validate=True)
            decoded_str = decoded_bytes.decode("utf-8")
            creds_dict = json.loads(decoded_str)
            print("✅ Credenciais decodificadas de Base64 com sucesso.")
        except (binascii.Error, json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"❌ Erro Crítico: Falha ao ler credenciais. Detalhe: {e}")
            return None

    if not creds_dict:
        return None

    try:
        cliente = gspread.service_account_from_dict(creds_dict, scopes=SCOPES)
        return cliente
    except Exception as e:
        print(f"❌ Erro ao conectar com gspread: {e}")
        return None


def identificar_turno(hora):
    if 6 <= hora < 14:
        return "Turno 1"
    elif 14 <= hora < 22:
        return "Turno 2"
    else:
        return "Turno 3"


def obter_dados_expedicao(cliente, spreadsheet_id):
    if not cliente:
        return None, "⚠️ Não foi possível autenticar o cliente."

    try:
        planilha = cliente.open_by_key(spreadsheet_id)
        aba = planilha.worksheet(NOME_ABA)
        dados = aba.get(INTERVALO)
    except Exception as e:
        return None, f"⚠️ Erro ao acessar planilha: {e}"

    if not dados or len(dados) < 2:
        return None, "⚠️ Nenhum dado encontrado na planilha."

    df = pd.DataFrame(dados[1:], columns=dados[0])
    df.columns = df.columns.str.strip()

    cols_necessarias = ['Doca', 'LH Trip Number', 'Station Name', 'CPT']
    for col in cols_necessarias:
        if col not in df.columns:
            return None, f"⚠️ Coluna '{col}' não encontrada."

    df = df[df['LH Trip Number'].str.strip() != '']
    df['CPT'] = pd.to_datetime(df['CPT'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['CPT'])
    df['Turno'] = df['CPT'].dt.hour.apply(identificar_turno)

    return df, None


def formatar_doca(doca):
    """
    Deixa ESTRITAMENTE os números.
    """
    doca = str(doca).strip()
    if not doca or doca == '-' or doca == '':
        return "--"
    
    numeros = ''.join(filter(str.isdigit, doca))
    return numeros if numeros else "--"


def montar_mensagem(df):
    agora = datetime.now(timezone('America/Sao_Paulo')).replace(tzinfo=None)
    limite_2h = agora + timedelta(hours=2)
    turno_atual = identificar_turno(agora.hour)

    # Inicia o bloco de código com ESPAÇO
    mensagens = ["``` "] 

    # --- TÍTULO ---
    mensagens.append("🚛 LTs pendentes:")
    mensagens.append("") 

    # --- TABELA ---
    df_2h = df[(df['CPT'] >= agora) & (df['CPT'] < limite_2h)].copy()
    
    if df_2h.empty:
        mensagens.append("Sem pendências para as próximas 2h.")
    else:
        # --- LARGURAS FIXAS ---
        w_lt = 14
        w_doca = 6   
        w_cpt = 7    
        w_dest = 25  

        # --- CABEÇALHO (Tudo Centralizado com .center) ---
        header = f"{'LT'.center(w_lt)} | {'Doca'.center(w_doca)} | {'CPT'.center(w_cpt)} | {'Destino'.center(w_dest)}"
        separator = "─" * len(header)
        
        mensagens.append(header)
        mensagens.append(separator)

        # Adiciona coluna de Hora para agrupar
        df_2h['Hora'] = df_2h['CPT'].dt.hour
        df_2h = df_2h.sort_values(by=['CPT', 'Station Name'])

        # Loop agrupando por hora
        for hora, grupo in df_2h.groupby('Hora'):
            qtd = len(grupo)
            suffix = "s" if qtd > 1 else ""
            
            mensagens.append("") 
            mensagens.append(f"{qtd} LH{suffix} pendente{suffix} às {hora:02d}h")
            
            for _, row in grupo.iterrows():
                # Corta textos longos
                lt = row['LH Trip Number'].strip()[:w_lt]
                destino = row['Station Name'].strip()[:w_dest]
                cpt = row['CPT']
                cpt_str = cpt.strftime('%H:%M')
                doca = formatar_doca(row['Doca'])[:w_doca]

                # DADOS: LT e Destino à esquerda (leitura), Doca e CPT no centro
                linha = f"{lt.ljust(w_lt)} | {doca.center(w_doca)} | {cpt_str.center(w_cpt)} | {destino.ljust(w_dest)}"
                mensagens.append(linha)
        
    mensagens.append("") 

    # --- RODAPÉ ---
    mensagens.append("─" * 40)
    mensagens.append("LH´s pendentes para os próximos turnos:")
    mensagens.append("")
    
    totais = df['Turno'].value_counts().to_dict()
    prioridades_turno = {
        'Turno 1': ['Turno 2', 'Turno 3'],
        'Turno 2': ['Turno 3', 'Turno 1'],
        'Turno 3': ['Turno 1', 'Turno 2']
    }

    for turno in prioridades_turno.get(turno_atual, []):
        qtd = totais.get(turno, 0)
        suffix = "s" if qtd != 1 else ""
        if qtd > 0:
            mensagens.append(f"⚠️ {qtd} LH{suffix} pendente{suffix} no {turno}")
        else:
            mensagens.append(f"✅ 0 LHs pendentes no {turno}")

    # Fecha o bloco
    mensagens.append("```")

    return "\n".join(mensagens)


def enviar_webhook(mensagem, webhook_url):
    if not webhook_url:
        print("❌ Erro: WEBHOOK_URL não fornecida.")
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
        print("✅ Mensagem enviada com sucesso.")
    except Exception as e:
        print(f"❌ Erro ao enviar mensagem: {e}")


def enviar_em_blocos(mensagem, webhook_url, limite=3000):
    if len(mensagem) <= limite:
        enviar_webhook(mensagem, webhook_url)
        return

    linhas = mensagem.split('\n')
    bloco = []
    
    for linha in linhas:
        if len("\n".join(bloco)) + len(linha) + 1 > limite:
            enviar_webhook("\n".join(bloco), webhook_url)
            time.sleep(1)
            bloco = []
        bloco.append(linha)
    
    if bloco:
        enviar_webhook("\n".join(bloco), webhook_url)


def main():
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL')
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')

    if not webhook_url or not spreadsheet_id:
        print("❌ Erro: Variáveis de ambiente SEATALK_WEBHOOK_URL e/ou SPREADSHEET_ID não definidas.")
        return

    cliente = autenticar_google()
    if not cliente:
        print("❌ Falha na autenticação. Encerrando.")
        return

    df, erro = obter_dados_expedicao(cliente, spreadsheet_id)
    if erro:
        print(erro)
        return

    mensagem = montar_mensagem(df)
    enviar_em_blocos(mensagem, webhook_url)


if __name__ == "__main__":
    main()
