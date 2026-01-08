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
    """
    Autentica usando o Secret JSON do GitHub.
    Tenta ler como JSON puro primeiro. Se falhar, tenta decodificar de Base64.
    """
    creds_var = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    
    if not creds_var:
        print("❌ Erro: Variável de ambiente 'GOOGLE_SERVICE_ACCOUNT_JSON' não definida.")
        return None

    creds_dict = None

    # 1. Tenta carregar como JSON direto
    try:
        creds_dict = json.loads(creds_var)
        print("✅ Credenciais carregadas via JSON puro.")
    except json.JSONDecodeError:
        # 2. Se falhar, tenta decodificar Base64
        try:
            print("⚠️ JSON direto inválido, tentando decodificar Base64...")
            decoded_bytes = base64.b64decode(creds_var, validate=True)
            decoded_str = decoded_bytes.decode("utf-8")
            creds_dict = json.loads(decoded_str)
            print("✅ Credenciais decodificadas de Base64 com sucesso.")
        except (binascii.Error, json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"❌ Erro Crítico: Falha ao ler credenciais (Nem JSON puro, nem Base64 válido). Detalhe: {e}")
            return None

    if not creds_dict:
        return None

    try:
        cliente = gspread.service_account_from_dict(creds_dict, scopes=SCOPES)
        print("✅ Cliente gspread autenticado com Service Account.")
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

    # Verifica colunas essenciais
    cols_necessarias = ['Doca', 'LH Trip Number', 'Station Name', 'CPT']
    for col in cols_necessarias:
        if col not in df.columns:
            return None, f"⚠️ Coluna '{col}' não encontrada."

    # Limpeza e conversão
    df = df[df['LH Trip Number'].str.strip() != '']
    df['CPT'] = pd.to_datetime(df['CPT'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['CPT'])
    df['Turno'] = df['CPT'].dt.hour.apply(identificar_turno)

    return df, None


def formatar_doca(doca):
    """Formata a doca para caber na coluna da tabela."""
    doca = str(doca).strip()
    if not doca or doca == '-' or doca == '':
        return "--"
    elif doca.startswith("EXT.OUT"):
        numeros = ''.join(filter(str.isdigit, doca))
        return f"Ext{numeros}"
    elif doca.startswith("Doca"):
        # Remove a palavra "Doca" para economizar espaço
        return doca.replace("Doca", "").strip()
    else:
        return doca


def montar_mensagem(df):
    agora = datetime.now(timezone('America/Sao_Paulo')).replace(tzinfo=None)
    limite_2h = agora + timedelta(hours=2)
    turno_atual = identificar_turno(agora.hour)

    # Inicia a lista já abrindo o bloco de código
    linhas = ["```text"]

    # --- PARTE 1: TABELA DAS PRÓXIMAS 2H ---
    df_2h = df[(df['CPT'] >= agora) & (df['CPT'] < limite_2h)].copy()
    
    if df_2h.empty:
        linhas.append("🚛 LTs pendentes: Sem pendências para as próximas 2h.")
        linhas.append("")
    else:
        linhas.append("🚛 LTs pendentes (Próximas 2h):")
        linhas.append("") 

        # Configuração das larguras das colunas
        w_lt = 14
        w_doca = 6
        w_cpt = 7
        w_dest = 15

        # Cabeçalho: LT | Doca | CPT | Destino
        header = f"{'LT'.ljust(w_lt)} | {'Doca'.center(w_doca)} | {'CPT'.center(w_cpt)} | {'Destino'.ljust(w_dest)}"
        separator = "-" * len(header)
        
        linhas.append(header)
        linhas.append(separator)

        df_2h = df_2h.sort_values(by='CPT')

        for _, row in df_2h.iterrows():
            lt = row['LH Trip Number'].strip()[:w_lt]
            destino = row['Station Name'].strip()[:w_dest]
            cpt = row['CPT']
            cpt_str = cpt.strftime('%H:%M')
            doca = formatar_doca(row['Doca'])[:w_doca]

            # Monta a linha
            linha = f"{lt.ljust(w_lt)} | {doca.center(w_doca)} | {cpt_str.center(w_cpt)} | {destino.ljust(w_dest)}"
            linhas.append(linha)
        
        linhas.append("") # Linha em branco após a tabela

    # --- PARTE 2: RESUMO DOS TURNOS ---
    linhas.append("─" * 40)
    linhas.append("📊 Resumo Próximos Turnos:")
    
    totais = df['Turno'].value_counts().to_dict()
    prioridades_turno = {
        'Turno 1': ['Turno 2', 'Turno 3'],
        'Turno 2': ['Turno 3', 'Turno 1'],
        'Turno 3': ['Turno 1', 'Turno 2']
    }

    for turno in prioridades_turno.get(turno_atual, []):
        qtd = totais.get(turno, 0)
        linhas.append(f"{turno}: {qtd} pendentes")

    # Fecha o bloco de código
    linhas.append("```")

    return "\n".join(linhas)


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
    """
    Envia a mensagem. Se for maior que o limite, tenta dividir.
    NOTA: Como a mensagem agora já contém os ``` internos, 
    essa função apenas envia o texto cru, sem adicionar mais crases.
    """
    if len(mensagem) <= limite:
        enviar_webhook(mensagem, webhook_url)
        return

    # Divisão simples caso a mensagem seja gigante (fallback)
    linhas = mensagem.split('\n')
    bloco = []
    
    # Se precisarmos dividir, o visual da tabela pode quebrar entre mensagens,
    # mas garantimos que a mensagem chegue.
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
