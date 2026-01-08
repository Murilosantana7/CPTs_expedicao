import pandas as pd
import gspread
import requests
import time
import base64
import binascii
import json
import os
from datetime import datetime, timedelta
from pytz import timezone
from google.oauth2.service_account import Credentials # Importante para a correção

# --- CONSTANTES GLOBAIS ---
# Adicionado o escopo do Drive para evitar erros de permissão de token
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
NOME_ABA = 'Base Pending Tratado'
INTERVALO = 'A:F'

# --- AUTENTICAÇÃO ROBUSTA (CORREÇÃO DO ERRO DE TOKEN) ---
def autenticar_google():
    """
    Autentica usando google.oauth2.service_account diretamente.
    Suporta JSON puro ou Base64.
    """
    creds_var = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    
    if not creds_var:
        print("❌ Erro: Variável 'GOOGLE_SERVICE_ACCOUNT_JSON' vazia.")
        return None

    creds_dict = None

    # 1. Tenta carregar como JSON direto
    try:
        creds_dict = json.loads(creds_var)
    except json.JSONDecodeError:
        # 2. Se falhar, tenta decodificar Base64
        try:
            decoded_bytes = base64.b64decode(creds_var, validate=True)
            decoded_str = decoded_bytes.decode("utf-8")
            creds_dict = json.loads(decoded_str)
            print("✅ Credenciais decodificadas de Base64.")
        except Exception as e:
            print(f"❌ Erro Crítico: Falha ao processar credenciais (JSON/Base64 inválidos). Detalhe: {e}")
            return None

    try:
        # Cria as credenciais explicitamente com os escopos corretos
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        # Autoriza o cliente gspread
        cliente = gspread.authorize(creds)
        print("✅ Cliente gspread autenticado com sucesso.")
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

    cols_req = ['Doca', 'LH Trip Number', 'Station Name', 'CPT']
    for col in cols_req:
        if col not in df.columns:
            return None, f"⚠️ Coluna '{col}' não encontrada."

    df = df[df['LH Trip Number'].str.strip() != '']
    df['CPT'] = pd.to_datetime(df['CPT'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['CPT'])
    df['Turno'] = df['CPT'].dt.hour.apply(identificar_turno)

    return df, None

def formatar_doca_numero(doca):
    """Retorna apenas o número da doca ou '--' se vazio."""
    doca = str(doca).strip()
    # Pega apenas os dígitos
    numeros = ''.join(filter(str.isdigit, doca))
    return numeros if numeros else "--"

def montar_mensagem(df):
    agora = datetime.now(timezone('America/Sao_Paulo')).replace(tzinfo=None)
    limite_2h = agora + timedelta(hours=2)
    turno_atual = identificar_turno(agora.hour)

    mensagens = []
    totais = df['Turno'].value_counts().to_dict()

    # Filtra dados para as próximas 2 horas
    df_2h = df[(df['CPT'] >= agora) & (df['CPT'] < limite_2h)].copy()

    if df_2h.empty:
        mensagens.append("🚛 **LTs pendentes:**\n\n✅ Sem LT pendente para as próximas 2h.\n")
    else:
        mensagens.append("🚛 **LTs pendentes:**\n")
        df_2h['Hora'] = df_2h['CPT'].dt.hour

        # Configuração das larguras das colunas para alinhamento
        w_lt = 15      # Largura da coluna LT
        w_doca = 6     # Largura da coluna DOCA (centralizada)
        w_cpt = 5      # Largura da coluna CPT (centralizada)
        # Destino não precisa de largura fixa pois é a última coluna

        # Cabeçalho da tabela
        header = f"{'LT':<{w_lt}} {'DOCA':^{w_doca}} {'CPT':^{w_cpt}} DESTINO"

        for hora, grupo in df_2h.groupby('Hora', sort=True):
            qtd = len(grupo)
            mensagens.append(f"\n**{qtd} LH{'s' if qtd > 1 else ''} às {hora:02d}h**")
            
            # Inicia o bloco de código para garantir a fonte monoespaçada
            mensagens.append("```text")
            mensagens.append(header)
            mensagens.append("-" * (w_lt + w_doca + w_cpt + 10)) # Linha separadora

            for _, row in grupo.iterrows():
                lt = row['LH Trip Number'].strip()
                # Trunca o LT se for maior que a coluna para não quebrar o layout
                if len(lt) > w_lt:
                    lt = lt[:w_lt-1] + "…"
                
                destino = row['Station Name'].strip()
                cpt = row['CPT']
                cpt_str = cpt.strftime('%H:%M')
                doca_num = formatar_doca_numero(row['Doca'])

                # Calcula atraso para adicionar ícone de alerta
                minutos = int((cpt - agora).total_seconds() // 60)
                icone = ""
                if minutos < 0:
                    icone = "❗️"
                elif minutos <= 10:
                    icone = "⚠️"

                # Formata a linha da tabela
                # :< alinha à esquerda, :^ centraliza
                linha = f"{lt:<{w_lt}} {doca_num:^{w_doca}} {cpt_str:^{w_cpt}} {destino}"
                
                # Se tiver alerta, adiciona ao final da linha (dentro do bloco de código fica mais limpo assim)
                if icone:
                    linha += f" {icone}"
                
                mensagens.append(linha)
            
            mensagens.append("```") # Fecha o bloco de código

    mensagens.append("─" * 30)
    mensagens.append("**Resumo Próximos Turnos:**")
    
    prioridades_turno = {
        'Turno 1': ['Turno 2', 'Turno 3'],
        'Turno 2': ['Turno 3', 'Turno 1'],
        'Turno 3': ['Turno 1', 'Turno 2']
    }

    for turno in prioridades_turno.get(turno_atual, []):
        qtd = totais.get(turno, 0)
        mensagens.append(f"• {turno}: {qtd} pendente{'s' if qtd != 1 else ''}")

    return "\n".join(mensagens)

def enviar_webhook(mensagem, webhook_url):
    if not webhook_url:
        print("❌ Erro: WEBHOOK_URL não fornecida.")
        return
    try:
        payload = {
            "tag": "text",
            "text": {
                "format": 1, # Markdown ativado
                "content": mensagem
            }
        }
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("✅ Mensagem enviada com sucesso.")
    except Exception as e:
        print(f"❌ Erro ao enviar mensagem: {e}")

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
    enviar_webhook(mensagem, webhook_url)

if __name__ == "__main__":
    main()
