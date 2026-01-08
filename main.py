import pandas as pd
import gspread
import requests
import time
import base64  # CORRIGIDO: Espaços normais aqui
import binascii 
from datetime import datetime, timedelta
from pytz import timezone
import os
import json

# --- CONSTANTES GLOBAIS ---
SCOPES = ['[https://www.googleapis.com/auth/spreadsheets](https://www.googleapis.com/auth/spreadsheets)']
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
    except json.JSONDecodeError:
        try:
            decoded_bytes = base64.b64decode(creds_var, validate=True)
            decoded_str = decoded_bytes.decode("utf-8")
            creds_dict = json.loads(decoded_str)
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

    for col in ['Doca', 'LH Trip Number', 'Station Name', 'CPT']:
        if col not in df.columns:
            return None, f"⚠️ Coluna '{col}' não encontrada."

    df = df[df['LH Trip Number'].str.strip() != '']
    df['CPT'] = pd.to_datetime(df['CPT'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['CPT'])
    df['Turno'] = df['CPT'].dt.hour.apply(identificar_turno)

    return df, None

def formatar_doca(doca):
    """Deixa apenas o número ou código curto da doca para economizar espaço."""
    doca = str(doca).strip()
    if not doca or doca == '-':
        return "--"
    # Remove a palavra 'Doca ' se existir, mantendo só o número
    return doca.replace('Doca', '').replace('doca', '').strip()

def montar_mensagem(df):
    agora = datetime.now(timezone('America/Sao_Paulo')).replace(tzinfo=None)
    limite_2h = agora + timedelta(hours=2)
    turno_atual = identificar_turno(agora.hour)

    mensagens = []
    totais = df['Turno'].value_counts().to_dict()

    df_2h = df[(df['CPT'] >= agora) & (df['CPT'] < limite_2h)].copy()
    
    # Cabeçalho formatado com espaçamento fixo
    # Ajuste os números (15, 6, 6) conforme a largura desejada das colunas
    header = f"{'LT'.center(15)} {'DOCA'.center(6)} {'CPT'.center(6)} {'DESTINO'}"
    divisor = "-" * 45 # Linha divisória
    
    if df_2h.empty:
        mensagens.append("🚛 LTs pendentes:\n\n✅ Sem LT pendente para as próximas 2h.\n")
    else:
        mensagens.append("🚛 LTs pendentes (Próximas 2h):")
        
        # Adiciona o cabeçalho apenas uma vez no início da tabela
        mensagens.append(f"```{header}\n{divisor}")
        
        df_2h['Hora'] = df_2h['CPT'].dt.hour

        # Ordena por horário
        df_ordenado = df_2h.sort_values(by='CPT')

        for _, row in df_ordenado.iterrows():
            lt = row['LH Trip Number'].strip()
            destino = row['Station Name'].strip()
            cpt = row['CPT']
            cpt_str = cpt.strftime('%H:%M')
            doca = formatar_doca(row['Doca'])

            minutos = int((cpt - agora).total_seconds() // 60)
            
            # Definição de ícones (apenas visual, fora das colunas para não quebrar alinhamento)
            if minutos < 0:
                prefixo = "❗️" 
            elif minutos <= 10:
                prefixo = "⚠️"
            else:
                prefixo = "  " # Espaço em branco para manter alinhamento

            # FORMATAÇÃO DAS COLUNAS
            # :<15 (Alinha à esquerda, 15 espaços)
            # :^6  (Centraliza, 6 espaços)
            linha_formatada = f"{lt:<15} {doca:^6} {cpt_str:^6} {destino}"
            
            # Adiciona o prefixo fora da formatação fixa, ou ajusta se preferir
            mensagens.append(f"{linha_formatada} {prefixo}")

        mensagens.append("```") # Fecha o bloco de código da tabela

    mensagens.append("")
    mensagens.append("─" * 30)
    mensagens.append("📊 Resumo Próximos Turnos:\n")

    prioridades_turno = {
        'Turno 1': ['Turno 2', 'Turno 3'],
        'Turno 2': ['Turno 3', 'Turno 1'],
        'Turno 3': ['Turno 1', 'Turno 2']
    }

    for turno in prioridades_turno.get(turno_atual, []):
        qtd = totais.get(turno, 0)
        mensagens.append(f"🔹 {turno}: {qtd} LH(s)")

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
    # Simples envio direto, pois agora controlamos o ``` dentro da função montar_mensagem
    # Se a mensagem for muito grande, o Seatalk pode cortar, mas quebrar tabela no meio estraga a formatação.
    # Tentativa de envio único primeiro.
    if len(mensagem) > limite:
        print("⚠️ Mensagem muito grande, pode ser cortada.")
    
    enviar_webhook(mensagem, webhook_url)

def main():
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL')
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')

    if not webhook_url or not spreadsheet_id:
        print("❌ Erro: Variáveis de ambiente faltando.")
        return

    cliente = autenticar_google()
    if not cliente:
        print("❌ Falha na autenticação.")
        return

    df, erro = obter_dados_expedicao(cliente, spreadsheet_id)
    if erro:
        print(erro)
        return

    mensagem = montar_mensagem(df)
    enviar_em_blocos(mensagem, webhook_url)

if __name__ == "__main__":
    main()
