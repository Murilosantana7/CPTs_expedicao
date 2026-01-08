import pandas as pd
import gspread
import requests
import time
import base64
import binascii
import re
from datetime import datetime, timedelta
from pytz import timezone
import os
import json

# --- CONSTANTES GLOBAIS ---
SCOPES = ['[https://www.googleapis.com/auth/spreadsheets](https://www.googleapis.com/auth/spreadsheets)']
NOME_ABA = 'Base Pending Tratado'
INTERVALO = 'A:F'

# --- AUTENTICAÇÃO (COM SUPORTE A BASE64) ---
def autenticar_google():
    """
    Lê a variável de ambiente. Tenta ler como JSON puro. 
    Se falhar, decodifica de Base64 e lê o JSON.
    """
    creds_var = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    
    if not creds_var:
        print("❌ Erro: Variável 'GOOGLE_SERVICE_ACCOUNT_JSON' vazia.")
        return None

    creds_dict = None

    try:
        # 1. Tenta JSON direto
        creds_dict = json.loads(creds_var)
    except json.JSONDecodeError:
        try:
            # 2. Tenta decodificar Base64
            decoded_bytes = base64.b64decode(creds_var, validate=True)
            decoded_str = decoded_bytes.decode("utf-8")
            creds_dict = json.loads(decoded_str)
            print("✅ Credenciais decodificadas de Base64.")
        except Exception as e:
            print(f"❌ Erro Crítico na credencial: {e}")
            return None

    try:
        cliente = gspread.service_account_from_dict(creds_dict, scopes=SCOPES)
        return cliente
    except Exception as e:
        print(f"❌ Erro gspread: {e}")
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
        return None, "⚠️ Nenhum dado encontrado."

    df = pd.DataFrame(dados[1:], columns=dados[0])
    df.columns = df.columns.str.strip()

    cols_req = ['Doca', 'LH Trip Number', 'Station Name', 'CPT']
    for col in cols_req:
        if col not in df.columns:
            return None, f"⚠️ Coluna '{col}' ausente."

    df = df[df['LH Trip Number'].str.strip() != '']
    df['CPT'] = pd.to_datetime(df['CPT'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['CPT'])
    df['Turno'] = df['CPT'].dt.hour.apply(identificar_turno)

    return df, None

def formatar_doca_numero(doca):
    """Retorna apenas o número da doca ou '--' se vazio."""
    doca = str(doca).strip()
    numeros = ''.join(filter(str.isdigit, doca))
    return numeros if numeros else "--"

def montar_mensagem(df):
    agora = datetime.now(timezone('America/Sao_Paulo')).replace(tzinfo=None)
    limite_2h = agora + timedelta(hours=2)
    turno_atual = identificar_turno(agora.hour)

    mensagens = []
    totais = df['Turno'].value_counts().to_dict()

    # Filtro de 2h
    df_2h = df[(df['CPT'] >= agora) & (df['CPT'] < limite_2h)].copy()

    if df_2h.empty:
        mensagens.append("🚛 **LTs pendentes:**\n\n✅ Sem pendências próximas.\n")
    else:
        mensagens.append("🚛 **LTs pendentes:**\n")
        df_2h['Hora'] = df_2h['CPT'].dt.hour
        
        # Definição de Larguras para Alinhamento
        w_lt = 15
        w_doca = 6
        w_cpt = 5
        
        # Cabeçalho Formatado
        header = f"{'LT':<{w_lt}} {'DOCA':^{w_doca}} {'CPT':^{w_cpt}} DESTINO"
        
        for hora, grupo in df_2h.groupby('Hora', sort=True):
            qtd = len(grupo)
            mensagens.append(f"\n**{qtd} LH{'s' if qtd > 1 else ''} às {hora:02d}h**")
            
            # Inicia bloco de código para a tabela
            tabela_linhas = []
            tabela_linhas.append(header)
            tabela_linhas.append("-" * (w_lt + w_doca + w_cpt + 10)) # Linha separadora
            
            for _, row in grupo.iterrows():
                lt = row['LH Trip Number'].strip()
                # Corta LT se for muito grande para não quebrar a tabela
                lt = (lt[:w_lt-1] + '…') if len(lt) > w_lt else lt
                
                destino = row['Station Name'].strip()
                cpt = row['CPT']
                cpt_str = cpt.strftime('%H:%M')
                doca_num = formatar_doca_numero(row['Doca'])

                # Cálculo de atraso para o ícone (fora da tabela)
                minutos = int((cpt - agora).total_seconds() // 60)
                icone = ""
                if minutos < 0:
                    icone = "❗️"
                elif minutos <= 10:
                    icone = "⚠️"
                
                # Formatação da linha (f-string com padding)
                # < : Alinha à esquerda
                # ^ : Centraliza
                linha_formatada = f"{lt:<{w_lt}} {doca_num:^{w_doca}} {cpt_str:^{w_cpt}} {destino}"
                
                # Adiciona o ícone antes da linha se houver, mas fora do alinhamento da coluna LT
                # Para manter a tabela limpa, vamos adicionar o ícone na linha anterior ou usar marcador
                if icone:
                    # Opção: Colocar ícone no final ou marcar a linha
                    linha_formatada += f" {icone}"
                
                tabela_linhas.append(linha_formatada)
            
            # Fecha bloco de código
            mensagens.append("```text")
            mensagens.append("\n".join(tabela_linhas))
            mensagens.append("```")

    mensagens.append("─" * 30)
    mensagens.append("**Resumo Próximos Turnos:**")
    
    prioridades = {
        'Turno 1': ['Turno 2', 'Turno 3'],
        'Turno 2': ['Turno 3', 'Turno 1'],
        'Turno 3': ['Turno 1', 'Turno 2']
    }

    for turno in prioridades.get(turno_atual, []):
        qtd = totais.get(turno, 0)
        mensagens.append(f"• {turno}: {qtd} pendente{'s' if qtd != 1 else ''}")

    return "\n".join(mensagens)

def enviar_webhook(mensagem, webhook_url):
    if not webhook_url: return
    try:
        payload = {
            "tag": "text",
            "text": {
                "format": 1, # 1 = Markdown
                "content": mensagem
            }
        }
        requests.post(webhook_url, json=payload).raise_for_status()
        print("✅ Enviado.")
    except Exception as e:
        print(f"❌ Erro envio: {e}")

def main():
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL')
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')

    if not webhook_url or not spreadsheet_id:
        print("❌ Configuração incompleta.")
        return

    cliente = autenticar_google()
    if not cliente: return

    df, erro = obter_dados_expedicao(cliente, spreadsheet_id)
    if erro:
        print(erro)
        return

    mensagem = montar_mensagem(df)
    
    # Envio direto (o Seatalk suporta mensagens grandes, mas se precisar dividir, avise)
    enviar_webhook(mensagem, webhook_url)

if __name__ == "__main__":
    main()
