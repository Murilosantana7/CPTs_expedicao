import pandas as pd
import gspread
import requests
import base64
import json
import os
from datetime import datetime, timedelta
from pytz import timezone
from google.oauth2.service_account import Credentials

# --- CONSTANTES ---
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
NOME_ABA = 'Base Pending Tratado'
INTERVALO = 'A:F'

# --- AUTENTICAÇÃO ROBUSTA ---
def autenticar_google():
    creds_var = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    if not creds_var:
        print("❌ Erro: Variável 'GOOGLE_SERVICE_ACCOUNT_JSON' vazia.")
        return None

    creds_dict = None
    try:
        creds_dict = json.loads(creds_var)
    except json.JSONDecodeError:
        try:
            decoded_bytes = base64.b64decode(creds_var, validate=True)
            decoded_str = decoded_bytes.decode("utf-8")
            creds_dict = json.loads(decoded_str)
            print("✅ Credenciais decodificadas de Base64.")
        except Exception as e:
            print(f"❌ Erro Crítico credenciais: {e}")
            return None

    try:
        # Usa google-auth para garantir o token correto
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        cliente = gspread.authorize(creds)
        print("✅ Cliente gspread autenticado (Modo Robusto).")
        return cliente
    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")
        return None

def identificar_turno(hora):
    if 6 <= hora < 14: return "Turno 1"
    elif 14 <= hora < 22: return "Turno 2"
    else: return "Turno 3"

def obter_dados_expedicao(cliente, spreadsheet_id):
    try:
        planilha = cliente.open_by_key(spreadsheet_id)
        aba = planilha.worksheet(NOME_ABA)
        dados = aba.get(INTERVALO)
    except Exception as e:
        return None, f"⚠️ Erro planilha: {e}"

    if not dados or len(dados) < 2: return None, "⚠️ Nenhum dado encontrado."

    df = pd.DataFrame(dados[1:], columns=dados[0])
    df.columns = df.columns.str.strip()

    for col in ['Doca', 'LH Trip Number', 'Station Name', 'CPT']:
        if col not in df.columns: return None, f"⚠️ Coluna '{col}' ausente."

    df = df[df['LH Trip Number'].str.strip() != '']
    df['CPT'] = pd.to_datetime(df['CPT'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['CPT'])
    df['Turno'] = df['CPT'].dt.hour.apply(identificar_turno)

    return df, None

def formatar_doca_numero(doca):
    doca = str(doca).strip()
    numeros = ''.join(filter(str.isdigit, doca))
    return numeros if numeros else "--"

def montar_mensagem(df):
    agora = datetime.now(timezone('America/Sao_Paulo')).replace(tzinfo=None)
    limite_2h = agora + timedelta(hours=2)
    turno_atual = identificar_turno(agora.hour)

    mensagens = []
    totais = df['Turno'].value_counts().to_dict()

    df_2h = df[(df['CPT'] >= agora) & (df['CPT'] < limite_2h)].copy()

    if df_2h.empty:
        mensagens.append("🚛 **LTs pendentes:**\n\n✅ Sem pendências próximas.\n")
    else:
        mensagens.append("🚛 **LTs pendentes:**")
        
        # --- INÍCIO DO BLOCO ÚNICO DE CÓDIGO ---
        # Isso evita que o Seatalk "alterne" a formatação errada
        mensagens.append("```text")
        
        # Configuração de Colunas
        w_lt = 14
        w_doca = 4
        w_cpt = 5
        
        header = f"{'LT':<{w_lt}} {'DOCA':^{w_doca}} {'CPT':^{w_cpt}} DESTINO"
        mensagens.append(header)
        mensagens.append("=" * 45) # Linha dupla para separar cabeçalho geral

        df_2h['Hora'] = df_2h['CPT'].dt.hour
        
        primeiro_grupo = True
        for hora, grupo in df_2h.groupby('Hora', sort=True):
            qtd = len(grupo)
            
            # Cabeçalho da Hora (dentro do bloco de texto para manter alinhamento)
            if not primeiro_grupo:
                mensagens.append("-" * 45) # Separador entre horários
            
            mensagens.append(f">> {qtd} LHs às {hora:02d}h")
            
            for _, row in grupo.iterrows():
                lt = row['LH Trip Number'].strip()
                if len(lt) > w_lt: lt = lt[:w_lt-1] + "…"
                
                destino = row['Station Name'].strip()
                # Encurtar destino se for muito longo para não quebrar linha no celular
                if len(destino) > 20: destino = destino[:19] + "…"
                
                cpt = row['CPT']
                cpt_str = cpt.strftime('%H:%M')
                doca_num = formatar_doca_numero(row['Doca'])

                minutos = int((cpt - agora).total_seconds() // 60)
                icone = ""
                if minutos < 0: icone = "(!)" # Usar texto simples dentro do bloco code
                elif minutos <= 10: icone = "(!)"

                linha = f"{lt:<{w_lt}} {doca_num:^{w_doca}} {cpt_str:^{w_cpt}} {destino} {icone}"
                mensagens.append(linha)
            
            primeiro_grupo = False

        mensagens.append("```") 
        # --- FIM DO BLOCO ÚNICO ---

    # Resumo fora do bloco de código
    mensagens.append("")
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
                "format": 1, 
                "content": mensagem
            }
        }
        requests.post(webhook_url, json=payload).raise_for_status()
        print("✅ Mensagem enviada.")
    except Exception as e:
        print(f"❌ Erro envio: {e}")

def main():
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL')
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')

    if not webhook_url or not spreadsheet_id:
        print("❌ Configuração incompleta.")
        return

    cliente = autenticar_google()
    if not cliente: return # Para se a autenticação falhar

    df, erro = obter_dados_expedicao(cliente, spreadsheet_id)
    if erro:
        print(erro)
        return

    mensagem = montar_mensagem(df)
    enviar_webhook(mensagem, webhook_url)

if __name__ == "__main__":
    main()
