# WhatsApp_v5.1_master_logger_email.py
# ------------------------------------------------------------
# J&T Express – Bot de cadastro de motoristas (Black Friday)
# - ALTERNATIVA: Finaliza contato processando emails de notificação
# ------------------------------------------------------------

import os, csv, time, random, hashlib, logging, re, sys, json, shutil
import imaplib
import email
from email.header import decode_header
from datetime import datetime, date, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Bibliotecas para Excel
try:
    import pandas as pd
    from openpyxl import load_workbook
    from openpyxl.styles import Font, PatternFill, Alignment

    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False
    print("⚠️ Bibliotecas para Excel não instaladas. Execute: pip install pandas openpyxl")

# =========================
# CONFIGURAÇÕES
# =========================
SESSOES = [
    {"nome": "Conta_1", "user_data_dir": "C:/WhatsAppSession_1", "limite": 175},
    {"nome": "Conta_2", "user_data_dir": "C:/WhatsAppSession_2", "limite": 175},
]
NUMEROS = ["+5561994335940"]
UF_PADRAO = "DF"
HEADLESS = False
REPLY_TIMEOUT_HORAS = 12
REENVIO_ATIVO = True
INTERVALO_MIN, INTERVALO_MAX = 60, 120
MICROPAUSA_TECLA = (0.03, 0.12)
MICROPAUSA_LINHA = (0.4, 0.9)
HORA_INICIO, HORA_FIM = 8, 18
FINALIZAR_AO_TERMINAR = True
INATIVIDADE_AUTOEXIT_MIN = 120

# Configurações de Automação Diária
REINICIAR_DIARIAMENTE = True
MODO_PLANTAO = True
LIMPEZA_SEMANAL_ATIVA = True
DIA_LIMPEZA_SEMANAL = 6
HORA_LIMPEZA_SEMANAL = 23

# Configurações de Logs e Relatórios
SALVAR_EM_EXCEL = True
EXCEL_FILE = "Base_Master_Motoristas.xlsx"
GERAR_RELATORIO_DIARIO = True
GERAR_RELATORIO_SEMANAL = True

# Configurações de Notificações
NOTIFICACAO_ATIVA = False
WEBHOOK_URL = ""

# <<< NOVA FUNCIONALIDADE v5.1: Configurações de Finalização (via Email)
FINALIZAR_COM_FORMULARIO = True
INTERVALO_CHECAGEM_EMAIL_SEG = 300  # 300 segundos = 5 minutos
MSG_FORMULARIO_RECEBIDO = "🎉 Ótimo! Recebemos seu cadastro. Em breve, nossa equipe entrará em contato com os próximos passos. Muito obrigado!"
HASH_FILE = "processed_hashes.json"  # Arquivo para armazenar os hashes processados

# <<< CONFIGURAÇÕES DE EMAIL (PREENCHA COM SEUS DADOS)
EMAIL_IMAP_SERVER = "imap.gmail.com"  # Servidor IMAP (ex: imap.gmail.com)
EMAIL_IMAP_PORT = 993  # Porta IMAP (ex: 993 para Gmail com SSL)
EMAIL_ADDRESS = ""  # Seu endereço de email
EMAIL_PASSWORD = ""  # Sua senha de aplicativo (NÃO sua senha normal)
EMAIL_FOLDER = "INBOX"  # Pasta onde os emails de notificação chegam
EMAIL_SUBJECT_FILTER = "Google Forms"  # Filtro para identificar emails de notificação

PASTA_PRINTS = "prints_motoristas"
LOG_CSV = "log_motoristas.csv"
DNC_FILE = "nao_enviar_mais.csv"
AGENDA_REENVIO = "agenda_reenvio.csv"
os.makedirs(PASTA_PRINTS, exist_ok=True)

# =========================
# MENSAGENS
# =========================
MSG_INICIAL_VARIACOES = [
    (
        "Olá! Somos a J&T Express. Estamos cadastrando novos motoristas parceiros. "
        "Durante o período da Black Friday temos condições especiais: "
        "Acréscimo de R$ 0,50 por pacote no valor da sua região, além de bônus de até R$ 1,30! "
        "Para saber mais, responda com 'Sim'."
    )
]
MSG_INTERESSADO = "Perfeito! 🙌 Nosso time entrará em contato em breve. Por favor, preencha o formulário abaixo para acelerar seu cadastro:"
MSG_NAO_INTERESSADO = "Sem problema 👍 Agradecemos o seu tempo. Boa sorte e sucesso na estrada!"
MSG_PEDIR_CONF = "Por favor, responda apenas com 'sim' ou 'não' 👍"
MSG_REENVIO_12H = (
    "Oi! 👋 Relembrando a campanha Black Friday: bônus de até R$ 1,30 + R$ 0,50 extra por pacote. "
    "Responda 'Sim' para saber mais!"
)
MSG_FORA_HORARIO_NOITE = "Olá! Nosso horário são das 8h às 18h. Amanhã nossa equipe entrará em contato. 😊"
MSG_FORA_HORARIO_FDS = "Olá! Nosso horário são de segunda a sexta, das 8h às 18h. Segunda retornaremos o contato. 🚛"

LINK_FORM = "https://forms.gle/qckjgW3GkRiJ8uU56"


# =========================
# LOG COLORIDO
# =========================
class ColorFormatter(logging.Formatter):
    COLORS = {logging.INFO: "\033[92m", logging.WARNING: "\033[93m", logging.ERROR: "\033[91m"}

    def format(self, r):
        c = self.COLORS.get(r.levelno, "")
        reset = "\033[0m"
        return f"{c}{super().format(r)}{reset}"


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger()
for h in logger.handlers:
    h.setFormatter(ColorFormatter("%(message)s"))

# =========================
# CONTROLE DE DUPLICAÇÃO APRIMORADO
# =========================
enviados_hoje = set()
respostas_processadas = set()
finalizados_hoje = set()
processed_hashes = set()  # Conjunto de hashes já processados


def carregar_enviados_hoje():
    global enviados_hoje
    hoje = date.today().isoformat()
    if not os.path.exists(LOG_CSV): return
    with open(LOG_CSV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r.get("timestamp", "").startswith(hoje) and "ENVIO_OK" in r.get("etapa", ""):
                enviados_hoje.add(r.get("numero"))


def carregar_respostas_processadas():
    global respostas_processadas
    hoje = date.today().isoformat()
    if not os.path.exists(LOG_CSV): return
    with open(LOG_CSV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r.get("timestamp", "").startswith(hoje) and "RESPOSTA_PROCESSADA" in r.get("etapa", ""):
                respostas_processadas.add(f"{r.get('numero')}_{r.get('etapa').split('_')[-1]}")


def carregar_finalizados_hoje():
    global finalizados_hoje
    hoje = date.today().isoformat()
    if not os.path.exists(LOG_CSV): return
    with open(LOG_CSV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r.get("timestamp", "").startswith(hoje) and r.get("etapa") == "FORMULARIO_PREENCHIDO":
                finalizados_hoje.add(r.get("numero"))


def carregar_hashes_processados():
    global processed_hashes
    if os.path.exists(HASH_FILE):
        try:
            with open(HASH_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                processed_hashes = set(data.get("hashes", []))
                logger.info(f"🔑 Carregados {len(processed_hashes)} hashes processados.")
        except Exception as e:
            logger.error(f"Erro ao carregar hashes: {e}")
            processed_hashes = set()


def salvar_hashes_processados():
    try:
        with open(HASH_FILE, "w", encoding="utf-8") as f:
            json.dump({"hashes": list(processed_hashes)}, f, indent=2)
    except Exception as e:
        logger.error(f"Erro ao salvar hashes: {e}")


def gerar_hash_resposta(email_id, timestamp):
    """Gera um hash SHA-256 a partir do ID do email e do timestamp."""
    try:
        hash_object = hashlib.sha256(f"{email_id}_{timestamp}".encode())
        return hash_object.hexdigest()
    except Exception as e:
        logger.error(f"Erro ao gerar hash: {e}")
        return None


# =========================
# LOGS E EXCEL
# =========================
def escrever_log(ts, numero, etapa, detalhe="", sessao=""):
    novo = not os.path.exists(LOG_CSV)
    with open(LOG_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if novo:
            w.writerow(["timestamp", "numero", "etapa", "detalhe", "sessao"])
        w.writerow([ts, numero, etapa, detalhe, sessao])
    if SALVAR_EM_EXCEL and EXCEL_AVAILABLE:
        escrever_log_excel(ts, numero, etapa, detalhe, sessao)


def escrever_log_excel(ts, numero, etapa, detalhe="", sessao=""):
    try:
        hoje = date.today().isoformat()
        if not os.path.exists(EXCEL_FILE):
            with pd.ExcelWriter(EXCEL_FILE, engine='openpyxl') as writer:
                pd.DataFrame(columns=["timestamp", "numero", "etapa", "detalhe", "sessao"]).to_excel(writer,
                                                                                                     sheet_name=hoje,
                                                                                                     index=False)
        book = load_workbook(EXCEL_FILE)
        if hoje not in book.sheetnames:
            book.create_sheet(hoje)
            sheet = book[hoje]
            sheet.append(["timestamp", "numero", "etapa", "detalhe", "sessao"])
            for cell in sheet[1]:
                cell.font = Font(bold=True)
                cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                cell.font = Font(color="FFFFFF", bold=True)
                cell.alignment = Alignment(horizontal="center")
        sheet = book[hoje]
        sheet.append([ts, numero, etapa, detalhe, sessao])
        book.save(EXCEL_FILE)
    except Exception as e:
        logger.error(f"Erro ao escrever no Excel: {e}")


def ler_dnc():
    if not os.path.exists(DNC_FILE): return set()
    with open(DNC_FILE, encoding="utf-8") as f:
        return set(l.strip() for l in f if l.strip())


def add_dnc(numero):
    if numero not in ler_dnc():
        with open(DNC_FILE, "a", encoding="utf-8") as f:
            f.write(numero + "\n")
        logger.info(f"🔒 {numero} adicionado ao DNC")


def ja_enviado(numero):
    return numero in enviados_hoje


def resposta_ja_processada(numero, tipo):
    return f"{numero}_{tipo}" in respostas_processadas


def marcar_resposta_processada(numero, tipo):
    respostas_processadas.add(f"{numero}_{tipo}")


def foi_finalizado(numero):
    return numero in finalizados_hoje


def salvar_print(driver, tag):
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(PASTA_PRINTS, f"{tag}_{ts}.png")
        driver.save_screenshot(path)
        logger.info(f"📸 {path}")
        return path
    except Exception as e:
        logger.warning(f"Erro ao salvar print: {e}")
        return None


def pause_humano():
    t = random.uniform(INTERVALO_MIN, INTERVALO_MAX)
    logger.info(f"⏳ Pausa humanizada: {t:.0f}s")
    time.sleep(t)


def pausa_jitter(min_v, max_v):
    time.sleep(random.uniform(min_v, max_v))


def digitar_humano(campo, texto):
    partes = texto.split("\n")
    for i, parte in enumerate(partes):
        for ch in parte:
            campo.send_keys(ch)
            pausa_jitter(*MICROPAUSA_TECLA)
        if i < len(partes) - 1:
            campo.send_keys(Keys.SHIFT, Keys.ENTER)
            pausa_jitter(*MICROPAUSA_LINHA)


# =========================
# SELENIUM HELPERS + ENVIO SEGURO
# =========================
XPATH_INPUT = '//footer//div[@contenteditable="true" and @role="textbox"]'
XPATH_MSG_IN = '//div[contains(@class,"message-in")]//span[@dir="ltr"]'


def limpar_texto_bmp(s):
    return ''.join(ch for ch in s if ord(ch) <= 0xFFFF)


def criar_driver(sessao):
    opts = Options()
    opts.add_argument(f"--user-data-dir={sessao['user_data_dir']}")
    opts.add_argument("--start-maximized")
    opts.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36"
    ]
    opts.add_argument(f"--user-agent={random.choice(user_agents)}")
    if HEADLESS:
        opts.add_argument("--headless=new")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    logger.info(f"🌐 {sessao['nome']} inicializado.")
    return driver


def abrir_whatsapp(driver):
    driver.get("https://web.whatsapp.com/")
    logger.info("🔄 Aguardando login no WhatsApp Web (20 s)…")
    time.sleep(20)


def abrir_chat(driver, numero):
    driver.get(f"https://web.whatsapp.com/send?phone={numero}")
    WebDriverWait(driver, 40).until(EC.presence_of_element_located((By.XPATH, XPATH_INPUT)))


def enviar_texto(driver, texto, tentativa=1):
    texto = limpar_texto_bmp(texto)
    try:
        campo = driver.find_element(By.XPATH, XPATH_INPUT)
        campo.click()
        time.sleep(0.5)
        try:
            campo.clear()
        except Exception:
            pass
        digitar_humano(campo, texto)
        time.sleep(random.uniform(0.6, 1.2))
        campo.send_keys(Keys.ENTER)
        time.sleep(1.0)
        mensagens = driver.find_elements(By.XPATH, '//div[contains(@class,"message-out")]')
        if mensagens and texto[:15].lower() in mensagens[-1].text.strip().lower()[:15].lower():
            logger.info("✅ Mensagem enviada com sucesso.")
            return True
        try:
            botao = driver.find_element(By.XPATH, '//span[@data-icon="send"]')
            botao.click()
            logger.info("✅ Mensagem enviada via botão.")
            time.sleep(0.8)
            return True
        except Exception:
            logger.warning("⚠️ Falha ao clicar em Enviar; tentando ENTER novamente…")
            campo.send_keys(Keys.ENTER)
            time.sleep(1.0)
            return True
    except Exception as e:
        logger.error(f"❌ Erro ao enviar mensagem (tentativa {tentativa}): {e}")
        salvar_print(driver, f"erro_envio_tentativa{tentativa}")
        if tentativa < 2:
            time.sleep(2)
            return enviar_texto(driver, texto, tentativa + 1)
        return False


# =========================
# AGENDA DE REENVIO (12 h)
# =========================
def agendar_reenvio(numero, horas=12):
    when = (datetime.utcnow() + timedelta(hours=horas)).isoformat()
    novo = not os.path.exists(AGENDA_REENVIO)
    with open(AGENDA_REENVIO, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if novo: w.writerow(["numero", "iso_schedule"])
        w.writerow([numero, when])
    logger.info(f"🕐 Reenvio agendado para {numero} em {horas} h")


def carregar_agenda():
    if not os.path.exists(AGENDA_REENVIO): return []
    with open(AGENDA_REENVIO, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def salvar_agenda(itens):
    with open(AGENDA_REENVIO, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["numero", "iso_schedule"])
        for it in itens: w.writerow([it["numero"], it["iso_schedule"]])


def cancelar_reenvio(numero):
    itens = carregar_agenda()
    antes = len(itens)
    itens = [it for it in itens if it["numero"] != numero]
    if len(itens) != antes:
        salvar_agenda(itens)
        logger.info(f"❎ Reenvio cancelado para {numero}")


def processar_agenda_reenvio(driver):
    if not REENVIO_ATIVO: return
    itens = carregar_agenda()
    if not itens: return
    now = datetime.utcnow()
    due, keep = [], []
    for it in itens:
        try:
            when = datetime.fromisoformat(it["iso_schedule"])
            (due if when <= now else keep).append(it)
        except Exception:
            continue
    if not due: return
    for it in due:
        numero = it["numero"]
        try:
            abrir_chat(driver, numero)
            enviar_texto(driver, MSG_REENVIO_12H)
            escrever_log(datetime.utcnow().isoformat(), numero, "REENVIO_12H", "Lembrete enviado")
            salvar_print(driver, f"reenviou12h_{numero[-4:]}")
        except Exception as e:
            logger.error(f"Erro no reenvio 12 h para {numero}: {e}")
    salvar_agenda(keep)


# =========================
# CONTROLE DE HORÁRIO E AUTOMAÇÃO
# =========================
def dentro_do_horario():
    now = datetime.now()
    return now.weekday() < 5 and HORA_INICIO <= now.hour < HORA_FIM


def fora_horario_mensagem():
    now = datetime.now()
    return MSG_FORA_HORARIO_NOITE if now.weekday() < 5 else MSG_FORA_HORARIO_FDS


def aguardar_inicio_dia():
    now = datetime.now()
    proximo_inicio = datetime.combine(now.date(), datetime.min.time()) + timedelta(hours=HORA_INICIO)
    if now.hour >= HORA_FIM:
        proximo_inicio += timedelta(days=1)
    espera_segundos = (proximo_inicio - now).total_seconds()
    espera_horas = int(espera_segundos // 3600)
    espera_minutos = int((espera_segundos % 3600) // 60)
    logger.info(f"🌙 Fora do horário comercial. Modo plantão ativado.")
    logger.info(f"⏰ Próxima retomada em {espera_horas}h e {espera_minutos}min.")
    time.sleep(espera_segundos)
    logger.info("☀️ Horário comercial iniciado. Retomando operações.")


def executar_limpeza_semanal():
    logger.warning("🧹 Iniciando limpeza semanal dos perfis do Chrome...")
    for sessao in SESSOES:
        if os.path.exists(sessao["user_data_dir"]):
            try:
                shutil.rmtree(sessao["user_data_dir"])
                logger.info(f"   - Perfil '{sessao['nome']}' limpo com sucesso.")
            except Exception as e:
                logger.error(f"   - Erro ao limpar perfil '{sessao['nome']}': {e}")
    logger.warning("⚠️ Limpeza concluída. Um novo login será necessário na próxima execução.")


# =========================
# RELATÓRIOS AUTOMÁTICOS
# =========================
def gerar_relatorio_diario():
    if not GERAR_RELATORIO_DIARIO: return
    hoje = date.today().isoformat()
    stats = {"envios": 0, "respostas": 0, "interessados": 0, "nao_interessados": 0, "reenvios": 0, "formularios": 0}
    if not os.path.exists(LOG_CSV): return stats
    with open(LOG_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["timestamp"].startswith(hoje):
                etapa = row["etapa"]
                if "ENVIO_OK" in etapa:
                    stats["envios"] += 1
                elif "RESPONDEU_" in etapa:
                    stats["respostas"] += 1
                    if etapa == "RESPONDEU_SIM":
                        stats["interessados"] += 1
                    elif etapa == "RESPONDEU_NAO":
                        stats["nao_interessados"] += 1
                elif "REENVIO_12H" in etapa:
                    stats["reenvios"] += 1
                elif "FORMULARIO_PREENCHIDO" in etapa:
                    stats["formularios"] += 1
    with open(f"relatorio_{hoje}.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)
    if SALVAR_EM_EXCEL and EXCEL_AVAILABLE:
        try:
            book = load_workbook(EXCEL_FILE)
            relatorio_sheet_name = f"Relatorio_{hoje}"
            if relatorio_sheet_name not in book.sheetnames:
                book.create_sheet(relatorio_sheet_name)
            sheet = book[relatorio_sheet_name]
            sheet.append(["Métrica", "Quantidade", "Percentual"])
            total_envios = stats["envios"]
            for metrica, valor in stats.items():
                if metrica != "envios":
                    percentual = (valor / total_envios * 100) if total_envios > 0 else 0
                    sheet.append([metrica.capitalize(), valor, f"{percentual:.2f}%"])
            for cell in sheet[1]:
                cell.font = Font(bold=True)
                cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                cell.font = Font(color="FFFFFF", bold=True)
                cell.alignment = Alignment(horizontal="center")
            book.save(EXCEL_FILE)
            logger.info(f"📊 Relatório diário salvo em Excel: {relatorio_sheet_name}")
        except Exception as e:
            logger.error(f"Erro ao salvar relatório diário em Excel: {e}")
    return stats


def gerar_relatorio_semanal():
    if not GERAR_RELATORIO_SEMANAL: return
    hoje = date.today()
    inicio_semana = hoje - timedelta(days=hoje.weekday())
    fim_semana = inicio_semana + timedelta(days=6)
    stats = {"periodo": f"{inicio_semana.isoformat()} a {fim_semana.isoformat()}", "envios": 0, "respostas": 0,
             "interessados": 0, "nao_interessados": 0, "reenvios": 0, "formularios": 0, "dias": {}}
    if not os.path.exists(LOG_CSV): return stats
    for dia in range(7):
        data_dia = (inicio_semana + timedelta(days=dia)).isoformat()
        stats_dia = {"envios": 0, "respostas": 0, "interessados": 0, "nao_interessados": 0, "reenvios": 0,
                     "formularios": 0}
        with open(LOG_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row["timestamp"].startswith(data_dia):
                    etapa = row["etapa"]
                    if "ENVIO_OK" in etapa:
                        stats_dia["envios"] += 1; stats["envios"] += 1
                    elif "RESPONDEU_" in etapa:
                        stats_dia["respostas"] += 1;
                        stats["respostas"] += 1
                        if etapa == "RESPONDEU_SIM":
                            stats_dia["interessados"] += 1; stats["interessados"] += 1
                        elif etapa == "RESPONDEU_NAO":
                            stats_dia["nao_interessados"] += 1; stats["nao_interessados"] += 1
                    elif "REENVIO_12H" in etapa:
                        stats_dia["reenvios"] += 1; stats["reenvios"] += 1
                    elif "FORMULARIO_PREENCHIDO" in etapa:
                        stats_dia["formularios"] += 1; stats["formularios"] += 1
        stats["dias"][data_dia] = stats_dia
    with open(f"relatorio_semanal_{hoje.isoformat()}.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)
    if SALVAR_EM_EXCEL and EXCEL_AVAILABLE:
        try:
            book = load_workbook(EXCEL_FILE)
            relatorio_sheet_name = f"Relatorio_Semanal_{hoje.isoformat()}"
            if relatorio_sheet_name not in book.sheetnames:
                book.create_sheet(relatorio_sheet_name)
            sheet = book[relatorio_sheet_name]
            sheet.append(["Período", stats["periodo"]]);
            sheet.append([])
            sheet.append(["Resumo Geral"]);
            sheet.append(["Métrica", "Quantidade", "Percentual"])
            total_envios = stats["envios"]
            for metrica in ["envios", "respostas", "interessados", "nao_interessados", "reenvios", "formularios"]:
                if metrica != "envios":
                    valor = stats[metrica]
                    percentual = (valor / total_envios * 100) if total_envios > 0 else 0
                    sheet.append([metrica.capitalize(), valor, f"{percentual:.2f}%"])
            sheet.append([]);
            sheet.append(["Detalhe por Dia"]);
            sheet.append(["Data", "Envios", "Respostas", "Interessados", "Não Interessados", "Reenvios", "Formulários"])
            for data, dados in stats["dias"].items():
                sheet.append(
                    [data, dados["envios"], dados["respostas"], dados["interessados"], dados["nao_interessados"],
                     dados["reenvios"], dados["formularios"]])
            for cell in sheet[1]:
                cell.font = Font(bold=True)
                cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                cell.font = Font(color="FFFFFF", bold=True)
                cell.alignment = Alignment(horizontal="center")
            book.save(EXCEL_FILE)
            logger.info(f"📊 Relatório semanal salvo em Excel: {relatorio_sheet_name}")
        except Exception as e:
            logger.error(f"Erro ao salvar relatório semanal em Excel: {e}")
    return stats


# =========================
# NOTIFICAÇÕES
# =========================
def enviar_notificacao(stats):
    if not NOTIFICACAO_ATIVA or not WEBHOOK_URL: return
    try:
        hoje = date.today().isoformat()
        total_envios = stats.get("envios", 0)
        total_respostas = stats.get("respostas", 0)
        taxa_resposta = (total_respostas / total_envios * 100) if total_envios > 0 else 0
        mensagem = f"""
📊 Relatório Diário J&T Express - {hoje}
🚀 Envios: {total_envios}
💬 Respostas: {total_respostas} ({taxa_resposta:.2f}%)
✅ Interessados: {stats.get('interessados', 0)}
❌ Não Interessados: {stats.get('nao_interessados', 0)}
🔄 Reenvios: {stats.get('reenvios', 0)}
📝 Formulários Preenchidos: {stats.get('formularios', 0)}
        """
        payload = {"text": mensagem}
        response = requests.post(WEBHOOK_URL, json=payload)
        if response.status_code == 200:
            logger.info("📢 Notificação enviada com sucesso")
        else:
            logger.error(f"❌ Erro ao enviar notificação: {response.status_code}")
    except Exception as e:
        logger.error(f"❌ Erro ao enviar notificação: {e}")


# =========================
# CLASSIFICAÇÃO DE RESPOSTAS
# =========================
def classificar_resposta(txt):
    t = (txt or "").lower()
    sim = {"sim", "tenho interesse", "quero", "interessado", "ok"}
    nao = {"nao", "não", "não quero", "nao quero", "pare", "sem interesse", "n"}
    if any(p in t for p in sim): return "SIM"
    if any(p in t for p in nao): return "NAO"
    return "OUTRO"


# =========================
# TRATAMENTO DE RESPOSTAS
# =========================
def get_msgs(driver):
    try:
        return driver.find_elements(By.XPATH, XPATH_MSG_IN)
    except Exception:
        return []


def tratar_resposta(driver, numero, sessao):
    if foi_finalizado(numero): return False
    try:
        abrir_chat(driver, numero)
        msgs = get_msgs(driver)
        if not msgs: return False
        texto = (msgs[-1].text or "").strip()
        if not texto: return False
        tipo = classificar_resposta(texto)
        logger.info(f"📩 RESPOSTA RECEBIDA de {numero}: '{texto}' ({tipo})")
        if resposta_ja_processada(numero, tipo):
            logger.info(f"⏭️  Resposta de {numero} já foi processada. Ignorando.")
            return False
        marcar_resposta_processada(numero, tipo)
        cancelar_reenvio(numero)
        ts = datetime.utcnow().isoformat()
        if dentro_do_horario():
            if tipo == "SIM":
                enviar_texto(driver, f"{MSG_INTERESSADO}\n\n📝 Formulário de cadastro: {LINK_FORM}")
                salvar_print(driver, f"sim_{numero[-4:]}")
                escrever_log(ts, numero, "RESPONDEU_SIM", "Link do formulário enviado", sessao["nome"])
                return True
            elif tipo == "NAO":
                enviar_texto(driver, MSG_NAO_INTERESSADO)
                add_dnc(numero)
                salvar_print(driver, f"nao_{numero[-4:]}")
                escrever_log(ts, numero, "RESPONDEU_NAO", "", sessao["nome"])
                return True
            else:
                enviar_texto(driver, MSG_PEDIR_CONF)
                salvar_print(driver, f"confirma_{numero[-4:]}")
                escrever_log(ts, numero, "PEDIU_CONFIRMACAO", "", sessao["nome"])
                return True
        else:
            msg = fora_horario_mensagem()
            enviar_texto(driver, msg)
            salvar_print(driver, f"fora_horario_{numero[-4:]}")
            escrever_log(ts, numero, "AUTO_REPLY_FORA_HORARIO", "", sessao["nome"])
            return True
    except Exception as e:
        logger.error(f"Erro ao tratar resposta de {numero}: {e}")
        salvar_print(driver, f"erro_resposta_{numero[-4:]}")
        return False


def finalizar_contato(driver, numero, sessao):
    try:
        if foi_finalizado(numero):
            logger.info(f"⏭️  Contato {numero} já foi finalizado.")
            return False
        abrir_chat(driver, numero)
        enviar_texto(driver, MSG_FORMULARIO_RECEBIDO)
        salvar_print(driver, f"finalizado_{numero[-4:]}")
        ts = datetime.utcnow().isoformat()
        escrever_log(ts, numero, "FORMULARIO_PREENCHIDO", "Cadastro recebido e finalizado", sessao["nome"])
        finalizados_hoje.add(numero)
        logger.info(f"✅ Contato {numero} finalizado com sucesso!")
        return True
    except Exception as e:
        logger.error(f"Erro ao finalizar contato {numero}: {e}")
        return False


# =========================
# PROCESSAMENTO DE EMAILS (COM HASH)
# =========================
def conectar_email():
    """Conecta ao servidor de email e retorna a conexão e a caixa de entrada."""
    try:
        mail = imaplib.IMAP4_SSL(EMAIL_IMAP_SERVER, EMAIL_IMAP_PORT)
        mail.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        return mail
    except Exception as e:
        logger.error(f"❌ Erro ao conectar ao email: {e}")
        return None


def extrair_numero_telefone(corpo_email):
    """Extrai o número de telefone do corpo do email."""
    # Procura por padrões de número de telefone no corpo do email
    # Padrões possíveis: (61) 91234-5678, 61 91234-5678, 61912345678, etc.
    padroes = [
        r'\(\d{2}\)\s*\d{4,5}-\d{4}',  # (61) 91234-5678
        r'\d{2}\s*\d{4,5}-\d{4}',  # 61 91234-5678
        r'\d{10,11}'  # 61912345678 ou 11912345678
    ]

    for padrao in padroes:
        match = re.search(padrao, corpo_email)
        if match:
            numero = re.sub(r'\D', '', match.group())
            if not numero.startswith("55"):
                numero = "55" + numero
            return "+" + numero

    return None


def processar_emails(drivers, sessoes):
    """Processa emails de notificação do Google Forms."""
    if not FINALIZAR_COM_FORMULARIO or not EMAIL_ADDRESS or not EMAIL_PASSWORD:
        return

    mail = conectar_email()
    if not mail:
        return

    try:
        mail.select(EMAIL_FOLDER)

        # Busca emails não lidos com o filtro de assunto
        status, messages = mail.search(None, f'(UNSEEN SUBJECT "{EMAIL_SUBJECT_FILTER}")')

        if status != "OK" or not messages[0]:
            logger.info("ℹ️ Nenhum novo email de notificação encontrado.")
            return

        # Converte a lista de IDs para uma lista de strings
        email_ids = messages[0].split()
        logger.info(f"📧 Encontrados {len(email_ids)} emails de notificação.")

        novos_processados = 0
        for email_id in email_ids:
            # Obtém os dados do email
            status, msg_data = mail.fetch(email_id, "(RFC822)")

            if status != "OK":
                continue

            # Converte os dados do email para um objeto de email
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)

            # Obtém o timestamp do email
            timestamp = email.utils.parsedate_to_datetime(msg["Date"])
            timestamp_str = timestamp.isoformat()

            # Gera o hash para este email
            hash_email = gerar_hash_resposta(email_id.decode(), timestamp_str)

            if not hash_email or hash_email in processed_hashes:
                continue

            # Marca o email como processado
            processed_hashes.add(hash_email)

            # Extrai o corpo do email
            corpo = ""
            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    if content_type == "text/plain":
                        corpo = part.get_payload(decode=True).decode()
                        break
            else:
                corpo = msg.get_payload(decode=True).decode()

            # Extrai o número de telefone do corpo do email
            numero = extrair_numero_telefone(corpo)

            if not numero:
                logger.warning(f"⚠️ Não foi possível extrair o número de telefone do email {email_id.decode()}")
                continue

            logger.info(f"🔔 Novo formulário detectado no email para {numero}. Finalizando contato...")

            # Finaliza o contato
            for driver in drivers:
                if finalizar_contato(driver, numero, sessoes[0]):
                    novos_processados += 1
                    break

            # Marca o email como lido
            mail.store(email_id, '+FLAGS', '\\Seen')

        if novos_processados > 0:
            logger.info(f"✅ {novos_processados} novos formulários processados. Salvando hashes...")
            salvar_hashes_processados()

    except Exception as e:
        logger.error(f"❌ Erro ao processar emails: {e}")
    finally:
        try:
            mail.logout()
        except:
            pass


# =========================
# VALIDAÇÃO DE TELEFONES
# =========================
DDD_UF = {"AC": "68", "AL": "82", "AP": "96", "AM": "92", "BA": "71", "CE": "85", "DF": "61", "ES": "27", "GO": "62",
          "MA": "98", "MT": "65", "MS": "67", "MG": "31", "PA": "91", "PB": "83", "PR": "41", "PE": "81", "PI": "86",
          "RJ": "21", "RN": "84", "RS": "51", "RO": "69", "RR": "95", "SC": "48", "SP": "11", "SE": "79", "TO": "63"}


def validar_numeros_por_uf(lista_numeros, uf_padrao="DF"):
    ddd = DDD_UF.get(uf_padrao.upper(), "61")
    validos, invalidos = [], []
    for n in lista_numeros:
        n = (n or "").strip()
        digits = re.sub(r'\D', '', n)
        if len(digits) in (8, 9): digits = ddd + digits
        if not digits.startswith("55"): digits = "55" + digits
        if len(digits) in (12, 13):
            validos.append(f"+{digits}")
        else:
            invalidos.append(n)
    if invalidos:
        logger.warning(f"⚠️ Inválidos ({len(invalidos)}): {', '.join(invalidos)}")
        with open("telefones_invalidos.txt", "w", encoding="utf-8") as f: f.write("\n".join(invalidos))
    logger.info(f"✅ {len(validos)} números válidos (UF {uf_padrao}).")
    return list(dict.fromkeys(validos))


# =========================
# LOOP PRINCIPAL
# =========================
def main():
    logger.info("🚀 Iniciando WhatsApp_v5.1_master_logger_email")
    logger.info("📅 Recursos: Logs em Excel, controle de duplicação, finalização via email")

    # Carrega todos os dados já processados
    carregar_enviados_hoje()
    carregar_respostas_processadas()
    carregar_finalizados_hoje()
    carregar_hashes_processados()

    if LIMPEZA_SEMANAL_ATIVA and datetime.now().weekday() == DIA_LIMPEZA_SEMANAL and datetime.now().hour == HORA_LIMPEZA_SEMANAL:
        executar_limpeza_semanal()
        logger.info("🔄 Reiniciando após limpeza semanal...")
        time.sleep(5)
        os.execv(sys.executable, ['python'] + sys.argv)

    validos = validar_numeros_por_uf(NUMEROS, UF_PADRAO)
    if not validos:
        logger.error("Nenhum número válido para processar. Saindo.")
        return

    SESSOES_ATIVAS = [SESSOES[0]]
    LIMITE_POR_NUMERO = 175
    if len(validos) > LIMITE_POR_NUMERO: SESSOES_ATIVAS = SESSOES

    drivers = []
    for sessao in SESSOES_ATIVAS:
        driver = criar_driver(sessao)
        abrir_whatsapp(driver)
        drivers.append(driver)
        time.sleep(3)

    dia_atual = date.today()
    ultimo_evento = datetime.now()
    ultima_checagem_email = time.time()

    try:
        while True:
            if REINICIAR_DIARIAMENTE and date.today() != dia_atual:
                logger.info("🌅 Novo dia detectado — encerrando e reiniciando o bot.")
                for d in drivers: d.quit()
                rel = gerar_relatorio_diario()
                enviar_notificacao(rel)
                logger.info(f"📊 Relatório do dia {dia_atual} finalizado: {json.dumps(rel, indent=2)}")
                logger.info("🔄 Reiniciando script para o novo dia...")
                time.sleep(5)
                os.execv(sys.executable, ['python'] + sys.argv)

            if MODO_PLANTAO and not dentro_do_horario():
                for d in drivers: d.quit()
                aguardar_inicio_dia()
                drivers = []
                for sessao in SESSOES_ATIVAS:
                    driver = criar_driver(sessao)
                    abrir_whatsapp(driver)
                    drivers.append(driver)
                    time.sleep(3)
                continue

            # Processa emails periodicamente
            if FINALIZAR_COM_FORMULARIO and (time.time() - ultima_checagem_email > INTERVALO_CHECAGEM_EMAIL_SEG):
                processar_emails(drivers, SESSOES_ATIVAS)
                ultima_checagem_email = time.time()

            ativo = False
            for i, driver in enumerate(drivers):
                sessao = SESSOES_ATIVAS[i]
                numeros_restantes = [n for n in validos if
                                     n not in ler_dnc() and not ja_enviado(n) and not foi_finalizado(n)]
                logger.info(f"📋 Números restantes para processar: {len(numeros_restantes)}")

                if not numeros_restantes and not carregar_agenda():
                    if (datetime.now() - ultimo_evento).seconds > INATIVIDADE_AUTOEXIT_MIN * 60:
                        logger.info("🏁 Nenhum número ou reenvio pendente — encerrando automaticamente.")
                        for d in drivers: d.quit()
                        rel = gerar_relatorio_diario()
                        enviar_notificacao(rel)
                        logger.info(f"📊 Relatório final: {json.dumps(rel, indent=2)}")
                        return
                    else:
                        time.sleep(10)
                        continue

                for n in numeros_restantes[:sessao["limite"]]:
                    try:
                        if ja_enviado(n) or foi_finalizado(n):
                            logger.info(f"⏭️  Número {n} já foi processado ou finalizado. Pulando.")
                            continue
                        abrir_chat(driver, n)
                        msg = random.choice(MSG_INICIAL_VARIACOES)
                        enviar_texto(driver, msg)
                        enviados_hoje.add(n)
                        escrever_log(datetime.utcnow().isoformat(), n, "ENVIO_OK", "MSG_INICIAL", sessao["nome"])
                        salvar_print(driver, f"envio_{n[-4:]}")
                        agendar_reenvio(n, horas=12)
                        pause_humano()
                        ativo = True
                        ultimo_evento = datetime.now()
                    except Exception as e:
                        logger.error(f"Erro ao enviar para {n}: {e}")
                        salvar_print(driver, f"erro_envio_{n[-4:]}")
                        continue

                numeros_para_responder = [n for n in validos if
                                          ja_enviado(n) and n not in ler_dnc() and not foi_finalizado(n)]
                for n in numeros_para_responder:
                    if tratar_resposta(driver, n, sessao):
                        ativo = True
                        ultimo_evento = datetime.now()

                processar_agenda_reenvio(driver)
                time.sleep(5)

            if not ativo and (datetime.now() - ultimo_evento).seconds > INATIVIDADE_AUTOEXIT_MIN * 60:
                logger.info("🕒 Inatividade detectada — encerrando com segurança.")
                break

    except KeyboardInterrupt:
        logger.warning("🛑 Interrompido manualmente (Ctrl+C).")
    except Exception as e:
        logger.error(f"❌ Erro crítico: {e}")
    finally:
        for d in drivers:
            try:
                d.quit()
            except:
                pass
        rel = gerar_relatorio_diario()
        enviar_notificacao(rel)
        logger.info(f"📊 Relatório final: {json.dumps(rel, indent=2)}")
        logger.info("🏁 Bot finalizado.")


if __name__ == "__main__":
    main()