# ------------------------------------------------------------
# 📱 J&T Express – Bot de cadastro de motoristas (Black Friday)
# Versão: v5.6.6_defensive_full (NOV/2025)
# ------------------------------------------------------------
# ✅ Correções e recursos:
# - Login robusto (QR atualizado 2025) + prints de diagnóstico
# - Janela segura (abre sempre visível) + opção de minimizar após login
# - Fila persistente + DNC + agenda de reenvio (12h)
# - Tratamento automático de respostas (SIM / NAO / OUTRO)
# - Finalização via e-mail (IMAP / Gmail)
# - Logs CSV + planilha Excel diária
# - Reinício diário, plantão, anti-bloqueio e reciclagem de driver
# ------------------------------------------------------------

VERSION = "v5.6.6_defensive_full"

import os, csv, time, random, hashlib, re, sys, json, shutil
import imaplib, email
from datetime import datetime, date, timedelta
from collections import defaultdict
from pathlib import Path

# --- Selenium ---
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- Logging / libs opcionais ---
try:
    from loguru import logger
    logger.remove()
    logger.add(sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
        level="INFO")
    logger.add("logs/{time:YYYY-MM-DD}.log", rotation="1 day", level="INFO", enqueue=True, encoding="utf-8")
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    logger = logging.getLogger("default")
    print("⚠️ Instale loguru p/ logs coloridos: pip install loguru")

try:
    import pandas as pd
    from openpyxl import load_workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False

try:
    from filelock import FileLock
except ImportError:
    FileLock = None
    print("⚠️ Instale 'filelock': pip install filelock")

try:
    from tenacity import retry, stop_after_attempt, wait_random
except ImportError:
    retry = None
    print("⚠️ Instale 'tenacity': pip install tenacity")

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

try:
    import psutil
except ImportError:
    psutil = None

try:
    from rich.console import Console
    from rich.table import Table
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

# ------------------------------------------------------------
# CONFIGURAÇÕES PRINCIPAIS
# ------------------------------------------------------------
SESSOES = [
    {"nome": "Conta_1", "user_data_dir": "C:/WhatsAppSession_1", "limite": 175},
    {"nome": "Conta_2", "user_data_dir": "C:/WhatsAppSession_2", "limite": 175},
]

PLANILHA_NUMEROS = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Telefones\Telefones.xlsx"
UF_PADRAO = "DF"

DEFAULT_WINDOW_POSITION = (0, 0)    # abre sempre visível no monitor principal
DEFAULT_WINDOW_SIZE = (1600, 900)

PASTA_PRINTS = "prints_motoristas"
LOG_CSV = "log_motoristas.csv"
EXCEL_FILE = "Base_Master_Motoristas.xlsx"
DNC_FILE = "nao_enviar_mais.csv"
AGENDA_REENVIO = "agenda_reenvio.csv"
STATE_FILE = "state.json"
STATE_LOCK = "state.json.lock"
HASH_FILE = "processed_hashes.json"

for _dir in ("logs", PASTA_PRINTS):
    os.makedirs(_dir, exist_ok=True)

# Janelas / horários
HEADLESS = False
MINIMIZAR_APOS_LOGIN = False
REINICIAR_DIARIAMENTE = True
MODO_PLANTAO = True                      # fora do horário, pausa até 08:00 do próximo dia útil
HORA_INICIO, HORA_FIM = 8, 18
INATIVIDADE_AUTOEXIT_MIN = 120

# Planilha / Relatórios
SALVAR_EM_EXCEL = True
GERAR_RELATORIO_DIARIO = True

# Reenvio e limites
RENVIO_ATIVO = True
INTERVALO_CHECAGEM_EMAIL_SEG = 300
LIMITE_RECICLAGEM_DRIVER = 150

# E-mail (Gmail/IMAP) — preencha para finalizar automaticamente
EMAIL_IMAP_SERVER = "imap.gmail.com"
EMAIL_IMAP_PORT = 993
EMAIL_ADDRESS = ""        # preencha
EMAIL_PASSWORD = ""       # preencha
EMAIL_FOLDER = "INBOX"
EMAIL_SUBJECT_FILTER = "Google Forms"

# Mensagens
MSG_INICIAL_VARIACOES = [
    "Olá! Somos a J&T Express 🚚. Está disponível para novas coletas?",
    "Bom dia! 👋 Aqui é da J&T Express. Estamos cadastrando motoristas parceiros.",
    "Oi, tudo bem? 😊 A J&T Express está com bônus especiais na Black Friday!",
    "Fala parceiro! 🚛 Temos novas rotas disponíveis. Quer saber mais?",
    "Olá! 📦 A J&T Express está com oportunidades de entrega nesta semana."
]
MSG_INTERESSADO = "Perfeito! 🙌 Nosso time entrará em breve. Por favor, preencha o formulário abaixo para acelerar seu cadastro:"
MSG_NAO_INTERESSADO = "Sem problema 👍 Agradecemos o seu tempo. Boa sorte e sucesso na estrada!"
MSG_PEDIR_CONF = "Por favor, responda apenas com 'sim' ou 'não' 👍"
MSG_REENVIO_12H = "Oi! 👋 Relembrando a campanha Black Friday: bônus de até R$ 1,30 + R$ 0,50 extra por pacote. Responda 'Sim' para saber mais!"
MSG_FORA_HORARIO_NOITE = "Olá! Nosso horário é das 8h às 18h. Amanhã nossa equipe entrará em contato. 😊"
MSG_FORA_HORARIO_FDS = "Olá! Nosso horário é de segunda a sexta, das 8h às 18h. Segunda retornaremos o contato. 🚛"
LINK_FORM = "https://forms.gle/qckjgW3GkRiJ8uU56"

# Humanização
INTERVALO_CURTO = (35, 70)
INTERVALO_MEDIO = (90, 180)
INTERVALO_LONGO = (300, 600)
MICROPAUSA_TECLA = (0.05, 0.15)

# UFs / DDD
DDD_UF = {"AC": "68","AL":"82","AP":"96","AM":"92","BA":"71","CE":"85","DF":"61","ES":"27","GO":"62","MA":"98",
          "MT":"65","MS":"67","MG":"31","PA":"91","PB":"83","PR":"41","PE":"81","PI":"86","RJ":"21","RN":"84",
          "RS":"51","RO":"69","RR":"95","SC":"48","SP":"11","SE":"79","TO":"63"}
UFS_PRIORITARIAS = ["SP", "RJ", "MG", "DF"]
# ------------------------------------------------------------
# PRINTS E LOGS
# ------------------------------------------------------------
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
            with pd.ExcelWriter(EXCEL_FILE, engine="openpyxl") as writer:
                pd.DataFrame(columns=["timestamp", "numero", "etapa", "detalhe", "sessao"]).to_excel(
                    writer, sheet_name=hoje, index=False)
        book = load_workbook(EXCEL_FILE)
        if hoje not in book.sheetnames:
            book.create_sheet(hoje)
            sheet = book[hoje]
            sheet.append(["timestamp", "numero", "etapa", "detalhe", "sessao"])
            for cell in sheet[1]:
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                cell.alignment = Alignment(horizontal="center")
        sheet = book[hoje]
        sheet.append([ts, numero, etapa, detalhe, sessao])
        book.save(EXCEL_FILE)
    except Exception as e:
        logger.error(f"Erro ao escrever Excel: {e}")

def gerar_relatorio_diario():
    if not GERAR_RELATORIO_DIARIO or not os.path.exists(LOG_CSV):
        return {}
    hoje = date.today().isoformat()
    stats = {"envios": 0, "respostas": 0, "interessados": 0, "nao_interessados": 0, "reenvios": 0, "formularios": 0}
    with open(LOG_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if not row["timestamp"].startswith(hoje):
                continue
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
        json.dump(stats, f, indent=2, ensure_ascii=False)
    return stats

def dentro_do_horario():
    now = datetime.now()
    return now.weekday() < 5 and HORA_INICIO <= now.hour < HORA_FIM

def fora_horario_mensagem():
    now = datetime.now()
    return MSG_FORA_HORARIO_NOITE if now.weekday() < 5 else MSG_FORA_HORARIO_FDS

def aguardar_inicio_dia():
    now = datetime.now()
    proximo_inicio = datetime.combine(now.date(), datetime.min.time()) + timedelta(hours=HORA_INICIO)
    if now.hour >= HORA_FIM or now.weekday() >= 5:
        # se passou do fim ou é fim de semana, pula para próximo dia útil às 08h
        dias = 1
        while (now.weekday() + dias) % 7 >= 5:
            dias += 1
        proximo_inicio = datetime.combine((now + timedelta(days=dias)).date(), datetime.min.time()) + timedelta(hours=HORA_INICIO)
    espera = max(0, (proximo_inicio - now).total_seconds())
    logger.info(f"⏰ Plantão: aguardando {int(espera//3600)}h{int((espera%3600)//60):02d}m…")
    time.sleep(espera)
# ------------------------------------------------------------
# FILA PERSISTENTE + DNC + AGENDA_REENVIO
# ------------------------------------------------------------
class QueueManager:
    def __init__(self):
        self.state = {}
        self.dnc = set()
        self.enviados_hoje = set()
        self.lock = FileLock(STATE_LOCK, timeout=5) if FileLock else None
        self._load_state()
        self._load_dnc()
        self._load_enviados_hoje()

    # --- STATE ---
    def _load_state(self):
        try:
            if self.lock: self.lock.acquire()
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    self.state = json.load(f)
            else:
                self.state = {}
        except Exception as e:
            logger.error(f"Erro ao carregar estado: {e}")
            self.state = {}
        finally:
            if self.lock and self.lock.is_locked:
                self.lock.release()

    def _save_state(self):
        try:
            if self.lock: self.lock.acquire()
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Erro ao salvar estado: {e}")
        finally:
            if self.lock and self.lock.is_locked:
                self.lock.release()

    # --- DNC ---
    def _load_dnc(self):
        try:
            if os.path.exists(DNC_FILE):
                with open(DNC_FILE, encoding="utf-8") as f:
                    self.dnc = set(line.strip() for line in f if line.strip())
        except Exception as e:
            logger.error(f"Erro ao carregar DNC: {e}")
            self.dnc = set()

    def add_to_dnc(self, numero):
        if numero not in self.dnc:
            with open(DNC_FILE, "a", encoding="utf-8") as f:
                f.write(numero + "\n")
            self.dnc.add(numero)

    # --- ENVIADOS HOJE ---
    def _load_enviados_hoje(self):
        try:
            hoje = date.today().isoformat()
            if os.path.exists(LOG_CSV):
                with open(LOG_CSV, encoding="utf-8") as f:
                    for r in csv.DictReader(f):
                        if r.get("timestamp", "").startswith(hoje) and "ENVIO_OK" in r.get("etapa", ""):
                            self.enviados_hoje.add(r.get("numero"))
        except Exception as e:
            logger.error(f"Erro ao carregar enviados hoje: {e}")
            self.enviados_hoje = set()

    # --- NORMALIZAÇÃO ---
    def normalize_number(self, raw, uf_default="DF"):
        if not raw:
            return None
        digits = re.sub(r'\D', '', str(raw))
        if len(digits) < 8:
            return None
        ddd = DDD_UF.get((uf_default or UF_PADRAO).upper(), DDD_UF.get(UF_PADRAO, "61"))
        if len(digits) in (8, 9):
            digits = ddd + digits
        if len(digits) in (10, 11):
            digits = "55" + digits
        if not digits.startswith("55"):
            digits = "55" + digits
        if len(digits) not in (12, 13):
            return None
        return "+" + digits

    # --- IMPORT PLANILHA ---
    def load_numbers_from_excel(self, path=PLANILHA_NUMEROS):
        if not os.path.exists(path):
            logger.error(f"Planilha não encontrada: {path}")
            return []
        try:
            if POLARS_AVAILABLE:
                try:
                    df_pl = pl.read_excel(path)  # pode não existir dependendo da versão
                    df = df_pl.to_pandas()
                except Exception:
                    df = pd.read_excel(path, dtype=str)
            else:
                df = pd.read_excel(path, dtype=str)
        except Exception as e:
            logger.error(f"Erro lendo a planilha: {e}")
            return []

        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        col_num = next((c for c in df.columns if any(x in c for x in ["número","numero","telefone","contato","celular"])), None)
        col_uf = "uf" if "uf" in df.columns else next((c for c in df.columns if any(x in c for x in ["estado","região","regiao"])), None)

        logger.info(f"📞 Colunas detectadas: col_num={col_num} | col_uf={col_uf}")
        rows, seen, out = [], set(), []
        for _, r in df.iterrows():
            raw = r.get(col_num) if col_num else None
            uf = (r.get(col_uf) if col_uf else None) or UF_PADRAO
            num = self.normalize_number(raw, uf_default=uf)
            if num:
                rows.append({"nome": r.get("nome", ""), "numero": num, "uf": uf})
        for x in rows:
            if x["numero"] not in seen:
                seen.add(x["numero"]); out.append(x)
        logger.info(f"📞 {len(out)} contatos carregados da planilha.")
        return out

    # --- STATE INIT ---
    def init_state_from_sheet(self):
        rows = self.load_numbers_from_excel()
        changed = False
        for r in rows:
            num = r["numero"]
            if num not in self.state:
                self.state[num] = {"status": "pending", "attempts": 0, "last_attempt": None,
                                   "session": None, "nome": r.get("nome",""), "uf": r.get("uf","")}
                changed = True
        if changed: self._save_state()
        return self.state

    # --- ESCOLHA DO PRÓXIMO NÚMERO ---
    def _reclaim_stuck_claims(self, timeout_minutes=60):
        now = datetime.utcnow()
        changed = False
        for num, meta in self.state.items():
            if meta.get("status") == "in_progress" and meta.get("last_attempt"):
                try:
                    last = datetime.fromisoformat(meta["last_attempt"])
                    if (now - last).total_seconds() > timeout_minutes*60:
                        meta["status"] = "pending"; meta["session"] = None; changed = True
                except: pass
        if changed: self._save_state()

    def get_next_number_prioritario(self, session_name, max_attempts=3):
        for uf in UFS_PRIORITARIAS:
            for num, meta in self.state.items():
                if num in self.enviados_hoje or num in self.dnc: continue
                if meta.get("uf")==uf and meta.get("status") in ("pending","failed") and meta.get("attempts",0)<max_attempts:
                    meta["status"]="in_progress"; meta["session"]=session_name; meta["last_attempt"]=datetime.utcnow().isoformat()
                    self._save_state()
                    return num, meta
        return None, None

    def get_next_number_for_session(self, session_name, max_attempts=3):
        self._load_state(); self._load_dnc(); self._load_enviados_hoje(); self._reclaim_stuck_claims()
        num, meta = self.get_next_number_prioritario(session_name, max_attempts)
        if num: return num, meta
        for num, meta in self.state.items():
            if num in self.enviados_hoje or num in self.dnc: continue
            if meta.get("status") in ("pending","failed") and meta.get("attempts",0)<max_attempts:
                meta["status"]="in_progress"; meta["session"]=session_name; meta["last_attempt"]=datetime.utcnow().isoformat()
                self._save_state(); return num, meta
        return None, None

    def mark_sent(self, numero, session_name):
        self._load_state()
        meta = self.state.setdefault(numero, {})
        meta.update({"status":"sent", "attempts":meta.get("attempts",0)+1,
                     "last_attempt":datetime.utcnow().isoformat(), "session":session_name})
        self._save_state()

    def mark_failed(self, numero, session_name, reason="error"):
        self._load_state()
        meta = self.state.setdefault(numero, {})
        meta.update({"status":"failed", "attempts":meta.get("attempts",0)+1,
                     "last_attempt":datetime.utcnow().isoformat(), "session":session_name, "reason":reason})
        self._save_state()

    def mark_finalized(self, numero):
        self._load_state()
        meta = self.state.setdefault(numero, {})
        meta.update({"status":"finalized", "last_attempt":datetime.utcnow().isoformat()})
        self._save_state()

queue_manager = QueueManager()

# --- Agenda de reenvio (CSV) ---
def agendar_reenvio(numero, horas=12):
    when = (datetime.utcnow() + timedelta(hours=horas)).isoformat()
    novo = not os.path.exists(AGENDA_REENVIO)
    with open(AGENDA_REENVIO, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if novo: w.writerow(["numero","iso_schedule"])
        w.writerow([numero, when])
    logger.info(f"🕐 Reenvio agendado p/ {numero} em {horas}h")

def carregar_agenda():
    if not os.path.exists(AGENDA_REENVIO): return []
    with open(AGENDA_REENVIO, encoding="utf-8") as f:
        return list(csv.DictReader(f))

def salvar_agenda(itens):
    with open(AGENDA_REENVIO, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["numero","iso_schedule"])
        for it in itens: w.writerow([it["numero"], it["iso_schedule"]])
# ------------------------------------------------------------
# HASHES (anti-duplicação de eventos)
# ------------------------------------------------------------
processed_hashes = set()
hash_meta = {}

def carregar_hashes_processados():
    global processed_hashes, hash_meta
    if os.path.exists(HASH_FILE):
        try:
            data = json.load(open(HASH_FILE,"r",encoding="utf-8"))
            processed_hashes = set(data.get("hashes",[]))
            hash_meta = data.get("meta",{})
            logger.info(f"🔑 Carregados {len(processed_hashes)} hashes.")
        except Exception as e:
            logger.error(f"Erro ao carregar hashes: {e}")

def salvar_hashes_processados():
    try:
        json.dump({"hashes": list(processed_hashes), "meta": hash_meta}, open(HASH_FILE,"w",encoding="utf-8"), indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Erro ao salvar hashes: {e}")

def gerar_hash_local(numero, etapa, timestamp=None):
    if timestamp is None:
        timestamp = datetime.utcnow().isoformat()
    try:
        return hashlib.sha256(f"{numero}_{etapa}_{timestamp}".encode()).hexdigest()
    except Exception as e:
        logger.error(f"Erro ao gerar hash: {e}")
        return None

def registrar_hash_evento(numero, etapa):
    h = gerar_hash_local(numero, etapa)
    if not h or h in processed_hashes:
        return False
    processed_hashes.add(h); hash_meta[h] = {"numero":numero,"etapa":etapa}
    salvar_hashes_processados()
    return True

def ja_registrado(numero, etapa):
    for h, meta in hash_meta.items():
        if meta.get("numero")==numero and meta.get("etapa")==etapa:
            return True
    return False

# ------------------------------------------------------------
# Humanização / escolhas
# ------------------------------------------------------------
def escolher_pausa():
    hora = datetime.now().hour
    if 7 <= hora < 10:   return random.uniform(*INTERVALO_CURTO)
    if 10 <= hora < 18:  return random.uniform(*INTERVALO_MEDIO)
    return random.uniform(*INTERVALO_LONGO)

def escolher_mensagem():
    return random.choice(MSG_INICIAL_VARIACOES)

def digitar_humano(campo, texto):
    texto = ''.join(ch for ch in texto if ord(ch) <= 0xFFFF)  # limpa BMP
    base = random.uniform(*MICROPAUSA_TECLA)
    for ch in texto:
        campo.send_keys(ch)
        time.sleep(base + random.uniform(0, 0.12))
    if random.random() < 0.2:
        time.sleep(random.uniform(0.5, 1.5))

def simular_acao_humana(driver):
    try:
        a = random.random()
        if a < 0.12: driver.execute_script("window.scrollBy(0, 300);")
        elif a < 0.24: driver.execute_script("window.scrollBy(0, -300);")
        elif a < 0.30: driver.refresh(); time.sleep(5)
        time.sleep(random.uniform(1, 3))
    except Exception as e:
        logger.debug(f"simulação humana falhou: {e}")
# ------------------------------------------------------------
# DRIVER / LOGIN ROBUSTO
# ------------------------------------------------------------
XPATH_INPUTS = [
    '//footer//div[@contenteditable="true" and @role="textbox"]',
    '//div[@contenteditable="true"][@data-tab="10"]',
    '//div[@contenteditable="true"][@data-tab="6"]',
    '//div[@role="textbox"]',
]

def criar_driver_discreto(sessao, window_position=DEFAULT_WINDOW_POSITION, window_size=DEFAULT_WINDOW_SIZE, headless=False):
    opts = Options()
    profile_dir = sessao.get("user_data_dir")
    os.makedirs(profile_dir, exist_ok=True)
    opts.add_argument(f"--user-data-dir={os.path.abspath(profile_dir)}")
    opts.add_argument(f"--window-position={window_position[0]},{window_position[1]}")
    opts.add_argument(f"--window-size={window_size[0]},{window_size[1]}")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    if headless:
        opts.add_argument("--headless=new")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    return driver

def minimizar_janela(driver):
    try:
        driver.set_window_position(-10000, 0)
        logger.info("🔽 Janela minimizada.")
    except Exception:
        pass

def _esperar_input(driver, timeout=30):
    last_err = None
    for xp in XPATH_INPUTS:
        try:
            return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, xp)))
        except Exception as e:
            last_err = e
    raise last_err

def abrir_whatsapp(driver):
    logger.info("🌐 Acessando o WhatsApp Web…")
    driver.get("https://web.whatsapp.com/")

    try:
        WebDriverWait(driver, 30).until(lambda d: d.current_url.startswith("https://web.whatsapp.com"))
    except Exception as e:
        logger.error(f"❌ Falha ao carregar WhatsApp Web: {e}")
        salvar_print(driver, "erro_carregar_whatsapp")
        return False

    time.sleep(3)
    logger.info("🔍 Verificando estado de login…")

    # Já logado?
    for xp in ("//div[@role='grid']", "//div[@id='pane-side']", "//footer//div[@contenteditable='true' and @role='textbox']"):
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, xp)))
            logger.info("✅ Login detectado (já logado).")
            if MINIMIZAR_APOS_LOGIN: minimizar_janela(driver)
            return True
        except Exception:
            continue

    # Procurar QR
    for xp in ("//canvas[@aria-label='Scan me!']", "//div[@data-testid='qrcode']", "//img[contains(@src,'data:image/png')]"):
        try:
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, xp)))
            logger.info("📱 QR Code detectado — aguarde leitura (até 2 min)…")
            salvar_print(driver, "qrcode_detectado")
            WebDriverWait(driver, 120).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='grid'] | //div[@id='pane-side']"))
            )
            logger.info("✅ Login realizado com sucesso.")
            if MINIMIZAR_APOS_LOGIN: minimizar_janela(driver)
            return True
        except Exception:
            continue

    logger.error("❌ CRÍTICO: Não foi possível identificar o estado de login.")
    salvar_print(driver, "erro_login_whatsapp")
    return False

def abrir_chat(driver, numero):
    driver.get(f"https://web.whatsapp.com/send?phone={numero}")
    _esperar_input(driver, timeout=40)

# envio com retry (se tenacity disponível)
if retry:
    @retry(stop=stop_after_attempt(3), wait=wait_random(min=3, max=7))
    def enviar_texto_seguro(driver, texto):
        campo = _esperar_input(driver, timeout=30)
        campo.click(); time.sleep(0.3)
        try: campo.clear()
        except Exception: pass
        digitar_humano(campo, texto)
        time.sleep(random.uniform(0.4, 1.0))
        campo.send_keys(Keys.ENTER)
        time.sleep(0.8)
        logger.info("✅ Mensagem enviada.")
else:
    def enviar_texto_seguro(driver, texto):
        for _ in range(3):
            try:
                campo = _esperar_input(driver, timeout=30)
                campo.click(); time.sleep(0.3)
                try: campo.clear()
                except Exception: pass
                digitar_humano(campo, texto)
                time.sleep(random.uniform(0.4, 1.0))
                campo.send_keys(Keys.ENTER)
                time.sleep(0.8)
                logger.info("✅ Mensagem enviada.")
                return
            except Exception as e:
                logger.warning(f"Tentativa de envio falhou: {e}")
                time.sleep(random.uniform(3,7))
        raise RuntimeError("Falha ao enviar mensagem após 3 tentativas")
# ------------------------------------------------------------
# CLASSIFICAÇÃO E RESPOSTAS
# ------------------------------------------------------------
XPATH_MSG_IN = '//div[contains(@class,"message-in")]//span[@dir="ltr"]'

def classificar_resposta(txt):
    t = (txt or "").lower().strip()
    sim = {"sim","tenho interesse","quero","interessado","ok"}
    nao = {"nao","não","não quero","nao quero","pare","sem interesse","n"}
    if any(p == t or p in t for p in sim): return "SIM"
    if any(p == t or p in t for p in nao): return "NAO"
    return "OUTRO"

def get_msgs(driver):
    try:
        return driver.find_elements(By.XPATH, XPATH_MSG_IN)
    except Exception:
        return []

def tratar_resposta(driver, numero, sessao):
    try:
        abrir_chat(driver, numero)
        msgs = get_msgs(driver)
        if not msgs: return False
        texto = (msgs[-1].text or "").strip()
        if not texto: return False
        tipo = classificar_resposta(texto)
        logger.info(f"📩 RESPOSTA {numero}: '{texto}' ({tipo})")
        ts = datetime.utcnow().isoformat()

        if dentro_do_horario():
            if tipo == "SIM":
                enviar_texto_seguro(driver, f"{MSG_INTERESSADO}\n\n📝 Formulário de cadastro: {LINK_FORM}")
                salvar_print(driver, f"sim_{numero[-4:]}")
                if not ja_registrado(numero, "RESPONDEU_SIM"):
                    escrever_log(ts, numero, "RESPONDEU_SIM", "Link do formulário enviado", sessao["nome"])
                    registrar_hash_evento(numero, "RESPONDEU_SIM")
                time.sleep(random.uniform(15,35))
                return True

            elif tipo == "NAO":
                enviar_texto_seguro(driver, MSG_NAO_INTERESSADO)
                queue_manager.add_to_dnc(numero)
                salvar_print(driver, f"nao_{numero[-4:]}")
                if not ja_registrado(numero, "RESPONDEU_NAO"):
                    escrever_log(ts, numero, "RESPONDEU_NAO", "", sessao["nome"])
                    registrar_hash_evento(numero, "RESPONDEU_NAO")
                return True

            else:
                enviar_texto_seguro(driver, MSG_PEDIR_CONF)
                salvar_print(driver, f"confirma_{numero[-4:]}")
                if not ja_registrado(numero, "PEDIU_CONFIRMACAO"):
                    escrever_log(ts, numero, "PEDIU_CONFIRMACAO", "", sessao["nome"])
                    registrar_hash_evento(numero, "PEDIU_CONFIRMACAO")
                return True

        else:
            msg = fora_horario_mensagem()
            enviar_texto_seguro(driver, msg)
            salvar_print(driver, f"fora_horario_{numero[-4:]}")
            if not ja_registrado(numero, "AUTO_REPLY_FORA_HORARIO"):
                escrever_log(ts, numero, "AUTO_REPLY_FORA_HORARIO", "", sessao["nome"])
                registrar_hash_evento(numero, "AUTO_REPLY_FORA_HORARIO")
            return True

    except Exception as e:
        logger.error(f"Erro ao tratar resposta de {numero}: {e}")
        salvar_print(driver, f"erro_resposta_{numero[-4:]}")
        return False

def finalizar_contato(driver, numero, sessao):
    try:
        abrir_chat(driver, numero)
        enviar_texto_seguro(driver, "🎉 Ótimo! Recebemos seu cadastro. Em breve, nossa equipe entrará em contato com os próximos passos. Muito obrigado!")
        salvar_print(driver, f"finalizado_{numero[-4:]}")
        ts = datetime.utcnow().isoformat()
        if not ja_registrado(numero, "FORMULARIO_PREENCHIDO"):
            escrever_log(ts, numero, "FORMULARIO_PREENCHIDO", "Cadastro recebido e finalizado", sessao["nome"])
            registrar_hash_evento(numero, "FORMULARIO_PREENCHIDO")
        queue_manager.mark_finalized(numero)
        logger.info(f"✅ {numero} finalizado.")
        return True
    except Exception as e:
        logger.error(f"Erro ao finalizar {numero}: {e}")
        return False

# ------------------------------------------------------------
# EMAIL (finalização via formulário)
# ------------------------------------------------------------
def conectar_email():
    if not EMAIL_ADDRESS or not EMAIL_PASSWORD:
        return None
    try:
        mail = imaplib.IMAP4_SSL(EMAIL_IMAP_SERVER, EMAIL_IMAP_PORT)
        mail.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        return mail
    except Exception as e:
        logger.error(f"❌ Erro ao conectar IMAP: {e}")
        return None

def _decode_payload(msg):
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type()=="text/plain":
                try:
                    return part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8","ignore")
                except Exception: pass
        for part in msg.walk():
            if part.get_content_type().startswith("text/"):
                try:
                    return part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8","ignore")
                except Exception: pass
        return ""
    else:
        try:
            return msg.get_payload(decode=True).decode(msg.get_content_charset() or "utf-8","ignore")
        except Exception:
            return ""

def extrair_numero_telefone(corpo_email):
    padroes = [r'\(\d{2}\s*\d{4,5}-\d{4}\)', r'\d{2}\s*\d{4,5}-\d{4}', r'\b\d{10,11}\b']
    for p in padroes:
        m = re.search(p, corpo_email)
        if m:
            numero = re.sub(r'\D','', m.group())
            if not numero.startswith("55"): numero = "55"+numero
            return "+"+numero
    return None

def processar_emails(drivers, sessoes):
    mail = conectar_email()
    if not mail: return
    try:
        mail.select(EMAIL_FOLDER)
        status, messages = mail.search(None, f'(UNSEEN SUBJECT "{EMAIL_SUBJECT_FILTER}")')
        if status!="OK" or not messages or not messages[0]:
            logger.info("ℹ️ Nenhum novo email de notificação.")
            return
        email_ids = messages[0].split()
        logger.info(f"📧 Encontrados {len(email_ids)} emails de notificação.")
        for email_id in email_ids:
            status, msg_data = mail.fetch(email_id, "(RFC822)")
            if status!="OK": continue
            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)
            corpo = _decode_payload(msg)
            numero = extrair_numero_telefone(corpo)
            if not numero:
                logger.warning("⚠️ Não foi possível extrair número do email.")
                continue
            logger.info(f"🔔 Formulário detectado para {numero}. Finalizando contato…")
            for driver in drivers:
                if finalizar_contato(driver, numero, sessoes[0]):
                    break
            try: mail.store(email_id, '+FLAGS', '\\Seen')
            except Exception: pass
    except Exception as e:
        logger.error(f"❌ Erro ao processar emails: {e}")
    finally:
        try: mail.logout()
        except: pass
# ------------------------------------------------------------
# TABELA INICIAL (opcional) + SAÚDE DO CHROME
# ------------------------------------------------------------
def exibir_tabela_inicial():
    if not RICH_AVAILABLE: return
    console = Console()
    table = Table(title=f"🚀 WhatsApp {VERSION} - Sessões Ativas", show_header=True, header_style="bold magenta")
    table.add_column("Sessão", style="cyan", no_wrap=True)
    table.add_column("Perfil", style="magenta")
    table.add_column("Limite", justify="right")
    for s in SESSOES:
        table.add_row(s['nome'], s['user_data_dir'], str(s['limite']))
    console.print(table)

def chrome_ativo():
    if not psutil: return True
    for proc in psutil.process_iter(['name']):
        try:
            if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                return True
        except Exception:
            continue
    return False

# ------------------------------------------------------------
# LOOP PRINCIPAL
# ------------------------------------------------------------
def main():
    exibir_tabela_inicial()
    logger.info(f"🚀 Iniciando WhatsApp {VERSION}")
    logger.info(f"🖥️ Janela: pos {DEFAULT_WINDOW_POSITION}, tam {DEFAULT_WINDOW_SIZE}")

    queue_manager.init_state_from_sheet()
    carregar_hashes_processados()

    # Seleciona número de sessões ativas baseado no volume
    SESSOES_ATIVAS = [SESSOES[0]]
    if len(queue_manager.state) > SESSOES[0]["limite"]:
        SESSOES_ATIVAS = SESSOES

    # Inicia drivers e loga
    drivers = []
    for sessao in SESSOES_ATIVAS:
        driver = criar_driver_discreto(sessao, headless=HEADLESS)
        if not abrir_whatsapp(driver):
            logger.error("🛑 Falha crítica ao inicializar a sessão. Veja os prints.")
            try: driver.quit()
            except: pass
            return
        drivers.append(driver)
        time.sleep(1.5)

    logger.info("✅ Sessões ativas. Entrando no ciclo principal.")
    dia_atual = date.today()
    ultima_checagem_email = time.time()
    contador_envios = 0
    ultimo_evento = datetime.now()

    try:
        while True:
            # Reinício diário
            if REINICIAR_DIARIAMENTE and date.today() != dia_atual:
                for d in drivers:
                    try: d.quit()
                    except: pass
                rel = gerar_relatorio_diario()
                logger.info(f"📊 Relatório {dia_atual}: {json.dumps(rel, indent=2, ensure_ascii=False)}")
                os.execv(sys.executable, ['python'] + sys.argv)

            # Plantão fora do horário
            if MODO_PLANTAO and not dentro_do_horario():
                for d in drivers:
                    try: d.quit()
                    except: pass
                aguardar_inicio_dia()
                drivers = []
                for sessao in SESSOES_ATIVAS:
                    driver = criar_driver_discreto(sessao, headless=HEADLESS)
                    if not abrir_whatsapp(driver):
                        logger.error("🛑 Erro ao reiniciar após plantão. Encerrando.")
                        return
                    drivers.append(driver)
                    time.sleep(1.2)
                dia_atual = date.today()

            # Processa email a cada X segundos
            if time.time() - ultima_checagem_email > INTERVALO_CHECAGEM_EMAIL_SEG:
                processar_emails(drivers, SESSOES_ATIVAS)
                ultima_checagem_email = time.time()

            ativo = False
            for i, driver in enumerate(drivers):
                sessao = SESSOES_ATIVAS[i]

                # Saúde do Chrome
                if not chrome_ativo():
                    logger.warning("🚨 Chrome não encontrado. Reiniciando driver…")
                    try: driver.quit()
                    except: pass
                    driver = criar_driver_discreto(sessao, headless=HEADLESS)
                    if not abrir_whatsapp(driver):
                        logger.error("🛑 Falha ao reiniciar Chrome. Encerrando.")
                        return
                    drivers[i] = driver
                    continue

                # Sessão caiu?
                # (checagem leve: tentar localizar a caixa de input)
                try:
                    _esperar_input(driver, timeout=8)
                except Exception:
                    logger.warning("⚠️ Sessão possivelmente desconectada. Reiniciando driver…")
                    try: driver.quit()
                    except: pass
                    driver = criar_driver_discreto(sessao, headless=HEADLESS)
                    if not abrir_whatsapp(driver):
                        logger.error("🛑 Falha ao reabrir WhatsApp Web.")
                        return
                    drivers[i] = driver
                    continue

                # Próximo número da fila
                numero, meta = queue_manager.get_next_number_for_session(sessao["nome"])
                if not numero:
                    logger.info(f"🔎 Sem números para {sessao['nome']} no momento.")
                    time.sleep(3)
                    continue

                try:
                    abrir_chat(driver, numero)
                    mensagem = escolher_mensagem()
                    enviar_texto_seguro(driver, mensagem)
                    ts = datetime.utcnow().isoformat()
                    if not ja_registrado(numero, "ENVIO_OK"):
                        escrever_log(ts, numero, "ENVIO_OK", mensagem, sessao["nome"])
                        registrar_hash_evento(numero, "ENVIO_OK")
                    queue_manager.mark_sent(numero, sessao["nome"])
                    salvar_print(driver, f"envio_{numero[-4:]}")
                    agendar_reenvio(numero, horas=12)
                    simular_acao_humana(driver)
                    contador_envios += 1
                    ativo = True
                    ultimo_evento = datetime.now()

                    pausa = escolher_pausa()
                    logger.info(f"⏳ Aguardando {pausa:.1f}s…")
                    time.sleep(pausa)

                    # recicla driver periodicamente
                    if contador_envios > 0 and contador_envios % LIMITE_RECICLAGEM_DRIVER == 0:
                        logger.info("🔁 Reciclando driver preventivo…")
                        try: driver.quit()
                        except: pass
                        driver = criar_driver_discreto(sessao, headless=HEADLESS)
                        if not abrir_whatsapp(driver):
                            logger.error("🛑 Falha na reciclagem automática.")
                            return
                        drivers[i] = driver

                    # Responder quem já foi enviado
                    numeros_para_responder = [n for n, m in queue_manager.state.items() if m.get("status")=="sent"]
                    for n in numeros_para_responder:
                        if tratar_resposta(driver, n, sessao):
                            ativo = True
                            ultimo_evento = datetime.now()

                    # Processa agenda (reenviar lembrete)
                    itens = carregar_agenda()
                    if itens:
                        now = datetime.utcnow()
                        due, keep = [], []
                        for it in itens:
                            try:
                                when = datetime.fromisoformat(it["iso_schedule"])
                                (due if when <= now else keep).append(it)
                            except Exception:
                                keep.append(it)
                        for it in due:
                            try:
                                abrir_chat(driver, it["numero"])
                                enviar_texto_seguro(driver, MSG_REENVIO_12H)
                                ts = datetime.utcnow().isoformat()
                                if not ja_registrado(it["numero"], "REENVIO_12H"):
                                    escrever_log(ts, it["numero"], "REENVIO_12H", "Lembrete enviado", sessao["nome"])
                                    registrar_hash_evento(it["numero"], "REENVIO_12H")
                                salvar_print(driver, f"reenviou12h_{it['numero'][-4:]}")
                                time.sleep(random.uniform(5,10))
                            except Exception as e:
                                logger.error(f"Erro no reenvio 12h para {it['numero']}: {e}")
                        salvar_agenda(keep)

                except Exception as e:
                    logger.error(f"❌ Erro com {numero}: {e}")
                    queue_manager.mark_failed(numero, sessao["nome"], reason=str(e))
                    salvar_print(driver, f"erro_envio_{numero[-4:]}")
                    continue

            # auto-exit se inativo por muito tempo
            if not ativo and (datetime.now() - ultimo_evento).total_seconds() > INATIVIDADE_AUTOEXIT_MIN*60:
                logger.info("🕒 Inatividade prolongada — encerrando com segurança.")
                break

    except KeyboardInterrupt:
        logger.warning("🛑 Interrompido manualmente (Ctrl+C).")
    except Exception as e:
        logger.error(f"❌ Erro crítico: {e}")
    finally:
        for d in drivers:
            try: d.quit()
            except: pass
        rel = gerar_relatorio_diario()
        logger.info(f"📊 Relatório final: {json.dumps(rel, indent=2, ensure_ascii=False)}")
        logger.info("🏁 Bot finalizado.")

if __name__ == "__main__":
    main()
