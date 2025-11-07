# bot_motoristas_v2.2.py
# ------------------------------------------------------------
# Bot de cadastro de motoristas (Selenium + WhatsApp Web)
# - Envia mensagem inicial
# - Aguarda resposta até 60s e conversa com base em "sim/não"
# - Se a resposta for diferente, pede confirmação
# - Respeita 250 envios/dia e 60–120s entre contatos
# - Não recontata quem recusou (DNC)
# - Logs + prints + hash por envio
# ------------------------------------------------------------

import os, csv, time, random, hashlib, logging
from datetime import datetime, date
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from screeninfo import get_monitors

# =========================
# CONFIGURAÇÕES
# =========================
NUMEROS = ["+5561994089300"]   # teste com 1 número
LIMITE_DIARIO = 250
INTERVALO_MIN, INTERVALO_MAX = 60, 120
REPLY_TIMEOUT = 60

MSG_INICIAL = (
    "Olá! 🚛 Somos da J&T Express.\n"
    "Estamos cadastrando novos motoristas parceiros para transporte.\n"
    "Você tem interesse em carregar conosco? Responda apenas com 'sim' ou 'não'."
)
MSG_INTERESSADO = (
    "Perfeito! 🙌 Em breve enviaremos uma planilha para preencher as informações básicas "
    "(nome, CNH, cidade, veículo, etc.)."
)
MSG_NAO_INTERESSADO = (
    "Sem problema 👍. Agradecemos o seu tempo. Boa sorte e sucesso na estrada!"
)

PASTA_PRINTS = "prints_motoristas"
LOG_CSV, DNC_FILE = "log_motoristas.csv", "nao_enviar_mais.csv"
TEMPO_LOGIN = 20
USER_DATA_DIR = "C:/WhatsAppSession"
HEADLESS = False

os.makedirs(PASTA_PRINTS, exist_ok=True)

# =========================
# LOG COLORIDO
# =========================
class ColorFormatter(logging.Formatter):
    COLORS = {logging.INFO:"\033[92m",logging.WARNING:"\033[93m",logging.ERROR:"\033[91m"}
    def format(self, r):
        c=self.COLORS.get(r.levelno,""); reset="\033[0m"
        return f"{c}{super().format(r)}{reset}"
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger()
for h in logger.handlers: h.setFormatter(ColorFormatter("%(message)s"))

# =========================
# FUNÇÕES UTILITÁRIAS
# =========================
def limpar_texto_bmp(s): return ''.join(ch for ch in s if ord(ch)<=0xFFFF)
def salvar_print(d,tag):
    ts=datetime.now().strftime("%Y%m%d_%H%M%S")
    path=os.path.join(PASTA_PRINTS,f"{tag}_{ts}.png")
    d.save_screenshot(path); logger.info(f"📸 {path}"); return path
def gerar_hash_envio(n,t): return hashlib.sha256(f"{n}|{t}".encode()).hexdigest()
def escrever_log(ts,n,h,etapa,det=""):
    head=["timestamp","numero","hash","etapa","detalhe"]; novo=not os.path.exists(LOG_CSV)
    with open(LOG_CSV,"a",newline="",encoding="utf-8") as f:
        w=csv.writer(f)
        if novo: w.writerow(head)
        w.writerow([ts,n,h,etapa,det])
def ler_dnc():
    return set(open(DNC_FILE,encoding="utf-8").read().split()) if os.path.exists(DNC_FILE) else set()
def add_dnc(n):
    if n not in ler_dnc():
        with open(DNC_FILE,"a",encoding="utf-8") as f:f.write(n+"\n")
def contar_envios_hoje():
    if not os.path.exists(LOG_CSV):return 0
    hoje=date.today().isoformat(); total=0
    for r in csv.DictReader(open(LOG_CSV,encoding="utf-8")):
        if r["etapa"]=="ENVIO_INICIAL_OK" and r["timestamp"].startswith(hoje): total+=1
    return total
def pause_humano():
    t=random.uniform(INTERVALO_MIN,INTERVALO_MAX)
    logger.info(f"⏳ Aguardando {t:.0f}s..."); time.sleep(t)

# =========================
# SELENIUM DRIVER (corrigido)
# =========================
def criar_driver():
    opts=Options()
    opts.add_argument(f"--user-data-dir={USER_DATA_DIR}")
    opts.add_argument("--profile-directory=Default")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--remote-debugging-port=9222")
    opts.add_argument("--start-maximized")
    opts.add_experimental_option("excludeSwitches",["enable-logging","enable-automation"])
    if HEADLESS: opts.add_argument("--headless=new")
    driver=webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=opts)
    try:
        mons=get_monitors()
        if len(mons)>1 and not HEADLESS:
            seg=mons[1]; driver.set_window_position(seg.x,seg.y); driver.set_window_size(seg.width,seg.height)
            logger.info(f"🖥️ Janela 2ª tela ({seg.width}x{seg.height})")
    except Exception as e: logger.warning(f"Falha posição janela: {e}")
    return driver

def abrir_whatsapp(d):
    d.get("https://web.whatsapp.com/"); logger.info("🔄 Aguardando login..."); time.sleep(TEMPO_LOGIN)

# =========================
# HELPERS WHATSAPP
# =========================
XPATH_INPUT='//footer//div[@contenteditable="true" and @role="textbox"]'
XPATH_MSG_IN='//div[contains(@class,"message-in")]//span[@dir="ltr"]'
def abrir_chat(d,n): d.get(f"https://web.whatsapp.com/send?phone={n}"); WebDriverWait(d,40).until(EC.presence_of_element_located((By.XPATH,XPATH_INPUT)))
def enviar_texto(d,txt): c=d.find_element(By.XPATH,XPATH_INPUT); c.clear(); c.send_keys(limpar_texto_bmp(txt)); time.sleep(0.5); c.send_keys(Keys.ENTER)
def get_msgs(d): return d.find_elements(By.XPATH,XPATH_MSG_IN)
def get_ultima(d): m=get_msgs(d); return m[-1].text if m else None
def classificar(t):
    t=(t or "").lower()
    sim={"sim","tenho interesse","quero","quero sim","interessado","topo","ok"}
    nao={"nao","não","não quero","nao quero","pare","sem interesse","n"}
    if any(p in t for p in sim): return "SIM"
    if any(p in t for p in nao): return "NAO"
    return "OUTRO"

# =========================
# FLUXO DE CONTATO (v2.2)
# =========================
def processar(d,n):
    ts=datetime.utcnow().isoformat(); h=gerar_hash_envio(n,ts)
    if n in ler_dnc():
        logger.info(f"⏭️ {n} em DNC."); escrever_log(ts,n,h,"PULADO_DNC"); return
    try:
        logger.info(f"📲 Chat {n} (hash {h[:8]})"); abrir_chat(d,n)
        salvar_print(d,f"antes_{n[-4:]}"); enviar_texto(d,MSG_INICIAL); salvar_print(d,f"apos_{n[-4:]}")
        escrever_log(ts,n,h,"ENVIO_INICIAL_OK",MSG_INICIAL)
    except Exception as e:
        logger.error(f"Erro envio: {e}"); escrever_log(ts,n,h,"ENVIO_INICIAL_ERRO",str(e)); return

    logger.info(f"🕐 Esperando resposta até {REPLY_TIMEOUT}s ...")
    inicio=time.time(); ultimo_txt=None
    while time.time()-inicio<REPLY_TIMEOUT:
        try:
            msgs=get_msgs(d)
            if msgs:
                txt=msgs[-1].text.strip().lower()
                if txt!=ultimo_txt:  # nova mensagem
                    ultimo_txt=txt
                    logger.info(f"💬 {txt}")
                    tipo=classificar(txt)
                    if tipo=="SIM":
                        enviar_texto(d,MSG_INTERESSADO)
                        salvar_print(d,f"sim_{n[-4:]}"); escrever_log(ts,n,h,"RESPONDEU_SIM",txt)
                        return
                    elif tipo=="NAO":
                        enviar_texto(d,MSG_NAO_INTERESSADO)
                        add_dnc(n)
                        salvar_print(d,f"nao_{n[-4:]}"); escrever_log(ts,n,h,"RESPONDEU_NAO",txt)
                        return
                    else:
                        enviar_texto(d,"Por favor, responda apenas com 'sim' ou 'não' 👍")
                        salvar_print(d,f"pedir_confirmacao_{n[-4:]}"); escrever_log(ts,n,h,"PEDIU_CONFIRMACAO",txt)
            time.sleep(3)
        except Exception as e:
            logger.warning(f"⚠️ Erro durante espera: {e}")
            time.sleep(3)
    logger.info("😶 Sem resposta. Seguindo.")
    escrever_log(ts,n,h,"SEM_RESPOSTA")

# =========================
# MAIN
# =========================
def main():
    enviados=contar_envios_hoje()
    logger.info(f"📈 Já enviados hoje: {enviados}/{LIMITE_DIARIO}")
    if enviados>=LIMITE_DIARIO:
        logger.warning("🚫 Limite diário."); return
    d=criar_driver()
    try:
        abrir_whatsapp(d)
        for n in NUMEROS:
            if contar_envios_hoje()>=LIMITE_DIARIO: break
            processar(d,n); pause_humano()
        logger.info("🏁 Finalizado.")
    finally: d.quit()

if __name__=="__main__": main()
