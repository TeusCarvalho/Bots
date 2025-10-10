"""
baixar_jms_com_retry.py

Requisitos:
    pip install selenium webdriver-manager keyring

Descrição:
    - Reutiliza cookies salvos para sessão persistente.
    - Se não houver cookies tenta login automático via keyring (opcional).
    - Se não houver keyring ou falhar, pede login manual na primeira execução.
    - Clica no botão de export/download, monitora se apareceu a mensagem de erro
      "导出繁忙,请稍后再试" e faz retries com backoff.
    - Espera até o arquivo terminar de baixar (checa ausência de .crdownload).
"""

import os
import time
import json
import traceback
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# keyring é opcional (mais seguro para armazenar senha)
try:
    import keyring
except Exception:
    keyring = None

# ----------------- CONFIGURAÇÕES -----------------
PASTA_PROJETO = r"C:\Users\J&T-099\PycharmProjects\Bots"  # ajuste se quiser
PASTA_DOWNLOADS = os.path.join(PASTA_PROJETO, "downloads")
PASTA_COOKIES = os.path.join(PASTA_PROJETO, "cookies")
COOKIES_FILE = os.path.join(PASTA_COOKIES, "jms_cookies.json")
SCREENSHOT_ON_ERROR = os.path.join(PASTA_PROJETO, "erro_export.png")

DOMINIO_BASE = "https://jmsbr.jtjms-br.com"
URL_LOGIN = f"{DOMINIO_BASE}/login"
URL_INDEX = f"{DOMINIO_BASE}/index"

# XPATH do botão de download/export - ajuste conforme necessidade
XPATH_BOTAO_DOWNLOAD = "//button[contains(text(),'Download')]"

# XPATH/seletores para login automático (se for usar keyring) - ajustar conforme site
LOGIN_USER_SELECTOR = (By.ID, "username")  # ex.: (By.NAME, "username")
LOGIN_PASS_SELECTOR = (By.ID, "password")

# Texto/XPath para detectar a mensagem de erro em chinês
ERROR_XPATH = "//*[contains(text(),'导出繁忙') or contains(text(),'请稍后再试')]"

# Timeouts e retries
TIMEOUT_ESPERA_BOTAO = 60         # espera pelo botão clicável
TIMEOUT_DOWNLOAD = 300            # espera até o download terminar (segundos) por tentativa
MAX_RETRIES = 5                   # número máximo de tentativas (retry)
INITIAL_BACKOFF = 5               # segundos antes da 1ª retry
# --------------------------------------------------

os.makedirs(PASTA_DOWNLOADS, exist_ok=True)
os.makedirs(PASTA_COOKIES, exist_ok=True)


def criar_driver(download_dir: str):
    """Cria driver Chrome com prefs de download."""
    options = Options()
    # Se quiser ver o navegador, comente a linha abaixo
    # options.add_argument("--headless=new")  # cuidado: headless pode ter diferenças no download
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver


def salvar_cookies(driver, path: str):
    cookies = driver.get_cookies()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cookies, f, ensure_ascii=False, indent=2)


def carregar_cookies(driver, path: str) -> bool:
    """Carrega cookies para o driver. Retorna True se o arquivo existia (mesmo que algum cookie dê problema)."""
    if not os.path.exists(path):
        return False
    with open(path, "r", encoding="utf-8") as f:
        cookies = json.load(f)
    driver.get(DOMINIO_BASE)  # abrir domínio antes de adicionar cookies
    for c in cookies:
        cookie = {k: c[k] for k in ("name", "value", "domain", "path", "expiry", "secure", "httpOnly") if k in c}
        # remover campos None para evitar erro
        if "expiry" in cookie and cookie["expiry"] is None:
            cookie.pop("expiry")
        try:
            driver.add_cookie(cookie)
        except Exception as e:
            # apenas loga, continua
            print("⚠️ Erro ao adicionar cookie:", e)
    return True


def tentar_login_automatico_keyring(driver, username: str = None) -> bool:
    """
    Tenta efetuar login automático usando keyring.
    Requer que você já tenha salvo a senha com:
      keyring.set_password("jmsbr_site", "seu_usuario", "sua_senha")
    """
    if keyring is None:
        return False
    user = username
    if not user:
        # se KEYRING_USERNAME não fornecido no código, não tentamos adivinhar
        return False
    senha = keyring.get_password("jmsbr_site", user)
    if not senha:
        return False
    try:
        driver.get(URL_LOGIN)
        campo_usuario = WebDriverWait(driver, 10).until(EC.presence_of_element_located(LOGIN_USER_SELECTOR))
        campo_senha = driver.find_element(*LOGIN_PASS_SELECTOR)
        campo_usuario.clear()
        campo_usuario.send_keys(user)
        campo_senha.clear()
        campo_senha.send_keys(senha)
        campo_senha.submit()
        # aguardar redirecionamento para /index como indicação de sucesso
        WebDriverWait(driver, 15).until(EC.url_contains("/index"))
        print("✅ Login automático via keyring ok.")
        return True
    except Exception as e:
        print("⚠️ Falha no login automático via keyring:", e)
        return False


def aguardar_export_or_error(driver, download_dir, download_timeout=TIMEOUT_DOWNLOAD):
    """
    Espera até:
      - arquivo concluído aparecer na pasta -> retorna ('ok', caminho)
      - OU mensagem de erro (texto chinês) aparecer -> retorna ('error', texto)
      - OU timeout -> ('timeout', None)
    """
    inicio = time.time()
    while True:
        # 1) verifica se arquivo finalizado apareceu
        arquivos = [f for f in os.listdir(download_dir) if not f.endswith(".crdownload") and not f.endswith(".tmp")]
        if arquivos:
            # pega o mais recente
            arquivos_sorted = sorted(arquivos, key=lambda n: os.path.getmtime(os.path.join(download_dir, n)), reverse=True)
            caminho = os.path.join(download_dir, arquivos_sorted[0])
            # garantir que o arquivo não está sendo escrito (checa tamanho estável)
            try:
                tamanho1 = os.path.getsize(caminho)
                time.sleep(1)
                tamanho2 = os.path.getsize(caminho)
                if tamanho1 == tamanho2:
                    return ("ok", caminho)
            except Exception:
                pass

        # 2) procura mensagem de erro na página (curto wait)
        try:
            # esperamos 1s máximo pra encontrar o elemento de erro (não bloquear muito)
            err_el = WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.XPATH, ERROR_XPATH)))
            texto = err_el.text or err_el.get_attribute("innerText")
            return ("error", texto)
        except Exception:
            pass

        # 3) timeout geral
        if time.time() - inicio > download_timeout:
            return ("timeout", None)

        time.sleep(0.5)


def tentar_export_com_retries(driver, click_xpath, download_dir, max_retries=MAX_RETRIES):
    """
    Tenta clicar no botão e aguardar resultado. Em caso de erro detectado, realiza retry com backoff.
    Retorna caminho do arquivo em caso de sucesso, ou lança RuntimeError ao atingir max_retries.
    """
    for tentativa in range(1, max_retries + 1):
        try:
            botao = WebDriverWait(driver, TIMEOUT_ESPERA_BOTAO).until(
                EC.element_to_be_clickable((By.XPATH, click_xpath))
            )
            botao.click()
            print(f"[Tentativa {tentativa}] Botão clicado. Aguardando resultado...")
        except Exception as e:
            print(f"❌ Falha ao localizar/clicar no botão (tentativa {tentativa}): {e}")
            # tirar screenshot para debug
            try:
                driver.save_screenshot(f"screenshot_click_fail_{tentativa}.png")
            except Exception:
                pass
            # decide se quer tentar novamente
            wait = min(60, INITIAL_BACKOFF * (2 ** (tentativa - 1)))
            print(f"⏳ Aguardando {wait}s antes da próxima tentativa...")
            time.sleep(wait)
            continue

        # após o clique, aguarda o download ou a mensagem de erro
        status, payload = aguardar_export_or_error(driver, download_dir, download_timeout=TIMEOUT_DOWNLOAD)
        if status == "ok":
            print("✅ Download concluído:", payload)
            return payload
        if status == "error":
            print(f"⚠️ Erro detectado na exportação: {payload}")
            # salvar screenshot e html para debug
            try:
                sc_name = f"screenshot_erro_t{tentativa}.png"
                driver.save_screenshot(sc_name)
                with open(f"page_source_erro_t{tentativa}.html", "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                print("📸 Screenshot e page_source salvos para investigação.")
            except Exception:
                pass
            # backoff antes da próxima tentativa
            wait = min(60, INITIAL_BACKOFF * (2 ** (tentativa - 1)))
            print(f"⏳ Backoff: aguardando {wait}s antes de tentar novamente...")
            time.sleep(wait)
            # continua loop pra nova tentativa
            continue
        if status == "timeout":
            print("❌ Timeout: nem arquivo nem mensagem de erro detectados.")
            wait = min(60, INITIAL_BACKOFF * (2 ** (tentativa - 1)))
            print(f"⏳ Aguardando {wait}s antes de tentar novamente...")
            time.sleep(wait)
            continue

    raise RuntimeError("Máximo de tentativas atingido sem sucesso.")


def main():
    driver = None
    try:
        driver = criar_driver(PASTA_DOWNLOADS)

        # 1) tentar carregar cookies
        if carregar_cookies(driver, COOKIES_FILE):
            print("✅ Cookies carregados; abrindo página alvo...")
            driver.get(URL_INDEX)
            time.sleep(1)
        else:
            # 2) tentar login automático via keyring (se quiser configurar, ajuste 'username' aqui)
            auto_done = False
            # Se quiser usar keyring defina 'username' abaixo (ou ajuste para coletar)
            USERNAME_KEYRING = None  # ex: "meu_usuario" se tiver salvo no keyring
            if keyring is not None and USERNAME_KEYRING:
                print("🔐 Tentando login automático via keyring...")
                auto_done = tentar_login_automatico_keyring(driver, USERNAME_KEYRING)

            if not auto_done:
                # 3) pede login manual para a primeira execução
                print("🚨 Faça login manualmente na janela do navegador. Depois pressione ENTER neste terminal.")
                driver.get(URL_LOGIN)
                input("Depois de logar manualmente no navegador, pressione ENTER aqui...")
                try:
                    salvar_cookies(driver, COOKIES_FILE)
                    print("✅ Cookies salvos em:", COOKIES_FILE)
                except Exception as e:
                    print("⚠️ Falha ao salvar cookies:", e)

                driver.get(URL_INDEX)

        # 4) tentar exportar com retries/backoff
        arquivo = None
        try:
            arquivo = tentar_export_com_retries(driver, XPATH_BOTAO_DOWNLOAD, PASTA_DOWNLOADS, max_retries=MAX_RETRIES)
        except Exception as e:
            print("❌ Export falhou após retries:", e)
            # screenshot final
            try:
                driver.save_screenshot(SCREENSHOT_ON_ERROR)
                print("📸 Screenshot final salvo em:", SCREENSHOT_ON_ERROR)
            except Exception:
                pass

        if arquivo:
            print("✅ Processo concluído com sucesso. Arquivo:", arquivo)
        else:
            print("❌ Processo não obteve arquivo. Verifique os screenshots gerados e o page_source para debug.")

    except Exception:
        print("❌ Erro inesperado:")
        traceback.print_exc()
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    main()