# -*- coding: utf-8 -*-
"""
Script: Coleta automática dos resultados da Calculadora Melhor Envio
Autor: ChatGPT & Matheus 🚀
"""

import os
import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ========================================
# CONFIGURAÇÕES
# ========================================
URL = "https://app.melhorenvio.com.br/calculadora/resultados"
PASTA_SAIDA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Melhor Envio"
ARQUIVO_EXCEL = os.path.join(PASTA_SAIDA, "Fretes_MelhorEnvio.xlsx")

os.makedirs(PASTA_SAIDA, exist_ok=True)

# ========================================
# CONFIGURAÇÃO DO SELENIUM
# ========================================
chrome_options = Options()
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--log-level=3")

# (opcional: rodar sem abrir janela)
# chrome_options.add_argument("--headless=new")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

# ========================================
# ABRE E CAPTURA A PÁGINA
# ========================================
print("🌐 Acessando a página do Melhor Envio...")
driver.get(URL)

try:
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, ".shipping-companies-list__item, .shipping-data__banner.card.grid")
        )
    )
    time.sleep(2)
    print("✅ Resultados carregados com sucesso!")
except Exception as e:
    print("⚠️ Timeout ao esperar resultados:", e)

# Pega o HTML renderizado
html = driver.page_source
driver.quit()
print("🚪 Navegador fechado.")

# ========================================
# PARSE DO HTML
# ========================================
print("🔍 Extraindo informações...")
soup = BeautifulSoup(html, "html.parser")

linhas = []
blocos = soup.find_all("li", class_="shipping-companies-list__item")

for i, item in enumerate(blocos, 1):
    transportadora = item.find("p", class_="shipping-company-item__title")
    modalidade = item.find("button")
    preco = item.find("p", class_="shipping-company-item__price")
    prazo = item.find("p", class_="shipping-company-item__deadline")

    linhas.append({
        "Número": i,
        "Transportadora": transportadora.get_text(strip=True) if transportadora else "N/A",
        "Modalidade": modalidade.get_text(strip=True) if modalidade else "N/A",
        "Prazo (dias úteis)": prazo.get_text(strip=True) if prazo else "N/A",
        "Preço (R$)": preco.get_text(strip=True) if preco else "N/A",
        "CEP Destino": "N/A",   # Pode ser adicionado se for exibido na página
        "Peso (kg)": "N/A"      # Idem, se houver campo
    })

# ========================================
# EXPORTA PARA EXCEL
# ========================================
if linhas:
    df = pd.DataFrame(linhas)
    df.to_excel(ARQUIVO_EXCEL, index=False)
    print(f"\n✅ Planilha criada com {len(linhas)} registros!")
    print(f"📁 {ARQUIVO_EXCEL}")
else:
    print("\n⚠️ Nenhuma cotação encontrada na página.")
