# -*- coding: utf-8 -*-

import pandas as pd
import requests
import json
import time
import socket
import dns.resolver  # dnspython
from requests.exceptions import RequestException

# ===================== CONFIGURAÇÕES =====================
ARQUIVO_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Melhor Envio\modelo_upload_envios.xls"
ARQUIVO_SAIDA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Melhor Envio\Fretes_Calculados.xlsx"

TOKEN_API = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxIiwianRpIjoiZjFiMDQyNWM3N2YwM2ZjM2JmMzQ4NjY5OTcwN2Y1ODgzZDFiNDI4Nzk5ZmNjMGRlNTYyMzdmYTQwMDdmMWQ0YjhlM2VjYjlkOWQ0NGJkZjkiLCJpYXQiOjE3NjAzODE3MDEuODM2NTEsIm5iZiI6MTc2MDM4MTcwMS44MzY1MTIsImV4cCI6MTc5MTkxNzcwMS44MjQzODcsInN1YiI6ImEwMWIwYzliLTg3ZjMtNDZlOC1iODNjLTUyOGQ4NzVlOWRmNCIsInNjb3BlcyI6WyJjYXJ0LXJlYWQiLCJjYXJ0LXdyaXRlIiwiY29tcGFuaWVzLXJlYWQiLCJjb21wYW5pZXMtd3JpdGUiLCJjb3Vwb25zLXJlYWQiLCJjb3Vwb25zLXdyaXRlIiwibm90aWZpY2F0aW9ucy1yZWFkIiwib3JkZXJzLXJlYWQiLCJwcm9kdWN0cy1yZWFkIiwicHJvZHVjdHMtZGVzdHJveSIsInByb2R1Y3RzLXdyaXRlIiwicHVyY2hhc2VzLXJlYWQiLCJzaGlwcGluZy1jYWxjdWxhdGUiLCJzaGlwcGluZy1jYW5jZWwiLCJzaGlwcGluZy1jaGVja291dCIsInNoaXBwaW5nLWNvbXBhbmllcyIsInNoaXBwaW5nLWdlbmVyYXRlIiwic2hpcHBpbmctcHJldmlldyIsInNoaXBwaW5nLXByaW50Iiwic2hpcHBpbmctc2hhcmUiLCJzaGlwcGluZy10cmFja2luZyIsImVjb21tZXJjZS1zaGlwcGluZyIsInRyYW5zYWN0aW9ucy1yZWFkIiwidXNlcnMtcmVhZCIsInVzZXJzLXdyaXRlIiwid2ViaG9va3MtcmVhZCIsIndlYmhvb2tzLXdyaXRlIiwid2ViaG9va3MtZGVsZXRlIiwidGRlYWxlci13ZWJob29rIl19.siTWnWDspdZb8g8e745oH8xpg7krDEzDiDHoCJOkE88NbtIyLSW3fqBq_eBUvq0l9zS-YCgn12EtWWxyejqHtZWJ2_PW0bV6bA3e9WDYy_U0f-QY0D1dk-CuIVeFWLHJqbccmRP13y-Yr01D0YYMSWh0cWkcFpJrt5pmR01bxDnu2SAttu9ZMBkj43ZTSLX1peOcF7VCx0zk9mFnK6KRzsboOGnQ0HrXp46o0fAO6pbALk7en22x4XmLfGpmhvHKlRVbXcWRTPxJ5ueyJl6XPguJQ9fdbfckwCRoCJBaXM_7YPDpWk3lnBhEhTA5G51Dy2B33i8Zhrlm5TLInTHndl1mxMdKhJPTkt090j7jsbi-JcDiPEZQic6Q4bvsGnPDXnNHaRO1jcux-OdcEaVFb9KLNaI-QAcjbS4U_48zo6sKNpteaUDJkMgJqiRgFabAxakkO4Vnijd9Rs3L3DY7bwAPK46CPxRgvGnUhTbDXGA-HQkzwQ4SsxM9Fjs8qvdQvsENJNLSKiwNqk4OiecaIdgXvXpuMplvG532_iFIP1_aEiwqwaUCWdStbHs1YgVE3iNlaufyJXFpba9HjAS7-R_N0xr49s3SkRpmWmOgSfH92GUC8JdXzKiLB1jEBEwlNBYZtyUInIgupOZisViN_cskqwQ6qPtg_fmH6ZZL_To"

URL_HOST = "api.melhorenvio.com.br"
URL_PATH = "/api/v2/me/shipment/calculate"
URL_FULL = f"https://{URL_HOST}{URL_PATH}"

# fallback IPs (se precisar)
FALLBACK_IPS = ["104.21.17.35", "172.67.214.35"]

CEP_ORIGEM = "70000000"

HEADERS = {
    "Authorization": f"Bearer {TOKEN_API}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# PROXIES: se usar proxy corporativo, preencha aqui ("http://user:pass@host:port")
PROXIES = {"http": None, "https": None}

# timeout (s)
TIMEOUT = 12

# ===================== FUNÇÕES AUXILIARES =====================

def resolve_system(hostname):
    """Tenta resolver usando o resolver do sistema (socket). Retorna lista de IPs ou []"""
    try:
        infos = socket.getaddrinfo(hostname, None)
        ips = sorted({i[4][0] for i in infos})
        return ips
    except Exception:
        return []

def resolve_google(hostname):
    """Tenta resolver usando Google DNS via dnspython. Retorna lista de IPs ou []"""
    try:
        resolver = dns.resolver.Resolver()
        resolver.nameservers = ['8.8.8.8', '8.8.4.4']
        answers = resolver.resolve(hostname, 'A', lifetime=5)
        ips = [r.to_text() for r in answers]
        return ips
    except Exception:
        return []

def try_post(url, headers, payload, proxies=None, verify=True, extra_host_header=None, timeout=TIMEOUT):
    """Faz POST com requests. Se extra_host_header for fornecido, adiciona 'Host' no headers."""
    h = headers.copy()
    if extra_host_header:
        h['Host'] = extra_host_header
    resp = requests.post(url, headers=h, data=json.dumps(payload), proxies=proxies, timeout=timeout, verify=verify)
    try:
        return resp.status_code, resp.json(), resp.text
    except Exception:
        return resp.status_code, None, resp.text

# ===================== LÓGICA PRINCIPAL =====================

def calcular_fretes():
    print("🚀 Lendo planilha...")
    try:
        df = pd.read_excel(ARQUIVO_ENTRADA, engine='xlrd')
    except FileNotFoundError:
        print(f"❌ Arquivo não encontrado: {ARQUIVO_ENTRADA}")
        return
    except Exception as e:
        print("❌ Erro ao abrir planilha:", e)
        return

    resultados = []
    total = len(df)
    # tenta resolver DNS do sistema
    sys_ips = resolve_system(URL_HOST)
    print(f"🔎 Resolução (sistema): {sys_ips or 'nenhum'}")

    if not sys_ips:
        google_ips = resolve_google(URL_HOST)
        print(f"🔎 Resolução (Google DNS): {google_ips or 'nenhum'}")
    else:
        google_ips = []

    for idx, row in df.iterrows():
        i = idx + 1
        print(f"\n📦 ({i}/{total}) Preparando envio...")
        # prepara payload usando colunas informadas
        ar = str(row.get("AVISO DE RECEBIMENTO (AR)", "")).strip().lower() in ["sim", "s", "1", "true"]
        mp = str(row.get("MÃO PRÓPRIA (MP)", "")).strip().lower() in ["sim", "s", "1", "true"]

        payload = {
            "from": {"postal_code": CEP_ORIGEM},
            "to": {"postal_code": str(row.get("CEP DESTINO", "")).zfill(8)},
            "products": [
                {
                    "id": f"item_{i}",
                    "width": float(row.get("LARGURA (CM)", 1) or 0.1),
                    "height": float(row.get("ALTURA (CM)", 1) or 0.1),
                    "length": float(row.get("COMPRIMENTO (CM)", 1) or 0.1),
                    "weight": float(row.get("PESO (KG)", 0.1) or 0.1),
                    "insurance_value": float(row.get("VALOR SEGURADO", 0.0) or 0.0),
                    "quantity": 1
                }
            ],
            "options": {"receipt": ar, "own_hand": mp}
        }

        # 1) tenta a URL normal (nome)
        try:
            status, j, txt = try_post(URL_FULL, HEADERS, payload, proxies=PROXIES, verify=True)
            if status == 200:
                print(f"✅ Obtido via DNS normal ({URL_HOST}) - {len(j or [])} opções")
                for c in (j or []):
                    resultados.append({
                        "Ambiente": "Produção(DNS)",
                        "CEP_ORIGEM": CEP_ORIGEM,
                        "CEP_DESTINO": payload["to"]["postal_code"],
                        "Transportadora": c.get("company", {}).get("name"),
                        "Serviço": c.get("name"),
                        "Preço (R$)": c.get("price"),
                        "Prazo (dias úteis)": c.get("delivery_time"),
                        "Peso (kg)": payload["products"][0]["weight"],
                        "Valor Segurado": payload["products"][0]["insurance_value"],
                        "AR": "Sim" if ar else "Não",
                        "MP": "Sim" if mp else "Não"
                    })
                time.sleep(0.4)
                continue
            else:
                print(f"⚠️ Resposta via DNS: {status} — {txt[:200]}")
        except RequestException as e:
            print(f"❌ Falha na requisição via DNS: {e}")

        # 2) tenta Google DNS ips (se houver)
        attempted_ips = []
        if google_ips:
            for ip in google_ips:
                attempted_ips.append(ip)
                url_ip = f"https://{ip}{URL_PATH}"
                try:
                    # usa Host header para SNI/Host virtual
                    status, j, txt = try_post(url_ip, HEADERS, payload, proxies=PROXIES, verify=False, extra_host_header=URL_HOST)
                    if status == 200:
                        print(f"✅ Obtido via Google DNS IP {ip} (verify=False) - {len(j or [])} opções")
                        for c in (j or []):
                            resultados.append({
                                "Ambiente": f"Produção(IP {ip})",
                                "CEP_ORIGEM": CEP_ORIGEM,
                                "CEP_DESTINO": payload["to"]["postal_code"],
                                "Transportadora": c.get("company", {}).get("name"),
                                "Serviço": c.get("name"),
                                "Preço (R$)": c.get("price"),
                                "Prazo (dias úteis)": c.get("delivery_time"),
                                "Peso (kg)": payload["products"][0]["weight"],
                                "Valor Segurado": payload["products"][0]["insurance_value"],
                                "AR": "Sim" if ar else "Não",
                                "MP": "Sim" if mp else "Não"
                            })
                        break
                    else:
                        print(f"⚠️ Resposta via IP {ip}: {status} — {txt[:200]}")
                except RequestException as e:
                    print(f"❌ Falha via IP {ip}: {e}")

        # 3) tenta os FALLBACK_IPS predefinidos
        if not google_ips:
            ips_to_try = FALLBACK_IPS
        else:
            ips_to_try = [ip for ip in google_ips if ip not in attempted_ips] + FALLBACK_IPS

        success = False
        for ip in ips_to_try:
            try:
                url_ip = f"https://{ip}{URL_PATH}"
                status, j, txt = try_post(url_ip, HEADERS, payload, proxies=PROXIES, verify=False, extra_host_header=URL_HOST)
                if status == 200:
                    print(f"✅ Obtido via IP fallback {ip} (verify=False) - {len(j or [])} opções")
                    for c in (j or []):
                        resultados.append({
                            "Ambiente": f"Produção(IP {ip})",
                            "CEP_ORIGEM": CEP_ORIGEM,
                            "CEP_DESTINO": payload["to"]["postal_code"],
                            "Transportadora": c.get("company", {}).get("name"),
                            "Serviço": c.get("name"),
                            "Preço (R$)": c.get("price"),
                            "Prazo (dias úteis)": c.get("delivery_time"),
                            "Peso (kg)": payload["products"][0]["weight"],
                            "Valor Segurado": payload["products"][0]["insurance_value"],
                            "AR": "Sim" if ar else "Não",
                            "MP": "Sim" if mp else "Não"
                        })
                    success = True
                    break
                else:
                    print(f"⚠️ Resposta via IP {ip}: {status} — {txt[:200]}")
            except RequestException as e:
                print(f"❌ Falha via IP {ip}: {e}")

        if not success:
            print("🚫 Não foi possível consultar frete (DNS e IPs falharam).")

        time.sleep(0.6)

    # Exportar resultados
    if resultados:
        df_saida = pd.DataFrame(resultados)
        df_saida.to_excel(ARQUIVO_SAIDA, index=False)
        print(f"\n✅ Concluído! Planilha salva em:\n{ARQUIVO_SAIDA}")
    else:
        print("\n⚠️ Nenhum frete foi calculado. Verifique rede/proxy/token.")

# ===================== EXECUÇÃO =====================
if __name__ == "__main__":
    calcular_fretes()
