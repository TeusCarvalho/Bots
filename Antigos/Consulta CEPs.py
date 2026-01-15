# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import unicodedata
from pathlib import Path
import requests
import pandas as pd

PASTA_SAIDA = Path(r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Dez")
ARQ_XLSX = PASTA_SAIDA / "enderecos_por_cidade.xlsx"

BASE = "https://api.correios.com.br/cep"
URL_LOCALIDADES = f"{BASE}/v1/localidades"
URL_BAIRROS = f"{BASE}/v1/bairros"
URL_ENDERECOS = f"{BASE}/v2/enderecos"

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def pick_content(payload):
    # A API costuma devolver paginação com "content"; mas deixo robusto.
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if "content" in payload and isinstance(payload["content"], list):
            return payload["content"]
        if "items" in payload and isinstance(payload["items"], list):
            return payload["items"]
    return []

def main():
    PASTA_SAIDA.mkdir(parents=True, exist_ok=True)

    token = (os.getenv("CORREIOS_TOKEN") or "").strip()
    if not token:
        raise SystemExit(
            "Defina a variável de ambiente CORREIOS_TOKEN com seu Bearer Token dos Correios."
        )

    uf = input("UF (ex: GO): ").strip().upper()
    cidade = input("Cidade (ex: Goiânia): ").strip()

    session = requests.Session()
    session.headers.update({
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
    })

    # 1) Descobrir a grafia/padrão exato da localidade
    r = session.get(URL_LOCALIDADES, params={"uf": uf, "localidade": cidade, "page": 0, "size": 50}, timeout=30)
    r.raise_for_status()
    locs = pick_content(r.json())

    if not locs:
        raise SystemExit(f"Nenhuma localidade encontrada para UF={uf} e cidade={cidade}.")

    cidade_norm = _norm(cidade)
    best = None
    for item in locs:
        nome = item.get("localidade") or item.get("nome") or ""
        if _norm(nome) == cidade_norm:
            best = item
            break
    if best is None:
        best = locs[0]

    localidade_padrao = best.get("localidade") or best.get("nome") or cidade
    print(f"Localidade usada (padrão Correios): {localidade_padrao}")

    # 2) Listar bairros da localidade
    # Endpoint: /v1/bairros/{uf}/localidades/{localidade}
    r = session.get(f"{URL_BAIRROS}/{uf}/localidades/{localidade_padrao}", params={"page": 0, "size": 500}, timeout=30)
    r.raise_for_status()
    bairros_payload = r.json()
    bairros = pick_content(bairros_payload)
    if not bairros:
        # alguns retornos podem vir como lista direta
        if isinstance(bairros_payload, list):
            bairros = bairros_payload

    # Extrai nomes
    nomes_bairros = []
    for b in bairros:
        nome = b.get("bairro") or b.get("nome") or ""
        if nome:
            nomes_bairros.append(nome)

    # Se não vier bairro, ainda dá para tentar buscar endereços só por cidade (pode ser pesado)
    if not nomes_bairros:
        print("Aviso: não consegui listar bairros. Vou buscar endereços apenas por UF+localidade (paginado).")
        nomes_bairros = [""]  # busca sem filtro de bairro

    # 3) Para cada bairro, pagina /v2/enderecos
    all_rows = []
    for bairro in sorted(set(nomes_bairros)):
        page = 0
        while True:
            params = {"uf": uf, "localidade": localidade_padrao, "page": page, "size": 200, "sort": "cep,asc"}
            if bairro:
                params["bairro"] = bairro

            r = session.get(URL_ENDERECOS, params=params, timeout=30)
            r.raise_for_status()
            payload = r.json()

            content = pick_content(payload)
            for it in content:
                all_rows.append({
                    "UF": it.get("uf", uf),
                    "Cidade": it.get("localidade", localidade_padrao),
                    "Bairro": it.get("bairro", bairro),
                    "Logradouro": it.get("logradouro", ""),
                    "Complemento": it.get("complemento", ""),
                    "CEP": it.get("cep", ""),
                    "TipoCEP": it.get("tipoCEP", ""),
                    "TipoLogradouro": it.get("tipoLogradouro", ""),
                    "Abreviatura": it.get("abreviatura", ""),
                })

            page_info = payload.get("page") if isinstance(payload, dict) else None
            if not page_info:
                # sem paginação explícita, quebra quando não vier mais conteúdo
                if not content:
                    break
                page += 1
                continue

            total_pages = int(page_info.get("totalPages", 1))
            page += 1
            if page >= total_pages:
                break

    if not all_rows:
        raise SystemExit("Não retornou nenhum endereço para essa cidade (ou sua conta não tem permissão/dados).")

    df = pd.DataFrame(all_rows)

    # Aba de bairros (se houver)
    df_bairros = (
        df[["UF", "Cidade", "Bairro"]]
        .drop_duplicates()
        .sort_values(["UF", "Cidade", "Bairro"], kind="stable")
        .reset_index(drop=True)
    )

    # Aba resumo
    df_resumo = (
        df.groupby(["UF", "Cidade"], as_index=False)
          .agg(Qtd_Enderecos=("CEP", "size"), Qtd_Bairros=("Bairro", "nunique"))
          .sort_values(["UF", "Cidade"], kind="stable")
    )

    with pd.ExcelWriter(ARQ_XLSX, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Enderecos")
        df_bairros.to_excel(writer, index=False, sheet_name="Bairros")
        df_resumo.to_excel(writer, index=False, sheet_name="Resumo")

    print(f"OK: arquivo gerado em {ARQ_XLSX}")

if __name__ == "__main__":
    main()
