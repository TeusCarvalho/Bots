
import argparse
import os
import re
from datetime import datetime, date
from typing import Optional, Tuple

import pandas as pd


# -------------------------
# CONFIG PADRÃO (ajustável)
# -------------------------
PESOS = {
    "sla": 0.40,
    "semmov": 0.30,
    "retidos": 0.20,
    "pnr": 0.05,
    "custos": 0.05,
}

# Score acima disso = "Melhorando"; abaixo disso = "Piorando"
LIMITE_MELHORANDO = 5.0
LIMITE_PIORANDO = -5.0


# -------------------------
# UTIL
# -------------------------
WEEKDAY_PT = {
    0: "Segunda",
    1: "Terça",
    2: "Quarta",
    3: "Quinta",
    4: "Sexta",
    5: "Sábado",
    6: "Domingo",
}


def iso_week_id(d: date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"


def parse_date_from_filename(path: str) -> Optional[date]:
    """
    Tenta extrair uma data do nome do arquivo.
    Aceita: YYYY-MM-DD, YYYYMMDD, DD-MM-YYYY, DDMMYYYY.
    """
    name = os.path.basename(path)

    # YYYY-MM-DD
    m = re.search(r"(\d{4})[-_](\d{2})[-_](\d{2})", name)
    if m:
        y, mo, d = map(int, m.groups())
        return date(y, mo, d)

    # YYYYMMDD
    m = re.search(r"(\d{4})(\d{2})(\d{2})", name)
    if m:
        y, mo, d = map(int, m.groups())
        return date(y, mo, d)

    # DD-MM-YYYY
    m = re.search(r"(\d{2})[-_](\d{2})[-_](\d{4})", name)
    if m:
        d, mo, y = map(int, m.groups())
        return date(y, mo, d)

    # DDMMYYYY
    m = re.search(r"(\d{2})(\d{2})(\d{4})", name)
    if m:
        d, mo, y = map(int, m.groups())
        return date(y, mo, d)

    return None


# -------------------------
# LEITURA / LIMPEZA (seu layout)
# -------------------------
def load_base_dados(excel_path: str) -> pd.DataFrame:
    """
    Lê e limpa a aba 'Base Dados' (seu relatório).
    """
    raw = pd.read_excel(excel_path, sheet_name="Base Dados", header=None)

    headers = raw.iloc[1].tolist()
    df = raw.iloc[2:].copy()
    df.columns = headers
    df = df[df["UF"].notna()]

    # Renomear colunas repetidas "Desempenho"
    cols = list(df.columns)
    new_cols = []
    des_count = 0
    for c in cols:
        if c == "Desempenho":
            des_count += 1
            mapping = {
                1: "Desempenho_SLA",
                2: "Desempenho_Retidos10d",
                3: "Desempenho_SemMov5d",
                4: "Desempenho_Ressarcimento",
                5: "Desempenho_PNR",
            }
            new_cols.append(mapping.get(des_count, f"Desempenho_{des_count}"))
        else:
            new_cols.append(c)
    df.columns = new_cols

    # Coerção numérica (principais)
    num_cols = [
        "Qtd a entregar",
        "SLA",
        "Desempenho_SLA",
        "Retidos > 10d",
        "Desempenho_Retidos10d",
        "Sem Mov.>5d",
        "Desempenho_SemMov5d",
        "Custos de Ressarcimento",
        "Desempenho_Ressarcimento",
        "Total de PNR",
        "Desempenho_PNR",
        "Notas",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Normalizar textos
    text_cols = ["UF", "Coordenador", "Supervisor", "Lider", "Franqueado", "Nome da base"]
    for c in text_cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    return df


def load_base_nomes(excel_path: str) -> pd.DataFrame:
    """
    Lê a aba 'Base Nomes' (mapeamento de base -> estação + nomes).
    """
    raw = pd.read_excel(excel_path, sheet_name="Base Nomes", header=None)
    headers = raw.iloc[0].tolist()
    df = raw.iloc[1:].copy()
    df.columns = headers
    df = df[df["Nome da base"].notna()]

    # Limpeza básica
    df["Nome da base"] = df["Nome da base"].astype(str).str.strip()
    if "Estação No." in df.columns:
        df["Estação No."] = df["Estação No."].astype(str).str.strip()

    return df


# -------------------------
# CÁLCULO RESUMO SEMANAL
# -------------------------
def _rate_per_1000(n: float, denom: float) -> float:
    if denom is None or pd.isna(denom) or denom == 0:
        return float("nan")
    if n is None or pd.isna(n):
        return float("nan")
    return (float(n) / float(denom)) * 1000.0


def calcula_resumo_semana(df_hist: pd.DataFrame, semana_iso: str) -> pd.DataFrame:
    """
    Para cada base na semana, compara o 1º dia registrado na semana vs último dia registrado.
    Score usa:
      - ΔSLA em pontos percentuais
      - melhoria de SemMov/Retidos/PNR/Custos em taxa por 1000 (queda = positivo)
    """
    w = df_hist[df_hist["semana_iso"] == semana_iso].copy()
    if w.empty:
        return pd.DataFrame(
            columns=[
                "semana_iso",
                "Nome da base",
                "UF",
                "SLA_inicio",
                "SLA_atual",
                "ΔSLA_pp",
                "SemMov5d_inicio",
                "SemMov5d_atual",
                "Melhoria_SemMov5d",
                "Retidos10d_inicio",
                "Retidos10d_atual",
                "Melhoria_Retidos10d",
                "PNR_inicio",
                "PNR_atual",
                "Melhoria_PNR",
                "Custos_inicio",
                "Custos_atual",
                "Melhoria_Custos",
                "Score",
                "Status",
            ]
        )

    w["data_ref"] = pd.to_datetime(w["data_ref"]).dt.date

    rows = []
    for base, g in w.groupby("Nome da base", dropna=False):
        g = g.sort_values("data_ref")

        first = g.iloc[0]
        last = g.iloc[-1]

        uf = last.get("UF", "")
        sla_ini = first.get("SLA")
        sla_atual = last.get("SLA")

        delta_sla_pp = None
        if pd.notna(sla_ini) and pd.notna(sla_atual):
            delta_sla_pp = (float(sla_atual) - float(sla_ini)) * 100.0

        # Taxas por 1000, usando Qtd a entregar como denominador
        den_ini = first.get("Qtd a entregar")
        den_atual = last.get("Qtd a entregar")

        sem_ini = _rate_per_1000(first.get("Sem Mov.>5d"), den_ini)
        sem_atual = _rate_per_1000(last.get("Sem Mov.>5d"), den_atual)
        mel_sem = sem_ini - sem_atual if pd.notna(sem_ini) and pd.notna(sem_atual) else float("nan")

        ret_ini = _rate_per_1000(first.get("Retidos > 10d"), den_ini)
        ret_atual = _rate_per_1000(last.get("Retidos > 10d"), den_atual)
        mel_ret = ret_ini - ret_atual if pd.notna(ret_ini) and pd.notna(ret_atual) else float("nan")

        pnr_ini = _rate_per_1000(first.get("Total de PNR"), den_ini)
        pnr_atual = _rate_per_1000(last.get("Total de PNR"), den_atual)
        mel_pnr = pnr_ini - pnr_atual if pd.notna(pnr_ini) and pd.notna(pnr_atual) else float("nan")

        cus_ini = _rate_per_1000(first.get("Custos de Ressarcimento"), den_ini)
        cus_atual = _rate_per_1000(last.get("Custos de Ressarcimento"), den_atual)
        mel_cus = cus_ini - cus_atual if pd.notna(cus_ini) and pd.notna(cus_atual) else float("nan")

        # Score (trate NaN como 0 para não quebrar)
        score = 0.0
        score += PESOS["sla"] * (delta_sla_pp if delta_sla_pp is not None and pd.notna(delta_sla_pp) else 0.0)
        score += PESOS["semmov"] * (mel_sem if pd.notna(mel_sem) else 0.0)
        score += PESOS["retidos"] * (mel_ret if pd.notna(mel_ret) else 0.0)
        score += PESOS["pnr"] * (mel_pnr if pd.notna(mel_pnr) else 0.0)
        score += PESOS["custos"] * (mel_cus if pd.notna(mel_cus) else 0.0)

        if score >= LIMITE_MELHORANDO:
            status = "Melhorando"
        elif score <= LIMITE_PIORANDO:
            status = "Piorando"
        else:
            status = "Estável"

        rows.append(
            {
                "semana_iso": semana_iso,
                "Nome da base": base,
                "UF": uf,
                "SLA_inicio": sla_ini,
                "SLA_atual": sla_atual,
                "ΔSLA_pp": delta_sla_pp,
                "SemMov5d_inicio": sem_ini,
                "SemMov5d_atual": sem_atual,
                "Melhoria_SemMov5d": mel_sem,
                "Retidos10d_inicio": ret_ini,
                "Retidos10d_atual": ret_atual,
                "Melhoria_Retidos10d": mel_ret,
                "PNR_inicio": pnr_ini,
                "PNR_atual": pnr_atual,
                "Melhoria_PNR": mel_pnr,
                "Custos_inicio": cus_ini,
                "Custos_atual": cus_atual,
                "Melhoria_Custos": mel_cus,
                "Score": score,
                "Status": status,
            }
        )

    out = pd.DataFrame(rows)
    out = out.sort_values(["Status", "Score"], ascending=[True, False])
    return out


# -------------------------
# HISTÓRICO (append + dedup)
# -------------------------
def read_sheet_if_exists(path_xlsx: str, sheet_name: str) -> pd.DataFrame:
    try:
        return pd.read_excel(path_xlsx, sheet_name=sheet_name)
    except Exception:
        return pd.DataFrame()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arquivo_dia", required=True, help="Relatório do dia (xlsx)")
    ap.add_argument("--arquivo_base", required=True, help="Workbook base com HISTORICO_DIARIO/RESUMO_SEMANA")
    ap.add_argument("--data", required=False, help="Data do snapshot (YYYY-MM-DD). Se omitido, tenta extrair do nome do arquivo.")
    args = ap.parse_args()

    arquivo_dia = args.arquivo_dia
    arquivo_base = args.arquivo_base

    if not os.path.exists(arquivo_dia):
        raise FileNotFoundError(f"Não achei o arquivo_dia: {arquivo_dia}")
    if not os.path.exists(arquivo_base):
        raise FileNotFoundError(f"Não achei o arquivo_base: {arquivo_base}")

    # Data do snapshot
    if args.data:
        dt = datetime.strptime(args.data, "%Y-%m-%d").date()
    else:
        dt = parse_date_from_filename(arquivo_dia)
        if dt is None:
            dt = datetime.today().date()

    semana = iso_week_id(dt)
    dia_semana = WEEKDAY_PT[dt.weekday()]

    # Ler snapshot
    df_snap = load_base_dados(arquivo_dia)

    # (Opcional) enriquecer com "Estação No."
    try:
        df_map = load_base_nomes(arquivo_dia)
        # Evita duplicar colunas de nomes caso já existam
        keep_cols = ["Nome da base", "Estação No."]
        df_map = df_map[[c for c in keep_cols if c in df_map.columns]].drop_duplicates("Nome da base")
        df_snap = df_snap.merge(df_map, on="Nome da base", how="left")
    except Exception:
        pass

    # Montar histórico do dia
    df_today = df_snap.copy()
    df_today.insert(0, "data_ref", pd.to_datetime(dt))
    df_today.insert(1, "semana_iso", semana)
    df_today.insert(2, "dia_semana", dia_semana)

    # Ler histórico existente
    df_hist = read_sheet_if_exists(arquivo_base, "HISTORICO_DIARIO")
    if not df_hist.empty:
        # Normalizar data_ref
        df_hist["data_ref"] = pd.to_datetime(df_hist["data_ref"], errors="coerce")

    # Append + dedup
    df_all = pd.concat([df_hist, df_today], ignore_index=True)
    if "Nome da base" in df_all.columns and "data_ref" in df_all.columns:
        df_all["data_ref"] = pd.to_datetime(df_all["data_ref"], errors="coerce")
        df_all = df_all.drop_duplicates(subset=["data_ref", "Nome da base"], keep="last")

    # Resumo semana
    df_resumo = calcula_resumo_semana(df_all, semana)

    # Gravar no arquivo_base (substitui as abas)
    with pd.ExcelWriter(arquivo_base, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_all.to_excel(writer, sheet_name="HISTORICO_DIARIO", index=False)
        df_resumo.to_excel(writer, sheet_name="RESUMO_SEMANA", index=False)

    print(f"OK: histórico atualizado para {dt} ({semana}).")
    print(f"Arquivo base atualizado: {arquivo_base}")


if __name__ == "__main__":
    main()
