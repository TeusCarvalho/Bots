# =========================
# BLOCO 1/4 — IMPORTS / CONFIG / VARIÁVEIS
# =========================
import os
import requests
import warnings
import polars as pl
import pandas as pd
import multiprocessing
import logging
from datetime import datetime, timedelta, date

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ==========================================================
# CONFIGURAÇÃO DE LOGGING
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("Antigos/sla_franquias.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# ==========================================================
# VARIÁVEIS DE CONFIGURAÇÃO
# ==========================================================
os.environ["POLARS_MAX_THREADS"] = str(multiprocessing.cpu_count())

PASTA_ENTRADA = r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Área de Trabalho\Testes\06-  SLA Entrega Realizada Franquia"

# ✅ PASTA DE SAÍDA (onde você quer que gere a planilha nova)
PASTA_SAIDA = r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\SLA - Franquias"

WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/92a82aea-9b5c-4e3d-9169-8d4753ecef38"
LINK_PASTA = "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/IgB0KPVG-6d0Qqxw1Tk7GypbAZqn_A3JLD5XXsi7cBiWlHQ?e=gXLZ41"

COL_DATA_ORIGINAL = "Data prevista de entrega"
COL_DATA_UPPER = "DATA PREVISTA DE ENTREGA"
COL_DATA_REF = "DATA_REF"

# Ative para logar diferenças de schema por arquivo
DIAGNOSTICO_SCHEMA = True
# =========================
# BLOCO 2/4 — BASES + HELPERS + LEITURA/CONSOLIDAÇÃO
# =========================

# ==========================================================
# LISTA DE BASES VÁLIDAS
# ==========================================================
BASES_VALIDAS = [
    'F CHR-AM', 'F CAC-RO', 'F PDR-GO', 'F PVH-RO', 'F ARQ - RO',
    'F AGB-MT', 'F GYN 03-GO', 'F RBR-AC','F GYN - GO', 'F VHL-RO', 'F PON-GO', 'F ANP-GO', 'F GYN 02-GO', 'F CDN-AM',
    'F AGL-GO', 'F APG - GO', 'F RVD - GO', 'F PDT-TO', 'F PLN-DF', 'F SEN-GO', 'F PVL-MT',
    'F TRD-GO', 'F CEI-DF', 'F CNF-MT', 'F FMA-GO', 'F ALV-AM', 'F POS-GO', 'F PPA-MS', 'F MAC-AP',
    'F GAI-TO', 'F CRX-GO', 'F DOM -PA', 'F CCR-MT', 'F GRP-TO', 'F PVL 02-MT', 'F AMB-MS', 'F BVB-RR',
    'F SVC-RR', 'F MCP-AP', 'F JPN 02-RO', 'F MCP 02-AP', 'F BSL-AC', 'F PVH 02-RO', 'F JPN-RO',
    'F CMV-MT', 'F DOU-MS', 'F PGM-PA', 'F RDC -PA', 'F XIG-PA', 'F TGT-DF', 'F CGR - MS', 'F VLP-GO',
    'F CGR 02-MS', 'F PLA-GO', 'F TGA-MT', 'F RFI-DF', 'F ORL-PA', 'F ITI-PA', 'F PCA-PA',
    'F CNC-PA', 'F SJA-GO', 'F IGA-PA', 'F PAZ-AM', 'F TUR-PA', 'F JCD-PA', 'F TLA-PA',
    'F ELD-PA', 'F BSB-DF', 'F OCD-GO', 'F EMA-DF', 'F GUA-DF', 'F STM-PA', 'F SBN-DF',
    'F AGB 02-MT', 'F ANA-PA', 'F ARQ 02-RO', 'F BAO-PA', 'F BGA-MT', 'F BTS-RO', 'F CDN 02-AM',
    'F CGR 03-MS', 'F CGR 04-MS', 'F CRH-PA', 'F CTL-GO', 'F DOU 02-MS', 'F GFN-PA', 'F GNS-PA',
    'F GYN 04-GO', 'F HMT-AM', 'F IGM-PA', 'F IPX-PA', 'F ITT-PA', 'F JAU-RO', 'F JRG-GO',
    'F MDO-RO', 'F MDR-PA', 'F MRL-AM', 'F MTB-PA', 'F NDI-MS', 'F NMB-PA', 'F PDP-PA', 'F PMW-TO',
    'F PNA-TO', 'F PTD-MT', 'F PVH 03-RO', 'F QUI-GO', 'F RBR 02-AC', 'F ROO-MT', 'F SAM-DF', 'F SBS-DF',
    'F SBZ-PA', 'F SFX-PA', 'F SNP-MT', 'F TPN-PA','F ANP 02-GO', 'F APG 02-GO', 'F BBG-MT', 'F BRV-PA', 'F CAM-PA',
    'F CDN 03-AM', 'F CGR 05-MS', 'F CNA-PA', 'F CNP-MT', 'F CRJ-RO',
    'F GAM-DF', 'F GYN 06-GO', 'F GYN 07-GO', 'F JTI-GO', 'F MCP 04-AP',
    'F MDT-MT', 'F PMG-GO', 'F PVH 04-RO', 'F RDM-RO', 'F TGT 02-DF'
]


# ==========================================================
# HELPERS (NORMALIZAÇÃO / SCHEMA)
# ==========================================================
def _make_unique_columns(cols: list[str]) -> list[str]:
    """
    Garante nomes únicos de colunas (caso alguma planilha venha com duplicatas).
    Ex: ["A","A"] -> ["A","A__2"]
    """
    seen = {}
    out = []
    for c in cols:
        base = (c if c is not None else "").strip()
        if base == "":
            base = "COL"
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}__{seen[base]}")
    return out


def _normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normaliza nomes: strip + UPPER e resolve duplicatas.
    """
    if df.is_empty():
        return df

    cols_norm = []
    for c in df.columns:
        c2 = (c if c is not None else "").strip().upper()
        cols_norm.append(c2 if c2 != "" else "COL")

    cols_unique = _make_unique_columns(cols_norm)

    if cols_unique != df.columns:
        df = df.rename({old: new for old, new in zip(df.columns, cols_unique)})

    return df


def _align_schemas(dfs: list[pl.DataFrame]) -> list[pl.DataFrame]:
    """
    Faz a união de colunas entre todos dfs e adiciona colunas faltantes como NULL,
    retornando todos com a MESMA ordem de colunas.
    """
    if not dfs:
        return dfs

    all_cols: list[str] = []
    seen = set()
    for df in dfs:
        for c in df.columns:
            if c not in seen:
                seen.add(c)
                all_cols.append(c)

    aligned = []
    for df in dfs:
        missing = [c for c in all_cols if c not in df.columns]
        if missing:
            df = df.with_columns([pl.lit(None).alias(c) for c in missing])
        df = df.select(all_cols)
        aligned.append(df)

    return aligned


# ==========================================================
# FUNÇÕES DE PROCESSAMENTO DE DADOS
# ==========================================================
def ler_planilha_rapido(caminho: str) -> pl.DataFrame:
    """Lê um arquivo (Excel ou CSV) de forma rápida e segura + normaliza colunas."""
    try:
        if caminho.lower().endswith(".csv"):
            df = pl.read_csv(caminho)
        else:
            df = pl.read_excel(caminho)

        df = _normalize_columns(df)
        return df

    except Exception as e:
        logging.error(f"Erro ao ler {caminho}: {e}")
        return pl.DataFrame()


def consolidar_planilhas(pasta: str) -> pl.DataFrame:
    """Lê e consolida arquivos de forma sequencial e alinha schemas para evitar erro no concat."""
    arquivos = [
        os.path.join(pasta, f)
        for f in os.listdir(pasta)
        if f.lower().endswith((".xlsx", ".xls", ".csv")) and not f.startswith("~$")
    ]

    if not arquivos:
        raise FileNotFoundError("Nenhum arquivo encontrado na pasta de entrada.")

    dfs: list[pl.DataFrame] = []
    schemas_info = []  # (arquivo, n_cols, cols)

    logging.info(f"📂 Encontrados {len(arquivos)} arquivos. Iniciando leitura sequencial...")

    for i, arquivo in enumerate(arquivos):
        nome = os.path.basename(arquivo)
        logging.info(f"Lendo arquivo {i + 1}/{len(arquivos)}: {nome}...")

        df = ler_planilha_rapido(arquivo)

        if not df.is_empty():
            df = df.with_columns(pl.lit(nome).alias("_ARQUIVO_ORIGEM"))
            dfs.append(df)
            schemas_info.append((nome, len(df.columns), df.columns))
        else:
            logging.warning(f"Arquivo ignorado (vazio ou erro de leitura): {nome}")

    if not dfs:
        raise ValueError("Nenhum DataFrame válido foi lido dos arquivos.")

    if DIAGNOSTICO_SCHEMA:
        base_nome, base_n, base_cols = schemas_info[0]
        for nome, n, cols in schemas_info[1:]:
            if n != base_n:
                logging.warning(f"⚠️ Schema diferente: {nome} tem {n} colunas (base {base_nome} tem {base_n}).")

            set_base = set(base_cols)
            set_cols = set(cols)
            missing = sorted(list(set_base - set_cols))
            extra = sorted(list(set_cols - set_base))
            if missing or extra:
                if missing:
                    logging.warning(f"   - {nome} faltando colunas: {missing[:20]}{'...' if len(missing) > 20 else ''}")
                if extra:
                    logging.warning(f"   - {nome} colunas extras: {extra[:20]}{'...' if len(extra) > 20 else ''}")

    logging.info("🔄 Todos os arquivos lidos. Alinhando schemas (união de colunas) ...")
    dfs = _align_schemas(dfs)

    logging.info("🔄 Schemas alinhados. Iniciando concatenação...")
    df_final = pl.concat(dfs, how="vertical_relaxed")

    logging.info(f"📂 Base consolidada com {df_final.height} linhas e {len(df_final.columns)} colunas.")
    return df_final
# =========================
# BLOCO 3/4 — DATAS + SLA + SALVAR EXCEL
# =========================

def preparar_coluna_data(df: pl.DataFrame) -> pl.DataFrame:
    """Padroniza e converte a coluna de data para o tipo Date."""
    df = _normalize_columns(df)

    if COL_DATA_UPPER not in df.columns:
        raise KeyError(f"Coluna '{COL_DATA_ORIGINAL}' não encontrada no DataFrame. (Esperado: '{COL_DATA_UPPER}')")

    temp_col = "temp_data_str"

    df = df.with_columns(
        pl.col(COL_DATA_UPPER)
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.replace(r" .*$", "")
        .alias(temp_col)
    )

    formatos_data = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%Y%m%d"]
    tentativas_parse = [pl.col(temp_col).str.strptime(pl.Date, fmt, strict=False) for fmt in formatos_data]

    df = df.with_columns(pl.coalesce(tentativas_parse).alias(COL_DATA_REF)).drop(temp_col)
    df = df.filter(pl.col(COL_DATA_REF).is_not_null())

    logging.info(f"📅 Datas convertidas e validadas. Restam {df.height} linhas.")
    return df


def calcular_sla(df: pl.DataFrame) -> pd.DataFrame | None:
    """
    Calcula o SLA por base de entrega.
    Retorna um DataFrame com o resumo ou None se a coluna de prazo não for encontrada.
    """
    df = _normalize_columns(df)

    if "BASE DE ENTREGA" not in df.columns:
        logging.warning("Coluna 'BASE DE ENTREGA' não encontrada. Não será possível calcular SLA.")
        return None

    possiveis_nomes_coluna = ["ENTREGUE NO PRAZO?", "ENTREGUE NO PRAZO？"]
    col_upper = [c.upper() for c in df.columns]
    col_prazo = next((df.columns[i] for i, nome in enumerate(col_upper) if nome in possiveis_nomes_coluna), None)

    if col_prazo is None:
        logging.warning("Coluna 'ENTREGUE NO PRAZO?' não encontrada. Não será possível calcular SLA para este DataFrame.")
        return None

    df = df.with_columns(
        pl.when(pl.col(col_prazo).cast(pl.Utf8).str.to_uppercase() == "Y")
        .then(1)
        .otherwise(0)
        .alias("_ENTREGUE")
    )

    resumo = (
        df.group_by("BASE DE ENTREGA")
        .agg([
            pl.len().alias("Total"),
            pl.col("_ENTREGUE").sum().alias("No Prazo"),
            (pl.len() - pl.col("_ENTREGUE").sum()).alias("Fora"),
            (pl.col("_ENTREGUE").sum() / pl.len()).alias("SLA"),
        ])
        .sort("SLA")
    )

    r = resumo.to_pandas()
    r.rename(columns={"BASE DE ENTREGA": "Base De Entrega"}, inplace=True)
    return r


def salvar_relatorio_excel(
    resumo_mes: pd.DataFrame,
    resumo_domingos: pd.DataFrame | None,
    primeiro_dia: date,
    ultimo_dia: date,
    pasta_saida: str,
) -> str:
    """
    ✅ Gera uma planilha NOVA com os resumos do mês e domingos.
    Retorna o caminho completo do arquivo gerado.
    """
    if not os.path.exists(pasta_saida):
        logging.warning(f"⚠️ Pasta de saída não existe. Vou tentar criar: {pasta_saida}")
        os.makedirs(pasta_saida, exist_ok=True)

    agora = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_arquivo = f"SLA_Franquias_{primeiro_dia.strftime('%Y-%m')}_{agora}.xlsx"
    caminho_saida = os.path.join(pasta_saida, nome_arquivo)

    with pd.ExcelWriter(caminho_saida, engine="openpyxl") as writer:
        resumo_mes.to_excel(writer, index=False, sheet_name="SLA_MES")

        if resumo_domingos is not None and not resumo_domingos.empty:
            resumo_domingos.to_excel(writer, index=False, sheet_name="SLA_DOMINGOS")
        else:
            pd.DataFrame({"Info": [f"Sem registros de domingos no período {primeiro_dia} a {ultimo_dia}."]}).to_excel(
                writer, index=False, sheet_name="SLA_DOMINGOS"
            )

        # Aba auxiliar com top piores
        piores_mes = resumo_mes.sort_values("SLA", ascending=True).head(10).copy()
        piores_mes["SLA_%"] = (piores_mes["SLA"] * 100).round(2)
        piores_mes.to_excel(writer, index=False, sheet_name="TOP_10_PIORES")

    logging.info(f"📝 Planilha gerada com sucesso: {caminho_saida}")
    return caminho_saida
# =========================
# BLOCO 4/4 — CARD FEISHU + MAIN
# =========================

def exibir_e_enviar_card(
    resumo_mes: pd.DataFrame,
    resumo_domingos: pd.DataFrame | None,
    primeiro_dia: date,
    ultimo_dia: date
):
    """Exibe o relatório simplificado no terminal e o envia para o Feishu."""
    try:
        logging.info("📤 Preparando relatório simplificado...")

        piores_df_mes = resumo_mes.sort_values(by="SLA", ascending=True).head(4)
        periodo_str = f"{primeiro_dia.strftime('%d/%m')} a {ultimo_dia.strftime('%d/%m')}"
        data_atual_str = datetime.now().strftime("%d/%m/%Y")

        conteudo_piores = (
            f"🚨 **Alerta de SLA — Franquias**\n"
            f"**Atualizado em:** {data_atual_str}\n"
            f"**📉 4 Piores Bases — {periodo_str}**\n\n"
        )

        for _, row in piores_df_mes.iterrows():
            sla_percent = row["SLA"] * 100
            conteudo_piores += f"{row['Base De Entrega']} | SLA: {sla_percent:.2f}%\n"

        if resumo_domingos is None or resumo_domingos.empty:
            conteudo_domingos = f"\n**📊 Domingos do mês — Nenhuma base registrada.**"
        else:
            piores_df_domingos = resumo_domingos.sort_values(by="SLA", ascending=True).head(4)
            conteudo_domingos = (
                f"\n**📉 4 Piores Bases — Domingos do mês ({primeiro_dia.strftime('%m/%Y')})**\n\n"
            )
            for _, row in piores_df_domingos.iterrows():
                sla_percent = row["SLA"] * 100
                conteudo_domingos += f"{row['Base De Entrega']} | SLA: {sla_percent:.2f}%\n"

        conteudo_final = conteudo_piores + conteudo_domingos

        print("\n" + "=" * 80)
        print("📊 RELATÓRIO DE SLA - VISUALIZAÇÃO LOCAL")
        print("=" * 80)
        print(conteudo_final)
        print("=" * 80)
        print("Enviando este relatório para o Feishu...")
        print("=" * 80 + "\n")

        msg = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "template": "red",
                    "title": {"tag": "plain_text", "content": f"SLA Franquias — {periodo_str}"}
                },
                "elements": [
                    {"tag": "markdown", "content": conteudo_final},
                    {
                        "tag": "action",
                        "actions": [
                            {
                                "tag": "button",
                                "text": {"tag": "plain_text", "content": "📁 Abrir Pasta dos Relatórios"},
                                "type": "primary",
                                "url": LINK_PASTA
                            }
                        ]
                    }
                ]
            }
        }

        response = requests.post(WEBHOOK_URL, json=msg, timeout=15)
        response_data = response.json()

        if response.status_code == 200 and response_data.get("code") == 0:
            logging.info("✅ Card enviado com sucesso para o Feishu!")
        else:
            logging.error(f"❌ Erro ao enviar card para o Feishu. Status: {response.status_code}, Resposta: {response.text}")

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Erro de conexão com o Feishu: {e}")
    except Exception as e:
        logging.error(f"❌ Erro inesperado ao enviar card: {e}", exc_info=True)


# ==========================================================
# BLOCO PRINCIPAL DE EXECUÇÃO (MENSAL + DEDUP + SALVAR EXCEL)
# ==========================================================
if __name__ == "__main__":
    try:
        logging.info("🚀 Iniciando script de SLA v12.4 (com export Excel)...")

        # ✅ Check rápido de pasta de saída
        logging.info(f"📌 Pasta de saída configurada: {PASTA_SAIDA}")

        # 1) Processar os dados
        df_consolidado = consolidar_planilhas(PASTA_ENTRADA)
        df_preparado = preparar_coluna_data(df_consolidado)

        df_filtrado = df_preparado.with_columns(
            pl.col("BASE DE ENTREGA").cast(pl.Utf8).str.to_uppercase().str.strip_chars()
        ).filter(
            pl.col("BASE DE ENTREGA").is_in([b.upper() for b in BASES_VALIDAS])
        )

        # 2) Remover duplicatas
        linhas_antes = df_filtrado.height
        df_filtrado = df_filtrado.unique(keep="first")
        linhas_depois = df_filtrado.height
        duplicatas_removidas = linhas_antes - linhas_depois

        if duplicatas_removidas > 0:
            logging.info(f"🧹 Foram encontradas e removidas {duplicatas_removidas} linhas duplicadas.")
        else:
            logging.info("✅ Nenhuma linha duplicada encontrada nos dados.")

        if df_filtrado.is_empty():
            raise ValueError("Nenhuma linha restante após o filtro de bases válidas e remoção de duplicatas.")

        # 3) Definir período de análise (MÊS INTEIRO)
        data_ref = df_filtrado.select(pl.col(COL_DATA_REF)).max().item()
        primeiro_dia = data_ref.replace(day=1)

        if data_ref.month == 12:
            proximo_mes = date(data_ref.year + 1, 1, 1)
        else:
            proximo_mes = date(data_ref.year, data_ref.month + 1, 1)
        ultimo_dia = proximo_mes - timedelta(days=1)

        logging.info(
            f"📆 Período de análise: Mês de {primeiro_dia.strftime('%m/%Y')} "
            f"({primeiro_dia.strftime('%d/%m/%Y')} a {ultimo_dia.strftime('%d/%m/%Y')})"
        )

        # 4) SLA do mês
        df_mes = df_filtrado.filter(pl.col(COL_DATA_REF).is_between(primeiro_dia, ultimo_dia))
        if df_mes.is_empty():
            raise ValueError("Sem dados para o mês atual.")

        resumo_mes = calcular_sla(df_mes)
        if resumo_mes is None:
            raise ValueError("Não foi possível calcular o SLA para o mês.")

        # 5) SLA domingos
        df_domingos = df_mes.filter(pl.col(COL_DATA_REF).dt.weekday() == 6)  # 6 = Domingo
        resumo_domingos = calcular_sla(df_domingos) if not df_domingos.is_empty() else None

        # ✅ 6) GERAR PLANILHA NOVA NA PASTA DE SAÍDA
        caminho_arquivo = salvar_relatorio_excel(
            resumo_mes=resumo_mes,
            resumo_domingos=resumo_domingos,
            primeiro_dia=primeiro_dia,
            ultimo_dia=ultimo_dia,
            pasta_saida=PASTA_SAIDA,
        )
        logging.info(f"✅ Relatório salvo em: {caminho_arquivo}")

        # 7) Exibir + enviar card
        exibir_e_enviar_card(resumo_mes, resumo_domingos, primeiro_dia, ultimo_dia)

        logging.info("🏁 Processo finalizado com sucesso.")

    except Exception as e:
        logging.critical(f"❌ ERRO FATAL: {e}", exc_info=True)
