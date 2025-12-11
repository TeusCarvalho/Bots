# -*- coding: utf-8 -*-
"""
Resumo de entregas por motorista (Entregador)
+ Detalhe de remessas apenas de motoristas selecionados

Baseado no layout real.

Colunas esperadas (com variações toleradas):
- Remessa
- Entregador
- Entregue no prazo？  (ou ?)

Regra de prazo:
- Y = no prazo
- N e vazio = fora do prazo
  (implementado como: tudo que não for Y)

Saídas:
1) resumo_entregas_por_motorista.xlsx
2) remessas_motoristas_alvo.xlsx  (somente se lista tiver itens)
"""

import os
import re
from pathlib import Path
from typing import List, Dict

import polars as pl
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed


# ==========================================================
# CONFIG
# ==========================================================
PASTA_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Nova pasta\Entrega Realizada"

ARQUIVO_SAIDA_RESUMO = os.path.join(PASTA_ENTRADA, "resumo_entregas_por_motorista.xlsx")
ARQUIVO_SAIDA_ALVO = os.path.join(PASTA_ENTRADA, "remessas_motoristas_alvo.xlsx")

# Nomes canônicos internos
COL_REMESSA = "Remessa"
COL_MOTORISTA = "Entregador"
COL_PRAZO = "Entregue no prazo"

# ==========================================================
# LISTA DE MOTORISTAS ALVO (SUA LISTA)
# ==========================================================
MOTORISTAS_ALVO_RAW = [
    "TAC CARRO SNP EURIPEDES ANTONIO GOMES",
    "F PVH - JOAO KLEBER MARQUES NASCIMENTO",
    "TAC MOTO CST - DANIEL DO NASCIMENTO ROCHA",
    "F PVL - RAMON DA MOTA LIMA",
    "TAC CARRO TGA FABIO DE SOUZA DIAS",
    "F PVL 02  -ROGÉRIO SILVA DA ROCHA",
    "TAC CARRO AUG - CARLOS EDUARDO ALMEIDA DOS SANTOS",
    "TAC MOTO ANA - GERLEN JACKELINE DA SILVA ALMEIDA",
    "F PDR - EMERSON VITOR DA SILVA REZENDE",
    "F TLA - JEIFSON SOUZA NEVES",
    "Jefferson Fernando Verão Jara",
    "F SAM - AGNES CARDOSO CIRQUEIRA",
    "F SAM - THIAGO CARDOSO CIQUEIRA",
    "TAC MOTO NMB - JACKSON SILVA BRITO",
    "TAC CARRO - JEAN FIGUEIREDO RANGEL",
    "TAC BICICLETA FMCP - HEVERTON JUNIOR DE SOUZA",
    "TAC CARRO SDA - EDVALDO DOS SANTOS",
    "TAC CARRO FDOM - CLAUDINETO FREITAS MESQUITA",
    "F ALV - BEATRIZ GOMES RODRIGUES",
    "TAC MOTO BRV ANAJAS - ANDERSON ROSA RODRIGUES",
    "TAC MOTO BRC - JOSUE GONCALVES PANTOJA",
    "F CGR 02 -  GEOVANA MARTINE ROCHA",
    "TAC CARRO - SERGIO DE OLIVEIRA PAIVA",
    "TAC CARRO ANA PAULA MIRANDA",
    "TAC MOTO CKS - ALLYSON MENDES DE ALMEIDA",
    "F JPN 02 - JULIERMES SIQUEIRA DE ABREU",
    "F DOU - KARYNE MIRANDA GUEDES",
    "TAC CARRO DNP - NILVA GONÇALVES SANTOS",
    "F POS - SIRLEI ALVES DOS SANTOS",
    "F GYN 03 - DUIANK PATRICIA PEREIRA OLIVEIRA",
    "F CAC - SIDNEY AUGUSTO DE CASTRO",
    "TAC CARRO ARI MYCAELLA LORENA DA COSTA NUNES",
    "F CGR - ELTON SILVA DOURADO",
    "TAC BICICLETA PNA - TATYANE VELENE F PIMENTEL",
    "TAC MOTO - MARCIO RODRIGO",
    "TAC CARRO AUX - WANDERSON GERALD",
    "F GYN - GUILHERME THOMAZ DE ASSIS",
    "F SFX - GEOVANI CABRAL BARBOSA",
    "F POS - MICHEL KENNEDY NERES DE SOUZA",
    "TAC CARRO BEL001- MANOELA DE JESUS SANTOS AMARAL",
    "F PVH - MARIA JOELMA DE OLIVEIRA DA SILVA",
    "TAC MOTO NRE - POLIANA DA SILVA",
    "TAC MOTO ANA - CASSIO DA SILVA COSTA",
    "F CNC - CLEIDE NEVES RODRIGUES",
    "F CEI - RAFAEL RIBEIRO VASCONCELOS",
    "TAC CARRO CGB ELAINE ALENCAR SILVA CALISTO",
    "TAC BICICLETA ANAF - MANOEL PAIXAO NOGUEIRA SA",
    "TAC CARRO TAUA - ALEX DA SILVA MONTEIRO",
    "F BSB - NAOR GOMES DA SILVA FARIAS",
    "F RBR - ROSVALDO RODRIGUES DA SILVA",
    "F SEN - VERA LUCIA QUERINO DA SILVA",
    "F TGA -  REGERIO FELIX DA SILVA",
    "F SEN - MARIO VINICIUS SILVA DA CRUZ",
    "TAC CARRO CKS - ROBERTO FERREIRA DOS SANTOS",
    "TAC MOTO PDT - FERNANDO MATHEUS MOURA ALVES",
    "F GYN 02 - WELLINGTON EDMUNDO DE OLIVEIRA",
    "F PVL - LUCIENE E R CARNEIRO",
    "TAC CARRO CNA - AUGUSTO CESAR MAMEDE OEIRAS",
    "TAC CARRO VCP - BENEDITO BRUNO MIRANDA PEREIRA",
    "F TGA - KELLYANE GONÇALVES",
    "F AGL - THIAGO AROUCHA DE OLIVEIRA",
    "TAC BICICLETA PMW3 - CARLOS DANIEL CHAGAS MAMEDEIO",
    "TAC CARRO - ADRIENE MOREIRA BARBOSA",
    "TAC MOTO ICR - LETICIA MENDES SACRAMENTO",
    "MEI CARRO GNT - MATHEUS GLOSS SILVA",
    "F CAC - BRUNA FERNANDES PAZARRO",
    "F GYN 03 - JOAO FERNANDES RODRIGUES PEREIRA",
    "F ELD - WYCLEFF CUNHA DE OLIVEIRA",
    "F VHL - RODRIGO DOS SANTOS VIEIRA",
    "F SJA - KISLEI DANILO TAVARES DE SOUSA",
    "TAC CARRO - ALESSANDRO BATISTA PIRES",
    "F PVH - CAIO HENRIQUE SANTOS DO NASCIMENTO",
    "F TLA - EDGAR SIDNEY DA SILVA BARROS",
    "MEI CARRO- EPITACIO CANDIDO RODRIGUES NETO",
    "TAC MOTO SDA - FRANK JUNIO RIBEIRO BATISTA",
    "TAC MOTO FTUR - GILVAN LIRA DA SILVA",
    "F APG - VANJHONATA DE SOUZA OLIVEIRA",
    "TAC MOTO BRG - JORDANA BIANCA DAMACENO SOUSA",
    "TAC MOTO MJU - IVANILDO DO NASCIMENTO RODRIGUES",
    "TAC BIKE MAO F - RAIMUNDO NONATO RAMOS DOS SANTOS",
    "F GYN 03 - JOAO PAULO FERREIRA DE SOUSA",
    "TAC MOTO BRENO DANRLEY CARNEIRO DA SILVA",
    "TAC CARRO STM - DAIAN BRANCHES FIGUEIREDO",
    "TAC CARRO - JORGE BATISTA DE SOUZA",
    "F TGT - ERIKA APARECIDA DE OLIVEIRA SILVA LOBO",
    "F PGM - NAILSON OLIVEIRA DA SILVA",
    "F PVH - VINICIUS MARTINS DE OLIVEIRA",
    "TAC CARRO-ANDERSON RIVANI BASTISTA DOS SANTOS",
    "F DOU - ROSANA DA CONCEICAO DA SILVA",
    "F EMA - MARIA JOSE DA ROCHA",
    "F VHL - JOSE WILLIAN GONCALVES DE SOUZA",
    "F PVH - AFONSO LACERDA GOMES",
    "F CAC - OSCAR ROMERIO GOMES",
    "TAC MOTO ELSON DA ROCHA FROTA",
    "F GYN - FRANCIELLE CORDEIRO DOS SANTOS DIAS",
    "F PDT - SIMIAO PINTO DA COSTA",
    "Erivania da Silva",
    "Daianne Fernandes Silva",
    "F GYN 02 - MIKAEL DOUGLAS SILVA BASTOS",
    "MEI CARRO - LUCAS NASCIMENTO LIMA",
    "F  STM - SAMARA DOS SANTOS LIMA",
    "F PVL - ALEX MACEDO DA SILVA",
    "F RBR - IAGO DA SILVA FERNANDES LEON",
    "TAC CARRO BVB - MISCILENE PEREIRA SILVA",
    "TAC CARRO - RODRIGO LUIZ VICENTE",
    "F GYN 2 - MARCO AURELIO DE SOUSA TRINDADE",
    "F AMB - NILDA CLARA DA SILVA PIVETA",
    "F RVD - SANDY EMANEULLE SILVA LIMA",
    "TAC CARRO CKS - ANTONIA MARIA SABINA DA SILVA",
    "TAC MOTO - ANTÔNIO LUCAS LOPES DE SOUZA",
    "F GYN - SEGINALDO ANTONIO DA SILVA",
    "TAC CARRO CGB GEFFERSON CARLOS DE ALMEIDA",
    "TAC MOTO BVD - ESAEL FERNANDO LIMA FERREIRA",
    "F MRL - CARLOS CAJUEIRO GOMES",
    "F VHL - JULIANE DE MORAES BACH DA SILVA",
    "TAC MOTO BRC - YURI BALIEIRO OLIVEIRA",
    "TAC CARRO NVT JULIO CESAR",
    "F CGR 02 -  ROBERT DANIEL FREIRE CORDOBA",
    "F GYN - CARLOS HELIABY NEVES VIEIRA",
    "F MCP 02 - GUIOVANE ARAÚJO ALVES",
    "TAC CARRO CST - JORGE NILSON SIQUEIRA GASPAR",
    "F CGR - YASMIN SANDIM DE OLIVEIRA",
    "TAC CARRO SDA - SANDRA RAIMUNDA FEITOSA FARIAS",
    "F JPN 02 - DOUGLAS SIQUEIRA FIRMINO",
    "TAC MOTO FTLA - JOAO GABRIEL DE ARAUJO GOMES",
    "F DOM - JORDESSON SILVA MARQUES",
    "MEI CARRO - PAULO MURILO SANTOS DA SILVA",
    "TAC BICICLETA ANAF - IVAN GONDIN DE PAIVA",
    "TAC CARRO - RUBIA CORTES OTERO",
    "F PVH 02 - JULIANA CORREIA",
    "F TRD - MARISETE MARTINS ROSA",
    "MEI CARRO - MACKELLE DE OLIVEIRA MUMBACH",
    "TAC CARRO AUX - MARCOS ANTONIO ROCHA BARCELOS",
    "F MCP - AURELIANO COELHO SFAIR PIRES",
    "F PVH - PATRICIA SUANE DE ANDRADE DOS SANTOS",
    "TAC CARRO BVB - NATALIA RAMALHO FERREIRA",
    "TAC MOTO NRE -  GABRIEL DE MATOS",
    "TAC MOTO BVD - CLAUDIO DO SOCORRO SANTOS PAIVA",
    "F ARQ - ELIZA LOPES DALLA COSTA",
    "TAC CARRO SRS JOSÉ FRANCISCO DA SILVA VERAS",
    "F TGT - Bruno Miranda Carvalho",
    "TAC CARRO - JEFERSON EDUARDO DA SILVA MUNIZ",
    "TAC MOTO BRC - EVALDO SOUZA DE SENA",
    "F MAC - JAQUELINE GUEDES RAMOS",
    "F PVH 02 - RODRIGO ATILIO MONTEIRO",
    "TAC CARRO CDT - JUNIOR BATISTA DA SILVA",
    "F STM - RIVELINO FERREIRA DOS SANTOS",
    "MEI CARRO VGR RONAIR JOSE DE PAULA",
    "TAC CARRO MAO - MARIA TANIA SILVA DA COSTA",
    "F FMA - FLAVIO VIEIRA RODRIGUES",
    "TAC MOTO BRC - EDER LUIS MIRANDA PEREIRA",
    "F PON - KAREN LAURITA PANTA DE FREITAS",
    "F BSB - ANA LUISA RUFINO MIRA",
    "TAC MOTO ABT - ISRAEL CARDOSO MEDEIROS",
    "TAC MOTO ELISSON BARBOSA DE CASTRO",
    "F CGR - SAMELA ARAUJO DE ARRUDA",
    "TAC MOTO ABT - MANUEL DE JESUS DA FONSECA ALMEIDA",
    "F GYN - ILDICLEIA DE LIMA FERREIRA",
    "F GYN - SIRLEY CASTILHO CABRAL DA CRUZ",
    "TAC CARRO NVT LETICIA LAMPERT",
    "TAC CARRO - JADSON LUAN SILVA GOMES",
    "TAC CARRO PMW 002 - HELIO MIRANDA DOS SANTOS",
    "F MAC - ADENOR NOGUEIRA DOS SANTOS",
    "TAC CARRO - JANE MICHELE  ALVES SIQUEIRA",
    "F VLP - MANOEL RAIMUNDO CHAVES SIMAS",
    "F TLA - ANTONIO ROGERIO ACACIO DE SOUSA",
    "F ANP - PRICILA ALVES SANTOS",
    "TAC MOTO CDT - EURISMAR VALADARES SARAIVA",
    "TAC CARRO NMB - LILIAN VITORIA REIS VILANOVA",
    "TAC MOTO - ESTEVAO FERREIRA DOS SANTOS NETO",
    "F SAM - EDMILSON DIAS DE OLIVEIRA",
    "F GYN - CRISTIANO PEREIRA ROSA",
    "F ANA - MAYCON FELIPE MARQUES DE OLIVEIRA",
    "F MCP - RUAN CARLOS FURTADO CABRAL",
    "F PVH - LEONAN FERREIRA DOS SANTOS",
    "TAC MOTO ANA - ADRIANO FERREIRA SALES FILHO",
    "F ARQ - GILBERTO FERREIRA DE JESUS DOS SANTOS",
    "TAC CARRO ROO - FABRICIO SANTOS FREITAS",
    "F PDT - FERNANDO MATHEUS MOURA ALVES",
    "F BVB - JOSE CARLOS NUNES",
    "TAC CARRO - JAMES PEREIRA DE SOUZA",
]

# Matching:
# - "contains": tolerante
# - "exact": rigoroso
MOTORISTAS_MATCH_MODE = "contains"

# ==========================================================
# PERFORMANCE
# ==========================================================
CPU = os.cpu_count() or 2
BASE_MAX_WORKERS = min(6, CPU)
# ==========================================================
# NORMALIZAÇÃO DE COLUNAS
# ==========================================================
def _norm_col_name(c: str) -> str:
    if not isinstance(c, str):
        return str(c)
    return c.strip().replace("？", "?").replace("\u00A0", " ")


def _canonicalize_columns(df_pd: pd.DataFrame) -> pd.DataFrame:
    col_map: Dict[str, str] = {}

    for col in df_pd.columns:
        raw = col
        norm = _norm_col_name(col).lower()

        if norm == _norm_col_name("Remessa").lower():
            col_map[raw] = COL_REMESSA

        elif norm == _norm_col_name("Entregador").lower():
            col_map[raw] = COL_MOTORISTA

        elif norm in {
            _norm_col_name("Entregue no prazo?").lower(),
            _norm_col_name("Entregue no prazo").lower()
        }:
            col_map[raw] = COL_PRAZO

    if col_map:
        df_pd = df_pd.rename(columns=col_map)

    df_pd.columns = [_norm_col_name(c) for c in df_pd.columns]
    return df_pd


# ==========================================================
# LISTA ALVO (LIMPEZA)
# ==========================================================
def _clean_name(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.strip()
    # troca múltiplos espaços por um
    s = re.sub(r"\s+", " ", s)
    return s


def build_motoristas_alvo() -> List[str]:
    cleaned = [_clean_name(x) for x in MOTORISTAS_ALVO_RAW]
    cleaned = [c for c in cleaned if c]

    # dedup mantendo ordem
    seen = set()
    out = []
    for c in cleaned:
        up = c.upper()
        if up not in seen:
            seen.add(up)
            out.append(c)
    return out


# ==========================================================
# LEITURA
# ==========================================================
def listar_excels(pasta: str) -> List[Path]:
    p = Path(pasta)
    if not p.exists() or not p.is_dir():
        return []
    files: List[Path] = []
    for ext in ("*.xlsx", "*.xls"):
        files.extend(p.glob(ext))
    files = [f for f in files if not f.name.startswith("~$")]
    return sorted(files)


def ler_excel_todas_abas(path: str) -> pl.DataFrame:
    try:
        xls = pd.ExcelFile(path)
    except Exception:
        return pl.DataFrame()

    dfs = []

    for sheet in xls.sheet_names:
        try:
            df_pd = pd.read_excel(xls, sheet_name=sheet)
            if df_pd is None or df_pd.empty:
                continue

            df_pd = _canonicalize_columns(df_pd)

            if COL_MOTORISTA not in df_pd.columns or COL_REMESSA not in df_pd.columns:
                continue

            if COL_PRAZO not in df_pd.columns:
                df_pd[COL_PRAZO] = ""

            # reduz RAM: mantém só o necessário
            keep = [COL_REMESSA, COL_MOTORISTA, COL_PRAZO]
            keep = [c for c in keep if c in df_pd.columns]
            df_pd = df_pd[keep].copy()

            df_pd["__arquivo_origem"] = Path(path).name
            df_pd["__aba_origem"] = sheet

            dfs.append(df_pd)

        except Exception:
            continue

    if not dfs:
        return pl.DataFrame()

    df_all = pd.concat(dfs, ignore_index=True)
    return pl.from_pandas(df_all)


def processar_arquivo(path: str) -> pl.DataFrame:
    return ler_excel_todas_abas(path)


# ==========================================================
# PREPARAÇÃO
# ==========================================================
def preparar_df(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    if COL_MOTORISTA not in df.columns or COL_REMESSA not in df.columns:
        return pl.DataFrame()

    if COL_PRAZO not in df.columns:
        df = df.with_columns(pl.lit("").alias(COL_PRAZO))

    df = df.with_columns([
        pl.col(COL_MOTORISTA).cast(pl.Utf8).fill_null("").str.strip_chars().alias(COL_MOTORISTA),
        pl.col(COL_REMESSA).cast(pl.Utf8).fill_null("").str.strip_chars().alias(COL_REMESSA),
        pl.col(COL_PRAZO).cast(pl.Utf8).fill_null("").str.strip_chars().str.to_uppercase().alias("__prazo_flag"),
    ])

    # Motorista-chave: antes do "|"
    df = df.with_columns([
        pl.col(COL_MOTORISTA)
          .str.split("|")
          .list.get(0)
          .fill_null("")
          .str.strip_chars()
          .alias("__motorista_key")
    ])

    # regra de prazo
    df = df.with_columns([
        (pl.col("__prazo_flag") == "Y").alias("__no_prazo"),
        (pl.col("__prazo_flag") != "Y").alias("__fora_prazo"),
    ])

    return df


# ==========================================================
# RESUMO
# ==========================================================
def gerar_resumo(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    resumo = (
        df.group_by("__motorista_key")
          .agg([
              pl.len().alias("Linhas"),
              pl.col(COL_REMESSA).n_unique().alias("Qtd pedidos (únicos)"),
              pl.sum("__no_prazo").alias("Qtd no prazo"),
              pl.sum("__fora_prazo").alias("Qtd fora do prazo"),
          ])
          .with_columns([
              pl.when(pl.col("Qtd pedidos (únicos)") > 0)
                .then(pl.col("Qtd no prazo") / pl.col("Qtd pedidos (únicos)"))
                .otherwise(0.0)
                .alias("Taxa no prazo (calc.)")
          ])
          .rename({"__motorista_key": COL_MOTORISTA})
          .sort("Qtd pedidos (únicos)", descending=True)
    )

    return resumo


# ==========================================================
# FILTRO ALVO
# ==========================================================
def filtrar_motoristas_alvo(df: pl.DataFrame, motoristas_alvo: List[str]) -> pl.DataFrame:
    if df.is_empty() or not motoristas_alvo:
        return pl.DataFrame()

    alvo_up = [m.upper() for m in motoristas_alvo if m.strip()]
    if not alvo_up:
        return pl.DataFrame()

    base = df.with_columns(
        pl.col("__motorista_key").cast(pl.Utf8).fill_null("").str.to_uppercase().alias("__motorista_key_upper")
    )

    if MOTORISTAS_MATCH_MODE.lower() == "exact":
        return base.filter(pl.col("__motorista_key_upper").is_in(alvo_up)).drop("__motorista_key_upper")

    # contains
    cond = None
    for a in alvo_up:
        c = pl.col("__motorista_key_upper").str.contains(a, literal=True)
        cond = c if cond is None else (cond | c)

    return base.filter(cond).drop("__motorista_key_upper")


# ==========================================================
# EXPORT
# ==========================================================
def salvar_excel_resumo(resumo: pl.DataFrame, saida: str):
    df_pd = resumo.to_pandas()
    if "Taxa no prazo (calc.)" in df_pd.columns:
        df_pd["Taxa no prazo (calc.)"] = (df_pd["Taxa no prazo (calc.)"] * 100).round(2)

    with pd.ExcelWriter(saida, engine="openpyxl") as writer:
        df_pd.to_excel(writer, index=False, sheet_name="resumo_motoristas")


def salvar_excel_alvo(df_alvo: pl.DataFrame, saida: str):
    if df_alvo.is_empty():
        return

    cols = [
        COL_REMESSA,
        "__motorista_key",
        COL_MOTORISTA,
        COL_PRAZO,
        "__no_prazo",
        "__fora_prazo",
        "__arquivo_origem",
        "__aba_origem",
    ]
    cols = [c for c in cols if c in df_alvo.columns]

    out = df_alvo.select(cols).sort("__motorista_key")

    df_pd = out.to_pandas()
    with pd.ExcelWriter(saida, engine="openpyxl") as writer:
        df_pd.to_excel(writer, index=False, sheet_name="remessas_motoristas_alvo")


# ==========================================================
# MAIN
# ==========================================================
def main():
    os.environ["POLARS_MAX_THREADS"] = str(CPU)

    if not os.path.isdir(PASTA_ENTRADA):
        print(f"❌ Pasta não encontrada: {PASTA_ENTRADA}")
        return

    arquivos = listar_excels(PASTA_ENTRADA)
    if not arquivos:
        print("❌ Nenhum Excel encontrado na pasta.")
        return

    motoristas_alvo = build_motoristas_alvo()
    print(f"✅ Motoristas alvo carregados: {len(motoristas_alvo)}")

    max_workers = min(BASE_MAX_WORKERS, len(arquivos))
    max_workers = max(1, max_workers)

    dfs: List[pl.DataFrame] = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(processar_arquivo, str(arq)): arq for arq in arquivos}

        for fut in as_completed(futures):
            arq = futures[fut]
            try:
                df_part = fut.result()
                if df_part is not None and not df_part.is_empty():
                    dfs.append(df_part)
            except Exception as e:
                print(f"⚠️ Falha ao ler: {arq.name} | {e}")

    if not dfs:
        print("❌ Nenhuma aba válida com Remessa/Entregador.")
        return

    df = pl.concat(dfs, how="vertical_relaxed")
    df = preparar_df(df)

    if df.is_empty():
        print("❌ Estrutura inválida após preparação.")
        return

    # 1) Resumo geral
    resumo = gerar_resumo(df)
    if resumo.is_empty():
        print("❌ Não foi possível gerar resumo.")
        return

    salvar_excel_resumo(resumo, ARQUIVO_SAIDA_RESUMO)
    print(f"✅ Resumo gerado: {ARQUIVO_SAIDA_RESUMO}")

    # 2) Detalhe só dos motoristas alvo
    if motoristas_alvo:
        df_alvo = filtrar_motoristas_alvo(df, motoristas_alvo)

        if df_alvo.is_empty():
            print("⚠️ Nenhuma remessa encontrada para os motoristas alvo.")
        else:
            salvar_excel_alvo(df_alvo, ARQUIVO_SAIDA_ALVO)
            print(f"✅ Detalhe dos motoristas alvo: {ARQUIVO_SAIDA_ALVO}")


if __name__ == "__main__":
    main()
