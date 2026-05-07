import os
import re
import unicodedata
import logging
from pathlib import Path
from datetime import datetime, date, timedelta

import pandas as pd


# ============================================================
# CONFIGURAÇÕES
# ============================================================

PASTA_ENTRADA = Path(
    r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Área de Trabalho\Isso ai é uma pasta, só não abre\06.1-  SLA Entrega Realizada Franquia - mês passado"
)

PASTA_SAIDA = PASTA_ENTRADA / "_Resultado_SLA_Mes_Passado"

META_NORMAL = 0.95
META_DOMINGO_FERIADO = 0.70

LER_TODAS_ABAS = False

EXTENSOES_ACEITAS = [".xlsx", ".xls", ".csv"]

DATA_HORA_EXECUCAO = datetime.now().strftime("%Y%m%d_%H%M%S")

NOME_ARQUIVO_RESULTADO = f"Resumo_SLA_Mes_Passado_{DATA_HORA_EXECUCAO}.xlsx"
NOME_ARQUIVO_BASE_CSV = f"Base_Calculada_SLA_Mes_Passado_{DATA_HORA_EXECUCAO}.csv"


# ============================================================
# LOG
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def remover_acentos(texto: str) -> str:
    if texto is None:
        return ""

    texto = str(texto)
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))

    return texto


def normalizar_coluna(coluna: str) -> str:
    coluna = remover_acentos(coluna)
    coluna = coluna.lower().strip()
    coluna = re.sub(r"[^a-z0-9]+", " ", coluna)
    coluna = re.sub(r"\s+", " ", coluna).strip()

    return coluna


def achar_coluna(df: pd.DataFrame, candidatos: list[str]) -> str | None:
    mapa_colunas = {normalizar_coluna(c): c for c in df.columns}
    candidatos_norm = [normalizar_coluna(c) for c in candidatos]

    for cand in candidatos_norm:
        if cand in mapa_colunas:
            return mapa_colunas[cand]

    for cand in candidatos_norm:
        for col_norm, col_original in mapa_colunas.items():
            if cand in col_norm or col_norm in cand:
                return col_original

    return None


def calcular_pascoa(ano: int) -> date:
    """
    Calcula a data da Páscoa.
    Usado para identificar Sexta-feira Santa, Carnaval e Corpus Christi.
    """

    a = ano % 19
    b = ano // 100
    c = ano % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 100 % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    mes = (h + l - 7 * m + 114) // 31
    dia = ((h + l - 7 * m + 114) % 31) + 1

    return date(ano, mes, dia)


def feriados_nacionais_br(ano: int) -> set[date]:
    pascoa = calcular_pascoa(ano)

    feriados = {
        date(ano, 1, 1),     # Confraternização Universal
        date(ano, 4, 21),    # Tiradentes
        date(ano, 5, 1),     # Dia do Trabalho
        date(ano, 9, 7),     # Independência do Brasil
        date(ano, 10, 12),   # Nossa Senhora Aparecida
        date(ano, 11, 2),    # Finados
        date(ano, 11, 15),   # Proclamação da República
        date(ano, 11, 20),   # Consciência Negra
        date(ano, 12, 25),   # Natal

        pascoa - timedelta(days=2),    # Sexta-feira Santa
        pascoa - timedelta(days=47),   # Carnaval segunda
        pascoa - timedelta(days=46),   # Carnaval terça
        pascoa + timedelta(days=60),   # Corpus Christi
    }

    return feriados


def converter_data_serie(serie: pd.Series) -> pd.Series:
    """
    Converte datas em formatos mistos:
    - 2026-04-01
    - 09/04/2026
    - datetime do Excel
    - números seriais do Excel
    """

    s_original = serie.copy()

    resultado = pd.to_datetime(
        s_original,
        errors="coerce",
        dayfirst=True
    )

    mascara_vazia = resultado.isna()

    if mascara_vazia.any():
        s_num = pd.to_numeric(
            s_original[mascara_vazia],
            errors="coerce"
        )

        datas_excel = pd.to_datetime(
            s_num,
            errors="coerce",
            unit="D",
            origin="1899-12-30"
        )

        resultado.loc[mascara_vazia] = datas_excel

    return resultado


def classificar_tipo_dia(data_ref: date, feriados: set[date]) -> str:
    if data_ref in feriados:
        return "Feriado"

    if data_ref.weekday() == 6:
        return "Domingo"

    return "Normal"


def meta_por_tipo_dia(tipo_dia: str) -> float:
    if tipo_dia in ["Domingo", "Feriado"]:
        return META_DOMINGO_FERIADO

    return META_NORMAL


def ler_arquivo(caminho: Path) -> pd.DataFrame:
    logging.info(f"Lendo arquivo: {caminho.name}")

    if caminho.suffix.lower() == ".csv":
        try:
            df = pd.read_csv(
                caminho,
                sep=None,
                engine="python",
                encoding="utf-8",
                dtype=object
            )
        except UnicodeDecodeError:
            df = pd.read_csv(
                caminho,
                sep=None,
                engine="python",
                encoding="latin1",
                dtype=object
            )

        df["__arquivo_origem"] = caminho.name
        return df

    if caminho.suffix.lower() in [".xlsx", ".xls"]:
        if LER_TODAS_ABAS:
            abas = pd.read_excel(
                caminho,
                sheet_name=None,
                dtype=object
            )

            lista = []

            for nome_aba, df_aba in abas.items():
                df_aba["__arquivo_origem"] = caminho.name
                df_aba["__aba_origem"] = nome_aba
                lista.append(df_aba)

            if lista:
                return pd.concat(lista, ignore_index=True)

            return pd.DataFrame()

        df = pd.read_excel(caminho, dtype=object)
        df["__arquivo_origem"] = caminho.name

        return df

    return pd.DataFrame()


def listar_arquivos(pasta: Path) -> list[Path]:
    arquivos = []

    for ext in EXTENSOES_ACEITAS:
        arquivos.extend(pasta.rglob(f"*{ext}"))

    arquivos_validos = []

    for arq in arquivos:
        nome = arq.name.lower()
        caminho_txt = str(arq).lower()

        if nome.startswith("~$"):
            continue

        if "_resultado_sla_mes_passado" in caminho_txt:
            continue

        arquivos_validos.append(arq)

    return arquivos_validos


# ============================================================
# PROCESSAMENTO PRINCIPAL
# ============================================================

def main():
    if not PASTA_ENTRADA.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {PASTA_ENTRADA}")

    PASTA_SAIDA.mkdir(parents=True, exist_ok=True)

    arquivos = listar_arquivos(PASTA_ENTRADA)

    if not arquivos:
        raise FileNotFoundError("Nenhum arquivo Excel/CSV encontrado na pasta informada.")

    logging.info(f"Total de arquivos encontrados: {len(arquivos)}")

    bases = []

    for arquivo in arquivos:
        try:
            df = ler_arquivo(arquivo)

            if df.empty:
                logging.warning(f"Arquivo vazio ignorado: {arquivo.name}")
                continue

            df["__arquivo_origem"] = arquivo.name
            bases.append(df)

        except Exception as e:
            logging.error(f"Erro ao ler {arquivo.name}: {e}")

    if not bases:
        raise ValueError("Nenhuma base válida foi carregada.")

    base = pd.concat(bases, ignore_index=True)

    logging.info(f"Total de linhas carregadas: {len(base):,}")

    # ------------------------------------------------------------
    # IDENTIFICAR COLUNAS
    # ------------------------------------------------------------

    col_data_prevista = achar_coluna(
        base,
        [
            "DATA PREVISTA DE ENTREGA",
            "Data prevista de entrega",
            "Data prevista entrega",
            "Previsão de entrega",
            "Previsao de entrega",
            "Data prevista",
        ]
    )

    col_data_entrega = achar_coluna(
        base,
        [
            "Horário da entrega",
            "Horario da entrega",
            "Data da entrega",
            "Data entrega",
            "Entrega realizada",
            "Horário de entrega",
            "Horario de entrega",
            "Delivery time",
        ]
    )

    col_base = achar_coluna(
        base,
        [
            "Base de entrega",
            "Nome da base de entrega",
            "Nome da base",
            "Base",
            "Unidade responsável",
            "Unidade responsavel",
        ]
    )

    col_pedido = achar_coluna(
        base,
        [
            "Remessa",
            "Pedido",
            "Número do pedido",
            "Numero do pedido",
            "Nº pedido",
            "N pedido",
            "Waybill",
            "Waybill No",
            "Tracking",
            "Código do pedido",
            "Codigo do pedido",
        ]
    )

    if col_data_prevista is None:
        raise ValueError(
            "Não encontrei a coluna de Data Prevista de Entrega. "
            "Verifique se existe uma coluna como 'DATA PREVISTA DE ENTREGA'."
        )

    if col_data_entrega is None:
        raise ValueError(
            "Não encontrei a coluna de Horário/Data da Entrega. "
            "Verifique se existe uma coluna como 'Horário da entrega'."
        )

    logging.info(f"Coluna de data prevista encontrada: {col_data_prevista}")
    logging.info(f"Coluna de entrega encontrada: {col_data_entrega}")

    if col_base:
        logging.info(f"Coluna de base encontrada: {col_base}")
    else:
        logging.warning("Coluna de base não encontrada. O resumo por base será gerado sem base.")

    if col_pedido:
        logging.info(f"Coluna de pedido encontrada: {col_pedido}")
    else:
        logging.warning("Coluna de pedido não encontrada. O total será contado por linha.")

    # ------------------------------------------------------------
    # TRATAMENTO DAS DATAS
    # ------------------------------------------------------------

    base["DATA_PREVISTA_TRATADA"] = converter_data_serie(base[col_data_prevista])
    base["DATA_ENTREGA_TRATADA"] = converter_data_serie(base[col_data_entrega])

    base = base[base["DATA_PREVISTA_TRATADA"].notna()].copy()

    if base.empty:
        raise ValueError("Após tratar as datas, nenhuma linha ficou com Data Prevista válida.")

    base["DATA_PREVISTA_DIA"] = base["DATA_PREVISTA_TRATADA"].dt.date
    base["DATA_ENTREGA_DIA"] = base["DATA_ENTREGA_TRATADA"].dt.date

    # ------------------------------------------------------------
    # REMOVER DUPLICADOS POR PEDIDO/DATA
    # ------------------------------------------------------------

    if col_pedido:
        antes = len(base)

        base[col_pedido] = base[col_pedido].astype(str).str.strip()

        base = base.sort_values(
            by=["DATA_PREVISTA_TRATADA", "DATA_ENTREGA_TRATADA"],
            ascending=[True, True]
        )

        base = base.drop_duplicates(
            subset=[col_pedido, "DATA_PREVISTA_DIA"],
            keep="first"
        )

        depois = len(base)

        logging.info(f"Duplicados removidos por pedido/data: {antes - depois:,}")

    # ------------------------------------------------------------
    # CÁLCULO DE ENTREGUE NO PRAZO
    # ------------------------------------------------------------

    base["ENTREGUE"] = base["DATA_ENTREGA_DIA"].notna()

    base["ENTREGUE_NO_PRAZO"] = (
        base["ENTREGUE"]
        & (base["DATA_ENTREGA_DIA"] <= base["DATA_PREVISTA_DIA"])
    )

    base["FORA_DO_PRAZO"] = ~base["ENTREGUE_NO_PRAZO"]

    # ------------------------------------------------------------
    # DOMINGO / FERIADO / META
    # ------------------------------------------------------------

    anos = sorted({d.year for d in base["DATA_PREVISTA_DIA"].dropna()})

    feriados = set()

    for ano in anos:
        feriados.update(feriados_nacionais_br(ano))

    base["TIPO_DIA"] = base["DATA_PREVISTA_DIA"].apply(
        lambda d: classificar_tipo_dia(d, feriados)
    )

    base["META_SLA"] = base["TIPO_DIA"].apply(meta_por_tipo_dia)
    base["META_SLA_%"] = base["META_SLA"] * 100

    # ------------------------------------------------------------
    # RESUMO POR DATA
    # ------------------------------------------------------------

    resumo_data = (
        base
        .groupby(["DATA_PREVISTA_DIA", "TIPO_DIA", "META_SLA_%"], dropna=False)
        .agg(
            TOTAL_PEDIDOS=("ENTREGUE_NO_PRAZO", "size"),
            PEDIDOS_ENTREGUES_NO_PRAZO=("ENTREGUE_NO_PRAZO", "sum"),
            PEDIDOS_FORA_DO_PRAZO=("FORA_DO_PRAZO", "sum"),
        )
        .reset_index()
    )

    resumo_data["SLA_%"] = (
        resumo_data["PEDIDOS_ENTREGUES_NO_PRAZO"]
        / resumo_data["TOTAL_PEDIDOS"]
        * 100
    ).round(2)

    resumo_data["STATUS_META"] = resumo_data.apply(
        lambda row: "Dentro da meta"
        if row["SLA_%"] >= row["META_SLA_%"]
        else "Abaixo da meta",
        axis=1
    )

    resumo_data = resumo_data.sort_values("DATA_PREVISTA_DIA")

    # ------------------------------------------------------------
    # RESUMO POR BASE E DATA
    # ------------------------------------------------------------

    if col_base:
        resumo_base_data = (
            base
            .groupby([col_base, "DATA_PREVISTA_DIA", "TIPO_DIA", "META_SLA_%"], dropna=False)
            .agg(
                TOTAL_PEDIDOS=("ENTREGUE_NO_PRAZO", "size"),
                PEDIDOS_ENTREGUES_NO_PRAZO=("ENTREGUE_NO_PRAZO", "sum"),
                PEDIDOS_FORA_DO_PRAZO=("FORA_DO_PRAZO", "sum"),
            )
            .reset_index()
        )

        resumo_base_data["SLA_%"] = (
            resumo_base_data["PEDIDOS_ENTREGUES_NO_PRAZO"]
            / resumo_base_data["TOTAL_PEDIDOS"]
            * 100
        ).round(2)

        resumo_base_data["STATUS_META"] = resumo_base_data.apply(
            lambda row: "Dentro da meta"
            if row["SLA_%"] >= row["META_SLA_%"]
            else "Abaixo da meta",
            axis=1
        )

        resumo_base_data = resumo_base_data.sort_values(
            by=["DATA_PREVISTA_DIA", col_base]
        )

    else:
        resumo_base_data = pd.DataFrame()

    # ------------------------------------------------------------
    # RESUMO GERAL
    # ------------------------------------------------------------

    total_pedidos = len(base)
    total_no_prazo = int(base["ENTREGUE_NO_PRAZO"].sum())
    total_fora_prazo = int(base["FORA_DO_PRAZO"].sum())

    sla_geral = round(
        total_no_prazo / total_pedidos * 100,
        2
    ) if total_pedidos else 0

    resumo_geral = pd.DataFrame(
        [
            {
                "TOTAL_PEDIDOS": total_pedidos,
                "PEDIDOS_ENTREGUES_NO_PRAZO": total_no_prazo,
                "PEDIDOS_FORA_DO_PRAZO": total_fora_prazo,
                "SLA_GERAL_%": sla_geral,
                "META_NORMAL_%": META_NORMAL * 100,
                "META_DOMINGO_FERIADO_%": META_DOMINGO_FERIADO * 100,
                "DATA_INICIAL": min(base["DATA_PREVISTA_DIA"]),
                "DATA_FINAL": max(base["DATA_PREVISTA_DIA"]),
            }
        ]
    )

    # ------------------------------------------------------------
    # FORMATAR DATAS PARA SAÍDA
    # ------------------------------------------------------------

    resumo_data["DATA_PREVISTA_DIA"] = pd.to_datetime(
        resumo_data["DATA_PREVISTA_DIA"]
    )

    base["DATA_PREVISTA_DIA"] = pd.to_datetime(
        base["DATA_PREVISTA_DIA"]
    )

    base["DATA_ENTREGA_DIA"] = pd.to_datetime(
        base["DATA_ENTREGA_DIA"]
    )

    if not resumo_base_data.empty:
        resumo_base_data["DATA_PREVISTA_DIA"] = pd.to_datetime(
            resumo_base_data["DATA_PREVISTA_DIA"]
        )

    # ------------------------------------------------------------
    # DEFINIR COLUNAS DA BASE CALCULADA
    # ------------------------------------------------------------

    colunas_base_saida = []

    if col_pedido:
        colunas_base_saida.append(col_pedido)

    if col_base:
        colunas_base_saida.append(col_base)

    colunas_base_saida.extend([
        col_data_prevista,
        col_data_entrega,
        "__arquivo_origem",
        "DATA_PREVISTA_DIA",
        "DATA_ENTREGA_DIA",
        "TIPO_DIA",
        "META_SLA_%",
        "ENTREGUE",
        "ENTREGUE_NO_PRAZO",
        "FORA_DO_PRAZO",
    ])

    colunas_base_saida = [c for c in colunas_base_saida if c in base.columns]

    # ------------------------------------------------------------
    # SALVAR RESULTADOS
    # ------------------------------------------------------------

    caminho_saida_excel = PASTA_SAIDA / NOME_ARQUIVO_RESULTADO
    caminho_saida_csv = PASTA_SAIDA / NOME_ARQUIVO_BASE_CSV

    # Salva a base completa em CSV, pois passa do limite do Excel.
    base[colunas_base_saida].to_csv(
        caminho_saida_csv,
        index=False,
        sep=";",
        encoding="utf-8-sig"
    )

    logging.info(f"Base calculada completa salva em CSV: {caminho_saida_csv}")

    # Salva o Excel apenas com os resumos.
    with pd.ExcelWriter(caminho_saida_excel, engine="openpyxl") as writer:
        resumo_geral.to_excel(
            writer,
            index=False,
            sheet_name="RESUMO_GERAL"
        )

        resumo_data.to_excel(
            writer,
            index=False,
            sheet_name="RESUMO_POR_DATA"
        )

        if not resumo_base_data.empty:
            resumo_base_data.to_excel(
                writer,
                index=False,
                sheet_name="RESUMO_BASE_DATA"
            )

    logging.info("Processamento finalizado com sucesso.")
    logging.info(f"Resumo salvo em Excel: {caminho_saida_excel}")
    logging.info(f"Base completa salva em CSV: {caminho_saida_csv}")

    print("\n================ RESUMO GERAL ================")
    print(f"Total de pedidos: {total_pedidos:,}")
    print(f"Entregues no prazo: {total_no_prazo:,}")
    print(f"Fora do prazo: {total_fora_prazo:,}")
    print(f"SLA geral: {sla_geral}%")
    print(f"Resumo salvo em: {caminho_saida_excel}")
    print(f"Base completa salva em: {caminho_saida_csv}")
    print("==============================================\n")


if __name__ == "__main__":
    main()