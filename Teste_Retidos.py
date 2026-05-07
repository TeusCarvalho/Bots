import os
import re
import math
import unicodedata
from pathlib import Path
from datetime import timedelta, datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from PIL import Image, ImageDraw, ImageFont


# ============================================================
# CONFIGURAÇÕES PRINCIPAIS
# ============================================================

PASTA_RAIZ = Path(
    r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Área de Trabalho\Testes\13 - Teste dos Relatorios\Retidos"
)

# Se quiser usar somente essa planilha nova, deixe assim:
CAMINHO_PLANILHA_UNICA = PASTA_RAIZ / "Estatística de itens retidos na base (dias corridos)(Lista)2209120260428140654.xlsx"

# Se quiser voltar a ler todas as planilhas da pasta, coloque:
# CAMINHO_PLANILHA_UNICA = None

PASTA_SAIDA = PASTA_RAIZ / "Imagens_Retidos"

TITULO = "Jose Marlon — Retidos por Base"
NOME_ARQUIVO_SAIDA = "Jose_Marlon_Retidos_por_Base"


# ============================================================
# CONFIGURAÇÃO DE DATA
# ============================================================

DATA_REFERENCIA_FIXA = None
# DATA_REFERENCIA_FIXA = "2026-04-27"

DATA_MINIMA_ANALISE = None
IGNORAR_DATAS_FUTURAS = True
DATA_MAXIMA_ANALISE_FIXA = None

QUANTIDADE_DIAS_NA_IMAGEM = 5
QUANTIDADE_SEMANAS_NA_IMAGEM = 3

GERAR_EXCEL_CONFERENCIA = True


# ============================================================
# WEBHOOKS
# ============================================================

COORDENADOR_WEBHOOKS = {
    "Jose Marlon": "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b",

    # "Rodrigo Castro": "COLE_AQUI_O_WEBHOOK",
    # "Marcelo Medina": "COLE_AQUI_O_WEBHOOK",
    # "João Melo": "COLE_AQUI_O_WEBHOOK",
    # "Anderson Matheus": "COLE_AQUI_O_WEBHOOK",
    # "Ana Cunha": "COLE_AQUI_O_WEBHOOK",
}

MODO_TESTE_ENVIAR_SOMENTE_UM = True
WEBHOOK_TESTE_NOME = "Jose Marlon"
MODO_TESTE_ENVIAR_SOMENTE_PRIMEIRA_IMAGEM = True


# ============================================================
# FEISHU
# ============================================================

ENVIAR_FEISHU = True
ENVIAR_IMAGEM_FEISHU = True

FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "cli_a906d2d682f8dbd8").strip()
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "Fzh1cr6K55a3oQUBV9wCZd6AWiZH5ONw").strip()
FEISHU_BASE_DOMAIN = "https://open.feishu.cn"


# ============================================================
# CONFIGURAÇÕES DA IMAGEM
# ============================================================

LINHAS_POR_PAGINA = 24
MAX_WORKERS = min(8, os.cpu_count() or 4)

MOSTRAR_ZERO_COMO_TRACO = False


# ============================================================
# COLUNAS DA PLANILHA NOVA
# ============================================================

COLUNA_DATA_PREVISTA = "Data prevista de entrega"
COLUNA_BASE = "Nome da base de entrega"
COLUNA_PEDIDOS = "Pedidos"
COLUNA_REMESSA = "Remessa"
COLUNA_TEMPO_RETENCAO = "Tempo de retenção"


# ============================================================
# COLUNAS DO MODELO ANTIGO
# ============================================================
# O código ainda aceita o modelo antigo.
# Se essas colunas existirem, ele soma elas.
# Se não existirem, ele usa a coluna "Pedidos".

COLUNAS_RETIDOS_MODELO_ANTIGO = [
    "Retidos até 5 dias",
    "Retidos até 7 dias",
    "Retidos até 10 dias",
    "Retidos há mais de 10 dias",
    "Retidos até 15 dias",
    "超15天内滞留",
]


# ============================================================
# CORES
# ============================================================

JT_RED = "#E30613"
JT_RED_DARK = "#C00000"
BG = "#F2F2F2"
WHITE = "#FFFFFF"
TEXT = "#333333"
GRID = "#BFBFBF"

ROW_BG = "#EEEEEE"
ROW_ALT = "#F7F7F7"

GREEN = "#009E49"
ORANGE = "#D97800"
RED = "#D71920"
GRAY = "#777777"

WEEKEND_HEADER = "#9A3412"
WEEKEND_ROW_BG = "#FFF2CC"
WEEKEND_ROW_ALT = "#FFF7DD"


# ============================================================
# MESES PARA PEGAR DATA PELO NOME DO ARQUIVO ANTIGO
# ============================================================

MESES_ARQUIVO = {
    "JANEIRO": 1,
    "FEVEREIRO": 2,
    "MARCO": 3,
    "MARÇO": 3,
    "ABRIL": 4,
    "MAIO": 5,
    "JUNHO": 6,
    "JULHO": 7,
    "AGOSTO": 8,
    "SETEMBRO": 9,
    "OUTUBRO": 10,
    "NOVEMBRO": 11,
    "DEZEMBRO": 12,
}


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def limpar_texto_base(valor):
    texto = str(valor)

    texto = texto.replace("\ufeff", "")
    texto = texto.replace("\u200b", "")
    texto = texto.replace("\u200c", "")
    texto = texto.replace("\u200d", "")
    texto = texto.replace("\xa0", " ")

    texto = unicodedata.normalize("NFKC", texto)
    texto = texto.strip()
    texto = re.sub(r"\s+", " ", texto)

    return texto


def remover_acentos(texto):
    texto = str(texto)
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    return texto


def normalizar_texto(valor):
    if pd.isna(valor):
        return ""

    texto = limpar_texto_base(valor)
    texto = texto.lower()
    texto = remover_acentos(texto)
    texto = re.sub(r"\s+", " ", texto)

    return texto


def normalizar_coluna(nome):
    return normalizar_texto(nome)


def limpar_colunas_df(df):
    df = df.copy()
    df.columns = [limpar_texto_base(col) for col in df.columns]
    return df


def encontrar_coluna(df, nome_procurado):
    nome_procurado_norm = normalizar_coluna(nome_procurado)

    mapa_colunas = {
        normalizar_coluna(col): col
        for col in df.columns
    }

    return mapa_colunas.get(nome_procurado_norm)


def encontrar_colunas_modelo_antigo(df):
    encontradas = []

    for nome_coluna in COLUNAS_RETIDOS_MODELO_ANTIGO:
        coluna_encontrada = encontrar_coluna(df, nome_coluna)

        if coluna_encontrada:
            encontradas.append(coluna_encontrada)

    return encontradas


def converter_numero(valor):
    if pd.isna(valor):
        return 0

    if isinstance(valor, (int, float)):
        try:
            return float(valor)
        except Exception:
            return 0

    texto = limpar_texto_base(valor)

    if texto == "" or texto.lower() in ["nan", "none", "-", "--"]:
        return 0

    texto = texto.replace("%", "").replace(" ", "")

    if "," in texto and "." in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif "," in texto and "." not in texto:
        texto = texto.replace(",", ".")
    elif "." in texto and "," not in texto:
        if re.fullmatch(r"\d{1,3}(\.\d{3})+", texto):
            texto = texto.replace(".", "")

    texto = re.sub(r"[^0-9.\-]", "", texto)

    if texto in ["", "-", "."]:
        return 0

    try:
        return float(texto)
    except Exception:
        return 0


# ============================================================
# DATA PELO NOME DO ARQUIVO
# ============================================================

def extrair_data_do_nome_arquivo(nome_arquivo):
    if not nome_arquivo:
        return pd.NaT

    texto = limpar_texto_base(nome_arquivo).upper()
    texto_sem_acento = remover_acentos(texto).upper()

    ano_match = re.search(r"(20\d{2})", texto_sem_acento)
    dia_match = re.search(r"\((\d{1,2})\)", texto_sem_acento)

    if not ano_match or not dia_match:
        return pd.NaT

    ano = int(ano_match.group(1))
    dia = int(dia_match.group(1))

    mes = None

    for nome_mes, numero_mes in MESES_ARQUIVO.items():
        nome_mes_norm = remover_acentos(nome_mes).upper()

        if nome_mes_norm in texto_sem_acento:
            mes = numero_mes
            break

    if mes is None:
        return pd.NaT

    try:
        return pd.Timestamp(year=ano, month=mes, day=dia).normalize()
    except Exception:
        return pd.NaT


# ============================================================
# CONVERSÃO DE DATA
# ============================================================

def converter_data_valor(valor):
    if pd.isna(valor):
        return pd.NaT

    if isinstance(valor, pd.Timestamp):
        return valor.normalize()

    if isinstance(valor, datetime):
        return pd.Timestamp(valor).normalize()

    if isinstance(valor, date):
        return pd.Timestamp(valor).normalize()

    if isinstance(valor, (int, float)):
        try:
            numero = float(valor)

            if 20000 <= numero <= 60000:
                return pd.to_datetime(
                    numero,
                    unit="D",
                    origin="1899-12-30",
                    errors="coerce"
                ).normalize()
        except Exception:
            pass

    texto = limpar_texto_base(valor)

    if texto == "" or texto.lower() in ["nan", "none", "-", "--"]:
        return pd.NaT

    texto = texto.replace("T", " ")
    parte_data = texto.split(" ")[0].strip()

    texto_num = parte_data.replace(",", ".")

    if re.fullmatch(r"\d+(\.\d+)?", texto_num):
        try:
            numero = float(texto_num)

            if 20000 <= numero <= 60000:
                return pd.to_datetime(
                    numero,
                    unit="D",
                    origin="1899-12-30",
                    errors="coerce"
                ).normalize()
        except Exception:
            pass

    if re.fullmatch(r"\d{1,2}[-/]\d{1,2}[-/]\d{4}", parte_data):
        parte_br = parte_data.replace("-", "/")

        data = pd.to_datetime(
            parte_br,
            format="%d/%m/%Y",
            errors="coerce"
        )

        if not pd.isna(data):
            return data.normalize()

    if re.fullmatch(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", parte_data):
        parte_iso = parte_data.replace("/", "-")

        data = pd.to_datetime(
            parte_iso,
            format="%Y-%m-%d",
            errors="coerce"
        )

        if not pd.isna(data):
            return data.normalize()

    if re.fullmatch(r"\d{8}", parte_data):
        data = pd.to_datetime(
            parte_data,
            format="%Y%m%d",
            errors="coerce"
        )

        if not pd.isna(data):
            return data.normalize()

        data = pd.to_datetime(
            parte_data,
            format="%d%m%Y",
            errors="coerce"
        )

        if not pd.isna(data):
            return data.normalize()

    data = pd.to_datetime(
        texto,
        errors="coerce",
        dayfirst=True
    )

    if pd.isna(data):
        return pd.NaT

    return data.normalize()


def converter_data_coluna(serie):
    datas = serie.apply(converter_data_valor)
    datas = pd.to_datetime(datas, errors="coerce")
    return datas.dt.normalize()


def obter_data_limite_d1():
    if DATA_REFERENCIA_FIXA:
        data_ref = converter_data_valor(DATA_REFERENCIA_FIXA)

        if pd.isna(data_ref):
            raise ValueError(f"DATA_REFERENCIA_FIXA inválida: {DATA_REFERENCIA_FIXA}")

        return data_ref.normalize()

    if DATA_MAXIMA_ANALISE_FIXA:
        data_maxima = converter_data_valor(DATA_MAXIMA_ANALISE_FIXA)

        if pd.isna(data_maxima):
            raise ValueError(f"DATA_MAXIMA_ANALISE_FIXA inválida: {DATA_MAXIMA_ANALISE_FIXA}")

        return data_maxima.normalize()

    return pd.Timestamp.today().normalize() - timedelta(days=1)


def carregar_fonte(tamanho, negrito=False):
    if negrito:
        caminhos = [
            r"C:\Windows\Fonts\arialbd.ttf",
            r"C:\Windows\Fonts\calibrib.ttf",
        ]
    else:
        caminhos = [
            r"C:\Windows\Fonts\arial.ttf",
            r"C:\Windows\Fonts\calibri.ttf",
        ]

    for caminho in caminhos:
        if Path(caminho).exists():
            return ImageFont.truetype(caminho, tamanho)

    return ImageFont.load_default()


def formatar_mes_abreviado(data):
    meses = {
        1: "Jan.",
        2: "Fev.",
        3: "Mar.",
        4: "Abr.",
        5: "Mai.",
        6: "Jun.",
        7: "Jul.",
        8: "Ago.",
        9: "Set.",
        10: "Out.",
        11: "Nov.",
        12: "Dez.",
    }

    return meses[pd.Timestamp(data).month]


def formatar_label_dia(data):
    data = pd.Timestamp(data)
    return data.strftime("%d/%m")


def formatar_label_semana(data):
    data = pd.Timestamp(data)
    semana = data.isocalendar().week
    return f"W{semana}"


def cor_por_valor(valor):
    try:
        valor = int(valor)
    except Exception:
        return GRAY

    if valor >= 30:
        return RED
    elif valor >= 10:
        return ORANGE
    else:
        return GREEN


def formatar_valor(valor):
    if pd.isna(valor):
        return "0"

    try:
        valor_int = int(round(float(valor), 0))
    except Exception:
        return "0"

    if MOSTRAR_ZERO_COMO_TRACO and valor_int == 0:
        return "-"

    return str(valor_int)


def texto_centralizado(draw, caixa, texto, fonte, fill):
    x1, y1, x2, y2 = caixa
    bbox = draw.textbbox((0, 0), texto, font=fonte)

    largura_texto = bbox[2] - bbox[0]
    altura_texto = bbox[3] - bbox[1]

    x = x1 + ((x2 - x1) - largura_texto) / 2
    y = y1 + ((y2 - y1) - altura_texto) / 2 - 1

    draw.text((x, y), texto, font=fonte, fill=fill)


def texto_esquerda(draw, caixa, texto, fonte, fill, padding=10):
    x1, y1, x2, y2 = caixa
    bbox = draw.textbbox((0, 0), texto, font=fonte)

    altura_texto = bbox[3] - bbox[1]
    y = y1 + ((y2 - y1) - altura_texto) / 2 - 1

    draw.text((x1 + padding, y), texto, font=fonte, fill=fill)


# ============================================================
# LEITURA DOS ARQUIVOS
# ============================================================

def listar_arquivos(pasta):
    if CAMINHO_PLANILHA_UNICA and Path(CAMINHO_PLANILHA_UNICA).exists():
        print(f"📌 Modo planilha única ativo: {CAMINHO_PLANILHA_UNICA}")
        return [Path(CAMINHO_PLANILHA_UNICA)]

    arquivos = []

    for arquivo in pasta.rglob("*"):
        if not arquivo.is_file():
            continue

        if arquivo.name.startswith("~$"):
            continue

        if arquivo.suffix.lower() not in [".xlsx", ".xls", ".csv"]:
            continue

        if "Imagens_Retidos" in str(arquivo):
            continue

        arquivos.append(arquivo)

    return arquivos


def ler_arquivo(arquivo):
    try:
        if arquivo.suffix.lower() == ".csv":
            try:
                df = pd.read_csv(
                    arquivo,
                    sep=None,
                    engine="python",
                    encoding="utf-8-sig",
                    dtype=str
                )
            except Exception:
                df = pd.read_csv(
                    arquivo,
                    sep=None,
                    engine="python",
                    encoding="latin1",
                    dtype=str
                )
        else:
            df = pd.read_excel(arquivo, dtype=str)

        df = limpar_colunas_df(df)
        df["__arquivo_origem"] = arquivo.name
        df["__caminho_origem"] = str(arquivo)

        return df

    except Exception as e:
        print(f"❌ Erro ao ler {arquivo.name}: {e}")
        return None


def carregar_base():
    arquivos = listar_arquivos(PASTA_RAIZ)

    if not arquivos:
        raise FileNotFoundError(f"Nenhum Excel/CSV encontrado em: {PASTA_RAIZ}")

    print(f"📁 Arquivos encontrados: {len(arquivos)}")

    frames = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futuros = {
            executor.submit(ler_arquivo, arquivo): arquivo
            for arquivo in arquivos
        }

        for futuro in as_completed(futuros):
            arquivo = futuros[futuro]
            df = futuro.result()

            if df is not None and not df.empty:
                frames.append(df)
                print(f"✅ Lido: {arquivo.name}")

    if not frames:
        raise RuntimeError("Nenhum arquivo válido foi carregado.")

    df_final = pd.concat(frames, ignore_index=True)

    print(f"📊 Total de linhas carregadas: {len(df_final):,}".replace(",", "."))

    return df_final


# ============================================================
# PREPARAÇÃO DOS DADOS
# ============================================================

def preparar_base(df):
    df = limpar_colunas_df(df)

    coluna_data = encontrar_coluna(df, COLUNA_DATA_PREVISTA)
    coluna_base = encontrar_coluna(df, COLUNA_BASE)
    coluna_pedidos = encontrar_coluna(df, COLUNA_PEDIDOS)
    coluna_remessa = encontrar_coluna(df, COLUNA_REMESSA)
    coluna_tempo_retencao = encontrar_coluna(df, COLUNA_TEMPO_RETENCAO)

    colunas_retidos_antigo = encontrar_colunas_modelo_antigo(df)

    if not coluna_data:
        raise ValueError(f"Não encontrei a coluna: {COLUNA_DATA_PREVISTA}")

    if not coluna_base:
        raise ValueError(f"Não encontrei a coluna: {COLUNA_BASE}")

    print(f"\n✅ Coluna Data prevista: {coluna_data}")
    print(f"✅ Coluna Base: {coluna_base}")

    if coluna_pedidos:
        print(f"✅ Coluna Pedidos: {coluna_pedidos}")

    if coluna_remessa:
        print(f"✅ Coluna Remessa: {coluna_remessa}")

    if coluna_tempo_retencao:
        print(f"✅ Coluna Tempo de retenção: {coluna_tempo_retencao}")

    df = df.copy()

    df["__data_coluna_convertida"] = converter_data_coluna(df[coluna_data])

    df["__data_arquivo"] = pd.NaT
    if "__arquivo_origem" in df.columns:
        df["__data_arquivo"] = df["__arquivo_origem"].apply(extrair_data_do_nome_arquivo)

    df["__data_prevista"] = df["__data_arquivo"].where(
        df["__data_arquivo"].notna(),
        df["__data_coluna_convertida"]
    )

    df["__data_prevista"] = pd.to_datetime(
        df["__data_prevista"],
        errors="coerce"
    ).dt.normalize()

    df["__base"] = df[coluna_base].astype(str).str.strip()

    print("\n📌 Datas convertidas finais:")
    datas_convertidas = (
        df["__data_prevista"]
        .dropna()
        .drop_duplicates()
        .sort_values()
    )

    if datas_convertidas.empty:
        print("   - Nenhuma data válida convertida.")
    else:
        for data in datas_convertidas:
            print(f"   - {data.strftime('%d/%m/%Y')}")

    df = df[df["__data_prevista"].notna()]
    df = df[df["__base"].notna()]
    df = df[df["__base"].str.strip() != ""]
    df = df[df["__base"].str.lower() != "nan"]

    df["__retidos_total"] = 0
    colunas_numericas_debug = []

    if colunas_retidos_antigo:
        print("\n📌 Modelo detectado: ANTIGO — somando colunas de retidos.")

        for idx_col, coluna in enumerate(colunas_retidos_antigo, start=1):
            nome_coluna_num = f"__retido_num_{idx_col}_{coluna}"

            df[nome_coluna_num] = df[coluna].apply(converter_numero)
            colunas_numericas_debug.append(nome_coluna_num)

            soma_coluna = df[nome_coluna_num].sum()

            print(f"   - {coluna}: {soma_coluna:,.0f}".replace(",", "."))

            df["__retidos_total"] = df["__retidos_total"] + df[nome_coluna_num]

    elif coluna_pedidos:
        print("\n📌 Modelo detectado: NOVO — somando coluna Pedidos.")

        df["__pedidos_num"] = df[coluna_pedidos].apply(converter_numero)

        if df["__pedidos_num"].sum() == 0 and coluna_remessa:
            print("⚠️ Coluna Pedidos está zerada. Usando contagem de Remessa.")
            df["__retidos_total"] = 1
        else:
            df["__retidos_total"] = df["__pedidos_num"]

        colunas_numericas_debug.append("__pedidos_num")

        print(
            f"   - Soma da coluna Pedidos: "
            f"{df['__retidos_total'].sum():,.0f}".replace(",", ".")
        )

    elif coluna_remessa:
        print("\n📌 Modelo detectado: NOVO — sem Pedidos, contando Remessa.")
        df["__retidos_total"] = 1

    else:
        raise ValueError(
            "Não encontrei colunas de retidos, nem coluna Pedidos, nem coluna Remessa."
        )

    df["__retidos_total"] = df["__retidos_total"].fillna(0).round(0).astype(int)

    print(
        f"\n📊 Soma geral dos retidos antes do filtro de data: "
        f"{df['__retidos_total'].sum():,.0f}".replace(",", ".")
    )

    print("\n📌 Exemplo da base tratada:")

    colunas_exemplo = [
        "__arquivo_origem",
        "__data_prevista",
        "__base",
        "__retidos_total",
    ] + colunas_numericas_debug

    colunas_exemplo = [col for col in colunas_exemplo if col in df.columns]

    print(
        df[colunas_exemplo]
        .head(15)
        .to_string(index=False)
    )

    data_limite_d1 = obter_data_limite_d1()

    antes_filtro_data = len(df)

    if DATA_MINIMA_ANALISE:
        data_minima = converter_data_valor(DATA_MINIMA_ANALISE)

        if pd.isna(data_minima):
            raise ValueError(f"DATA_MINIMA_ANALISE inválida: {DATA_MINIMA_ANALISE}")

        df = df[df["__data_prevista"] >= data_minima].copy()

    if IGNORAR_DATAS_FUTURAS:
        df = df[df["__data_prevista"] <= data_limite_d1].copy()

    depois_filtro_data = len(df)

    print(f"\n📅 Data limite D-1: {data_limite_d1.strftime('%d/%m/%Y')}")
    print(
        f"🧹 Linhas removidas pelo filtro de data: "
        f"{antes_filtro_data - depois_filtro_data:,}".replace(",", ".")
    )

    if df.empty:
        raise ValueError("Após o filtro de data, a base ficou vazia.")

    print(
        f"\n📊 Soma geral dos retidos após filtro de data: "
        f"{df['__retidos_total'].sum():,.0f}".replace(",", ".")
    )

    print("\n📌 Datas existentes após filtro:")
    datas = (
        df["__data_prevista"]
        .dropna()
        .drop_duplicates()
        .sort_values()
    )

    for data in datas:
        print(f"   - {data.strftime('%d/%m/%Y')}")

    return df


# ============================================================
# DATAS E SEMANAS
# ============================================================

def obter_datas_existentes_mes_atual(df, primeiro_dia_mes_atual, data_limite_d1):
    df_datas = df[
        (df["__data_prevista"] >= primeiro_dia_mes_atual)
        & (df["__data_prevista"] <= data_limite_d1)
    ].copy()

    datas = (
        df_datas["__data_prevista"]
        .dropna()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )

    return [pd.Timestamp(data).normalize() for data in datas]


def obter_datas_para_exibir(df, primeiro_dia_mes_atual, data_limite_d1):
    datas_existentes = obter_datas_existentes_mes_atual(
        df=df,
        primeiro_dia_mes_atual=primeiro_dia_mes_atual,
        data_limite_d1=data_limite_d1
    )

    if datas_existentes:
        if QUANTIDADE_DIAS_NA_IMAGEM is None:
            datas_finais = datas_existentes
        else:
            datas_finais = datas_existentes[-QUANTIDADE_DIAS_NA_IMAGEM:]

        print("\n📌 Dias que aparecerão na imagem:")

        for data in datas_finais:
            print(f"   - {data.strftime('%d/%m/%Y')}")

        return datas_finais

    dias_corridos = [
        data_limite_d1 - timedelta(days=i)
        for i in range(4, -1, -1)
    ]

    return [pd.Timestamp(dia).normalize() for dia in dias_corridos]


def obter_semanas_para_exibir(df, primeiro_dia_mes_atual, data_limite_d1):
    datas_existentes = obter_datas_existentes_mes_atual(
        df=df,
        primeiro_dia_mes_atual=primeiro_dia_mes_atual,
        data_limite_d1=data_limite_d1
    )

    semanas_dict = {}

    for data in datas_existentes:
        data = pd.Timestamp(data).normalize()

        inicio_semana = data - timedelta(days=data.weekday())
        fim_semana = inicio_semana + timedelta(days=6)

        if inicio_semana < primeiro_dia_mes_atual:
            inicio_semana = primeiro_dia_mes_atual

        if fim_semana > data_limite_d1:
            fim_semana = data_limite_d1

        label = formatar_label_semana(data)

        semanas_dict[label] = {
            "label": label,
            "inicio": inicio_semana.normalize(),
            "fim": fim_semana.normalize(),
        }

    semanas = list(semanas_dict.values())
    semanas = sorted(semanas, key=lambda x: x["inicio"])

    if QUANTIDADE_SEMANAS_NA_IMAGEM is not None:
        semanas = semanas[-QUANTIDADE_SEMANAS_NA_IMAGEM:]

    print("\n📌 Semanas que aparecerão na imagem:")

    for semana in semanas:
        print(
            f"   - {semana['label']}: "
            f"{semana['inicio'].strftime('%d/%m/%Y')} até "
            f"{semana['fim'].strftime('%d/%m/%Y')}"
        )

    return semanas


# ============================================================
# RESUMO
# ============================================================

def montar_resumo_geral(df):
    data_limite_d1 = obter_data_limite_d1()

    primeiro_dia_mes_atual = data_limite_d1.replace(day=1)

    ultimo_dia_mes_anterior = primeiro_dia_mes_atual - timedelta(days=1)
    primeiro_dia_mes_anterior = ultimo_dia_mes_anterior.replace(day=1)

    label_mes_anterior = f"Retidos {formatar_mes_abreviado(ultimo_dia_mes_anterior)}"
    label_mes_atual = f"Retidos {formatar_mes_abreviado(data_limite_d1)}"

    df_periodo_base = df[
        (df["__data_prevista"] >= primeiro_dia_mes_anterior)
        & (df["__data_prevista"] <= data_limite_d1)
    ].copy()

    bases = sorted(df_periodo_base["__base"].dropna().unique())
    resumo = pd.DataFrame({"Base": bases})

    if resumo.empty:
        raise ValueError("Nenhuma base encontrada para o período analisado.")

    df_mes_anterior = df[
        (df["__data_prevista"] >= primeiro_dia_mes_anterior)
        & (df["__data_prevista"] <= ultimo_dia_mes_anterior)
    ].copy()

    cont_mes_anterior = (
        df_mes_anterior
        .groupby("__base")["__retidos_total"]
        .sum()
        .rename(label_mes_anterior)
        .reset_index()
        .rename(columns={"__base": "Base"})
    )

    resumo = resumo.merge(cont_mes_anterior, on="Base", how="left")

    df_mes_atual = df[
        (df["__data_prevista"] >= primeiro_dia_mes_atual)
        & (df["__data_prevista"] <= data_limite_d1)
    ].copy()

    cont_mes_atual = (
        df_mes_atual
        .groupby("__base")["__retidos_total"]
        .sum()
        .rename(label_mes_atual)
        .reset_index()
        .rename(columns={"__base": "Base"})
    )

    resumo = resumo.merge(cont_mes_atual, on="Base", how="left")

    semanas = obter_semanas_para_exibir(
        df=df,
        primeiro_dia_mes_atual=primeiro_dia_mes_atual,
        data_limite_d1=data_limite_d1
    )

    colunas_semanas = []

    for semana in semanas:
        label_semana = semana["label"]
        colunas_semanas.append(label_semana)

        cont_semana = (
            df[
                (df["__data_prevista"] >= semana["inicio"])
                & (df["__data_prevista"] <= semana["fim"])
            ]
            .groupby("__base")["__retidos_total"]
            .sum()
            .rename(label_semana)
            .reset_index()
            .rename(columns={"__base": "Base"})
        )

        resumo = resumo.merge(cont_semana, on="Base", how="left")

    datas_para_exibir = obter_datas_para_exibir(
        df=df,
        primeiro_dia_mes_atual=primeiro_dia_mes_atual,
        data_limite_d1=data_limite_d1
    )

    colunas_dias = []
    colunas_fim_semana = set()

    for dia in datas_para_exibir:
        dia = pd.Timestamp(dia).normalize()
        label_dia = formatar_label_dia(dia)
        colunas_dias.append(label_dia)

        if dia.weekday() in [5, 6]:
            colunas_fim_semana.add(label_dia)

        cont_dia = (
            df[df["__data_prevista"].dt.date == dia.date()]
            .groupby("__base")["__retidos_total"]
            .sum()
            .rename(label_dia)
            .reset_index()
            .rename(columns={"__base": "Base"})
        )

        resumo = resumo.merge(cont_dia, on="Base", how="left")

    colunas_valores = [col for col in resumo.columns if col != "Base"]
    resumo[colunas_valores] = resumo[colunas_valores].fillna(0).astype(int)

    colunas_ordenadas = (
        ["Base", label_mes_anterior, label_mes_atual]
        + colunas_semanas
        + colunas_dias
    )

    resumo = resumo[colunas_ordenadas].copy()

    resumo = resumo.sort_values(
        by=[label_mes_atual, label_mes_anterior],
        ascending=[False, False]
    ).reset_index(drop=True)

    print(f"\n📅 Limite D-1 oficial: {data_limite_d1.strftime('%d/%m/%Y')}")
    print(f"📊 Bases no resumo: {len(resumo)}")
    print(f"📊 Soma mês atual: {resumo[label_mes_atual].sum():,}".replace(",", "."))

    info_periodo = {
        "data_limite_d1": data_limite_d1,
        "primeiro_dia_mes_atual": primeiro_dia_mes_atual,
        "primeiro_dia_mes_anterior": primeiro_dia_mes_anterior,
        "ultimo_dia_mes_anterior": ultimo_dia_mes_anterior,
        "semanas": semanas,
        "datas_para_exibir": datas_para_exibir,
        "colunas_semanas": colunas_semanas,
        "colunas_dias": colunas_dias,
        "colunas_fim_semana": colunas_fim_semana,
        "label_mes_anterior": label_mes_anterior,
        "label_mes_atual": label_mes_atual,
    }

    return resumo, info_periodo


# ============================================================
# IMAGEM
# ============================================================

def largura_coluna(col):
    if col == "Base":
        return 245

    if str(col).startswith("Retidos"):
        return 140

    if str(col).startswith("W"):
        return 105

    return 105


def cor_header_coluna(col, colunas_fim_semana=None):
    colunas_fim_semana = colunas_fim_semana or set()

    if col in colunas_fim_semana:
        return WEEKEND_HEADER

    return JT_RED


def gerar_imagem_principal(df_resumo, info_periodo):
    PASTA_SAIDA.mkdir(parents=True, exist_ok=True)

    colunas = list(df_resumo.columns)
    larguras = [largura_coluna(col) for col in colunas]

    colunas_fim_semana = info_periodo.get("colunas_fim_semana", set())

    margem_x = 18
    topo = 86
    altura_header = 46
    altura_linha = 37
    margem_final = 24

    largura_img = margem_x * 2 + sum(larguras)

    fonte_titulo = carregar_fonte(28, True)
    fonte_subtitulo = carregar_fonte(14, False)
    fonte_header = carregar_fonte(13, True)
    fonte_linha = carregar_fonte(14, False)
    fonte_numero = carregar_fonte(14, True)

    total_linhas = len(df_resumo)
    total_paginas = max(1, math.ceil(total_linhas / LINHAS_POR_PAGINA))

    arquivos_gerados = []

    data_limite_d1 = info_periodo["data_limite_d1"]

    for pagina in range(total_paginas):
        inicio = pagina * LINHAS_POR_PAGINA
        fim = inicio + LINHAS_POR_PAGINA

        df_pagina = df_resumo.iloc[inicio:fim].copy()

        altura_img = topo + altura_header + len(df_pagina) * altura_linha + margem_final

        img = Image.new("RGB", (largura_img, altura_img), BG)
        draw = ImageDraw.Draw(img)

        subtitulo = (
            f"Pedidos Retidos — soma por Base (maior → menor) | "
            f"Semanas + últimos dias até {data_limite_d1.strftime('%d/%m/%Y')} | "
            f"Página {pagina + 1}/{total_paginas}"
        )

        draw.text((margem_x, 14), TITULO, font=fonte_titulo, fill=JT_RED_DARK)
        draw.text((margem_x, 50), subtitulo, font=fonte_subtitulo, fill=TEXT)

        x = margem_x
        y = topo

        for col, largura in zip(colunas, larguras):
            cor_header = cor_header_coluna(
                col,
                colunas_fim_semana=colunas_fim_semana
            )

            draw.rectangle(
                [x, y, x + largura, y + altura_header],
                fill=cor_header,
                outline=WHITE
            )

            texto_centralizado(
                draw,
                (x, y, x + largura, y + altura_header),
                str(col),
                fonte_header,
                WHITE
            )

            x += largura

        y += altura_header

        for idx, row in df_pagina.iterrows():
            x = margem_x
            cor_fundo_padrao = ROW_BG if idx % 2 == 0 else ROW_ALT

            for col, largura in zip(colunas, larguras):
                if col in colunas_fim_semana:
                    cor_fundo = WEEKEND_ROW_BG if idx % 2 == 0 else WEEKEND_ROW_ALT
                else:
                    cor_fundo = cor_fundo_padrao

                draw.rectangle(
                    [x, y, x + largura, y + altura_linha],
                    fill=cor_fundo,
                    outline=GRID
                )

                valor = row[col]

                if col == "Base":
                    texto_esquerda(
                        draw,
                        (x, y, x + largura, y + altura_linha),
                        str(valor),
                        fonte_linha,
                        TEXT,
                        padding=10
                    )
                else:
                    texto = formatar_valor(valor)
                    cor = cor_por_valor(valor)

                    texto_centralizado(
                        draw,
                        (x, y, x + largura, y + altura_linha),
                        texto,
                        fonte_numero,
                        cor
                    )

                x += largura

            y += altura_linha

        caminho_saida = PASTA_SAIDA / f"{NOME_ARQUIVO_SAIDA}_GERAL_pagina_{pagina + 1}.png"
        img.save(caminho_saida, quality=95)

        arquivos_gerados.append(caminho_saida)

        print(f"🖼️ Imagem gerada: {caminho_saida}")

    return arquivos_gerados


# ============================================================
# EXCEL DE CONFERÊNCIA
# ============================================================

def salvar_resumo_debug(df_resumo, df_base):
    if not GERAR_EXCEL_CONFERENCIA:
        return

    PASTA_SAIDA.mkdir(parents=True, exist_ok=True)

    caminho_excel = PASTA_SAIDA / "Resumo_Retidos_Conferencia.xlsx"

    try:
        with pd.ExcelWriter(caminho_excel, engine="openpyxl") as writer:
            df_resumo.to_excel(writer, sheet_name="Resumo Geral", index=False)

            colunas_debug = [
                "__arquivo_origem",
                "__data_coluna_convertida",
                "__data_arquivo",
                "__data_prevista",
                "__base",
                "__retidos_total",
            ]

            colunas_debug = [col for col in colunas_debug if col in df_base.columns]

            df_debug = df_base[colunas_debug].copy()
            df_debug.to_excel(writer, sheet_name="Base Conferencia", index=False)

        print(f"📄 Excel de conferência salvo em: {caminho_excel}")

    except Exception as e:
        print(f"⚠️ Não foi possível salvar o Excel de conferência: {e}")


# ============================================================
# FEISHU
# ============================================================

def obter_webhooks_validos():
    webhooks = []
    urls_vistas = set()

    for nome, url in COORDENADOR_WEBHOOKS.items():
        url = str(url).strip()

        if not url:
            continue

        if "COLE_AQUI" in url:
            continue

        if not url.startswith("https://open.feishu.cn/open-apis/bot/v2/hook/"):
            print(f"⚠️ Webhook inválido ignorado: {nome}")
            continue

        if url in urls_vistas:
            continue

        webhooks.append({
            "nome": nome,
            "url": url
        })

        urls_vistas.add(url)

    if MODO_TESTE_ENVIAR_SOMENTE_UM:
        for webhook in webhooks:
            if webhook["nome"] == WEBHOOK_TESTE_NOME:
                print(f"🧪 Modo teste ativo. Enviando somente para: {WEBHOOK_TESTE_NOME}")
                return [webhook]

        if webhooks:
            print(f"🧪 Usando primeiro webhook válido: {webhooks[0]['nome']}")
            return [webhooks[0]]

    return webhooks


def enviar_texto_webhook(webhook_url, nome_destino, texto):
    payload = {
        "msg_type": "text",
        "content": {
            "text": texto
        }
    }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=30)
        print(f"📨 Texto enviado para {nome_destino}: {resp.status_code} | {resp.text[:300]}")
        return resp.status_code == 200
    except Exception as e:
        print(f"❌ Erro ao enviar texto para {nome_destino}: {e}")
        return False


def obter_tenant_access_token():
    if not FEISHU_APP_ID or not FEISHU_APP_SECRET:
        print("⚠️ FEISHU_APP_ID/FEISHU_APP_SECRET não configurados.")
        return None

    url = f"{FEISHU_BASE_DOMAIN}/open-apis/auth/v3/tenant_access_token/internal"

    payload = {
        "app_id": FEISHU_APP_ID,
        "app_secret": FEISHU_APP_SECRET,
    }

    try:
        resp = requests.post(url, json=payload, timeout=30)
        data = resp.json()

        if data.get("code") != 0:
            print(f"❌ Erro ao obter tenant_access_token: {data}")
            return None

        return data.get("tenant_access_token")

    except Exception as e:
        print(f"❌ Erro no token Feishu: {e}")
        return None


def upload_imagem_feishu(caminho_imagem):
    token = obter_tenant_access_token()

    if not token:
        return None

    url = f"{FEISHU_BASE_DOMAIN}/open-apis/im/v1/images"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    data = {
        "image_type": "message"
    }

    try:
        with open(caminho_imagem, "rb") as f:
            files = {
                "image": (Path(caminho_imagem).name, f, "image/png")
            }

            resp = requests.post(
                url,
                headers=headers,
                data=data,
                files=files,
                timeout=60
            )

        retorno = resp.json()

        if retorno.get("code") != 0:
            print(f"❌ Erro no upload da imagem: {retorno}")
            return None

        image_key = retorno.get("data", {}).get("image_key")

        print(f"✅ Upload Feishu OK: {Path(caminho_imagem).name}")

        return image_key

    except Exception as e:
        print(f"❌ Erro ao fazer upload da imagem: {e}")
        return None


def enviar_imagem_webhook(webhook_url, nome_destino, image_key):
    payload = {
        "msg_type": "image",
        "content": {
            "image_key": image_key
        }
    }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=30)
        print(f"📨 Imagem enviada para {nome_destino}: {resp.status_code} | {resp.text[:300]}")
        return resp.status_code == 200
    except Exception as e:
        print(f"❌ Erro ao enviar imagem para {nome_destino}: {e}")
        return False


def enviar_resultado_feishu(arquivos, info_periodo):
    if not ENVIAR_FEISHU:
        print("📌 Envio Feishu desativado.")
        return

    webhooks = obter_webhooks_validos()

    if not webhooks:
        print("⚠️ Nenhum webhook válido configurado.")
        return

    texto_inicio = f"📊 {TITULO}"

    for destino in webhooks:
        enviar_texto_webhook(
            webhook_url=destino["url"],
            nome_destino=destino["nome"],
            texto=texto_inicio
        )

    if not ENVIAR_IMAGEM_FEISHU:
        print("📌 Envio de imagem Feishu desativado.")
        return

    if not FEISHU_APP_ID or not FEISHU_APP_SECRET:
        texto = (
            "⚠️ A imagem foi gerada, mas não foi enviada como imagem no Feishu.\n"
            "Motivo: FEISHU_APP_ID e FEISHU_APP_SECRET não estão configurados.\n\n"
            "Arquivos gerados:\n"
            + "\n".join(str(a) for a in arquivos)
        )

        for destino in webhooks:
            enviar_texto_webhook(
                webhook_url=destino["url"],
                nome_destino=destino["nome"],
                texto=texto
            )

        return

    imagens_para_enviar = arquivos

    if MODO_TESTE_ENVIAR_SOMENTE_PRIMEIRA_IMAGEM and imagens_para_enviar:
        imagens_para_enviar = [imagens_para_enviar[0]]

    for arquivo in imagens_para_enviar:
        image_key = upload_imagem_feishu(arquivo)

        if not image_key:
            for destino in webhooks:
                enviar_texto_webhook(
                    webhook_url=destino["url"],
                    nome_destino=destino["nome"],
                    texto=f"❌ Falha ao enviar imagem: {arquivo}"
                )
            continue

        for destino in webhooks:
            enviar_imagem_webhook(
                webhook_url=destino["url"],
                nome_destino=destino["nome"],
                image_key=image_key
            )


# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================

def main():
    print("🚀 Iniciando geração de imagem de Retidos por Base...")
    print(f"📁 Pasta origem: {PASTA_RAIZ}")

    data_limite_d1 = obter_data_limite_d1()
    print(f"📅 Limite oficial D-1: {data_limite_d1.strftime('%d/%m/%Y')}")

    df = carregar_base()
    df = preparar_base(df)

    if df.empty:
        print("⚠️ Nenhum dado válido encontrado após preparar a base.")
        return

    df_resumo, info_periodo = montar_resumo_geral(df)

    salvar_resumo_debug(df_resumo, df)

    arquivos = gerar_imagem_principal(df_resumo, info_periodo)

    enviar_resultado_feishu(arquivos, info_periodo)

    print("\n✅ Processo finalizado.")
    print(f"📂 Imagens salvas em: {PASTA_SAIDA}")

    for arquivo in arquivos:
        print(f"➡️ {arquivo}")


if __name__ == "__main__":
    main()