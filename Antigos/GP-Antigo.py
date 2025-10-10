import pandas as pd
import os
import logging

# ===========================================================
# Configuração de Logs
# ===========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ===========================================================
# Configuração de colunas
# ===========================================================
COLUNAS = {
    "coordenador": "Coordenador",
    "motorista": "Responsável pela entrega",
    "pedido": "Número de pedido JMS",
    "assinatura": "Marca de assinatura",
    "data": "Tempo de entrega",
    "base": "Base de entrega"
}


def encontrar_arquivos_por_prefixo(pasta, prefixo):
    arquivos = []
    try:
        for nome_arquivo in os.listdir(pasta):
            if nome_arquivo.startswith(prefixo) and nome_arquivo.endswith('.xlsx'):
                arquivos.append(os.path.join(pasta, nome_arquivo))
        if arquivos:
            logging.info(f"✅ {len(arquivos)} arquivo(s) encontrado(s) com prefixo '{prefixo}'")
        else:
            logging.error(f"❌ Nenhum arquivo Excel começando com '{prefixo}' foi encontrado na pasta '{pasta}'.")
        return arquivos
    except FileNotFoundError:
        logging.error(f"❌ O diretório especificado não foi encontrado: {pasta}")
        return []


def validar_colunas(df):
    colunas_faltando = [col for col in COLUNAS.values() if col not in df.columns]
    if colunas_faltando:
        logging.error(f"❌ Colunas não encontradas no arquivo: {colunas_faltando}")
        return False
    return True


def adicionar_coordenador(df):
    """Faz merge da Base com a planilha de referência e adiciona Coordenador + UF."""
    caminho_referencia = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"
    if not os.path.exists(caminho_referencia):
        logging.error("❌ Planilha de referência não encontrada.")
        return df

    df_ref = pd.read_excel(caminho_referencia)

    col_base_ref = "Nome da base"
    col_uf_ref = "UF"
    col_coord_ref = next((c for c in df_ref.columns if "coordenador" in c.lower()), None)

    if not col_coord_ref or col_base_ref not in df_ref.columns or col_uf_ref not in df_ref.columns:
        logging.error("❌ Não foi possível identificar as colunas de referência (Base, Coordenador, UF).")
        return df

    df_ref[col_base_ref] = df_ref[col_base_ref].astype(str).str.strip().str.upper()
    df[COLUNAS["base"]] = df[COLUNAS["base"]].astype(str).str.strip().str.upper()

    df = df.merge(
        df_ref[[col_base_ref, col_coord_ref, col_uf_ref]],
        left_on=COLUNAS["base"],
        right_on=col_base_ref,
        how="left"
    )

    df.rename(columns={
        col_coord_ref: COLUNAS["coordenador"],
        col_uf_ref: "UF"
    }, inplace=True)

    df[COLUNAS["coordenador"]] = df[COLUNAS["coordenador"]].fillna("Coordenador não encontrado")
    df["UF"] = df["UF"].fillna("UF não encontrado")

    return df


def analisar_entregas_consolidado(df, caminho_saida):
    """Gera consolidação em múltiplas abas no Excel, incluindo entregas válidas, inválidas e bases sem coordenador."""

    resumo_base = df.groupby([COLUNAS["base"], "UF"]).agg(
        Total_pedidos=(COLUNAS["pedido"], "nunique"),
        Motoristas_unicos=(COLUNAS["motorista"], lambda x: x.dropna().nunique()),
        Pedidos_domingo=("Entrega no Domingo", "sum")
    ).reset_index()

    resumo_coord = df.groupby(COLUNAS["coordenador"]).agg(
        Total_pedidos=(COLUNAS["pedido"], "nunique"),
        Motoristas_unicos=(COLUNAS["motorista"], lambda x: x.dropna().nunique()),
        Pedidos_domingo=("Entrega no Domingo", "sum")
    ).reset_index()

    resumo_uf = df.groupby("UF").agg(
        Total_pedidos=(COLUNAS["pedido"], "nunique"),
        Motoristas_unicos=(COLUNAS["motorista"], lambda x: x.dropna().nunique()),
        Pedidos_domingo=("Entrega no Domingo", "sum")
    ).reset_index()

    consolidado = pd.DataFrame({
        "Total de pedidos": [df[COLUNAS["pedido"]].nunique()],
        "Total de coordenadores": [df[COLUNAS["coordenador"]].nunique()],
        "Total de bases": [df[COLUNAS["base"]].nunique()],
        "Total de motoristas únicos": [df[COLUNAS["motorista"]].dropna().nunique()],
        "Total pedidos no domingo (assinatura normal)": [df["Entrega no Domingo"].sum()],
        "Total UFs": [df["UF"].nunique()]
    })

    pedidos_domingo_validos = df[df["Entrega no Domingo"] == True][[
        COLUNAS["pedido"], COLUNAS["base"], "UF", COLUNAS["coordenador"],
        COLUNAS["motorista"], COLUNAS["data"], COLUNAS["assinatura"]
    ]]

    pedidos_domingo_invalidos = df[
        (df["data_convertida"].dt.dayofweek == 6) &
        (df[COLUNAS["assinatura"]].astype(str).str.strip() != "Recebimento com assinatura normal")
    ][[
        COLUNAS["pedido"], COLUNAS["base"], "UF", COLUNAS["coordenador"],
        COLUNAS["motorista"], COLUNAS["data"], COLUNAS["assinatura"]
    ]]

    pedidos_total = df[[
        COLUNAS["pedido"], COLUNAS["base"], "UF",
        COLUNAS["coordenador"], COLUNAS["motorista"], COLUNAS["data"], "Entrega no Domingo"
    ]]

    datas_invalidas = df[df["data_convertida"].isna()][[
        COLUNAS["pedido"], COLUNAS["data"]
    ]]

    bases_sem_coord = df[df[COLUNAS["coordenador"]] == "Coordenador não encontrado"][[
        COLUNAS["pedido"], COLUNAS["base"], "UF", COLUNAS["data"]
    ]]

    with pd.ExcelWriter(caminho_saida, engine="openpyxl") as writer:
        resumo_base.to_excel(writer, sheet_name="Resumo por Base", index=False)
        resumo_coord.to_excel(writer, sheet_name="Resumo por Coordenador", index=False)
        resumo_uf.to_excel(writer, sheet_name="Resumo por UF", index=False)
        consolidado.to_excel(writer, sheet_name="Consolidação Geral", index=False)
        pedidos_domingo_validos.to_excel(writer, sheet_name="Domingo Válidos", index=False)
        pedidos_domingo_invalidos.to_excel(writer, sheet_name="Domingo Inválidos", index=False)
        pedidos_total.to_excel(writer, sheet_name="Pedidos Total", index=False)
        datas_invalidas.to_excel(writer, sheet_name="Datas Inválidas", index=False)
        bases_sem_coord.to_excel(writer, sheet_name="Bases sem Coordenador", index=False)

    logging.info(f"✅ Arquivo Excel consolidado salvo em: {caminho_saida}")


def processar_varios_arquivos(lista_arquivos):
    dfs = []
    for caminho in lista_arquivos:
        logging.info(f"📂 Lendo arquivo: {os.path.basename(caminho)}")
        df = pd.read_excel(caminho)

        df.columns = df.columns.astype(str).str.strip()
        df['data_convertida'] = pd.to_datetime(df[COLUNAS["data"]], errors='coerce')

        df['Entrega no Domingo'] = (
            (df['data_convertida'].dt.dayofweek == 6) &
            (df[COLUNAS["assinatura"]].astype(str).str.strip() == "Recebimento com assinatura normal")
        )

        df = adicionar_coordenador(df)
        dfs.append(df)

    if not dfs:
        logging.error("❌ Nenhum DataFrame carregado.")
        return None

    df_final = pd.concat(dfs, ignore_index=True)
    logging.info(f"✅ Total de registros consolidados: {len(df_final)}")
    return df_final


if __name__ == "__main__":
    PREFIXO_DO_ARQUIVO = "Exportar carta de porte de entrega"
    caminho_da_pasta = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Motorista"

    arquivos_encontrados = encontrar_arquivos_por_prefixo(caminho_da_pasta, PREFIXO_DO_ARQUIVO)

    if arquivos_encontrados:
        df_consolidado = processar_varios_arquivos(arquivos_encontrados)

        if df_consolidado is not None and validar_colunas(df_consolidado):
            caminho_saida = os.path.join(caminho_da_pasta, "Analise_Consolidada.xlsx")
            analisar_entregas_consolidado(df_consolidado, caminho_saida)
    else:
        logging.error("❌ Operação cancelada. Nenhum arquivo válido foi encontrado.")