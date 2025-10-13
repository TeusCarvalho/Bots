# -*- coding: utf-8 -*-
import pandas as pd
import os
import logging
from datetime import datetime

# ===========================================================
# Configuração de Logs Coloridos
# ===========================================================
class ColorFormatter(logging.Formatter):
    COLORS = {
        logging.INFO: "\033[92m",     # Verde
        logging.WARNING: "\033[93m",  # Amarelo
        logging.ERROR: "\033[91m",    # Vermelho
    }

    def format(self, record):
        color = self.COLORS.get(record.levelno, "")
        reset = "\033[0m"
        message = super().format(record)
        return f"{color}{message}{reset}"

handler = logging.StreamHandler()
handler.setFormatter(ColorFormatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.getLogger().handlers = [handler]
logging.getLogger().setLevel(logging.INFO)

# ===========================================================
# Configuração de Colunas
# ===========================================================
COLUNAS = {
    "coordenador": "Coordenador",
    "motorista": "Responsável pela entrega",
    "pedido": "Número de pedido JMS",
    "assinatura": "Marca de assinatura",
    "data": "Tempo de entrega",
    "base": "Base de entrega"
}

# ===========================================================
# Funções Principais
# ===========================================================
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
    """Gera consolidação em múltiplas abas no Excel, com controle de tamanho."""
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

    # 🚀 Motoristas únicos
    motoristas_por_base = df.groupby(["UF", COLUNAS["base"]])[COLUNAS["motorista"]].nunique().reset_index()
    motoristas_por_base.rename(columns={COLUNAS["motorista"]: "Motoristas Únicos"}, inplace=True)

    motoristas_por_uf = df.groupby("UF")[COLUNAS["motorista"]].nunique().reset_index()
    motoristas_por_uf.rename(columns={COLUNAS["motorista"]: "Motoristas Únicos"}, inplace=True)

    total_geral = pd.DataFrame({
        "UF": ["TOTAL GERAL"],
        "Motoristas Únicos": [df[COLUNAS["motorista"]].dropna().nunique()]
    })

    consolidado = pd.DataFrame({
        "Total de pedidos": [df[COLUNAS["pedido"]].nunique()],
        "Total de coordenadores": [df[COLUNAS["coordenador"]].nunique()],
        "Total de bases": [df[COLUNAS["base"]].nunique()],
        "Total de motoristas únicos": [df[COLUNAS["motorista"]].dropna().nunique()],
        "Total pedidos no domingo (assinatura normal)": [df["Entrega no Domingo"].sum()],
        "Total UFs": [df["UF"].nunique()]
    })

    pedidos_total = df[[
        COLUNAS["pedido"], COLUNAS["base"], "UF",
        COLUNAS["coordenador"], COLUNAS["motorista"], COLUNAS["data"], "Entrega no Domingo"
    ]]

    # 🚧 Divisão automática se ultrapassar limite do Excel
    MAX_LINHAS = 1_048_000

    with pd.ExcelWriter(caminho_saida, engine="openpyxl") as writer:
        resumo_base.to_excel(writer, sheet_name="Resumo por Base", index=False)
        resumo_coord.to_excel(writer, sheet_name="Resumo por Coordenador", index=False)
        resumo_uf.to_excel(writer, sheet_name="Resumo por UF", index=False)

        motoristas_por_base.to_excel(writer, sheet_name="Motoristas Únicos", index=False, startrow=0)
        motoristas_por_uf.to_excel(writer, sheet_name="Motoristas Únicos", index=False, startrow=len(motoristas_por_base) + 3)
        total_geral.to_excel(writer, sheet_name="Motoristas Únicos", index=False, startrow=len(motoristas_por_base) + len(motoristas_por_uf) + 6)

        consolidado.to_excel(writer, sheet_name="Consolidação Geral", index=False)

        # 🚀 Exportação segura da aba de pedidos
        if len(pedidos_total) > MAX_LINHAS:
            logging.warning(f"⚠️ Planilha grande ({len(pedidos_total):,} linhas). Quebrando em múltiplas abas...")
            num_parts = (len(pedidos_total) // MAX_LINHAS) + 1
            for i in range(num_parts):
                start = i * MAX_LINHAS
                end = start + MAX_LINHAS
                pedidos_total.iloc[start:end].to_excel(
                    writer,
                    sheet_name=f"Pedidos Total_{i+1}",
                    index=False
                )
        else:
            pedidos_total.to_excel(writer, sheet_name="Pedidos Total", index=False)

    # 🚀 Salvar CSV completo se exceder limite
    if len(pedidos_total) > MAX_LINHAS:
        csv_path = caminho_saida.replace(".xlsx", "_Pedidos_Total.csv")
        pedidos_total.to_csv(csv_path, index=False, encoding="utf-8-sig")
        logging.info(f"📁 Planilha detalhada salva separadamente: {csv_path}")

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


# ===========================================================
# Execução Principal
# ===========================================================
if __name__ == "__main__":
    PREFIXO_DO_ARQUIVO = "Exportar carta de porte de entrega"
    caminho_da_pasta = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Motorista"

    arquivos_encontrados = encontrar_arquivos_por_prefixo(caminho_da_pasta, PREFIXO_DO_ARQUIVO)

    if arquivos_encontrados:
        df_consolidado = processar_varios_arquivos(arquivos_encontrados)

        if df_consolidado is not None:
            agora = datetime.now().strftime("%Y%m%d_%H%M%S")
            caminho_saida = os.path.join(caminho_da_pasta, f"Analise_Consolidada_{agora}.xlsx")

            analisar_entregas_consolidado(df_consolidado, caminho_saida)

            # 📊 Resumo final no console
            logging.info(f"""
📊 Resumo Final:
• Pedidos: {df_consolidado[COLUNAS["pedido"]].nunique()}
• Coordenadores: {df_consolidado[COLUNAS["coordenador"]].nunique()}
• Bases: {df_consolidado[COLUNAS["base"]].nunique()}
• Motoristas únicos: {df_consolidado[COLUNAS["motorista"]].nunique()}
""")
    else:
        logging.error("❌ Operação cancelada. Nenhum arquivo válido foi encontrado.")
