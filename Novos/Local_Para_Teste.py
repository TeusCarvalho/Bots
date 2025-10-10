
import os
import pandas as pd
import logging
from datetime import datetime

# =====================================================================
# CONFIGURAÇÕES
# =====================================================================
PASTA_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Local de Teste\Entrega Realizada - Dia"
DATA_ATUAL = datetime.now().strftime("%Y-%m-%d")  # exemplo: 2025-10-08
ARQUIVO_SAIDA = os.path.join(PASTA_ENTRADA, f"Resumo_Entregas_{DATA_ATUAL}.xlsx")

# Configuração do log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("relatorio_entregas.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)


# =====================================================================
# FUNÇÕES
# =====================================================================

def listar_arquivos_excel(pasta: str):
    """Lista todos os arquivos Excel da pasta informada."""
    arquivos = [os.path.join(pasta, f) for f in os.listdir(pasta) if f.endswith((".xlsx", ".xls"))]
    logging.info(f"🔎 {len(arquivos)} arquivos encontrados.")
    return arquivos


def ler_arquivos(arquivos: list) -> pd.DataFrame:
    """Lê todos os arquivos Excel (todas as abas) e retorna um único DataFrame consolidado."""
    dfs = []
    for arquivo in arquivos:
        try:
            logging.info(f"📄 Lendo arquivo: {arquivo}")

            # Lê todas as abas do arquivo
            abas = pd.read_excel(arquivo, engine="openpyxl", sheet_name=None)

            for nome_aba, df in abas.items():
                if not df.empty:
                    logging.info(f"   ➕ Processando aba: {nome_aba} ({len(df)} linhas)")
                    df["Arquivo_Origem"] = os.path.basename(arquivo)
                    df["Aba_Origem"] = nome_aba
                    dfs.append(df)

        except Exception as e:
            logging.error(f"❌ Erro ao ler {arquivo}: {e}")

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def processar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra por data atual e consolida resultados por base de entrega."""
    if df.empty:
        return pd.DataFrame()

    # Nomes das colunas principais
    col_remessa = "Remessa"
    col_base = "Base de entrega"
    col_data = "Data prevista de entrega"
    col_prazo = "Entregue no prazo？"

    # Converter coluna de data para formato YYYY-MM-DD
    df[col_data] = pd.to_datetime(df[col_data], errors="coerce").dt.strftime("%Y-%m-%d")

    # Filtrar pelo dia atual
    df_filtrado = df[df[col_data] == DATA_ATUAL]

    if df_filtrado.empty:
        logging.warning(f"⚠️ Nenhum registro encontrado para {DATA_ATUAL}")
        return pd.DataFrame()

    # Consolidação apenas por base de entrega
    resumo = df_filtrado.groupby(col_base).agg(
        Total_Pedidos=(col_remessa, "count"),
        Entregues_no_Prazo=(col_prazo, lambda x: (x == "Sim").sum())
    ).reset_index()

    # Calcular taxa
    resumo["Taxa_de_Entrega_%"] = (
            resumo["Entregues_no_Prazo"] / resumo["Total_Pedidos"] * 100
    ).round(2)

    # Totais gerais
    total_pedidos = resumo["Total_Pedidos"].sum()
    total_entregues = resumo["Entregues_no_Prazo"].sum()
    taxa_geral = (total_entregues / total_pedidos * 100).round(2) if total_pedidos > 0 else 0

    logging.info(f"📊 Total do dia: {total_pedidos}")
    logging.info(f"📦 Entregues no prazo: {total_entregues}")
    logging.info(f"✅ Taxa geral: {taxa_geral}%")

    # Linha total
    resumo_total = pd.DataFrame([{
        col_base: "TOTAL GERAL",
        "Total_Pedidos": total_pedidos,
        "Entregues_no_Prazo": total_entregues,
        "Taxa_de_Entrega_%": taxa_geral
    }])

    resumo = pd.concat([resumo, resumo_total], ignore_index=True)

    return resumo


def salvar_arquivo(df: pd.DataFrame, caminho: str):
    """Salva o DataFrame consolidado em um arquivo Excel."""
    try:
        df.to_excel(caminho, index=False, engine="openpyxl")
        logging.info(f"✅ Arquivo resumo salvo em: {caminho}")
    except Exception as e:
        logging.error(f"❌ Erro ao salvar o arquivo: {e}")


# =====================================================================
# EXECUÇÃO PRINCIPAL
# =====================================================================

def main():
    arquivos = listar_arquivos_excel(PASTA_ENTRADA)
    if not arquivos:
        logging.warning("⚠️ Nenhum arquivo Excel encontrado. Encerrando.")
        return

    df_consolidado = ler_arquivos(arquivos)

    df_resumo = processar_dados(df_consolidado)

    if df_resumo.empty:
        logging.warning("⚠️ Nenhum dado válido para gerar resumo.")
        return

    salvar_arquivo(df_resumo, ARQUIVO_SAIDA)


if __name__ == "__main__":
    main()
