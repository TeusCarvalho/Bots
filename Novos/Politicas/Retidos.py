# -*- coding: utf-8 -*-
import os
import json
import re
import logging
import polars as pl
# CORREÇÃO 1: Importar timedelta junto com datetime
from datetime import datetime, timedelta
import requests


# ============================================================
# 🧩 FUNÇÕES AUXILIARES (GLOBAIS)
# ============================================================

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            # Verifique se este caminho de log está correto para você
            logging.FileHandler("../../Antigos/analise_retidos.log", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


def converter_datetime(df: pl.DataFrame, coluna: str) -> pl.DataFrame:
    if coluna not in df.columns:
        return df

    formatos = [
        "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M",
        "%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"
    ]

    for fmt in formatos:
        try:
            newdf = df.with_columns(
                pl.col(coluna).str.strptime(pl.Datetime, fmt, strict=False)
            )
            if newdf[coluna].is_not_null().any():
                logging.info(f"✔️ Coluna '{coluna}' convertida com sucesso usando o formato {fmt}.")
                return newdf
        except Exception as e:
            pass

    logging.warning(f"⚠️ Falha ao converter a coluna '{coluna}' com todos os formatos conhecidos.")
    return df


def detectar_coluna(df: pl.DataFrame, candidatos: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidatos:
        cand = cand.lower()
        if cand in cols:
            return cols[cand]
        for low, orig in cols.items():
            if cand in low:
                return orig
    return None


def safe_pick(df: pl.DataFrame, preferido: str, extras: list[str]) -> str | None:
    if preferido in df.columns:
        return preferido
    return detectar_coluna(df, extras)


def limpar_pedidos(df: pl.DataFrame, coluna: str) -> pl.DataFrame:
    if coluna in df.columns:
        df = df.with_columns(pl.col(coluna).cast(pl.Utf8).str.strip_chars())
    return df


def ler_planilhas(pasta: str, nome: str) -> pl.DataFrame:
    if not os.path.exists(pasta):
        logging.error(f"❌ Pasta '{pasta}' não existe.")
        return pl.DataFrame()

    arquivos = [a for a in os.listdir(pasta)
                if a.lower().endswith((".xls", ".xlsx"))
                and not a.startswith("~$")]

    if not arquivos:
        logging.warning(f"⚠️ Nenhum arquivo .xls ou .xlsx encontrado na pasta '{pasta}'.")
        return pl.DataFrame()

    logging.info(f"📂 {len(arquivos)} arquivos encontrados em '{nome}' para leitura.")

    dfs = []
    for arq in arquivos:
        path = os.path.join(pasta, arq)
        try:
            raw = pl.read_excel(path)
            df = next(iter(raw.values())) if isinstance(raw, dict) else raw
            dfs.append(df)
            logging.info(f"   ✔️ Arquivo '{arq}' lido com sucesso ({df.height} linhas).")
        except Exception as e:
            logging.error(f"   ❌ Erro ao ler o arquivo '{arq}': {e}")

    if not dfs:
        logging.warning(f"⚠️ Não foi possível ler nenhum arquivo da pasta '{pasta}'.")
        return pl.DataFrame()

    return pl.concat(dfs, how="diagonal_relaxed")


def salvar_resultado(df: pl.DataFrame, pasta: str, nome: str, limit: int):
    os.makedirs(pasta, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    ext = "csv" if df.height >= limit else "xlsx"
    caminho = os.path.join(pasta, f"{nome}_{ts}.{ext}")

    if ext == "csv":
        df.write_csv(caminho)
    else:
        df.write_excel(caminho)

    logging.info(f"💾 Resultado salvo em: {caminho}")
    return caminho


def limpar_nome(nome: str) -> str:
    if not nome:
        return ""
    nome = str(nome).upper().strip()

    nome = re.sub(r"[^\x00-\x7F]+", "", nome)
    nome = re.sub(r"[-_]+", " ", nome)
    nome = re.sub(r"\s+", " ", nome)

    partes = nome.split(" ")

    if len(partes) == 2 and len(partes[0]) == 3 and len(partes[1]) == 2:
        return f"{partes[1]} {partes[0]}"

    return nome.strip()


def salvar_relatorio_intermediario(df: pl.DataFrame, nome: str, config: dict):
    """Função auxiliar para salvar os DataFrames intermediários."""
    pasta_saida = config["caminhos"]["pasta_saida"]
    os.makedirs(pasta_saida, exist_ok=True)
    caminho = os.path.join(pasta_saida, f"{nome}.parquet")
    df.write_parquet(caminho)
    logging.info(f"📄 Relatório intermediário salvo: {caminho}")


class AnaliseRetidos:
    # 🔥 ALTERAÇÃO AQUI: Mudar o arquivo de configuração padrão
    def __init__(self, config_filename="config_2.json"):
        base = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(base, config_filename)

        if not os.path.exists(path):
            logging.error(f"❌ Arquivo de configuração não encontrado em: {path}")
            raise FileNotFoundError(f"Arquivo de configuração não encontrado: {path}")

        with open(path, "r", encoding="utf-8") as f:
            self.config = json.load(f)

        self.removidos = {"cluster": 0, "devolucao": 0, "problematicos": 0, "custodia": 0}
        self.total_inicial_filtrado = 0
        self.df_total_por_base = pl.DataFrame()

    def executar(self):
        logging.info("🚀 Iniciando análise de pacotes retidos...")

        df = self._processar_dados()

        if df.is_empty():
            logging.error("❌ Nenhum dado final encontrado após o processamento. O script será encerrado.")
            return

        logging.info(f"📊 DataFrame final possui {df.height} linhas e {len(df.columns)} colunas.")

        df = self._enriquecer_com_coordenadores(df)

        if "Coordenador" not in df.columns:
            logging.error(
                "❌ A coluna 'Coordenador' não foi encontrada no DataFrame após o enriquecimento. Não é possível enviar os cards.")
            return

        coordenadores_encontrados = df.filter(pl.col("Coordenador").is_not_null())["Coordenador"].unique().to_list()
        if not coordenadores_encontrados:
            logging.error("❌ Nenhum coordenador foi encontrado/enriquecido nos dados. Não há cards para enviar.")
            return

        logging.info(f"📊 Coordenadores encontrados para envio: {len(coordenadores_encontrados)}")

        df_anterior = self._carregar_snapshot_anterior()
        if df_anterior.is_empty():
            logging.info("📂 Nenhum snapshot do dia anterior encontrado. A variação será zero.")
        else:
            logging.info(f"📂 Snapshot do dia anterior carregado com {df_anterior.height} linhas.")

        self._gerar_log_comparativo(df, df_anterior)

        self._enviar_card_completo(df, df_anterior)

        self._salvar_snapshot_diario(df)

        caminho_final = self._salvar_resultado_final(df)
        self._exibir_resumo_console(caminho_final)

    # ============================================================
    # 🔧 PROCESSO PRINCIPAL — PIPELINE COMPLETO
    # ============================================================
    def _processar_dados(self):
        df = self._ler_e_preparar_retidos()
        if df.is_empty():
            logging.error("❌ Nenhum dado retido inicial foi lido. Abortando processamento.")
            return pl.DataFrame()

        logging.info(f"📊 Após leitura inicial: {df.height} pacotes retidos.")
        salvar_relatorio_intermediario(df, "00_Retidos_Iniciais", self.config)

        df = self._aplicar_filtro_devolucao(df)
        logging.info(f"📊 Após filtro de devolução: {df.height} pacotes restantes.")

        df = self._aplicar_filtro_problematicos(df)
        logging.info(f"📊 Após filtro de problemáticos: {df.height} pacotes restantes.")

        df = self._aplicar_filtro_custodia(df)
        logging.info(f"📊 Após filtro de custódia: {df.height} pacotes restantes (FINAL).")

        return df

    # ============================================================
    # 📥 Leitura + organização dos retidos
    # ============================================================
    def _ler_e_preparar_retidos(self):
        df = ler_planilhas(self.config["caminhos"]["pasta_retidos"], "Retidos")
        if df.is_empty():
            return pl.DataFrame()

        col_dias = safe_pick(df, "Dias Retidos 滞留日", ["滞留", "dias"])
        if col_dias:
            antes = df.height
            df = df.with_columns(pl.col(col_dias).cast(pl.Int64, strict=False))
            df = df.filter(pl.col(col_dias) > 6)
            self.removidos["cluster"] = antes - df.height
            logging.info(f"🔵 Filtro de >6 dias aplicado. Removidos: {self.removidos['cluster']}.")

        col_base = safe_pick(df, "Base de Entrega 派件网点", ["base", "网点"])
        if col_base:
            self.df_total_por_base = (
                df.with_columns(
                    pl.col(col_base)
                    .map_elements(limpar_nome, return_dtype=pl.Utf8)
                    .alias("Base_Clean")
                )
                .group_by("Base_Clean")
                .agg(pl.len().alias("Total de Pedidos"))
            )

        col_pedido = safe_pick(df, self.config["colunas"]["col_pedido_ret"], ["pedido", "运单"])
        col_data = safe_pick(df, self.config["colunas"]["col_data_atualizacao_ret"], ["data", "更新"])
        col_regional = safe_pick(df, self.config["colunas"]["col_regional_ret"], ["regional", "区域"])

        if not all([col_pedido, col_data, col_regional, col_base]):
            logging.error(
                "❌ Uma ou mais colunas essenciais não foram encontradas na planilha de Retidos. Verifique o config.json e os nomes das colunas.")
            return pl.DataFrame()

        df = df.select([col_pedido, col_data, col_regional, col_base]).rename({
            col_pedido: self.config["colunas"]["col_pedido_ret"],
            col_data: self.config["colunas"]["col_data_atualizacao_ret"],
            col_regional: self.config["colunas"]["col_regional_ret"],
            col_base: "Base de Entrega 派件网点"
        })

        df = limpar_pedidos(df, self.config["colunas"]["col_pedido_ret"])
        df = converter_datetime(df, self.config["colunas"]["col_data_atualizacao_ret"])

        df = df.filter(
            pl.col(self.config["colunas"]["col_regional_ret"])
            .is_in(pl.Series(self.config["parametros"]["regionais_desejadas"]))
        )

        self.total_inicial_filtrado = df.height
        return df

    # ============================================================
    # 🔵 Filtro: Devolução
    # ============================================================
    def _aplicar_filtro_devolucao(self, df):
        df_dev = ler_planilhas(self.config["caminhos"]["pasta_devolucao"], "Devolução")
        if df_dev.is_empty():
            logging.warning("⚠️ Planilha de devolução não encontrada ou vazia. Pulando este filtro.")
            return df

        col_pedido = safe_pick(df_dev, self.config["colunas"]["col_pedido_dev"], ["pedido"])
        col_data = safe_pick(df_dev, self.config["colunas"]["col_data_solicitacao_dev"], ["tempo", "solic"])

        if not col_pedido or not col_data:
            logging.warning(
                "⚠️ Colunas de pedido ou data não encontradas na planilha de devolução. Pulando este filtro.")
            return df

        df_dev = (
            df_dev.select([col_pedido, col_data])
            .rename({
                col_pedido: self.config["colunas"]["col_pedido_dev"],
                col_data: self.config["colunas"]["col_data_solicitacao_dev"]
            })
            .pipe(limpar_pedidos, self.config["colunas"]["col_pedido_dev"])
            .pipe(converter_datetime, self.config["colunas"]["col_data_solicitacao_dev"])
            .group_by(self.config["colunas"]["col_pedido_dev"])
            .agg(pl.col(self.config["colunas"]["col_data_solicitacao_dev"]).min())
        )

        dfj = df.join(
            df_dev,
            left_on=self.config["colunas"]["col_pedido_ret"],
            right_on=self.config["colunas"]["col_pedido_dev"],
            how="left"
        )

        df_rem = dfj.filter(
            (pl.col(self.config["colunas"]["col_data_solicitacao_dev"])
             > pl.col(self.config["colunas"]["col_data_atualizacao_ret"]))
            &
            pl.col(self.config["colunas"]["col_data_solicitacao_dev"]).is_not_null()
        )

        salvar_relatorio_intermediario(df_rem, "01_Removidos_Devolucao", self.config)

        remover = df_rem.select(self.config["colunas"]["col_pedido_ret"]).to_series()
        self.removidos["devolucao"] = remover.len()
        logging.info(f"🔵 Filtro de devolução aplicado. Removidos: {self.removidos['devolucao']}.")

        return df.filter(~pl.col(self.config["colunas"]["col_pedido_ret"]).is_in(remover))

    # ============================================================
    # 🟣 Filtro: Problemáticos
    # ============================================================
    def _aplicar_filtro_problematicos(self, df):
        df_prob = ler_planilhas(self.config["caminhos"]["pasta_problematicos"], "Problemáticos")
        if df_prob.is_empty():
            logging.warning("⚠️ Planilha de problemáticos não encontrada ou vazia. Pulando este filtro.")
            return df

        col_pedido = safe_pick(df_prob, "Número de pedido JMS", ["pedido", "运单"])
        col_data = safe_pick(df_prob, "data de registro", ["registro", "异常"])

        if not col_pedido or not col_data:
            logging.warning(
                "⚠️ Colunas de pedido ou data não encontradas na planilha de problemáticos. Pulando este filtro.")
            return df

        df_prob = (
            df_prob.select([col_pedido, col_data])
            .rename({
                col_pedido: "Pedido_Prob",
                col_data: "Registro_Prob"
            })
            .pipe(limpar_pedidos, "Pedido_Prob")
            .pipe(converter_datetime, "Registro_Prob")
            .group_by("Pedido_Prob")
            .agg(pl.col("Registro_Prob").min())
        )

        dfj = df.join(
            df_prob,
            left_on=self.config["colunas"]["col_pedido_ret"],
            right_on="Pedido_Prob",
            how="left"
        )

        df_rem = dfj.filter(
            pl.col("Registro_Prob") >= pl.col(self.config["colunas"]["col_data_atualizacao_ret"])
        )

        salvar_relatorio_intermediario(df_rem, "02_Removidos_Problematicos", self.config)

        remover = df_rem.select(self.config["colunas"]["col_pedido_ret"]).to_series()
        self.removidos["problematicos"] = remover.len()
        logging.info(f"🟣 Filtro de problemáticos aplicado. Removidos: {self.removidos['problematicos']}.")

        return df.filter(~pl.col(self.config["colunas"]["col_pedido_ret"]).is_in(remover))

    # ============================================================
    # 🟦 Filtro: Custódia
    # ============================================================
    def _aplicar_filtro_custodia(self, df):
        df_cust = ler_planilhas(self.config["caminhos"]["pasta_custodia"], "Custódia")
        if df_cust.is_empty():
            logging.warning("⚠️ Planilha de custódia não encontrada ou vazia. Pulando este filtro.")
            return df

        col_pedido = safe_pick(df_cust, self.config["colunas"]["col_pedido_cust"], ["pedido"])
        col_data = safe_pick(df_cust, self.config["colunas"]["col_data_registro_cust"], ["registro"])

        if not col_pedido or not col_data:
            logging.warning(
                "⚠️ Colunas de pedido ou data não encontradas na planilha de custódia. Pulando este filtro.")
            return df

        df_cust = (
            df_cust.select([col_pedido, col_data])
            .rename({
                col_pedido: self.config["colunas"]["col_pedido_cust"],
                col_data: self.config["colunas"]["col_data_registro_cust"]
            })
            .pipe(limpar_pedidos, self.config["colunas"]["col_pedido_cust"])
            .pipe(converter_datetime, self.config["colunas"]["col_data_registro_cust"])
            .group_by(self.config["colunas"]["col_pedido_cust"])
            .agg(pl.col(self.config["colunas"]["col_data_registro_cust"]).min())
            .with_columns(
                (
                        pl.col(self.config["colunas"]["col_data_registro_cust"])
                        + pl.duration(days=self.config["parametros"]["prazo_custodia_dias"])
                ).alias("Prazo_Limite")
            )
        )

        dfj = df.join(
            df_cust,
            left_on=self.config["colunas"]["col_pedido_ret"],
            right_on=self.config["colunas"]["col_pedido_cust"],
            how="left"
        )

        col_data_atual = self.config["colunas"]["col_data_atualizacao_ret"]

        dfj = dfj.with_columns([
            pl.when(
                (pl.col(col_data_atual) <= pl.col("Prazo_Limite"))
                & pl.col("Prazo_Limite").is_not_null()
            )
            .then(pl.lit("Dentro"))
            .otherwise(pl.lit("Fora"))
            .alias("Status_Custodia")
        ])

        df_rem = dfj.filter(pl.col("Status_Custodia") == "Dentro")

        salvar_relatorio_intermediario(df_rem, "03_Removidos_Custodia", self.config)

        self.removidos["custodia"] = df_rem.height
        logging.info(f"🟦 Filtro de custódia aplicado. Removidos: {self.removidos['custodia']}.")

        return dfj.filter(pl.col("Status_Custodia") == "Fora").drop("Status_Custodia", "Prazo_Limite")

    # ============================================================
    # 🤝 Coordenadores (Com Normalização)
    # ============================================================
    def _enriquecer_com_coordenadores(self, df):
        path = self.config["caminhos"]["caminho_coordenador"]
        if not os.path.exists(path):
            logging.warning(f"⚠️ Planilha de coordenadores não encontrada em: {path}")
            return df

        try:
            raw = pl.read_excel(path)
            dfc = next(iter(raw.values())) if isinstance(raw, dict) else raw

            logging.info(f"📂 Lendo planilha de coordenadores. Colunas encontradas: {dfc.columns}")

            col_base = detectar_coluna(dfc, ["base", "nome da base", "entrega", "派件网点"])
            col_coord = detectar_coluna(dfc, ["coordenador", "responsável", "负责人"])

            logging.info(f"📂 Coluna de Base detectada: '{col_base}' | Coluna de Coordenador detectada: '{col_coord}'")

            if not col_base or not col_coord:
                logging.warning(
                    "⚠️ Colunas de 'base' ou 'coordenador' não foram detectadas na planilha. Verifique os nomes.")
                return df

            dfc = dfc.with_columns([
                pl.col(col_base).map_elements(limpar_nome, return_dtype=pl.Utf8).alias("Base_Coord"),
                pl.col(col_coord).alias("Coordenador")
            ]).select(["Base_Coord", "Coordenador"])

            df = df.with_columns(
                pl.col("Base de Entrega 派件网点")
                .map_elements(limpar_nome, return_dtype=pl.Utf8)
                .alias("Base_Normalizada")
            )

            df_final = df.join(
                dfc,
                left_on="Base_Normalizada",
                right_on="Base_Coord",
                how="left"
            )

            nulos_apos_join = df_final.filter(pl.col("Coordenador").is_null()).height
            if nulos_apos_join > 0:
                logging.warning(
                    f"⚠️ Após o join, {nulos_apos_join} bases não encontraram um coordenador correspondente.")

            return df_final

        except Exception as e:
            logging.error(f"❌ Erro ao processar planilha de coordenadores: {e}")
            return df

    # ============================================================
    # 🆕 RELATÓRIO DE COMPARAÇÃO (NOVO)
    # ============================================================
    def _gerar_log_comparativo(self, df_atual: pl.DataFrame, df_anterior: pl.DataFrame):
        logging.info("📈 Gerando relatório de comparação com o dia anterior...")

        total_atual = df_atual.height
        total_anterior = df_anterior.height if not df_anterior.is_empty() else 0
        diff_total = total_atual - total_anterior

        atual_group = df_atual.group_by("Base_Normalizada").agg(pl.len().alias("Qtd_Atual"))

        if df_anterior.is_empty():
            ant_group = pl.DataFrame(schema={"Base_Normalizada": pl.Utf8, "Qtd_Anterior": pl.Int64})
        else:
            ant_group = df_anterior.group_by("Base_Normalizada").agg(pl.len().alias("Qtd_Anterior"))

        comparacao_bases = (
            atual_group.join(ant_group, on="Base_Normalizada", how="left")
            .with_columns([
                pl.col("Qtd_Anterior").fill_null(0),
                (pl.col("Qtd_Atual") - pl.col("Qtd_Anterior")).alias("Variacao")
            ])
        )

        top_aumentos = comparacao_bases.filter(pl.col("Variacao") > 0).sort("Variacao", descending=True).head(5)
        top_reducoes = comparacao_bases.filter(pl.col("Variacao") < 0).sort("Variacao").head(5)

        texto_log = [
            "=" * 50,
            f"📊 RELATÓRIO COMPARATIVO - {datetime.now():%d/%m/%Y}",
            "=" * 50,
            "",
            "📈 **RESUMO GERAL:**",
            f"  - Total de Retidos (Hoje): {total_atual}",
            f"  - Total de Retidos (Ontem): {total_anterior}",
            f"  - Variação Geral: {'+' if diff_total >= 0 else ''}{diff_total} ({'Aumento' if diff_total > 0 else 'Redução' if diff_total < 0 else 'Estável'})",
            "",
            "🔴 **TOP 5 BASES COM MAIOR AUMENTO:**",
        ]

        if top_aumentos.is_empty():
            texto_log.append("  - Nenhuma base apresentou aumento.")
        else:
            for row in top_aumentos.iter_rows(named=True):
                texto_log.append(
                    f"  - {row['Base_Normalizada']}: +{row['Variacao']} (De {row['Qtd_Anterior']} para {row['Qtd_Atual']})")

        texto_log.extend([
            "",
            "🟢 **TOP 5 BASES COM MAIOR REDUÇÃO:**",
        ])

        if top_reducoes.is_empty():
            texto_log.append("  - Nenhuma base apresentou redução.")
        else:
            for row in top_reducoes.iter_rows(named=True):
                texto_log.append(
                    f"  - {row['Base_Normalizada']}: {row['Variacao']} (De {row['Qtd_Anterior']} para {row['Qtd_Atual']})")

        texto_log.append("=" * 50)

        pasta_saida = self.config["caminhos"]["pasta_saida"]
        os.makedirs(pasta_saida, exist_ok=True)
        caminho_log = os.path.join(pasta_saida, f"log_comparativo_{datetime.now():%Y%m%d}.log")

        with open(caminho_log, "w", encoding="utf-8") as f:
            f.write("\n".join(texto_log))

        logging.info(f"📄 Relatório comparativo salvo em: {caminho_log}")

        logging.info("\n" + "\n".join(texto_log))

    # ============================================================
    # 💾 Salvamento final
    # ============================================================
    def _salvar_resultado_final(self, df):
        return salvar_resultado(
            df,
            self.config["caminhos"]["pasta_saida"],
            self.config["parametros"]["nome_arquivo_final"],
            self.config["parametros"]["excel_row_limit"]
        )

    # ============================================================
    # 📟 Resumo no console
    # ============================================================
    def _exibir_resumo_console(self, caminho):
        logging.info("=============== RESUMO FINAL ===============")
        logging.info(f"Retidos iniciais (filtrados por regional e >6 dias): {self.total_inicial_filtrado}")
        logging.info(f"Removidos - Cluster (>6 dias): {self.removidos['cluster']}")
        logging.info(f"Removidos - Devolução: {self.removidos['devolucao']}")
        logging.info(f"Removidos - Problemáticos: {self.removidos['problematicos']}")
        logging.info(f"Removidos - Custódia (no prazo): {self.removidos['custodia']}")
        total_final = self.total_inicial_filtrado - sum(self.removidos.values())
        logging.info(f"Total de pacotes retidos para ação: {total_final}")
        logging.info(f"Arquivo final salvo em: {caminho}")
        logging.info("===========================================")

    # ============================================================
    # 💾 Salvar snapshot do dia
    # ============================================================
    def _salvar_snapshot_diario(self, df: pl.DataFrame):
        pasta = os.path.join(self.config["caminhos"]["pasta_saida"], "Snapshots")
        os.makedirs(pasta, exist_ok=True)

        hoje = datetime.now().strftime("%Y%m%d")
        caminho = os.path.join(pasta, f"retidos_{hoje}.parquet")

        df.write_parquet(caminho)
        logging.info(f"📦 Snapshot diário salvo em: {caminho}")
        return caminho

    # ============================================================
    # 📂 Carregar snapshot anterior (VERSÃO MELHORADA E MAIS CONFIÁVEL)
    # ============================================================
    def _carregar_snapshot_anterior(self) -> pl.DataFrame:
        pasta = os.path.join(self.config["caminhos"]["pasta_saida"], "Snapshots")

        if not os.path.exists(pasta):
            logging.info("📂 Pasta de Snapshots não existe. Nenhum dado anterior para comparar.")
            return pl.DataFrame()

        # CORREÇÃO 2: Usar timedelta do Python para calcular a data de ontem
        ontem = datetime.now() - timedelta(days=1)
        nome_arquivo_anterior = ontem.strftime("retidos_%Y%m%d.parquet")
        caminho_anterior = os.path.join(pasta, nome_arquivo_anterior)

        if os.path.exists(caminho_anterior):
            try:
                df = pl.read_parquet(caminho_anterior)
                logging.info(f"📂 Snapshot do dia anterior ('{nome_arquivo_anterior}') carregado com sucesso.")
                return df
            except Exception as e:
                logging.error(f"❌ Falha ao ler o snapshot do dia anterior '{nome_arquivo_anterior}': {e}")
                return pl.DataFrame()
        else:
            logging.warning(
                f"⚠️ Snapshot do dia anterior ('{nome_arquivo_anterior}') não foi encontrado. A comparação não será feita.")
            return pl.DataFrame()

    # ============================================================
    # 📮 CARD COMPLETO POR COORDENADOR (Webhook fixo) - VERSÃO CORRIGIDA
    # ============================================================
    def _enviar_card_completo(self, df_atual: pl.DataFrame, df_anterior: pl.DataFrame):
        WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b"

        logging.info("📮 Iniciando a montagem e envio dos cards para os coordenadores...")

        atual_group = (
            df_atual.group_by(["Coordenador", "Base_Normalizada"])
            .agg(pl.len().alias("Qtd_Atual"))
        )

        if df_anterior.is_empty():
            ant_group = pl.DataFrame(
                schema={"Coordenador": pl.Utf8, "Base_Normalizada": pl.Utf8, "Qtd_Anterior": pl.Int64})
        else:
            ant_group = (
                df_anterior.group_by(["Coordenador", "Base_Normalizada"])
                .agg(pl.len().alias("Qtd_Anterior"))
            )

        resumo = (
            atual_group.join(
                ant_group,
                on=["Coordenador", "Base_Normalizada"],
                how="left"
            )
            .with_columns(
                pl.col("Qtd_Anterior").fill_null(0)
            )
            .with_columns(
                (pl.col("Qtd_Atual") - pl.col("Qtd_Anterior")).alias("Variacao")
            )
        )

        coordenadores = resumo.filter(pl.col("Coordenador").is_not_null())["Coordenador"].unique().to_list()
        logging.info(f"📮 Total de coordenadores únicos a processar: {len(coordenadores)}")

        for coord in coordenadores:
            if not coord:
                continue

            dfc = resumo.filter(pl.col("Coordenador") == coord)
            total_atual = dfc["Qtd_Atual"].sum()
            total_anterior = dfc["Qtd_Anterior"].sum()
            diff_total = total_atual - total_anterior

            top3 = dfc.sort("Qtd_Atual", descending=True).head(3)

            linhas = []
            for row in top3.iter_rows(named=True):
                base = row["Base_Normalizada"]
                qtd = row["Qtd_Atual"]
                var = row["Variacao"]

                seta = "🔺" if var > 0 else "🟢" if var < 0 else "⚪"
                legenda = f"aumentou {var}" if var > 0 else f"reduziu {abs(var)}" if var < 0 else "sem mudança"
                linhas.append(f"- {seta} **{base}**: **{qtd} pedidos** ({legenda})")

            texto = (
                    f"📅 **Data de Geração:**\n{datetime.now():%d/%m/%Y %H:%M}\n\n"
                    f"📦 **Qtd de Pacotes:** {total_atual}\n"
                    f"📊 **Variação de Pacotes:** "
                    f"{'📈 Aumentou' if diff_total > 0 else '📉 Reduziu' if diff_total < 0 else '➖ Igual'} "
                    f"{abs(diff_total)} pedidos\n\n"
                    f"🔴 **3 Piores Bases:**\n" + "\n".join(linhas)
            )

            card = {
                "msg_type": "interactive",
                "card": {
                    "header": {
                        "title": {"tag": "plain_text", "content": f"Retidos – {coord}"},
                        "template": "red"
                    },
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "tag": "lark_md",
                                "content": texto
                            }
                        }
                    ]
                }
            }

            logging.info(f"📮 Enviando card para o coordenador: {coord}...")

            try:
                response = requests.post(WEBHOOK, json=card, timeout=10)
                logging.info(
                    f"📮 Resposta do webhook para '{coord}': Status {response.status_code} - Conteúdo: {response.text}")

                if response.status_code == 200:
                    logging.info(f"✅ Card enviado com sucesso para o coordenador: {coord}")
                else:
                    logging.error(f"❌ Falha ao enviar card para '{coord}'. Status Code: {response.status_code}")
            except Exception as e:
                logging.error(f"❌ Erro na requisição de envio do card para o coordenador '{coord}': {e}")


# ============================================================
# 🏁 PONTO DE PARTIDA
# ============================================================
if __name__ == "__main__":
    setup_logging()
    try:
        analisador = AnaliseRetidos()
        analisador.executar()
    except Exception as e:
        logging.critical(f"💥 Ocorreu um erro crítico na execução principal: {e}", exc_info=True)