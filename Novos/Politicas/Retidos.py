# -*- coding: utf-8 -*-
import os
import json
import re
import logging
import polars as pl
from datetime import datetime
import requests


# ============================================================
# 🧩 FUNÇÕES AUXILIARES (GLOBAIS)
# ============================================================

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
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
            if newdf[ coluna ].is_not_null().any():
                logging.info(f"✔️ Coluna '{coluna}' convertida com {fmt}")
                return newdf
        except:
            pass

    logging.warning(f"⚠️ Falha ao converter coluna '{coluna}'.")
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

    logging.info(f"📂 {len(arquivos)} arquivos encontrados em '{nome}'")

    dfs = []
    for arq in arquivos:
        path = os.path.join(pasta, arq)
        try:
            raw = pl.read_excel(path)
            df = next(iter(raw.values())) if isinstance(raw, dict) else raw
            dfs.append(df)
            logging.info(f"   ✔️ {arq} ({df.height} linhas)")
        except Exception as e:
            logging.error(f"   ❌ Erro ao ler {arq}: {e}")

    return pl.concat(dfs, how="diagonal_relaxed") if dfs else pl.DataFrame()


def salvar_resultado(df: pl.DataFrame, pasta: str, nome: str, limit: int):
    os.makedirs(pasta, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    ext = "csv" if df.height >= limit else "xlsx"
    caminho = os.path.join(pasta, f"{nome}_{ts}.{ext}")

    df.write_csv(caminho) if ext == "csv" else df.write_excel(caminho)

    logging.info(f"💾 Resultado salvo: {caminho}")
    return caminho


def salvar_relatorio_intermediario(df: pl.DataFrame, nome: str, config: dict):
    if not config["parametros"].get("gerar_relatorios_intermediarios", False):
        return

    pasta = os.path.join(config["caminhos"]["pasta_saida"], "Intermediarios")
    os.makedirs(pasta, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    caminho = os.path.join(pasta, f"{nome}_{ts}.xlsx")

    try:
        df.write_excel(caminho)
        logging.info(f"📄 Intermediário salvo: {caminho}")
    except Exception as e:
        logging.error(f"❌ Falha salvando intermediário '{nome}': {e}")


# ============================================================
# 🔧 NORMALIZAÇÃO DO NOME DA BASE
# ============================================================

def limpar_nome(nome: str) -> str:
    """Normaliza qualquer nome de base em um formato padrão."""
    if not nome:
        return ""
    nome = str(nome).upper().strip()

    nome = re.sub(r"[^\x00-\x7F]+", "", nome)  # Remove chinês
    nome = re.sub(r"[-_]+", " ", nome)
    nome = re.sub(r"\s+", " ", nome)

    partes = nome.split(" ")

    if len(partes) == 2 and len(partes[0]) == 3 and len(partes[1]) == 2:
        return f"{partes[1]} {partes[0]}"

    return nome.strip()
# ============================================================
# 💬 FEISHU
# ============================================================

def _get_webhook_for(coord: str, webhooks: dict, default: str):
    return webhooks.get(coord, default)


def enviar_card_feishu(coord: str, qtd: int, perc: float, cfg: dict):
    webhook = _get_webhook_for(coord, cfg.get("webhooks_especificos", {}), cfg.get("default_webhook"))
    if not webhook:
        logging.warning(f"⚠️ Coordenador '{coord}' sem webhook.")
        return

    card = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": f"🚚 Retidos – {coord}"}, "template": "turquoise"},
            "elements": [{
                "tag": "div",
                "text": {"tag": "lark_md",
                         "content": f"**Pedidos:** {qtd}\n**%:** {perc:.2f}%\n📅 {datetime.now():%d/%m/%Y %H:%M}"}
            }]
        }
    }

    try:
        requests.post(webhook, json=card, timeout=10)
        logging.info(f"💬 Feishu enviado → {coord}")
    except:
        logging.error(f"❌ Falha Feishu → {coord}")


# ============================================================
# 🚀 CLASSE PRINCIPAL
# ============================================================

class AnaliseRetidos:
    def __init__(self, config_filename="config.json"):
        base = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(base, config_filename)

        with open(path, "r", encoding="utf-8") as f:
            self.config = json.load(f)

        self.removidos = {"cluster": 0, "devolucao": 0, "problematicos": 0, "custodia": 0}
        self.total_inicial_filtrado = 0
        self.df_total_por_base = pl.DataFrame()

    # ============================================================
    # PROCESSO PRINCIPAL
    # ============================================================
    def executar(self):
        logging.info("🚀 Iniciando análise…")

        df = self._processar_dados()
        if df.is_empty():
            logging.error("❌ Nenhum dado final encontrado.")
            return

        df = self._enriquecer_com_coordenadores(df)
        self._gerar_relatorio_comparativo(df)

        caminho_final = self._salvar_resultado_final(df)
        self._exibir_resumo_console(caminho_final)

    # ============================================================
    def _processar_dados(self):
        df = self._ler_e_preparar_retidos()
        if df.is_empty():
            return pl.DataFrame()

        salvar_relatorio_intermediario(df, "00_Retidos_Iniciais", self.config)

        df = self._aplicar_filtro_devolucao(df)
        df = self._aplicar_filtro_problematicos(df)
        df = self._aplicar_filtro_custodia(df)

        return df

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

        col_base = safe_pick(df, "Base de Entrega 派件网点", ["base", "网点"])

        if col_base:
            self.df_total_por_base = (
                df.with_columns(
                    pl.col(col_base).map_elements(limpar_nome, return_dtype=pl.Utf8).alias("Base_Clean")
                )
                .group_by("Base_Clean")
                .agg(pl.len().alias("Total de Pedidos"))
            )

        col_pedido = safe_pick(df, self.config["colunas"]["col_pedido_ret"], ["pedido", "运单"])
        col_data = safe_pick(df, self.config["colunas"]["col_data_atualizacao_ret"], ["data", "更新"])
        col_regional = safe_pick(df, self.config["colunas"]["col_regional_ret"], ["regional", "区域"])

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
            .is_in(self.config["parametros"]["regionais_desejadas"])
        )

        self.total_inicial_filtrado = df.height
        return df

    # ============================================================
    def _aplicar_filtro_devolucao(self, df):
        df_dev = ler_planilhas(self.config["caminhos"]["pasta_devolucao"], "Devolução")
        if df_dev.is_empty():
            return df

        col_pedido = safe_pick(df_dev, self.config["colunas"]["col_pedido_dev"], ["pedido"])
        col_data = safe_pick(df_dev, self.config["colunas"]["col_data_solicitacao_dev"], ["tempo", "solic"])

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

        return df.filter(~pl.col(self.config["colunas"]["col_pedido_ret"]).is_in(remover))

    # ============================================================
    def _aplicar_filtro_problematicos(self, df):
        df_prob = ler_planilhas(self.config["caminhos"]["pasta_problematicos"], "Problemáticos")
        if df_prob.is_empty():
            return df

        col_pedido = safe_pick(df_prob, "Número de pedido JMS", ["pedido", "运单"])
        col_data = safe_pick(df_prob, "data de registro", ["registro", "异常"])

        df_prob = (
            df_prob.select([col_pedido, col_data])
            .rename({"Número de pedido JMS": "Pedido", "data de registro": "Registro"})
            .pipe(limpar_pedidos, "Pedido")
            .pipe(converter_datetime, "Registro")
            .group_by("Pedido")
            .agg(pl.col("Registro").min())
        )

        dfj = df.join(
            df_prob,
            left_on=self.config["colunas"]["col_pedido_ret"],
            right_on="Pedido",
            how="left"
        )

        df_rem = dfj.filter(pl.col("Registro")
                            >= pl.col(self.config["colunas"]["col_data_atualizacao_ret"]))

        salvar_relatorio_intermediario(df_rem, "02_Removidos_Problematicos", self.config)

        remover = df_rem.select(self.config["colunas"]["col_pedido_ret"]).to_series()
        self.removidos["problematicos"] = remover.len()

        return df.filter(~pl.col(self.config["colunas"]["col_pedido_ret"]).is_in(remover))

    # ============================================================
    # 🔵 CUSTÓDIA — BLOCO CORRIGIDO
    # ============================================================
    def _aplicar_filtro_custodia(self, df):
        df_cust = ler_planilhas(self.config["caminhos"]["pasta_custodia"], "Custódia")
        if df_cust.is_empty():
            return df

        col_pedido = safe_pick(df_cust, self.config["colunas"]["col_pedido_cust"], ["pedido"])
        col_data = safe_pick(df_cust, self.config["colunas"]["col_data_registro_cust"], ["registro"])

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
                (pl.col(self.config["colunas"]["col_data_registro_cust"])
                 + pl.duration(days=self.config["parametros"]["prazo_custodia_dias"]))
                .alias("Prazo_Limite")
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

        # — Remover quem está dentro do prazo
        df_rem = dfj.filter(pl.col("Status_Custodia") == "Dentro")

        salvar_relatorio_intermediario(df_rem, "03_Removidos_Custodia", self.config)

        self.removidos["custodia"] = df_rem.height

        return dfj.filter(pl.col("Status_Custodia") == "Fora")

    # ============================================================
    # Coordenadores — BLOCO CORRIGIDO + NORMALIZAÇÃO
    # ============================================================
    def _enriquecer_com_coordenadores(self, df):
        path = self.config["caminhos"]["caminho_coordenador"]
        if not os.path.exists(path):
            logging.warning("⚠️ Planilha de coordenadores não encontrada.")
            return df

        try:
            raw = pl.read_excel(path)
            dfc = next(iter(raw.values())) if isinstance(raw, dict) else raw

            col_base = detectar_coluna(dfc, ["base", "nome da base", "entrega"])
            col_coord = detectar_coluna(dfc, ["coordenador", "responsável"])

            if not col_base or not col_coord:
                logging.warning("⚠️ Colunas de base/coordenador não encontradas.")
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

            df = df.join(
                dfc,
                left_on="Base_Normalizada",
                right_on="Base_Coord",
                how="left"
            )

            return df

        except Exception as e:
            logging.error(f"❌ Erro coordenadores: {e}")
            return df

    # ============================================================
    def _gerar_relatorio_comparativo(self, df):
        # (Bloco idêntico ao seu, removido por tamanho)
        pass

    # ============================================================
    def _salvar_resultado_final(self, df):
        return salvar_resultado(
            df,
            self.config["caminhos"]["pasta_saida"],
            self.config["parametros"]["nome_arquivo_final"],
            self.config["parametros"]["excel_row_limit"]
        )

    # ============================================================
    def _exibir_resumo_console(self, caminho):
        logging.info("=============== RESUMO ===============")
        logging.info(f"Retidos filtrados: {self.total_inicial_filtrado}")
        logging.info(f"Cluster removido: {self.removidos['cluster']}")
        logging.info(f"Devolução removido: {self.removidos['devolucao']}")
        logging.info(f"Problemáticos removido: {self.removidos['problematicos']}")
        logging.info(f"Custódia removido: {self.removidos['custodia']}")
        logging.info(f"Arquivo final: {caminho}")
        logging.info("======================================")
if __name__ == "__main__":
    setup_logging()
    try:
        AnaliseRetidos().executar()
    except Exception as e:
        logging.critical(f"Erro crítico: {e}", exc_info=True)
