# -*- coding: utf-8 -*-
import os
import json
import logging
import polars as pl
from datetime import datetime
import requests


# ============================================================
# 🧩 FUNÇÕES AUXILIARES (GLOBAIS)
# ============================================================

def setup_logging():
    """Configura o sistema de logging para console e arquivo."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("analise_retidos.log", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


def converter_datetime(df: pl.DataFrame, coluna: str) -> pl.DataFrame:
    """Converte coluna para datetime com tratamento robusto de erros e múltiplos formatos."""
    if coluna not in df.columns:
        return df

    formatos_comuns = [
        "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M", "%Y/%m/%d %H:%M",
        "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y%m%d"
    ]

    for fmt in formatos_comuns:
        try:
            df_temp = df.with_columns(
                pl.col(coluna).str.strptime(pl.Datetime, fmt, strict=False)
            )
            if df_temp.filter(pl.col(coluna).is_not_null()).height > 0:
                logging.info(f"✔️ Coluna '{coluna}' convertida usando o formato: {fmt}")
                return df_temp.filter(pl.col(coluna).is_not_null())
        except pl.ComputeError:
            continue

    logging.warning(f"⚠️ Não foi possível converter a coluna '{coluna}' para datetime com os formatos conhecidos.")
    return df


def detectar_coluna(df: pl.DataFrame, candidatos: list[str]) -> str | None:
    """Encontra a primeira coluna no DataFrame que corresponde a um dos candidatos."""
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidatos:
        cand_lower = cand.lower()
        if cand_lower in cols_lower:
            return cols_lower[cand_lower]
        for low, original in cols_lower.items():
            if cand_lower in low:
                return original
    return None


def safe_pick(df: pl.DataFrame, preferido: str, candidatos_extra: list[str]) -> str | None:
    """Retorna a coluna preferida ou detecta uma alternativa."""
    if preferido in df.columns:
        return preferido
    return detectar_coluna(df, candidatos_extra)


def limpar_pedidos(df: pl.DataFrame, coluna: str) -> pl.DataFrame:
    """Padroniza a coluna de pedido como string, sem espaços extras."""
    if coluna in df.columns:
        df = df.with_columns(pl.col(coluna).cast(pl.Utf8).str.strip_chars())
    return df


def ler_planilhas(pasta: str, nome_base: str) -> pl.DataFrame:
    """Lê todos os .xls/.xlsx de uma pasta e os concatena."""
    if not os.path.exists(pasta):
        logging.error(f"❌ Pasta '{pasta}' não encontrada.")
        return pl.DataFrame()

    arquivos = [f for f in os.listdir(pasta) if f.lower().endswith((".xls", ".xlsx")) and not f.startswith("~$")]
    if not arquivos:
        logging.warning(f"⚠️ Nenhum arquivo Excel encontrado em '{nome_base}'.")
        return pl.DataFrame()

    logging.info(f"📂 Lendo {len(arquivos)} arquivo(s) em '{nome_base}':")
    dfs = []
    for arq in arquivos:
        caminho_completo = os.path.join(pasta, arq)
        try:
            df_raw = pl.read_excel(caminho_completo)
            df = next(iter(df_raw.values())) if isinstance(df_raw, dict) else df_raw
            dfs.append(df)
            logging.info(f"   ✅ {arq} ({df.height} linhas)")
        except Exception as e:
            logging.error(f"   ❌ Erro ao ler {arq}: {e}")

    if not dfs:
        return pl.DataFrame()

    return pl.concat(dfs, how="diagonal_relaxed")


def salvar_resultado(df: pl.DataFrame, caminho_saida: str, nome_base: str, excel_row_limit: int) -> str:
    """Salva o DataFrame em XLSX ou CSV, dependendo do tamanho."""
    os.makedirs(caminho_saida, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    extensao = "csv" if df.height >= excel_row_limit else "xlsx"
    nome_arquivo = f"{nome_base}_{timestamp}.{extensao}"
    caminho_final = os.path.join(caminho_saida, nome_arquivo)

    if extensao == "csv":
        df.write_csv(caminho_final)
    else:
        df.write_excel(caminho_final)

    logging.info(f"✅ Resultado salvo em: {caminho_final}")
    return caminho_final


def salvar_relatorio_intermediario(df: pl.DataFrame, nome_base: str, config: dict):
    """Salva um DataFrame intermediário em uma subpasta 'Intermediarios' se habilitado na config."""
    if not config["parametros"].get("gerar_relatorios_intermediarios", False):
        return

    pasta_saida_intermediarios = os.path.join(config["caminhos"]["pasta_saida"], "Intermediarios")
    os.makedirs(pasta_saida_intermediarios, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_arquivo = f"{nome_base}_{timestamp}.xlsx"
    caminho_final = os.path.join(pasta_saida_intermediarios, nome_arquivo)

    try:
        df.write_excel(caminho_final)
        logging.info(f"📄 Relatório intermediário salvo: {caminho_final}")
    except Exception as e:
        logging.error(f"❌ Erro ao salvar relatório intermediário '{nome_base}': {e}")


# ============================================================
# 💬 FEISHU – ENVIO DE CARD
# ============================================================
def _get_webhook_for(coord: str, webhooks: dict, default_webhook: str) -> str:
    """Retorna o webhook específico de um coordenador ou o padrão."""
    return webhooks.get(coord, default_webhook)


def enviar_card_feishu(coordenador: str, qtd_retidos: int, percentual_regional: float, feishu_config: dict,
                       url_relatorio: str | None = None):
    """Envia um card formatado para o Feishu com os resultados."""
    webhook = _get_webhook_for(coordenador, feishu_config.get("webhooks_especificos", {}),
                               feishu_config.get("default_webhook"))
    if not webhook:
        logging.warning(f"   ⚠️ Sem webhook para {coordenador}. Envio cancelado.")
        return

    card = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": f"🚚 Análise de Retidos – {coordenador}"},
                "template": "turquoise"
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"**Pedidos fora do prazo:** {qtd_retidos}\n"
                            f"**% sobre o total da análise:** {percentual_regional:.2f}%\n"
                            f"📅 Atualizado em {datetime.now():%d/%m/%Y %H:%M}"
                        )
                    }
                },
            ]
        }
    }

    try:
        resp = requests.post(webhook, json=card, timeout=10)
        resp.raise_for_status()
        logging.info(f"   💬 Card enviado com sucesso para {coordenador}.")
    except requests.exceptions.RequestException as e:
        logging.error(f"   ❌ Falha ao enviar card para {coordenador}: {e}")


# ============================================================
# 🚀 CLASSE PRINCIPAL DE ANÁLISE
# ============================================================
class AnaliseRetidos:
    def __init__(self, config_filename: str = 'config.json'):
        self.logger = logging.getLogger(__name__)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, config_filename)

        self.config = self._carregar_configuracoes(config_path)
        self.removidos = {
            "cluster": 0, "devolucao": 0, "problematicos": 0, "custodia": 0
        }
        self.total_inicial_filtrado = 0
        self.df_total_por_base = pl.DataFrame()  # Armazenará o total de pedidos por base

    def _carregar_configuracoes(self, path: str) -> dict:
        """Carrega as configurações do arquivo JSON."""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            self.logger.info(f"✔️ Configurações carregadas de '{path}'.")
            return config
        except FileNotFoundError:
            self.logger.error(f"❌ Arquivo de configuração '{path}' não encontrado. Abortando.")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Erro ao ler o arquivo de configuração: {e}. Abortando.")
            raise

    def executar(self):
        """Orquestra todo o fluxo da análise."""
        self.logger.info("\n" + "=" * 30 + "\n🚀 INICIANDO ANÁLISE COMPLETA\n" + "=" * 30)

        df_final = self._processar_dados()
        if df_final is None or df_final.is_empty():
            self.logger.warning("🔚 Análise finalizada sem dados resultantes.")
            return

        df_final = self._enriquecer_com_coordenadores(df_final)
        self._gerar_relatorio_comparativo(df_final)
        caminho_final = self._salvar_resultado_final(df_final)
        self._enviar_notificacoes_feishu(df_final)
        self._exibir_resumo_console(caminho_final)

    def _processar_dados(self) -> pl.DataFrame | None:
        """Executa as etapas principais de filtragem e junção dos dados."""
        df = self._ler_e_preparar_retidos()
        if df is None: return None

        # Salva o estado inicial após os primeiros filtros
        salvar_relatorio_intermediario(df, "00_Retidos_Iniciais", self.config)

        df = self._aplicar_filtro_devolucao(df)
        df = self._aplicar_filtro_problematicos(df)
        df = self._aplicar_filtro_custodia(df)

        return df

    def _ler_e_preparar_retidos(self) -> pl.DataFrame | None:
        """Lê a base de retidos e aplica os filtros iniciais."""
        df_ret = ler_planilhas(self.config["caminhos"]["pasta_retidos"], "Retidos")
        if df_ret.is_empty():
            self.logger.error("❌ Nenhum dado em Retidos. Análise não pode continuar.")
            return None

        # NOVO: Filtrar por "Dias Retidos" em vez de "Cluster"
        col_dias = safe_pick(df_ret, "Dias Retidos 滞留日", ["dias", "滞留日", "retidos dias"])
        if col_dias:
            total_antes = df_ret.height
            # Converte a coluna para número (se for string) e filtra para manter apenas dias > 6
            df_ret = df_ret.with_columns(pl.col(col_dias).cast(pl.Int64, strict=False))
            df_ret = df_ret.filter(pl.col(col_dias) > 6)
            self.removidos["cluster"] = total_antes - df_ret.height
            self.logger.info(
                f"🧹 Dias Retidos (<= 6 dias) → Removidos: {self.removidos['cluster']} | Mantidos: {df_ret.height}")

        # NOVO: Calcular o total de pedidos por base ANTES de qualquer filtro
        col_base_total = safe_pick(df_ret, "Base de Entrega 派件网点", ["base", "网点", "派件"])
        if col_base_total:
            self.df_total_por_base = (
                df_ret
                .select([col_base_total])
                .rename({col_base_total: "Base de Entrega_Raw"})
                .with_columns(
                    pl.col("Base de Entrega_Raw").str.strip_chars().str.to_uppercase().alias("Base de Entrega_Clean"))
                .group_by("Base de Entrega_Clean")
                .agg(pl.len().alias("Total de Pedidos"))
            )

        cols_map = {
            "pedido": safe_pick(df_ret, self.config["colunas"]["col_pedido_ret"],
                                ["Número do Pedido JMS 运单号", "pedido"]),
            "data": safe_pick(df_ret, self.config["colunas"]["col_data_atualizacao_ret"], ["data", "atualiza", "更新"]),
            "regional": safe_pick(df_ret, self.config["colunas"]["col_regional_ret"], ["regional", "区域"]),
            "base_entrega": safe_pick(df_ret, "Base de Entrega 派件网点", ["base", "网点", "派件"])
        }

        cols_validas = {k: v for k, v in cols_map.items() if v}
        df_ret = df_ret.select(list(cols_validas.values())).rename({
            cols_validas["pedido"]: self.config["colunas"]["col_pedido_ret"],
            cols_validas["data"]: self.config["colunas"]["col_data_atualizacao_ret"],
            cols_validas["regional"]: self.config["colunas"]["col_regional_ret"],
            cols_validas["base_entrega"]: "Base de Entrega 派件网点"
        })

        df_ret = limpar_pedidos(df_ret, self.config["colunas"]["col_pedido_ret"])
        df_ret = converter_datetime(df_ret, self.config["colunas"]["col_data_atualizacao_ret"])

        if self.config["colunas"]["col_regional_ret"] in df_ret.columns:
            df_ret = df_ret.filter(pl.col(self.config["colunas"]["col_regional_ret"]).is_in(
                self.config["parametros"]["regionais_desejadas"]))

        self.total_inicial_filtrado = df_ret.height
        self.logger.info(f"🟢 Retidos iniciais (após filtros): {self.total_inicial_filtrado}")
        return df_ret

    def _aplicar_filtro_devolucao(self, df: pl.DataFrame) -> pl.DataFrame:
        """Remove pedidos que foram para devolução após a atualização."""
        df_dev = ler_planilhas(self.config["caminhos"]["pasta_devolucao"], "Devolução")
        if df_dev.is_empty(): return df

        col_pedido_dev = safe_pick(df_dev, self.config["colunas"]["col_pedido_dev"], ["pedido", "jms"])
        col_data_dev = safe_pick(df_dev, self.config["colunas"]["col_data_solicitacao_dev"], ["solicit", "data"])
        if not (col_pedido_dev and col_data_dev): return df

        df_dev = (
            df_dev.select([col_pedido_dev, col_data_dev])
            .rename({col_pedido_dev: self.config["colunas"]["col_pedido_dev"],
                     col_data_dev: self.config["colunas"]["col_data_solicitacao_dev"]})
            .pipe(limpar_pedidos, self.config["colunas"]["col_pedido_dev"])
            .pipe(converter_datetime, self.config["colunas"]["col_data_solicitacao_dev"])
            .group_by(self.config["colunas"]["col_pedido_dev"]).agg(
                pl.col(self.config["colunas"]["col_data_solicitacao_dev"]).min())
        )

        df_merge = df.join(df_dev, left_on=self.config["colunas"]["col_pedido_ret"],
                           right_on=self.config["colunas"]["col_pedido_dev"], how="left")

        df_removidos = df_merge.filter(
            (pl.col(self.config["colunas"]["col_data_solicitacao_dev"]) > pl.col(
                self.config["colunas"]["col_data_atualizacao_ret"])) &
            pl.col(self.config["colunas"]["col_data_solicitacao_dev"]).is_not_null()
        )

        salvar_relatorio_intermediario(df_removidos, "01_Removidos_Devolucao", self.config)

        pedidos_para_remover = df_removidos.select(self.config["colunas"]["col_pedido_ret"]).to_series()

        removidos_count = pedidos_para_remover.len()
        self.removidos["devolucao"] = removidos_count
        self.logger.info(f"🟡 Devolução → Removidos: {removidos_count} | Mantidos: {df.height - removidos_count}")

        return df.filter(~pl.col(self.config["colunas"]["col_pedido_ret"]).is_in(pedidos_para_remover))

    def _aplicar_filtro_problematicos(self, df: pl.DataFrame) -> pl.DataFrame:
        """Remove pedidos que se tornaram problemáticos após a atualização."""
        df_prob = ler_planilhas(self.config["caminhos"]["pasta_problematicos"], "Problemáticos")
        if df_prob.is_empty(): return df

        col_pedido_prob = safe_pick(df_prob, "Número de pedido JMS", ["pedido", "jms"])
        col_data_prob = safe_pick(df_prob, "data de registro", ["data", "registro", "anormal"])
        if not (col_pedido_prob and col_data_prob): return df

        df_prob = (
            df_prob.select([col_pedido_prob, col_data_prob])
            .rename({col_pedido_prob: "Número de pedido JMS", col_data_prob: "data de registro"})
            .pipe(limpar_pedidos, "Número de pedido JMS")
            .pipe(converter_datetime, "data de registro")
            .group_by("Número de pedido JMS").agg(pl.col("data de registro").min())
        )

        df_merge = df.join(df_prob, left_on=self.config["colunas"]["col_pedido_ret"], right_on="Número de pedido JMS",
                           how="left")

        df_removidos = df_merge.filter(
            (pl.col("data de registro") >= pl.col(self.config["colunas"]["col_data_atualizacao_ret"])) &
            pl.col("data de registro").is_not_null()
        )

        salvar_relatorio_intermediario(df_removidos, "02_Removidos_Problematicos", self.config)

        pedidos_para_remover = df_removidos.select(self.config["colunas"]["col_pedido_ret"]).to_series()

        removidos_count = pedidos_para_remover.len()
        self.removidos["problematicos"] = removidos_count
        self.logger.info(f"🟠 Problemáticos → Removidos: {removidos_count} | Mantidos: {df.height - removidos_count}")

        return df.filter(~pl.col(self.config["colunas"]["col_pedido_ret"]).is_in(pedidos_para_remover))

    def _aplicar_filtro_custodia(self, df: pl.DataFrame) -> pl.DataFrame:
        """Identifica pedidos em custódia e remove os que estão dentro do prazo."""
        df_cust = ler_planilhas(self.config["caminhos"]["pasta_custodia"], "Custódia")
        if df_cust.is_empty(): return df

        col_pedido_c = safe_pick(df_cust, self.config["colunas"]["col_pedido_cust"], ["pedido", "jms"])
        col_data_c = safe_pick(df_cust, self.config["colunas"]["col_data_registro_cust"], ["data", "registro"])
        if not (col_pedido_c and col_data_c): return df

        df_cust = (
            df_cust.select([col_pedido_c, col_data_c])
            .rename({col_pedido_c: self.config["colunas"]["col_pedido_cust"],
                     col_data_c: self.config["colunas"]["col_data_registro_cust"]})
            .pipe(limpar_pedidos, self.config["colunas"]["col_pedido_cust"])
            .pipe(converter_datetime, self.config["colunas"]["col_data_registro_cust"])
            .group_by(self.config["colunas"]["col_pedido_cust"]).agg(
                pl.col(self.config["colunas"]["col_data_registro_cust"]).min())
            .with_columns(
                (pl.col(self.config["colunas"]["col_data_registro_cust"]) + pl.duration(
                    days=self.config["parametros"]["prazo_custodia_dias"])).alias("Prazo_Limite")
            )
        )

        df_join = df.join(df_cust, left_on=self.config["colunas"]["col_pedido_ret"],
                          right_on=self.config["colunas"]["col_pedido_cust"], how="left")
        df_join = df_join.with_columns(
            pl.when(
                (pl.col(self.config["colunas"]["col_data_atualizacao_ret"]) <= pl.col("Prazo_Limite")) &
                pl.col("Prazo_Limite").is_not_null()
            ).then(pl.lit("Dentro do Prazo")).otherwise(pl.lit("Fora do Prazo")).alias("Status_Custodia")
        )

        df_removidos = df_join.filter(pl.col("Status_Custodia") == "Dentro do Prazo")
        salvar_relatorio_intermediario(df_removidos, "03_Removidos_Custodia", self.config)

        self.removidos["custodia"] = df_removidos.height
        self.logger.info(
            f"🔵 Custódia → Removidos: {self.removidos['custodia']} | Mantidos: {df_join.height - self.removidos['custodia']}")

        return df_join.filter(pl.col("Status_Custodia") == "Fora do Prazo")

    def _enriquecer_com_coordenadores(self, df: pl.DataFrame) -> pl.DataFrame:
        """Adiciona a coluna de coordenadores ao DataFrame final."""
        if not os.path.exists(self.config["caminhos"]["caminho_coordenador"]):
            self.logger.warning("⚠️ Planilha de Coordenadores não encontrada; seguindo sem coordenador.")
            return df

        try:
            df_coord_raw = pl.read_excel(self.config["caminhos"]["caminho_coordenador"])
            df_coord = next(iter(df_coord_raw.values())) if isinstance(df_coord_raw, dict) else df_coord_raw

            col_base_coord = detectar_coluna(df_coord, ["nome da base", "base", "entrega"])
            col_coord = detectar_coluna(df_coord, ["coordenador", "responsável", "coordenadores"])

            if col_base_coord and col_coord:
                df_coord = df_coord.select([col_base_coord, col_coord]).rename({
                    col_base_coord: "Nome da Base de Entrega",
                    col_coord: "Coordenador"
                })
                if "Base de Entrega 派件网点" in df.columns:
                    return df.join(df_coord, left_on="Base de Entrega 派件网点", right_on="Nome da Base de Entrega",
                                   how="left")

            else:
                self.logger.warning(
                    "⚠️ Colunas 'Nome da base' ou 'Coordenadores' não encontradas na planilha de coordenadores.")
                return df
        except Exception as e:
            self.logger.error(f"❌ Erro ao integrar coordenadores: {e}")
            return df

    def _gerar_relatorio_comparativo(self, df_final: pl.DataFrame):
        """Gera o relatório final com Total, Lista e Retidos por base."""
        if self.df_total_por_base.is_empty():
            self.logger.warning("⚠️ Pulando relatório comparativo: dados de total por base não encontrados.")
            return

        # --- PASSO 1: Preparar o resumo dos retidos encontrados na análise ---
        df_resumo = (
            df_final
            .group_by("Base de Entrega 派件网点")
            .agg(pl.len().alias("Retidos (Análise Final)"))
            .rename({"Base de Entrega 派件网点": "Base de Entrega_Raw"})
        ).with_columns(
            pl.col("Base de Entrega_Raw").str.strip_chars().str.to_uppercase().alias("Base de Entrega_Clean")
        )

        # --- PASSO 2: Preparar a base da lista ---
        df_lista = ler_planilhas(self.config["caminhos"]["pasta_base_lista"], "Base Retidos (Lista)")
        if df_lista.is_empty():
            self.logger.warning("⚠️ Pulando relatório comparativo: 'Base Lista' não encontrada.")
            return

        col_base_lista = safe_pick(df_lista, "Nome da base de entrega", ["base", "entrega", "网点"])
        col_qtd_lista = safe_pick(df_lista, "Qtd a entregar há mais de 10 dias", ["qtd", "10", "dias"])
        if not (col_base_lista and col_qtd_lista): return

        df_lista = df_lista.select([col_base_lista, col_qtd_lista]).rename({
            col_base_lista: "Base de Entrega_Raw",
            col_qtd_lista: "Entregas (Lista > 10d)"
        }).with_columns([
            pl.col("Entregas (Lista > 10d)").cast(pl.Int64, strict=False),
            pl.col("Base de Entrega_Raw").str.strip_chars().str.to_uppercase().alias("Base de Entrega_Clean")
        ])

        # --- PASSO 3: Juntar as três tabelas ---
        df_compara = self.df_total_por_base.join(
            df_lista, on="Base de Entrega_Clean", how="left"
        ).join(
            df_resumo, on="Base de Entrega_Clean", how="left"
        ).with_columns([
            pl.col("Entregas (Lista > 10d)").fill_null(0).cast(pl.Int64),
            pl.col("Retidos (Análise Final)").fill_null(0).cast(pl.Int64),
        ]).sort("Total de Pedidos", descending=True)

        # --- PASSO 4: Selecionar e salvar o relatório final ---
        df_final_report = df_compara.select([
            "Base de Entrega_Raw",
            "Total de Pedidos",
            "Entregas (Lista > 10d)",
            "Retidos (Análise Final)"
        ]).rename({
            "Base de Entrega_Raw": "Base de Entrega"
        })

        out_lista = salvar_resultado(df_final_report, self.config["caminhos"]["pasta_saida"], "Resumo_Geral_Por_Base",
                                     self.config["parametros"]["excel_row_limit"])

        self.logger.info("\n🏆 TOP 5 BASES – RESUMO GERAL")
        for row in df_final_report.head(5).iter_rows(named=True):
            self.logger.info(
                f"Base: {row['Base de Entrega']} | "
                f"Total: {row['Total de Pedidos']} | "
                f"Lista >10d: {row['Entregas (Lista > 10d)']} | "
                f"Retidos: {row['Retidos (Análise Final)']}"
            )
        self.logger.info("=" * 30)

    def _salvar_resultado_final(self, df: pl.DataFrame) -> str:
        """Salva o DataFrame final processado."""
        return salvar_resultado(
            df,
            self.config["caminhos"]["pasta_saida"],
            self.config["parametros"]["nome_arquivo_final"],
            self.config["parametros"]["excel_row_limit"]
        )

    def _enviar_notificacoes_feishu(self, df: pl.DataFrame):
        """Prepara e envia os cards para o Feishu (DESATIVADO por padrão)."""
        self.logger.info("\n📢 Envio de cards Feishu está DESATIVADO neste modo de teste.")
        if "Coordenador" not in df.columns:
            self.logger.warning("⚠️ Coluna 'Coordenador' não encontrada. Nenhum card preparado.")
            return

        coords_unicos = df.select("Coordenador").unique().to_series().drop_nulls().to_list()
        total_amostra = df.height if df.height > 0 else 1
        self.logger.info(f"   Coordenadores impactados: {len(coords_unicos)}")

        for coord in coords_unicos:
            qtd = df.filter(pl.col("Coordenador") == coord).height
            percentual = (qtd / total_amostra) * 100.0
            self.logger.info(f"   - {coord}: {qtd} pedidos ({percentual:.2f}%)")
            # enviar_card_feishu(coord, qtd, percentual, self.config["feishu"])

    def _exibir_resumo_console(self, caminho_final: str):
        """Exibe um resumo detalhado de todo o processamento no console."""
        self.logger.info("\n" + "=" * 30 + "\n📦 RESUMO FINAL DE PROCESSAMENTO\n" + "=" * 30)
        self.logger.info(f"📊 Total Retidos iniciais (após filtro regional): {self.total_inicial_filtrado}")
        self.logger.info(f"🟣 Removidos por Dias Retidos (<= 6 dias): {self.removidos['cluster']}")
        self.logger.info(f"🟡 Removidos por Devolução: {self.removidos['devolucao']}")
        self.logger.info(f"🟠 Removidos por Problemáticos: {self.removidos['problematicos']}")
        self.logger.info(f"🔵 Removidos por Custódia: {self.removidos['custodia']}")
        total_final = self.total_inicial_filtrado - sum(self.removidos.values())
        self.logger.info(f"✅ Pedidos restantes (fora do prazo): {total_final}")
        self.logger.info(f"📄 Arquivo final: {caminho_final}")
        self.logger.info("=" * 30 + "\n")


# ============================================================
# ▶️ EXECUÇÃO
# ============================================================
if __name__ == "__main__":
    setup_logging()
    try:
        analisador = AnaliseRetidos()
        analisador.executar()
    except Exception as e:
        logging.critical(f"Ocorreu um erro crítico e a execução foi interrompida: {e}", exc_info=True)