# -*- coding: utf-8 -*-
import os
import polars as pl
from datetime import datetime
import requests

# ============== CAMINHOS PRINCIPAIS ========================
PASTA_RETIDOS   = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\06 - Retidos"
PASTA_DEVOLUCAO = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\00.3 - Base Devolução"
PASTA_PROBLEMATICOS = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\00.2 - Base de Problematicos (Gestão de Anormalidade)"
PASTA_CUSTODIA  = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\00.4 - Base Custodia"
PASTA_BASE_LISTA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\00.1 - Base Retidos(Lista)"
PASTA_SAIDA     = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\Resultados"

# Coordenadores
CAMINHO_COORDENADOR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"

# ============== COLUNAS PADRÃO (NOMES-ALVO) ================
COL_PEDIDO_RET           = "Número do Pedido JMS 运单号"
COL_DATA_ATUALIZACAO_RET = "Data prevista de entrega"
COL_REGIONAL_RET         = "Regional 区域"

COL_PEDIDO_DEV            = "Número de pedido JMS"
COL_DATA_SOLICITACAO_DEV  = "Tempo de solicitação"

COL_PEDIDO_CUST           = "Número de pedido JMS"
COL_DATA_REGISTRO_CUST    = "data de registro"

# ============== PARÂMETROS GERAIS ==========================
REGIONAIS_DESEJADAS = ["GP", "PA", "GO"]
PRAZO_CUSTODIA_DIAS = 9
NOME_ARQUIVO_FINAL  = "resultado_final_analise_retidos"
EXCEL_ROW_LIMIT = 1_048_000

# ============== FEISHU (Webhooks) ==========================
DEFAULT_FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b"
FEISHU_WEBHOOKS = {}

# ============================================================
# 🧩 FUNÇÕES AUXILIARES
# ============================================================
def converter_datetime(df: pl.DataFrame, coluna: str) -> pl.DataFrame:
    """Converte coluna (string) para datetime, tolerante a múltiplos formatos."""
    if coluna not in df.columns:
        return df
    try:
        df = df.with_columns(pl.col(coluna).str.to_datetime(strict=False))
    except Exception:
        for fmt in ["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S",
                    "%Y/%m/%d %H:%M", "%d/%m/%Y", "%Y-%m-%d"]:
            try:
                df = df.with_columns(pl.col(coluna).str.strptime(pl.Datetime, fmt, strict=False))
                break
            except Exception:
                continue
    return df.filter(pl.col(coluna).is_not_null())

def detectar_coluna(df: pl.DataFrame, candidatos) -> str | None:
    """Encontra uma coluna por aproximação (case-insensitive, substring)."""
    cols_low = {c.lower(): c for c in df.columns}
    for cand in candidatos:
        cand = cand.lower()
        for low, original in cols_low.items():
            if cand in low:
                return original
    return None

def safe_pick(df: pl.DataFrame, preferido: str, candidatos_extra) -> str | None:
    """Prefere um nome de coluna padrão; caso não exista, detecta por candidatos."""
    if preferido in df.columns:
        return preferido
    return detectar_coluna(df, candidatos_extra)

def limpar_pedidos(df: pl.DataFrame, coluna: str) -> pl.DataFrame:
    """Padroniza a coluna de pedido como string stripada."""
    if coluna in df.columns:
        df = df.with_columns(pl.col(coluna).cast(pl.Utf8).str.strip_chars())
    return df

def ler_planilhas(pasta: str, nome_base: str) -> pl.DataFrame:
    """Lê todos os .xls/.xlsx de uma pasta (ignora arquivos temporários ~)."""
    if not os.path.exists(pasta):
        print(f"\033[91m❌ Pasta '{pasta}' não encontrada.\033[0m")
        return pl.DataFrame()

    arquivos = [os.path.join(pasta, f) for f in os.listdir(pasta)
                if f.lower().endswith((".xls", ".xlsx")) and not f.startswith("~$")]
    if not arquivos:
        print(f"\033[93m⚠️ Nenhum arquivo Excel encontrado em {nome_base}.\033[0m")
        return pl.DataFrame()

    print(f"📂 {len(arquivos)} arquivo(s) encontrado(s) em {nome_base}:")
    dfs = []
    for arq in arquivos:
        try:
            df_raw = pl.read_excel(arq)
            df = next(iter(df_raw.values())) if isinstance(df_raw, dict) else df_raw
            dfs.append(df)
            print(f"   ✅ {os.path.basename(arq)} ({df.height} linhas)")
        except Exception as e:
            print(f"\033[91m   ❌ Erro ao ler {os.path.basename(arq)}: {e}\033[0m")

    return pl.concat(dfs, how="diagonal_relaxed") if dfs else pl.DataFrame()

def salvar_resultado(df: pl.DataFrame, caminho_saida: str, nome_base: str) -> str:
    """Salva em XLSX se caber na folha do Excel, senão em CSV."""
    if not os.path.exists(caminho_saida):
        os.makedirs(caminho_saida)
        print(f"\033[94m📁 Pasta criada: {caminho_saida}\033[0m")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = os.path.join(
        caminho_saida,
        f"{nome_base}_{timestamp}.{'csv' if df.height >= EXCEL_ROW_LIMIT else 'xlsx'}"
    )
    if out.endswith(".csv"):
        df.write_csv(out)
    else:
        df.write_excel(out)

    print(f"\n✅ Resultado salvo em: {out}")
    return out

# ============================================================
# 💬 FEISHU – ENVIO DE CARD (NÃO SERÁ USADO POR ENQUANTO)
# ============================================================
def _get_webhook_for(coord: str) -> str:
    """Retorna webhook específico do coordenador, ou o default (teste)."""
    return FEISHU_WEBHOOKS.get(coord, DEFAULT_FEISHU_WEBHOOK)

def enviar_card_feishu(coordenador: str, qtd_retidos: int, percentual_regional: float, url_relatorio: str | None = None):
    """Envia um card por coordenador com os principais indicadores (DESATIVADO NO FLUXO)."""
    webhook = _get_webhook_for(coordenador)
    if not webhook:
        print(f"   ⚠️ Sem webhook para {coordenador}. Pulei envio.")
        return

    card = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": f"🚚 Retidos – {coordenador}"},
                "template": "turquoise"
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"**Pedidos fora do prazo:** {qtd_retidos}\n"
                            f"**% sobre total (amostra):** {percentual_regional:.2f}%\n"
                            f"Atualizado em {datetime.now():%d/%m/%Y %H:%M}"
                        )
                    }
                },
            ]
        }
    }

    try:
        resp = requests.post(webhook, json=card)
        if resp.status_code == 200:
            print(f"   💬 Card enviado para {coordenador}.")
        else:
            print(f"   ⚠️ Erro ao enviar para {coordenador}: {resp.status_code} - {resp.text}")
    except Exception as e:
        print(f"   ❌ Falha ao enviar card para {coordenador}: {e}")
# ============================================================
# 🚀 ANÁLISE PRINCIPAL
# ============================================================
def analisar_retidos():
    print("\n==============================")
    print("🚀 INICIANDO ANÁLISE COMPLETA")
    print("==============================")

    removidos_dev = removidos_cust = removidos_cluster = removidos_prob = 0

    # ---------- 1) RETIDOS ----------
    df_ret = ler_planilhas(PASTA_RETIDOS, "Retidos")
    if df_ret.is_empty():
        print("❌ Nenhum dado em Retidos.")
        return

    # Remover clusters "1 到 2" e "3 到 5" → mantém 6 dias ou mais
    col_cluster = safe_pick(df_ret, "Cluster Retidos 分类", ["cluster", "分类", "retidos"])
    if col_cluster and col_cluster in df_ret.columns:
        total_antes = df_ret.height
        df_ret = df_ret.with_columns(
            pl.col(col_cluster).cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias(col_cluster)
        )
        df_ret = df_ret.filter(
            ~(
                pl.col(col_cluster).str.contains("1 到 2") |
                pl.col(col_cluster).str.contains("3 到 5")
            )
        )
        removidos_cluster = total_antes - df_ret.height
        print(f"\033[95m🧹 Cluster Retidos (1–5 dias) → Removidos: {removidos_cluster} | Mantidos: {df_ret.height}\033[0m")

    # Seleção e padronização de colunas importantes
    col_pedido_ret = safe_pick(df_ret, COL_PEDIDO_RET, ["pedido", "运单", "jms", "remessa"])
    col_data_ret   = safe_pick(df_ret, COL_DATA_ATUALIZACAO_RET, ["data", "prevista", "entrega", "更新"])
    col_regional   = safe_pick(df_ret, COL_REGIONAL_RET, ["regional", "区域"])
    col_base_entrega = safe_pick(df_ret, "Base de Entrega 派件网点", ["base", "网点", "派件"])

    cols = [c for c in [col_pedido_ret, col_data_ret, col_regional, col_base_entrega] if c]
    df_ret = df_ret.select(cols).rename({
        col_pedido_ret: COL_PEDIDO_RET,
        col_data_ret: COL_DATA_ATUALIZACAO_RET,
        col_regional: COL_REGIONAL_RET if col_regional else None,
        col_base_entrega: "Base de Entrega 派件网点" if col_base_entrega else None
    })

    df_ret = limpar_pedidos(df_ret, COL_PEDIDO_RET)
    df_ret = converter_datetime(df_ret, COL_DATA_ATUALIZACAO_RET)

    # Filtra regionais de interesse, se existir a coluna
    if COL_REGIONAL_RET in df_ret.columns:
        df_ret = df_ret.filter(pl.col(COL_REGIONAL_RET).is_in(REGIONAIS_DESEJADAS))

    total_inicial_filtrado = df_ret.height
    print(f"\033[92m🟢 Retidos filtrados ({', '.join(REGIONAIS_DESEJADAS)}): {total_inicial_filtrado}\033[0m")

    # ---------- 2) DEVOLUÇÃO ----------
    df_dev = ler_planilhas(PASTA_DEVOLUCAO, "Devolução")
    if not df_dev.is_empty():
        col_pedido_dev = safe_pick(df_dev, COL_PEDIDO_DEV, ["pedido", "jms"])
        col_data_dev   = safe_pick(df_dev, COL_DATA_SOLICITACAO_DEV, ["solicit", "data"])
        if col_pedido_dev and col_data_dev:
            df_dev = (
                df_dev
                .select([col_pedido_dev, col_data_dev])
                .rename({col_pedido_dev: COL_PEDIDO_DEV, col_data_dev: COL_DATA_SOLICITACAO_DEV})
            )
            df_dev = limpar_pedidos(df_dev, COL_PEDIDO_DEV)
            df_dev = converter_datetime(df_dev, COL_DATA_SOLICITACAO_DEV)
            df_dev = df_dev.group_by(COL_PEDIDO_DEV).agg(pl.col(COL_DATA_SOLICITACAO_DEV).min())

            df_merge = df_ret.join(df_dev, left_on=COL_PEDIDO_RET, right_on=COL_PEDIDO_DEV, how="left")
            df_merge = df_merge.with_columns(
                ((pl.col(COL_DATA_SOLICITACAO_DEV) > pl.col(COL_DATA_ATUALIZACAO_RET))
                 & pl.col(COL_DATA_SOLICITACAO_DEV).is_not_null()).alias("Remover_Dev")
            )
            removidos_dev = df_merge.filter(pl.col("Remover_Dev")).height
            df_ret = df_merge.filter(~pl.col("Remover_Dev")).drop(
                ["Remover_Dev", COL_PEDIDO_DEV, COL_DATA_SOLICITACAO_DEV], strict=False
            )
            print(f"\033[93m🟡 Devolução → Removidos: {removidos_dev} | Mantidos: {df_ret.height}\033[0m")

    # ---------- 3) PROBLEMÁTICOS ----------
    df_prob = ler_planilhas(PASTA_PROBLEMATICOS, "Problemáticos")
    if not df_prob.is_empty():
        col_pedido_prob = safe_pick(df_prob, "Número de pedido JMS", ["pedido", "jms"])
        col_data_prob   = safe_pick(df_prob, "data de registro", ["data", "registro", "anormal"])
        if col_pedido_prob and col_data_prob:
            df_prob = (
                df_prob.select([col_pedido_prob, col_data_prob]).rename({
                    col_pedido_prob: "Número de pedido JMS",
                    col_data_prob: "data de registro"
                })
            )
            df_prob = limpar_pedidos(df_prob, "Número de pedido JMS")
            df_prob = converter_datetime(df_prob, "data de registro")
            df_prob = df_prob.group_by("Número de pedido JMS").agg(pl.col("data de registro").min())

            df_merge_prob = df_ret.join(df_prob, left_on=COL_PEDIDO_RET, right_on="Número de pedido JMS", how="left")
            df_merge_prob = df_merge_prob.with_columns(
                ((pl.col("data de registro") >= pl.col(COL_DATA_ATUALIZACAO_RET))
                 & pl.col("data de registro").is_not_null()).alias("Remover_Prob")
            )
            removidos_prob = df_merge_prob.filter(pl.col("Remover_Prob")).height
            df_ret = df_merge_prob.filter(~pl.col("Remover_Prob")).drop(
                ["Remover_Prob", "Número de pedido JMS", "data de registro"], strict=False
            )
            print(f"\033[38;5;208m🟠 Problemáticos → Removidos: {removidos_prob} | Mantidos: {df_ret.height}\033[0m")

    # ---------- 4) CUSTÓDIA ----------
    df_final = df_ret
    df_cust = ler_planilhas(PASTA_CUSTODIA, "Custódia")
    if not df_cust.is_empty():
        col_pedido_c = safe_pick(df_cust, COL_PEDIDO_CUST, ["pedido", "jms"])
        col_data_c   = safe_pick(df_cust, COL_DATA_REGISTRO_CUST, ["data", "registro"])
        if col_pedido_c and col_data_c:
            df_cust = (
                df_cust
                .select([col_pedido_c, col_data_c])
                .rename({col_pedido_c: COL_PEDIDO_CUST, col_data_c: COL_DATA_REGISTRO_CUST})
            )
            df_cust = limpar_pedidos(df_cust, COL_PEDIDO_CUST)
            df_cust = converter_datetime(df_cust, COL_DATA_REGISTRO_CUST)
            df_cust = df_cust.group_by(COL_PEDIDO_CUST).agg(
                pl.col(COL_DATA_REGISTRO_CUST).min().alias(COL_DATA_REGISTRO_CUST)
            )
            df_cust = df_cust.with_columns(
                (pl.col(COL_DATA_REGISTRO_CUST) + pl.duration(days=PRAZO_CUSTODIA_DIAS)).alias("Prazo_Limite")
            )

            df_join = df_ret.join(df_cust, left_on=COL_PEDIDO_RET, right_on=COL_PEDIDO_CUST, how="left")
            df_join = df_join.with_columns(
                pl.when(
                    (pl.col(COL_DATA_ATUALIZACAO_RET) <= pl.col("Prazo_Limite")) &
                    pl.col("Prazo_Limite").is_not_null()
                )
                .then(pl.lit("Dentro do Prazo"))
                .otherwise(pl.lit("Fora do Prazo"))
                .alias("Status_Custodia")
            )

            removidos_cust = df_join.filter(pl.col("Status_Custodia") == "Dentro do Prazo").height
            df_final = df_join.filter(pl.col("Status_Custodia") == "Fora do Prazo")
            print(f"\033[94m🔵 Custódia → Removidos: {removidos_cust} | Mantidos: {df_final.height}\033[0m")
    # ---------- 5) BASE LISTA (comparativo) ----------
    df_lista = ler_planilhas(PASTA_BASE_LISTA, "Base Retidos (Lista)")
    if not df_lista.is_empty():
        col_base_lista = safe_pick(df_lista, "Nome da base de entrega", ["base", "entrega", "网点"])
        col_qtd_lista  = safe_pick(df_lista, "Qtd a entregar há mais de 10 dias", ["qtd", "10", "dias"])
        if col_base_lista and col_qtd_lista:
            df_lista = df_lista.select([col_base_lista, col_qtd_lista]).rename({
                col_base_lista: "Nome da Base de Entrega",
                col_qtd_lista: "Qtd_Entregas_>10d"
            })
            df_lista = df_lista.with_columns(pl.col("Qtd_Entregas_>10d").cast(pl.Int64, strict=False))

            if "Base de Entrega 派件网点" in df_final.columns:
                # 1. Criar o resumo dos retidos
                df_resumo = (
                    df_final.group_by("Base de Entrega 派件网点")
                    .agg(pl.len().alias("Qtd_Retidos"))
                    .rename({"Base de Entrega 派件网点": "Nome da Base de Entrega"})
                )

                # --- INÍCIO DO TRECHO DE DIAGNÓSTICO ---
                print("\n🔍 DIAGNÓSTICO: Comparando nomes das bases...")
                print("\n>>> Nomes das bases na planilha 'Base Retidos (Lista)':")
                print(df_lista["Nome da Base de Entrega"].unique().sort().to_list())

                print("\n>>> Nomes das bases encontradas nos dados de 'Retidos':")
                print(df_resumo["Nome da Base de Entrega"].unique().sort().to_list())
                print("====================================================\n")
                # --- FIM DO TRECHO DE DIAGNÓSTICO ---

                # REVERTIDO: Mantendo os nomes das bases originais conforme solicitado.
                # O join será feito com os nomes exatos como vêm das planilhas.
                df_compara = df_lista.join(df_resumo, on="Nome da Base de Entrega", how="left")
                df_compara = df_compara.with_columns([
                    pl.col("Qtd_Retidos").fill_null(0).cast(pl.Int64).alias("Qtd_Retidos"),
                    ((pl.col("Qtd_Retidos") / pl.col("Qtd_Entregas_>10d")) * 100)
                    .round(2)
                    .alias("Percentual_Retidos")
                ])

                # formata visual opcional
                df_compara = df_compara.select([
                    "Nome da Base de Entrega", "Qtd_Entregas_>10d", "Qtd_Retidos", "Percentual_Retidos"
                ]).sort("Qtd_Retidos", descending=True)

                out_lista = os.path.join(PASTA_SAIDA, f"Comparativo_Base_Lista_{datetime.now():%Y%m%d_%H%M%S}.xlsx")
                try:
                    df_compara.write_excel(out_lista)
                    print(f"\n📊 Comparativo com Base Lista exportado: {out_lista}")
                except Exception as e:
                    print(f"\033[91m❌ Erro ao salvar comparativo Base Lista: {e}\033[0m")

                # ---------- TOP 5 BASES NO TERMINAL ----------
                try:
                    top5 = (
                        df_compara
                        .with_columns(
                            (pl.col("Qtd_Entregas_>10d") - pl.col("Qtd_Retidos")).alias("Diferença_Lista_vs_Retidos")
                        )
                        .sort("Percentual_Retidos", descending=True)
                        .head(5)
                    )

                    print("\n==============================")
                    print("🏆 TOP 5 BASES – DIAGNÓSTICO")
                    print("==============================")
                    for row in top5.iter_rows(named=True):
                        print(f"""
Base: {row['Nome da Base de Entrega']}
  • Retidos encontrados ............ {row['Qtd_Retidos']}
  • Lista >10 dias ................. {row['Qtd_Entregas_>10d']}
  • Percentual calculado ........... {row['Percentual_Retidos']} %
  • Diferença (Lista - Retidos) .... {row['Diferença_Lista_vs_Retidos']}
""")
                    print("==============================\n")
                except Exception as e:
                    print(f"⚠️ Erro ao exibir top 5 no terminal: {e}")

    # ---------- 6) COORDENADORES (merge) ----------
    if os.path.exists(CAMINHO_COORDENADOR):
        try:
            df_coord_raw = pl.read_excel(CAMINHO_COORDENADOR)
            df_coord = next(iter(df_coord_raw.values())) if isinstance(df_coord_raw, dict) else df_coord_raw

            col_base_coord = detectar_coluna(df_coord, ["nome da base", "base", "entrega"])
            col_coord = detectar_coluna(df_coord, ["coordenador", "responsável", "coordenadores"])

            if col_base_coord and col_coord:
                df_coord = df_coord.select([col_base_coord, col_coord]).rename({
                    col_base_coord: "Nome da Base de Entrega",
                    col_coord: "Coordenador"
                })
                if "Base de Entrega 派件网点" in df_final.columns:
                    df_final = df_final.join(
                        df_coord,
                        left_on="Base de Entrega 派件网点",
                        right_on="Nome da Base de Entrega",
                        how="left"
                    )
                print(f"\033[96m👥 Coordenadores adicionados com sucesso.\033[0m")
            else:
                print("\033[93m⚠️ Colunas 'Nome da base' ou 'Coordenadores' não encontradas em Base_Atualizada.xlsx.\033[0m")
        except Exception as e:
            print(f"\033[91m❌ Erro ao integrar coordenadores: {e}\033[0m")
    else:
        print("\033[93m⚠️ Planilha de Coordenadores não encontrada; seguindo sem coordenador.\033[0m")

    # ---------- 7) SALVAR RESULTADO FINAL ----------
    out_final = salvar_resultado(df_final, PASTA_SAIDA, NOME_ARQUIVO_FINAL)

    # ---------- 8) ENVIAR CARDS FEISHU (DESATIVADO) ----------
    if "Coordenador" in df_final.columns:
        coords_unicos = df_final.select("Coordenador").unique().to_series().drop_nulls().to_list()
        total_amostra = df_final.height if df_final.height else 1
        print(f"\n📢 Envio de cards Feishu está DESATIVADO neste modo de teste.")
        print(f"   Coordenadores impactados: {len(coords_unicos)}")
        for coord in coords_unicos:
            qtd = df_final.filter(pl.col("Coordenador") == coord).height
            percentual = (qtd / total_amostra) * 100.0
            print(f"   - {coord}: {qtd} pedidos ({percentual:.2f}%)")
            # enviar_card_feishu(coord, qtd, percentual, url_relatorio=None)  # <- DESATIVADO
    else:
        print("\033[93m⚠️ Coluna 'Coordenador' não encontrada. Nenhum card preparado.\033[0m")

    # ---------- 9) RESUMO NO CONSOLE ----------
    print("\n==============================")
    print("📦 RESUMO FINAL DE PROCESSAMENTO")
    print("==============================")
    print(f"📊 Total Retidos iniciais (após filtro regional): {total_inicial_filtrado + removidos_cluster}")
    print(f"🟣 Removidos por Cluster (1–5 dias): {removidos_cluster}")
    print(f"🟡 Removidos por Devolução: {removidos_dev}")
    print(f"🟠 Removidos por Problemáticos: {removidos_prob}")
    print(f"🔵 Removidos por Custódia: {removidos_cust}")
    print(f"✅ Pedidos restantes (fora do prazo): {df_final.height}")
    print(f"📄 Arquivo final: {out_final}")

# ============================================================
# ▶️ EXECUÇÃO
# ============================================================
if __name__ == "__main__":
    analisar_retidos()