# -*- coding: utf-8 -*-
# 🚀 T-0 - Processamento Consolidado (Lê todas as planilhas) — v1.9 (SLA por dia e prazo de coleta)
import pandas as pd
import numpy as np
from pathlib import Path
import traceback
import glob
import re

# --- Caminho da pasta ---
CAMINHO_PASTA_RELATORIO = Path(
    r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Semanal\2. Taxa T0'
)

# --- Nomes-alvo unificados ---
COL_NOME_BASE = 'Nome da base'
COL_REMESSA = 'Remessa'
COL_STATUS_ENTREGA = 'Status de Entrega'
COL_SIGNED = 'T日签收率-已签收量'
COL_SHOULD = 'T日签收率-应签收量'
COL_PRAZO_COLETA = 'Horário de término do prazo de coleta'  # NOVA COLUNA

# --- Variações para mapear colunas (PT/中文) ---
VARIACOES = {
    COL_NOME_BASE: {'Nome da base', '网点名称', '所属网点', '网点', 'filial', 'unidade', 'base'},
    COL_REMESSA: {'运单号', '面单号', 'waybill', 'awb', 'pedido', 'número do pedido', 'remessa', '订单号'},
    COL_STATUS_ENTREGA: {'派件状态', '状态', '是否签收', '签收状态', 'delivery status', 'status'},
    COL_SIGNED: {COL_SIGNED, '已签收量', 'T日已签收量', 'signed qty', 'assinados'},
    COL_SHOULD: {COL_SHOULD, '应签收量', 'T日应签收量', 'should sign qty', 'deveriam'},
    COL_PRAZO_COLETA: {COL_PRAZO_COLETA, '揽收时效截止时间', 'prazo coleta', 'deadline coleta', 'collection deadline'}
}

TRADUCOES_STATUS_BINARIO = {'是': 'ENTREGUE', '否': 'EM ROTA', 'Yes': 'ENTREGUE', 'No': 'EM ROTA'}
STATUS_ENTREGUE_TOKENS = {'签收', '已签收', 'delivered', 'entregue', 'sucesso', '成功'}
STATUS_NAO_ENTREGUE_TOKENS = {'未签收', '未派送', 'em rota', '失败', 'failed'}


def _norm(s: str) -> str:
    s = str(s).strip().lower()
    return re.sub(r'\s+', ' ', s)


def _encontra_coluna(df_cols, targets: set[str]) -> str | None:
    norm_map = {c: _norm(c) for c in df_cols}
    targets_norm = {_norm(t) for t in targets}
    # match exato
    for c, cn in norm_map.items():
        if cn in targets_norm:
            return c
    # substring
    for c, cn in norm_map.items():
        for t in targets_norm:
            if t and t in cn:
                return c
    return None


def _mapear(df: pd.DataFrame) -> pd.DataFrame:
    ren = {}
    for alvo, vars_ in VARIACOES.items():
        col = _encontra_coluna(df.columns, vars_ | {alvo})
        if col:
            ren[col] = alvo
    return df.rename(columns=ren)


def _normaliza_status_serie(serie: pd.Series) -> pd.Series:
    s = serie.astype(str).str.strip().replace(TRADUCOES_STATUS_BINARIO)
    s_low = s.str.lower()
    ent = np.full(len(s_low), False)
    nao = np.full(len(s_low), False)
    for tok in STATUS_ENTREGUE_TOKENS:
        ent |= s_low.str.contains(_norm(tok), na=False)
    for tok in STATUS_NAO_ENTREGUE_TOKENS:
        nao |= s_low.str.contains(_norm(tok), na=False)
    out = np.where(ent, 'ENTREGUE', s_low)
    out = np.where(nao & (out != 'ENTREGUE'), 'EM ROTA', out)
    out = pd.Series(out).where(pd.Series(out).isin(['ENTREGUE', 'EM ROTA']), 'EM ROTA')
    return out


class ReportProcessor:
    def __init__(self, relatorio_path: Path):
        self.relatorio_path = relatorio_path
        print("🚀 Iniciando processamento T-0 consolidado")
        print(f"📂 Pasta: {self.relatorio_path}")

    def _carregar(self) -> tuple[pd.DataFrame | None, list[tuple[str, pd.DataFrame]]]:
        """Lê todos os arquivos e retorna o DF consolidado e uma lista de DFs por arquivo."""
        arquivos = sorted(glob.glob(str(self.relatorio_path / '*.xls*')))
        if not arquivos:
            print("⚠️ Nenhum arquivo encontrado na pasta!")
            return None, []
        print(f"📄 {len(arquivos)} arquivo(s) encontrado(s). Lendo todos...")

        dfs_consolidados = []
        dados_por_arquivo = []
        for arq in arquivos:
            try:
                nome = Path(arq).name
                print(f"→ Lendo: {nome}")
                df = pd.read_excel(arq, dtype=object)
                if df.empty:
                    print(f"   ⚠️ Planilha vazia: {nome} (ignorando)")
                    continue
                df = _mapear(df)
                if COL_STATUS_ENTREGA in df.columns:
                    df[COL_STATUS_ENTREGA] = _normaliza_status_serie(df[COL_STATUS_ENTREGA])
                for c in [COL_SIGNED, COL_SHOULD]:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors='coerce')
                df['Arquivo_Origem'] = nome
                dfs_consolidados.append(df)
                dados_por_arquivo.append((nome, df))
            except Exception as e:
                print(f"❌ Erro ao ler {arq}: {e}")

        if not dfs_consolidados:
            print("⚠️ Nenhuma planilha pôde ser lida.")
            return None, []

        df_consolidado = pd.concat(dfs_consolidados, ignore_index=True)
        print(f"✅ Total combinado: {len(df_consolidado):,} linhas.")
        return df_consolidado, dados_por_arquivo

    def _sla_por_base_contadores(self, df: pd.DataFrame):
        """Calcula SLA por base usando colunas T日 (已签收量 / 应签收量)."""
        if not ({COL_NOME_BASE, COL_SIGNED, COL_SHOULD} <= set(df.columns)):
            return None

        base = df[[COL_NOME_BASE, COL_SIGNED, COL_SHOULD]].copy()
        base[COL_SIGNED] = pd.to_numeric(base[COL_SIGNED], errors='coerce').fillna(0)
        base[COL_SHOULD] = pd.to_numeric(base[COL_SHOULD], errors='coerce').fillna(0)

        grp = (
            base.groupby(COL_NOME_BASE, dropna=False)
            .sum(numeric_only=True)
            .reset_index()
            .rename(columns={COL_SIGNED: 'Entregues', COL_SHOULD: 'Deveriam'})
        )
        grp['SLA (%)'] = np.where(grp['Deveriam'] > 0, grp['Entregues'] / grp['Deveriam'] * 100, np.nan)

        tot_entr = grp['Entregues'].sum()
        tot_dev = grp['Deveriam'].sum()
        sla_geral = (tot_entr / tot_dev * 100) if tot_dev > 0 else np.nan

        return grp, sla_geral

    def _sla_por_base_fallback_status(self, df: pd.DataFrame):
        """Fallback caso os contadores T日 não existam."""
        if COL_NOME_BASE not in df.columns:
            return None, np.nan
        if (COL_REMESSA not in df.columns) and (COL_STATUS_ENTREGA not in df.columns):
            return None, np.nan

        if COL_REMESSA in df.columns:
            total = df.groupby(COL_NOME_BASE)[COL_REMESSA].nunique().reset_index(name='Deveriam')
        else:
            total = df.groupby(COL_NOME_BASE).size().reset_index(name='Deveriam')

        if COL_STATUS_ENTREGA in df.columns:
            if COL_REMESSA in df.columns:
                entr = (
                    df.loc[df[COL_STATUS_ENTREGA] == 'ENTREGUE']
                    .groupby(COL_NOME_BASE)[COL_REMESSA]
                    .nunique()
                    .reset_index(name='Entregues')
                )
            else:
                entr = (
                    df.loc[df[COL_STATUS_ENTREGA] == 'ENTREGUE']
                    .groupby(COL_NOME_BASE)
                    .size()
                    .reset_index(name='Entregues')
                )
        else:
            entr = total[[COL_NOME_BASE]].copy()
            entr['Entregues'] = 0

        grp = pd.merge(total, entr, on=COL_NOME_BASE, how='left').fillna(0)
        grp['SLA (%)'] = np.where(grp['Deveriam'] > 0, grp['Entregues'] / grp['Deveriam'] * 100, np.nan)

        tot_entr = grp['Entregues'].sum()
        tot_dev = grp['Deveriam'].sum()
        sla_geral = (tot_entr / tot_dev * 100) if tot_dev > 0 else np.nan

        return grp, sla_geral

    def _calcular_sla_para_df(self, df: pd.DataFrame) -> float | None:
        """Função auxiliar para calcular o SLA geral de um único DataFrame."""
        res = self._sla_por_base_contadores(df)
        if res is not None:
            _, sla_geral = res
            return sla_geral

        # Fallback
        _, sla_geral = self._sla_por_base_fallback_status(df)
        return sla_geral

    def _mostrar_top5(self, grp: pd.DataFrame, sla_geral: float):
        print(
            "\n📊 --- SLA por Base (usando 已签收量/应签收量) ---" if 'Deveriam' in grp.columns else "\n📊 --- SLA por Base ---")
        grp_ord = grp.copy()
        grp_ord['_ord_sla'] = grp_ord['SLA (%)'].fillna(-1)

        top5 = grp_ord.sort_values(['_ord_sla', 'Entregues', 'Deveriam'], ascending=[False, False, False]).head(5)
        base_validas = grp_ord[grp_ord['Deveriam'] > 0].copy()
        worst5 = base_validas.sort_values(['SLA (%)', 'Deveriam'], ascending=[True, False]).head(5)

        def _fmt(df_show: pd.DataFrame) -> pd.DataFrame:
            out = df_show[[COL_NOME_BASE, 'Entregues', 'Deveriam', 'SLA (%)']].copy()
            out['Entregues'] = out['Entregues'].astype('int64')
            out['Deveriam'] = out['Deveriam'].astype('int64')
            out['SLA (%)'] = out['SLA (%)'].map(lambda x: f"{x:.2f}%" if pd.notna(x) else "n/d")
            return out

        print("\n🥇 Top 5 Melhores Bases:")
        print(_fmt(top5).to_string(index=False))

        print("\n🟥 Top 5 Piores Bases (com demanda):")
        if worst5.empty:
            print("Nenhuma base com 'Deveriam' > 0 encontrada.")
        else:
            print(_fmt(worst5).to_string(index=False))

        if pd.notna(sla_geral):
            print(f"\n📌 SLA Geral (Σ Entregues / Σ Deveriam): {sla_geral:.2f}%")
        else:
            print("\n📌 SLA Geral: n/d")

    def run(self):
        df_consolidado, dados_por_arquivo = self._carregar()
        if df_consolidado is None:
            return

        # 1) Calcular e mostrar SLA por dia (arquivo) e por prazo de coleta
        print("\n" + "=" * 50)
        print("📅 --- SLA por Arquivo (Dia) e Prazo de Coleta ---")
        for nome_arq, df_arq in dados_por_arquivo:
            print(f"\n📊 Arquivo: {nome_arq}")

            # Verifica se a coluna de prazo de coleta existe no arquivo
            if COL_PRAZO_COLETA not in df_arq.columns:
                print("   ⚠️ Coluna de prazo de coleta não encontrada. Calculando SLA geral do arquivo.")
                sla_geral_arq = self._calcular_sla_para_df(df_arq)
                if sla_geral_arq is not None and pd.notna(sla_geral_arq):
                    print(f"   - SLA Geral: {sla_geral_arq:.2f}%")
                else:
                    print("   - SLA Geral: n/d (dados insuficientes)")
                continue  # Pula para o próximo arquivo

            # Agrupa pelo horário de prazo de coleta e calcula o SLA para cada grupo
            try:
                # Dropna=False garante que prazos vazios também sejam considerados
                for prazo, grupo_df in df_arq.groupby(COL_PRAZO_COLETA, dropna=False):
                    sla_prazo = self._calcular_sla_para_df(grupo_df)

                    prazo_str = str(prazo)
                    if pd.isna(prazo):
                        prazo_str = 'Prazo Não Definido'

                    if sla_prazo is not None and pd.notna(sla_prazo):
                        print(f"   - {prazo_str}: SLA = {sla_prazo:.2f}%")
                    else:
                        print(f"   - {prazo_str}: SLA = n/d (dados insuficientes)")
            except Exception as e:
                print(f"   ❌ Erro ao agrupar por prazo de coleta: {e}")

        # 2) Calcular e mostrar SLA geral e por base (consolidado de todos os dias)
        print("\n" + "=" * 50)
        print("📈 --- Análise Consolidada (Todos os Arquivos) ---")
        res = self._sla_por_base_contadores(df_consolidado)
        if res is not None:
            grp, sla_geral = res
            self._mostrar_top5(grp, sla_geral)
        else:
            print("ℹ️ Colunas T日 não encontradas. Usando fallback por Status/Remessa.")
            grp, sla_geral = self._sla_por_base_fallback_status(df_consolidado)
            if grp is None:
                print("❌ Não foi possível calcular SLA (faltam colunas).")
                print(f"Colunas disponíveis: {list(df_consolidado.columns)}")
                return
            self._mostrar_top5(grp, sla_geral)


# ======================================================
if __name__ == "__main__":
    try:
        ReportProcessor(CAMINHO_PASTA_RELATORIO).run()
    except Exception:
        print("\n--- ERRO FATAL ---")
        traceback.print_exc()