# -*- coding: utf-8 -*-
# 🚀 T-0 - Processamento Consolidado (Lê todas as planilhas) — v1.7 (SLA pelos contadores T日)
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

# --- Variações para mapear colunas (PT/中文) ---
VARIACOES = {
    COL_NOME_BASE: {'Nome da base', '网点名称', '所属网点', '网点', 'filial', 'unidade', 'base'},
    COL_REMESSA: {'运单号', '面单号', 'waybill', 'awb', 'pedido', 'número do pedido', 'remessa', '订单号'},
    COL_STATUS_ENTREGA: {'派件状态', '状态', '是否签收', '签收状态', 'delivery status', 'status'},
    COL_SIGNED: {COL_SIGNED, '已签收量', 'T日已签收量', 'signed qty', 'assinados'},
    COL_SHOULD: {COL_SHOULD, '应签收量', 'T日应签收量', 'should sign qty', 'deveriam'}
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

    def _carregar(self) -> pd.DataFrame | None:
        arquivos = sorted(glob.glob(str(self.relatorio_path / '*.xls*')))
        if not arquivos:
            print("⚠️ Nenhum arquivo encontrado na pasta!")
            return None
        print(f"📄 {len(arquivos)} arquivo(s) encontrado(s). Lendo todos...")

        dfs = []
        for arq in arquivos:
            try:
                nome = Path(arq).name
                print(f"→ Lendo: {nome}")
                df = pd.read_excel(arq, dtype=object)
                if df.empty:
                    print(f"   ⚠️ Planilha vazia: {nome} (ignorando)")
                    continue
                df = _mapear(df)
                # Normaliza status (fallback futuro)
                if COL_STATUS_ENTREGA in df.columns:
                    df[COL_STATUS_ENTREGA] = _normaliza_status_serie(df[COL_STATUS_ENTREGA])
                # Garante tipos numéricos para contadores se existirem
                for c in [COL_SIGNED, COL_SHOULD]:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors='coerce')
                df['Arquivo_Origem'] = nome
                dfs.append(df)
            except Exception as e:
                print(f"❌ Erro ao ler {arq}: {e}")

        if not dfs:
            print("⚠️ Nenhuma planilha pôde ser lida.")
            return None
        out = pd.concat(dfs, ignore_index=True)
        print(f"✅ Total combinado: {len(out):,} linhas.")
        return out

    def _sla_por_base_contadores(self, df: pd.DataFrame):
        """Calcula SLA por base usando colunas T日 (已签收量 / 应签收量)."""
        if not ({COL_NOME_BASE, COL_SIGNED, COL_SHOULD} <= set(df.columns)):
            return None  # deixa o caller decidir o fallback

        base = df[[COL_NOME_BASE, COL_SIGNED, COL_SHOULD]].copy()
        base[COL_SIGNED] = pd.to_numeric(base[COL_SIGNED], errors='coerce').fillna(0)
        base[COL_SHOULD] = pd.to_numeric(base[COL_SHOULD], errors='coerce').fillna(0)

        grp = (
            base.groupby(COL_NOME_BASE, dropna=False)
                .sum(numeric_only=True)
                .reset_index()
                .rename(columns={COL_SIGNED: 'Entregues', COL_SHOULD: 'Deveriam'})
        )
        # Evita divisão por zero
        grp['SLA (%)'] = np.where(grp['Deveriam'] > 0, grp['Entregues'] / grp['Deveriam'] * 100, np.nan)

        # SLA Geral
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

    def _mostrar_top5(self, grp: pd.DataFrame, sla_geral: float):
        print("\n📊 --- SLA por Base (usando 已签收量/应签收量) ---" if 'Deveriam' in grp.columns else "\n📊 --- SLA por Base ---")
        # Ordenações
        grp_ord = grp.copy()
        # Se houver SLA nula, tratamos como -1 para jogar ao fim no ranking de melhores
        grp_ord['_ord_sla'] = grp_ord['SLA (%)'].fillna(-1)

        # Top 5 Melhores
        top5 = grp_ord.sort_values(['_ord_sla', 'Entregues', 'Deveriam'], ascending=[False, False, False]).head(5)
        # Top 5 Piores (considerando bases com Deveriam > 0)
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
        df = self._carregar()
        if df is None:
            return

        # 1) Preferência: usar contadores T日 (已签收量/应签收量)
        res = self._sla_por_base_contadores(df)
        if res is not None:
            grp, sla_geral = res
            self._mostrar_top5(grp, sla_geral)
            return

        # 2) Fallback: usar Status/Remessa
        print("ℹ️ Colunas T日 não encontradas. Usando fallback por Status/Remessa.")
        grp, sla_geral = self._sla_por_base_fallback_status(df)
        if grp is None:
            print("❌ Não foi possível calcular SLA (faltam colunas).")
            print(f"Colunas disponíveis: {list(df.columns)}")
            return
        self._mostrar_top5(grp, sla_geral)

# ======================================================
if __name__ == "__main__":
    try:
        ReportProcessor(CAMINHO_PASTA_RELATORIO).run()
    except Exception:
        print("\n--- ERRO FATAL ---")
        traceback.print_exc()
