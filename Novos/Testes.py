import os
import re
import numpy as np
import pandas as pd

# =========================
# CONFIG
# =========================
pasta = r"C:\Users\J&T-099\Downloads\CEPS MANAUS"
base_path = os.path.join(pasta, "BASE - MANAUS.xlsx")
faixa_path = os.path.join(pasta, "FAIXA CEP MANAUS.xlsx")

# =========================
# Leitura
# =========================
base_df = pd.read_excel(base_path, sheet_name=0)
faixa_df = pd.read_excel(faixa_path, sheet_name=0)

print("Colunas BASE:", list(base_df.columns))
print("Colunas FAIXA:", list(faixa_df.columns))

# =========================
# Funções utilitárias
# =========================
def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip().lower())

def limpar_cep(x):
    """
    Mantém apenas dígitos e converte para int de 8 dígitos.
    Retorna <NA> se inválido.
    """
    if pd.isna(x):
        return pd.NA
    s = str(x).strip()
    s = re.sub(r"\D", "", s)
    if not s:
        return pd.NA
    if len(s) < 8:
        s = s.zfill(8)
    elif len(s) > 8:
        return pd.NA
    return int(s)

def achar_coluna(df: pd.DataFrame, preferidas: list[str], contem: list[str] = None):
    """
    Tenta achar coluna por lista de nomes preferidos (match normalizado),
    senão procura por termos 'contem' no nome da coluna.
    """
    cols = list(df.columns)
    cols_norm = {_norm(c): c for c in cols}

    for nome in preferidas:
        n = _norm(nome)
        if n in cols_norm:
            return cols_norm[n]

    if contem:
        for c in cols:
            cn = _norm(c)
            if all(t in cn for t in contem):
                return c

    return None

# =========================
# Detectar colunas (já batendo com seus arquivos)
# =========================
col_base_cep = achar_coluna(
    base_df,
    preferidas=["CEP destino", "CEP", "CEP Destino", "cep destino"],
    contem=["cep"]
)
if not col_base_cep:
    raise KeyError(f"Não encontrei coluna de CEP na BASE. Colunas: {list(base_df.columns)}")

col_faixa_ini = achar_coluna(
    faixa_df,
    preferidas=["CEP inicial", "CEP_INICIAL", "CEP INICIAL"],
    contem=["cep", "inicial"]
)
col_faixa_fim = achar_coluna(
    faixa_df,
    preferidas=["CEP final", "CEP_FINAL", "CEP FINAL"],
    contem=["cep", "final"]
)

# “Bairro”/região no seu arquivo está como "Nome de região"
col_faixa_bairro = achar_coluna(
    faixa_df,
    preferidas=["Nome de região", "Nome de regiao", "BAIRRO", "Bairro", "Região", "Regiao"],
    contem=["reg"]
)

if not col_faixa_ini or not col_faixa_fim or not col_faixa_bairro:
    raise KeyError(
        "Não consegui detectar as colunas da FAIXA.\n"
        f"Detectado: ini={col_faixa_ini}, fim={col_faixa_fim}, bairro={col_faixa_bairro}\n"
        f"Colunas FAIXA: {list(faixa_df.columns)}"
    )

print("\nUsando colunas:")
print(f"BASE CEP -> {col_base_cep}")
print(f"FAIXA INI -> {col_faixa_ini}")
print(f"FAIXA FIM -> {col_faixa_fim}")
print(f"FAIXA BAIRRO/REGIAO -> {col_faixa_bairro}\n")

# =========================
# Limpar CEPs
# =========================
base_df["_CEP_LIMPO"] = base_df[col_base_cep].apply(limpar_cep).astype("Int64")

faixa_ok = faixa_df.copy()
faixa_ok["_INI"] = faixa_ok[col_faixa_ini].apply(limpar_cep).astype("Int64")
faixa_ok["_FIM"] = faixa_ok[col_faixa_fim].apply(limpar_cep).astype("Int64")

# remove linhas inválidas
faixa_ok = faixa_ok.dropna(subset=["_INI", "_FIM", col_faixa_bairro]).copy()
faixa_ok = faixa_ok[faixa_ok["_INI"] <= faixa_ok["_FIM"]].copy()
faixa_ok.sort_values("_INI", inplace=True)

starts = faixa_ok["_INI"].to_numpy(dtype=np.int64)
ends = faixa_ok["_FIM"].to_numpy(dtype=np.int64)
bairros = faixa_ok[col_faixa_bairro].astype(str).to_numpy()

# =========================
# Match vetorizado por faixa (rápido)
# =========================
ceps_series = base_df["_CEP_LIMPO"]

mask_valid = ceps_series.notna().to_numpy()
ceps = np.zeros(len(base_df), dtype=np.int64)
ceps[mask_valid] = ceps_series[mask_valid].astype(np.int64).to_numpy()

idx = np.searchsorted(starts, ceps, side="right") - 1

ok_idx = (idx >= 0) & mask_valid
ok_range = np.zeros(len(base_df), dtype=bool)
ok_range[ok_idx] = ceps[ok_idx] <= ends[idx[ok_idx]]

resultado = np.full(len(base_df), pd.NA, dtype=object)
resultado[ok_range] = bairros[idx[ok_range]]

# coluna final (nome mais “amigável”)
base_df["BAIRRO_FAIXA_CEP"] = resultado

# =========================
# Relatório e export
# =========================
total = len(base_df)
encontrados = base_df["BAIRRO_FAIXA_CEP"].notna().sum()
nao_encontrados = total - encontrados

print(f"Total linhas: {total}")
print(f"Encontrados: {encontrados}")
print(f"Não encontrados: {nao_encontrados}")

nao = base_df.loc[base_df["BAIRRO_FAIXA_CEP"].isna(), [col_base_cep]].copy()

# remove auxiliar
base_df.drop(columns=["_CEP_LIMPO"], inplace=True)

saida_path = os.path.join(pasta, "BASE_MANAUS_COM_BAIRRO.xlsx")
with pd.ExcelWriter(saida_path, engine="openpyxl") as writer:
    base_df.to_excel(writer, index=False, sheet_name="BASE_COM_BAIRRO")
    nao.to_excel(writer, index=False, sheet_name="CEPS_NAO_ENCONTRADOS")

print("\nArquivo gerado com sucesso:")
print(saida_path)
