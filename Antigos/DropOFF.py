import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import io

# === Configuração do app ===
st.set_page_config(page_title="Dashboard DROP OFF", layout="wide")
st.title("📊 Dashboard DROP OFF - Mapa Interativo")

# === Upload de arquivo ===
st.sidebar.header("📂 Enviar arquivo")
uploaded_file = st.sidebar.file_uploader(
    "Envie um arquivo Excel ou CSV",
    type=["xlsx", "csv"]
)

if not uploaded_file:
    st.warning("📌 Envie um arquivo Excel ou CSV na barra lateral para começar.")
    st.stop()

# === Função para carregar dados ===
@st.cache_data
def carregar_dados(arquivo):
    if arquivo is None:
        return pd.DataFrame()

    if hasattr(arquivo, "name") and arquivo.name.endswith(".csv"):
        df = pd.read_csv(arquivo, encoding="utf-8", sep=",")
    else:
        df = pd.read_excel(arquivo, sheet_name=None)
        if "DROP OFF" in df:
            df = df["DROP OFF"]
        else:
            df = list(df.values())[0]

    # Renomear colunas principais (tratar variações de nomes)
    col_map = {
        "UF": "UF 州",
        "Estado": "UF 州",
        "Unidade Federativa": "UF 州",
        "Cidade": "Cidade 城市",
        "Municipio": "Cidade 城市",
        "Município": "Cidade 城市"
    }
    df.rename(columns=lambda c: c.strip(), inplace=True)  # remove espaços extras
    df.rename(columns=col_map, inplace=True)

    # Padronizar UF
    if "UF 州" in df.columns:
        df["UF 州"] = df["UF 州"].astype(str).str.strip().str.upper()
        mapa_uf_extra = {
            "SAO PAULO": "SP", "SÃO PAULO": "SP",
            "RIO DE JANEIRO": "RJ", "MINAS GERAIS": "MG",
            "DISTRITO FEDERAL": "DF"
        }
        df["UF 州"] = df["UF 州"].replace(mapa_uf_extra)

    # Padronizar Cidade
    if "Cidade 城市" in df.columns:
        df["Cidade 城市"] = df["Cidade 城市"].astype(str).str.strip().str.title()
        mapa_cidades_extra = {
            "Sp": "São Paulo",
            "Sao Paulo": "São Paulo",
            "S. Paulo": "São Paulo",
            "Rj": "Rio De Janeiro",
            "Rio De Janiero": "Rio De Janeiro",
            "Bhz": "Belo Horizonte"
        }
        df["Cidade 城市"] = df["Cidade 城市"].replace(mapa_cidades_extra)

    if "Status 状态" in df.columns:
        df["Status 状态"] = df["Status 状态"].astype(str).str.strip()

    if "Responsável que prospectou 负责人" in df.columns:
        df["Responsável que prospectou 负责人"] = df["Responsável que prospectou 负责人"].astype(str).str.strip()

    return df

# === Carregar os dados ===
df = carregar_dados(uploaded_file)

# === Baixar GeoJSON dos estados do Brasil ===
url_geojson = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
geojson = requests.get(url_geojson).json()

mapa_ufs = {
    "AC": "Acre", "AL": "Alagoas", "AP": "Amapá", "AM": "Amazonas", "BA": "Bahia",
    "CE": "Ceará", "DF": "Distrito Federal", "ES": "Espírito Santo", "GO": "Goiás",
    "MA": "Maranhão", "MT": "Mato Grosso", "MS": "Mato Grosso do Sul", "MG": "Minas Gerais",
    "PA": "Pará", "PB": "Paraíba", "PR": "Paraná", "PE": "Pernambuco", "PI": "Piauí",
    "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte", "RS": "Rio Grande do Sul",
    "RO": "Rondônia", "RR": "Roraima", "SC": "Santa Catarina", "SP": "São Paulo",
    "SE": "Sergipe", "TO": "Tocantins"
}

uf_counts = df["UF 州"].value_counts().reset_index()
uf_counts.columns = ["UF", "Quantidade"]
uf_counts["Estado"] = uf_counts["UF"].map(mapa_ufs)

# === Filtros ===
st.markdown("### 🔍 Filtros")

estado_temp = st.selectbox("📍 Estado (UF):", ["Todos"] + sorted(df["UF 州"].dropna().unique()))
filtro_status_temp = st.multiselect("📌 Status:", options=sorted(df["Status 状态"].dropna().unique()), default=[])
filtro_resp_temp = st.multiselect("👤 Responsável que prospectou:", options=sorted(df["Responsável que prospectou 负责人"].dropna().unique()), default=[])
filtro_cidade_temp = st.multiselect("🏙️ Cidade:", options=sorted(df["Cidade 城市"].dropna().unique()), default=[])
filtro_base_temp = st.multiselect("🏢 Base Consolidadora:", options=sorted(df["Base Consolidadora 覆盖网点"].dropna().unique()), default=[])

col_filtros1, col_filtros2 = st.columns([1, 1])
aplicar = col_filtros1.button("✅ Aplicar filtros")
limpar = col_filtros2.button("🧹 Limpar filtros")

if aplicar:
    estado_selecionado = estado_temp
    filtro_status = filtro_status_temp
    filtro_resp = filtro_resp_temp
    filtro_cidade = filtro_cidade_temp
    filtro_base = filtro_base_temp
elif limpar:
    estado_selecionado = "Todos"
    filtro_status = []
    filtro_resp = []
    filtro_cidade = []
    filtro_base = []
else:
    estado_selecionado = "Todos"
    filtro_status = []
    filtro_resp = []
    filtro_cidade = []
    filtro_base = []

# === Aplicar filtros ===
df_filtrado = df.copy()
if estado_selecionado != "Todos":
    df_filtrado = df_filtrado[df_filtrado["UF 州"] == estado_selecionado]
if filtro_status:
    df_filtrado = df_filtrado[df_filtrado["Status 状态"].isin(filtro_status)]
if filtro_resp:
    df_filtrado = df_filtrado[df_filtrado["Responsável que prospectou 负责人"].isin(filtro_resp)]
if filtro_cidade:
    df_filtrado = df_filtrado[df_filtrado["Cidade 城市"].isin(filtro_cidade)]
if filtro_base:
    df_filtrado = df_filtrado[df_filtrado["Base Consolidadora 覆盖网点"].isin(filtro_base)]

# === Mapa com destaque ===
uf_counts["Destaque"] = uf_counts["UF"].apply(lambda x: "Selecionado" if x == estado_selecionado else "Outros")
fig_map = px.choropleth(
    uf_counts,
    geojson=geojson,
    featureidkey="properties.name",
    locations="Estado",
    color="Destaque",
    color_discrete_map={"Selecionado": "red", "Outros": "lightblue"},
    title="Distribuição de DROP OFF por Estado"
)
fig_map.update_geos(fitbounds="locations", visible=False)
fig_map.update_layout(height=600, margin=dict(l=0, r=0, t=40, b=0), plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
st.subheader("🗺️ Distribuição de DROP OFF por Estado")
st.plotly_chart(fig_map, use_container_width=True, config={"staticPlot": True})

# === Indicadores ===
col1, col2, col3 = st.columns(3)
col1.metric("Total de Bases", len(df_filtrado))
col2.metric("Funcionando/Ativo", df_filtrado["Status 状态"].eq("Funcionando/Ativo 已开始营业").sum())
col3.metric("Em negociação", df_filtrado["Status 状态"].eq("Em negociação 谈判当中").sum())

# === Distribuição por Status ===
st.subheader("📊 Distribuição por Status")
status_counts = df_filtrado["Status 状态"].value_counts().reset_index()
status_counts.columns = ["Status", "Quantidade"]
fig_status = px.bar(status_counts, x="Quantidade", y="Status", orientation="h", text="Quantidade", color="Quantidade", color_continuous_scale="Blues")
fig_status.update_layout(height=300, margin=dict(l=10, r=10, t=30, b=30), xaxis_title="Qtd", yaxis_title="")
st.plotly_chart(fig_status, use_container_width=True)

# === Tabela detalhada ===
st.subheader("📋 Detalhamento das Bases (planilha completa)")
colunas_ordenadas = [
    "Razão social 公司名称","UF 州","Cidade 城市","Base Consolidadora 覆盖网点","Status 状态","Responsável que prospectou 负责人",
    "Indicação 推荐人","PROPRIETARIO(A) 房东","CNPJ","DADOS BANCÁRIOS 银行信息","PIX CNPJ = conta","E-MAIL 电子邮件","ENDEREÇO 地址",
    "CEL 电话号码","Localização 地图位置","CNAE 服务编号","Documentos enviados","contrato assinado","HORÁRIO DE FUNCIONAMENTO 营业时间",
    "Login  YoYi YoYi注册","PIN site oficial 官网地图标记","Cadastro TOTVS TOTVS注册","Cadastro JMS JMS注册","Treinamento 培训",
    "Documentos Necessários Para Finalização Cadastro YoYi (Foto CPF/RG, Alvará de Funcionamento, Foto da Visão EXTERNA do estabelecimento)",
    "FOTO EXTERNA DA LOJA ","Data de encaminhamento para assinatura de contrato","Data da assinatura do contrato 签合同日期"
]
colunas_existentes = [c for c in colunas_ordenadas if c in df_filtrado.columns]
st.dataframe(df_filtrado[colunas_existentes])

# === Download Excel filtrado ===
buffer = io.BytesIO()
df_filtrado.to_excel(buffer, index=False, engine="openpyxl")
buffer.seek(0)
st.download_button("⬇️ Baixar dados filtrados (Excel)", buffer, file_name=f"dados_{estado_selecionado}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
