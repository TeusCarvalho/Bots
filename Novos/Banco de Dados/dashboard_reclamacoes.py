import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
from sqlalchemy import create_engine, text

# ===============================
# 🔌 CONFIGURAÇÃO DO SEU BANCO
# ===============================
DB = {
    "host": "localhost",
    "database": "analytics",
    "user": "postgres",
    "password": "Jt2025"
}

TABELA = "col_2_relatorio_de_reclamacoes"

# ===============================
# ⚙️ ÁREA DE CONFIGURAÇÃO DE COLUNAS
# ===============================
# Mude os nomes aqui se forem diferentes no seu banco de dados
COL_DATA = 'data_registro'  # <--- MUDANÇA FEITA AQUI! De 'data de registro' para 'data_registro'
COL_VALOR = 'Valor do item'
COL_BASE = 'Base responsável'
COL_MOTORISTA = 'Motorista de entrega'
COL_TIPO_PRIMARIO = 'Tipo primário'
COL_TIPO_SECUNDARIO = 'Tipo secundário'


# ===============================


# ===============================
# 🔌 FUNÇÕES DE DADOS (com Cache)
# ===============================
@st.cache_data(ttl=600)
def carregar_dados():
    try:
        db_uri = f'postgresql+psycopg2://{DB["user"]}:{DB["password"]}@{DB["host"]}/{DB["database"]}'
        engine = create_engine(db_uri)
        query = text(f"SELECT * FROM {TABELA};")
        df = pd.read_sql(query, engine)
        engine.dispose()
        return df
    except Exception as e:
        st.error(f"Erro ao conectar ou consultar o banco de dados: {e}")
        return pd.DataFrame()


@st.cache_data
def preparar_dados(df, col_data, col_valor):
    if df.empty:
        return df

    df_clean = df.copy()

    if col_data in df_clean.columns:
        df_clean[col_data] = pd.to_datetime(df_clean[col_data], errors='coerce')
    else:
        st.error(f"Coluna de data '{col_data}' não encontrada. Verifique a configuração.")
        st.stop()  # Para a execução se a coluna principal não existir

    if col_valor in df_clean.columns:
        df_clean[col_valor] = pd.to_numeric(df_clean[col_valor].astype(str).str.replace(',', '.', regex=False),
                                            errors='coerce')

    return df_clean


# ===============================
# 🎨 STREAMLIT UI
# ===============================
st.set_page_config(page_title="Relatório de Reclamações", layout="wide")

# Carregar e preparar dados
df_raw = carregar_dados()
df = preparar_dados(df_raw, COL_DATA, COL_VALOR)

if df.empty:
    st.warning("Não foi possível carregar os dados. Verifique a conexão com o banco.")
else:
    # ===============================
    # 📅 PAINEL LATERAL COM FILTROS
    # ===============================
    st.sidebar.title("Filtros do Relatório")

    data_min = df[COL_DATA].min().date()
    data_max = df[COL_DATA].max().date()

    data_inicio, data_fim = st.sidebar.date_input("Selecione o período:", value=(data_min, data_max),
                                                  min_value=data_min, max_value=data_max)
    total_entregas = st.sidebar.number_input("Total de Entregas no Período", min_value=1, value=100000,
                                             help="Informe o número total de entregas no período para calcular a taxa.")

    df_filtrado = df[(df[COL_DATA].dt.date >= data_inicio) & (df[COL_DATA].dt.date <= data_fim)].copy()

    # ===============================
    # 📈 CÁLCULO DE MÉTRICAS (KPIs)
    # ===============================
    total_reclamacoes = len(df_filtrado)
    taxa_reclamacao = (total_reclamacoes / total_entregas) * 100 if total_entregas > 0 else 0

    # ===============================
    # 📄 CABEÇALHO DO RELATÓRIO
    # ===============================
    st.title("📊 Relatório de Reclamações")
    st.markdown(f"**Período:** {data_inicio.strftime('%d/%m/%Y')} à {data_fim.strftime('%d/%m/%Y')}")

    col_kpi1, col_kpi2 = st.columns(2)
    with col_kpi1:
        st.metric("Total de Reclamações", f"{total_reclamacoes:,}")
    with col_kpi2:
        st.metric("Taxa de Reclamação", f"{taxa_reclamacao:.4f}%")

    st.markdown("---")

    # ===============================
    # 📊 GRÁFICOS
    # ===============================
    st.subheader("N° Reclamações por base")
    if COL_BASE in df_filtrado.columns:
        reclamacoes_por_base = df_filtrado[COL_BASE].value_counts().nlargest(10).reset_index()
        reclamacoes_por_base.columns = ['Base', 'N° Reclamações']
        fig_base = px.bar(reclamacoes_por_base, x='N° Reclamações', y='Base', orientation='h', color='N° Reclamações',
                          color_continuous_scale=px.colors.sequential.Blues)
        fig_base.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_base, use_container_width=True)

    col_graf1, col_graf2 = st.columns(2, gap="large")
    with col_graf1:
        st.subheader("Taxa de Reclamação")
        fig_gauge = go.Figure(
            go.Indicator(mode="gauge+number+delta", value=taxa_reclamacao, domain={'x': [0, 1], 'y': [0, 1]},
                         title={'text': "Taxa (%)"},
                         gauge={'axis': {'range': [None, 0.5]}, 'bar': {'color': "darkblue"},
                                'steps': [{'range': [0, 0.15], 'color': "lightgray"},
                                          {'range': [0.15, 0.3], 'color': "yellow"}],
                                'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 0.3}}))
        fig_gauge.update_layout(height=300)
        st.plotly_chart(fig_gauge, use_container_width=True)

    with col_graf2:
        st.subheader("N° Reclamações por Dia")
        reclamacoes_por_dia = df_filtrado.set_index(COL_DATA).resample('D').size().reset_index(name='contagem')
        fig_linha = px.line(reclamacoes_por_dia, x=COL_DATA, y='contagem', title='Volume Diário', markers=True)
        fig_linha.update_layout(xaxis_title='', yaxis_title='N° de Reclamações')
        st.plotly_chart(fig_linha, use_container_width=True)

    st.markdown("---")
    st.subheader("Motoristas com mais reclamações")
    if COL_MOTORISTA in df_filtrado.columns:
        motoristas_reclamacoes = df_filtrado[COL_MOTORISTA].value_counts().nlargest(15).reset_index()
        motoristas_reclamacoes.columns = ['Motorista', 'N° Reclamações']
        st.dataframe(motoristas_reclamacoes, use_container_width=True, hide_index=True)
