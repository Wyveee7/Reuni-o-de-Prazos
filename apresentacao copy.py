import streamlit as st
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import altair as alt
import datetime
import numpy as np
import requests

# ========================================================
#          CONFIGURAÇÕES DO BANCO DE DADOS
# ========================================================
DB_CONFIG = {
    "host": st.secrets["db_host"],
    "user": st.secrets["db_user"],
    "password": st.secrets["db_password"],
    "database": st.secrets["db_name"],
    "port": 3306
}

DB_URL = f"mysql+mysqlconnector://{st.secrets['db_user']}:{st.secrets['db_password']}@{st.secrets['db_host']}:3306/{st.secrets['db_name']}"

# ========================================================
#     FUNÇÃO PARA LER DADOS (POR SEMANA)
# ========================================================
@st.cache_data(ttl=300)
def carregar_dados():
    conn = mysql.connector.connect(**DB_CONFIG)
    query = """
        WITH AllData AS (
            SELECT 
                nomeObra AS Obra, 
                CAST(DATE_SUB(data_Projeto, INTERVAL WEEKDAY(data_Projeto) DAY) AS DATE) AS Semana_Inicio, 
                volumeProjetado AS Volume_Projetado, 0 AS Volume_Fabricado, 0 AS Volume_Montado
            FROM `plannix-db`.`plannix` WHERE data_Projeto IS NOT NULL AND volumeProjetado > 0
            UNION ALL
            SELECT 
                nomeObra AS Obra, 
                CAST(DATE_SUB(data_Acabamento, INTERVAL WEEKDAY(data_Acabamento) DAY) AS DATE) AS Semana_Inicio, 
                0 AS Volume_Projetado, volumeFabricado AS Volume_Fabricado, 0 AS Volume_Montado
            FROM `plannix-db`.`plannix` WHERE data_Acabamento IS NOT NULL AND volumeFabricado > 0
            UNION ALL
            SELECT 
                nomeObra AS Obra, 
                CAST(DATE_SUB(dataMontada, INTERVAL WEEKDAY(dataMontada) DAY) AS DATE) AS Semana_Inicio, 
                0 AS Volume_Projetado, 0 AS Volume_Fabricado, volumeMontado AS Volume_Montado
            FROM `plannix-db`.`plannix` WHERE dataMontada IS NOT NULL AND volumeMontado > 0
        )
        SELECT
            Obra, Semana_Inicio AS Semana, 
            SUM(Volume_Projetado) AS Volume_Projetado,
            SUM(Volume_Fabricado) AS Volume_Fabricado,
            SUM(Volume_Montado) AS Volume_Montado
        FROM AllData
        GROUP BY Obra, Semana_Inicio ORDER BY Obra, Semana_Inicio;
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # --- LÓGICA DE UNIFICAÇÃO ---
    df.loc[df['Obra'] == 'MALL SILVIO SILVEIRA - LOJAS', 'Obra'] = 'MALL SILVIO SILVEIRA - POA'
    df = df.groupby(['Obra', 'Semana'], as_index=False)[['Volume_Projetado', 'Volume_Fabricado', 'Volume_Montado']].sum()
    return df

# ========================================================
# FUNÇÃO PARA LER DADOS (TOTAIS POR OBRA)
# ========================================================
@st.cache_data(ttl=300)
def carregar_dados_gerais():
    conn = mysql.connector.connect(**DB_CONFIG)
    query = """
        SELECT
            nomeObra AS Obra,
            SUM(volumeProjetado) AS Projetado,
            SUM(volumeFabricado) AS Fabricado,
            SUM(volumeAcabado) AS Acabado,
            SUM(volumeExpedido) AS Expedido,
            SUM(volumeMontado) AS Montado,
            AVG(peso_frouxo_por_volume) AS "Taxa de Aço" 
        FROM `plannix-db`.`plannix`
        GROUP BY nomeObra
        ORDER BY nomeObra;
    """
    df_geral = pd.read_sql(query, conn)
    conn.close()

    # --- LÓGICA DE UNIFICAÇÃO ---
    df_geral.loc[df_geral['Obra'] == 'MALL SILVIO SILVEIRA - LOJAS', 'Obra'] = 'MALL SILVIO SILVEIRA - POA'
    df_geral = df_geral.groupby('Obra', as_index=False).agg({
        'Projetado': 'sum', 'Fabricado': 'sum', 'Acabado': 'sum',
        'Expedido': 'sum', 'Montado': 'sum', 'Taxa de Aço': 'mean' 
    })
    return df_geral

# ========================================================
# FUNÇÃO PARA LER DADOS (POR FAMÍLIA)
# ========================================================
@st.cache_data(ttl=300)
def carregar_dados_familias():
    conn = mysql.connector.connect(**DB_CONFIG)
    query = """
        SELECT nomeObra AS Obra, familia AS Familia, COUNT(nomePeca) AS unidade, SUM(volumeReal) AS Volume
        FROM `plannix-db`.`plannix`
        WHERE familia IS NOT NULL AND nomePeca IS NOT NULL AND volumeReal IS NOT NULL
        GROUP BY nomeObra, familia ORDER BY Obra, Familia;
    """
    df_familias = pd.read_sql(query, conn)
    conn.close()
    
    # --- LÓGICA DE UNIFICAÇÃO ---
    df_familias.loc[df_familias['Obra'] == 'MALL SILVIO SILVEIRA - LOJAS', 'Obra'] = 'MALL SILVIO SILVEIRA - POA'
    df_familias = df_familias.groupby(['Obra', 'Familia'], as_index=False).sum()
    return df_familias

# ========================================================
# FUNÇÕES RESTAURADAS PARA O PLANEJADOR
# ========================================================
@st.cache_data(ttl=300)
def carregar_datas_limite_etapas(obra_nome):
    conn = mysql.connector.connect(**DB_CONFIG)
    query = f"""
        SELECT 
            MIN(data_Projeto) as ini_proj, MAX(data_Projeto) as fim_proj,
            MIN(data_Acabamento) as ini_fab, MAX(data_Acabamento) as fim_fab,
            MIN(dataMontada) as ini_mont, MAX(dataMontada) as fim_mont
        FROM `plannix-db`.`plannix`
        WHERE nomeObra = '{obra_nome}'
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

@st.cache_data(ttl=300)
def calcular_medias_cronograma():
    conn = mysql.connector.connect(**DB_CONFIG)
    query = """
        SELECT
            AVG(DATEDIFF(fim_p, ini_p)) as dias_duracao_proj,
            AVG(DATEDIFF(ini_f, ini_p)) as dias_lag_fab, 
            AVG(DATEDIFF(fim_f, ini_f)) as dias_duracao_fab,
            AVG(DATEDIFF(ini_m, ini_p)) as dias_lag_mont, 
            AVG(DATEDIFF(fim_m, ini_m)) as dias_duracao_mont
        FROM (
            SELECT
                nomeObra,
                MIN(data_Projeto) as ini_p, MAX(data_Projeto) as fim_p,
                MIN(data_Acabamento) as ini_f, MAX(data_Acabamento) as fim_f,
                MIN(dataMontada) as ini_m, MAX(dataMontada) as fim_m
            FROM `plannix-db`.`plannix`
            GROUP BY nomeObra
            HAVING ini_p IS NOT NULL AND ini_f IS NOT NULL AND ini_m IS NOT NULL
        ) as sub
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# ========================================================
# FUNÇÃO PARA CARREGAR DADOS SALVOS DO USUÁRIO
# ========================================================
def carregar_dados_usuario():
    engine = create_engine(DB_URL)
    df_orcamentos_salvos = pd.DataFrame(columns=["Obra", "Orcamento", "Orcamento Lajes"])
    df_previsoes_salvas = pd.DataFrame(columns=["Obra", "Semana", "Projeto Previsto %", "Fabricação Prevista %", "Montagem Prevista %"])

    try:
        df_orcamentos_salvos = pd.read_sql("SELECT * FROM orcamentos_usuario", con=engine)
    except:
        # Tabela ainda não existe, será criada depois
        pass
    
    try:
        df_previsoes_salvas = pd.read_sql("SELECT * FROM previsoes_usuario", con=engine)
        if not df_previsoes_salvas.empty:
            df_previsoes_salvas['Semana'] = pd.to_datetime(df_previsoes_salvas['Semana'])
    except:
        pass

    engine.dispose()
    return df_orcamentos_salvos, df_previsoes_salvas

# ========================================================
# FUNÇÃO HELPER PARA FORMATAR A SEMANA
# ========================================================
def formatar_semana(date):
    if pd.isna(date): return None
    if isinstance(date, str):
        try: date = pd.to_datetime(date)
        except: return date
    start_str = date.strftime('%d/%m')
    end_str = (date + pd.Timedelta(days=6)).strftime('%d/%m')
    return f"{start_str} á {end_str} ({date.strftime('%Y')})"

# ========================================================
# FUNÇÃO PARA BUSCAR DADOS DO WAR ROOM (API)
# ========================================================
WAR_ROOM_URL = "https://war-room-vejv.vercel.app/api/war-room"

@st.cache_data(ttl=10)
def carregar_war_room():
    resp = requests.get(WAR_ROOM_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError("Resposta inesperada da API War Room.")
    return data

# ========================================================
# FUNÇÃO PARA BUSCAR DADOS DO WAR ROOM SEMANAL (API)
# ========================================================
WAR_ROOM_WEEK_URL = "https://war-room-vejv.vercel.app/api/war-room-week"

@st.cache_data(ttl=300)
def carregar_war_room_week():
    resp = requests.get(WAR_ROOM_WEEK_URL, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError("Resposta inesperada da API War Room Week.")
    return data

# ========================================================
# FUNÇÃO PARA SALVAR DADOS NO MYSQL
# ========================================================
def salvar_dados_usuario(df_previsoes, df_orcamentos):
    engine = create_engine(DB_URL)
    try:
        df_previsoes_limpo = df_previsoes.dropna(subset=['Obra', 'Semana'])
        df_save_previsoes = df_previsoes_limpo[[
            "Obra", "Semana", "Projeto Previsto %", "Fabricação Prevista %", "Montagem Prevista %"
        ]].copy()
        df_save_previsoes['Semana'] = pd.to_datetime(df_save_previsoes['Semana']).dt.strftime('%Y-%m-%d')
        
        df_save_previsoes.to_sql('previsoes_usuario', con=engine, if_exists='replace', index=False)
        df_orcamentos.to_sql('orcamentos_usuario', con=engine, if_exists='replace', index=False)
        st.success("✅ **Alterações salvas com sucesso no banco de dados!**")
    except Exception as e:
        st.error(f"❌ Erro ao salvar dados no banco de dados: {e}")
    finally:
        engine.dispose()
        
# ========================================================
#                INTERFACE STREAMLIT
# ========================================================
st.set_page_config(page_title="Reunião de Prazos", layout="wide")
st.title("📊 Reunião de Prazos")

# --- 1. CARREGAMENTO INICIAL ---
try:
    df_base = carregar_dados()
    df_orcamentos_salvos, df_previsoes_salvas = carregar_dados_usuario()
except Exception as e:
    st.error(f"Erro fatal ao carregar dados do MySQL: {e}")
    st.stop()

df_base['Semana'] = pd.to_datetime(df_base['Semana'])
todas_obras_lista = df_base["Obra"].unique().tolist()

# --- 2.5 INICIALIZAÇÃO DO SESSION STATE ---
if 'orcamentos' not in st.session_state:
    df_orcamentos_base = pd.DataFrame({"Obra": todas_obras_lista})
    df_orcamentos_para_editor = df_orcamentos_base.merge(df_orcamentos_salvos, on="Obra", how="left")
    st.session_state['orcamentos'] = df_orcamentos_para_editor

# --- LIMPEZA DE COLUNAS ANTIGAS ---
cols_remover = ["Prazo Projeto", "Prazo Fabricacao", "Prazo Montagem", "Data Inicio"]
st.session_state['orcamentos'] = st.session_state['orcamentos'].drop(
    columns=[c for c in cols_remover if c in st.session_state['orcamentos'].columns], 
    errors='ignore'
)

# Blindagem de Colunas Novas (Cria se não existir)
cols_datas_necessarias = ["Ini Projeto", "Fim Projeto", "Ini Fabricacao", "Fim Fabricacao", "Ini Montagem", "Fim Montagem"]
for col in cols_datas_necessarias:
    if col not in st.session_state['orcamentos'].columns:
        st.session_state['orcamentos'][col] = None

defaults_num = {'Orcamento': 100.0, 'Orcamento Lajes': 0.0}
for col, val in defaults_num.items():
    if col not in st.session_state['orcamentos'].columns:
        st.session_state['orcamentos'][col] = val
    else:
        st.session_state['orcamentos'][col] = st.session_state['orcamentos'][col].fillna(val)

# --- 3. FILTRO GLOBAL ---
st.subheader("⚙️ Filtros Globais")
col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    obras_selecionadas = st.multiselect("Selecione as Obras:", options=todas_obras_lista, default=todas_obras_lista)
with col2:
    data_inicio = st.date_input("Data de Início:", value=df_base['Semana'].min() - pd.Timedelta(weeks=10))
with col3:
    data_fim = st.date_input("Data Final:", value=df_base['Semana'].max())

data_inicio = pd.to_datetime(data_inicio)
data_fim = pd.to_datetime(data_fim)
    
# --- 4. PREPARAÇÃO DOS DADOS ---
df_para_cumsum = df_base[(df_base["Obra"].isin(obras_selecionadas))].copy()

# Preenchimento de Lacunas (Lógica Otimizada)
zero_rows = []
weeks_to_add = 10 
for obra in obras_selecionadas:
    obra_df = df_para_cumsum[df_para_cumsum['Obra'] == obra]
    if not obra_df.empty:
        curr = obra_df['Semana'].min()
        for _ in range(weeks_to_add):
            curr -= pd.Timedelta(days=7)
            zero_rows.append({'Obra': obra, 'Semana': curr, 'Volume_Projetado': 0, 'Volume_Fabricado': 0, 'Volume_Montado': 0})
        curr_fut = obra_df['Semana'].max()
        for _ in range(weeks_to_add):
            curr_fut += pd.Timedelta(days=7)
            zero_rows.append({'Obra': obra, 'Semana': curr_fut, 'Volume_Projetado': 0, 'Volume_Fabricado': 0, 'Volume_Montado': 0})

if zero_rows:
    df_para_cumsum = pd.concat([pd.DataFrame(zero_rows), df_para_cumsum], ignore_index=True)

dfs_preenchidos = []
if not df_para_cumsum.empty:
    for obra, dados in df_para_cumsum.groupby("Obra"):
        dados = dados.groupby("Semana").sum(numeric_only=True).reset_index().set_index("Semana").sort_index()
        idx_completo = pd.date_range(start=dados.index.min(), end=dados.index.max(), freq='7D')
        dados = dados.reindex(idx_completo).fillna(0)
        dados['Obra'] = obra
        dfs_preenchidos.append(dados)
    df_para_cumsum = pd.concat(dfs_preenchidos).reset_index().rename(columns={'index': 'Semana'})

df_para_cumsum = df_para_cumsum.sort_values(["Obra", "Semana"]) 
for col in ["Volume_Projetado", "Volume_Fabricado", "Volume_Montado"]:
    df_para_cumsum[col] = df_para_cumsum.groupby("Obra")[col].cumsum()

df = df_para_cumsum[(df_para_cumsum["Semana"] >= data_inicio) & (df_para_cumsum["Semana"] <= data_fim)].copy()

if df.empty and not obras_selecionadas:
    st.warning("Nenhuma obra encontrada.")
    st.stop()

df['Semana_Display'] = df['Semana'].apply(formatar_semana)
    
# --- 5. ABAS ---
tab_cadastro, tab_tabelas, tab_graficos, tab_geral, tab_planejador, tab_warroom = st.tabs([
    "📁 Cadastro", "📊 Tabelas", "📈 Gráficos", "🌍 Tabela Geral", "📅 Planejador", "🏗️ War Room"
])

# --- ABA 1: CADASTRO (COM CORREÇÃO DE WIDTH E CALLBACK) ---
with tab_cadastro:
    st.subheader("💰 1. Orçamento e Datas das Etapas")
    st.info("Cadastre o orçamento e as datas de **Início e Fim** de cada etapa.")
    
    # 1. Copia e filtra os dados da memória
    orcamentos_filtrado = st.session_state['orcamentos'][st.session_state['orcamentos']['Obra'].isin(obras_selecionadas)].copy()
    
    # 2. Conversão de tipos (Blindagem)
    for col in cols_datas_necessarias:
        if col not in orcamentos_filtrado.columns: orcamentos_filtrado[col] = None
        orcamentos_filtrado[col] = pd.to_datetime(orcamentos_filtrado[col], errors='coerce')

    # --- CALLBACK DE SALVAMENTO AUTOMÁTICO ---
    def atualizar_session_state():
        edits = st.session_state["editor_cadastro"]
        if edits["edited_rows"]:
            for index, changes in edits["edited_rows"].items():
                # Recupera o índice real
                real_index = orcamentos_filtrado.index[index]
                # Aplica mudanças
                for col_name, new_value in changes.items():
                    st.session_state['orcamentos'].at[real_index, col_name] = new_value

    # 3. O Editor de Dados
    st.data_editor(
        orcamentos_filtrado, 
        key="editor_cadastro", # Chave para o callback
        on_change=atualizar_session_state, # Ativa o salvamento imediato
        hide_index=True, 
        use_container_width=True, # Substitui o width=None
        disabled=["Obra"], 
        column_config={
            "Obra": st.column_config.TextColumn("Obra", disabled=True),
            "Orcamento": st.column_config.NumberColumn("Orçamento (Vol)", min_value=0.01, format="%.2f"),
            "Orcamento Lajes": st.column_config.NumberColumn("Orç. Lajes", min_value=0.00, format="%.2f"),
            
            # DATAS POR ETAPA (INICIO E FIM)
            "Ini Projeto": st.column_config.DateColumn("Ini Proj.", format="DD/MM/YYYY"),
            "Fim Projeto": st.column_config.DateColumn("Fim Proj.", format="DD/MM/YYYY"),
            
            "Ini Fabricacao": st.column_config.DateColumn("Ini Fab.", format="DD/MM/YYYY"),
            "Fim Fabricacao": st.column_config.DateColumn("Fim Fab.", format="DD/MM/YYYY"),
            
            "Ini Montagem": st.column_config.DateColumn("Ini Mont.", format="DD/MM/YYYY"),
            "Fim Montagem": st.column_config.DateColumn("Fim Mont.", format="DD/MM/YYYY"),
            
            # Ocultar
            "Data Inicio": None
        }
    )
    
    # Botão de Salvar apenas para o Banco de Dados
    if st.button("💾 Salvar Cadastro no Banco de Dados", key="btn_salvar_cadastro"):
        salvar_dados_usuario(df_previsoes_salvas, st.session_state['orcamentos'])

# --- 6. MERGE FINAL ---
df_orcamentos_atual = st.session_state['orcamentos']
df = df.merge(df_orcamentos_atual, on="Obra", how="left")
for col in ["Projetado", "Fabricado", "Montado"]:
    df[f"{col} %"] = (df[f"Volume_{col}"] / df["Orcamento"]) * 100

if not df_previsoes_salvas.empty:
    df = df.merge(df_previsoes_salvas, on=["Obra", "Semana"], how="left")
for col in ["Projeto Previsto %", "Fabricação Prevista %", "Montagem Prevista %"]:
    df[col] = df[col].fillna(0.0)
df_para_edicao = df.copy() 

# --- ABA 2: TABELAS ---
with tab_tabelas:
    st.subheader("Controles de Visualização")
    c1, c2 = st.columns(2)
    with c1: show_editor = st.checkbox("Mostrar Edição de Previsões", value=True)
    with c2: show_result_table = st.checkbox("Mostrar Tabela Completa", value=True)
    
    df_editado = df_para_edicao 
    if show_editor:
        st.markdown("---")
        st.subheader("✏️ 2. Edite as Previsões Semanais")
        # Esconde colunas que não são de previsão
        cols_ocultar = ["Obra", "Semana", "Semana_Display", "Volume_Projetado", "Projetado %", "Volume_Fabricado", "Fabricado %", "Volume_Montado", "Montado %", "Orcamento", "Orcamento Lajes"] + cols_datas_necessarias
        
        df_editado = st.data_editor(
            df_para_edicao, key="dados_editor", use_container_width=True, hide_index=True, disabled=cols_ocultar,
            column_config={
                "Semana_Display": "Semana", 
                "Projeto Previsto %": st.column_config.NumberColumn(format="%.0f%%"),
                "Fabricação Prevista %": st.column_config.NumberColumn(format="%.0f%%"),
                "Montagem Prevista %": st.column_config.NumberColumn(format="%.0f%%"),
            }
        )
        st.markdown("---")

    # Lógica de Corte
    df_calculado = df_editado.copy().sort_values(['Obra', 'Semana'])
    for col in ["Projeto Previsto %", "Fabricação Prevista %", "Montagem Prevista %"]:
        df_calculado[col] = df_calculado[col].replace(0.0, np.nan)
        df_calculado[col] = df_calculado.groupby('Obra')[col].ffill().fillna(0.0)
        mask_concluido = df_calculado.groupby('Obra')[col].shift(1) >= 100.0
        df_calculado.loc[mask_concluido, col] = np.nan

    if st.button("💾 Salvar Previsões no Banco de Dados", type="primary"):
        salvar_dados_usuario(df_editado, st.session_state['orcamentos'])

    if show_result_table:
        cols_res = ["Obra", "Semana_Display", "Projetado %", "Projeto Previsto %", "Fabricado %", "Fabricação Prevista %", "Montado %", "Montagem Prevista %"]
        st.dataframe(df_calculado[[c for c in cols_res if c in df_calculado.columns]], use_container_width=True, hide_index=True)

# --- ABA 3: GRÁFICOS (MODO SLIDESHOW) ---
with tab_graficos:
    st.subheader("📈 Tendências e Resumo por Obra")

    if not obras_selecionadas:
        st.warning("⚠️ Selecione pelo menos uma obra no filtro global acima.")
    elif df_calculado.empty:
        st.warning("Nenhum dado calculado para as obras selecionadas.")
    else:
        # 1. INICIALIZA O ESTADO DO SLIDE
        if 'slide_index' not in st.session_state:
            st.session_state.slide_index = 0

        # Garantia de segurança: se o usuário mudar o filtro global e o array diminuir
        if st.session_state.slide_index >= len(obras_selecionadas):
            st.session_state.slide_index = 0

        # 2. CONTROLES DE NAVEGAÇÃO (Botões)
        col_prev, col_title, col_next = st.columns([1, 4, 1])
        
        with col_prev:
            if st.button("⬅️ Obra Anterior", use_container_width=True):
                # O módulo (%) faz o carrossel dar a volta quando chega no início
                st.session_state.slide_index = (st.session_state.slide_index - 1) % len(obras_selecionadas)
                
        with col_title:
            obra_atual = obras_selecionadas[st.session_state.slide_index]
            st.markdown(f"<h3 style='text-align: center; color: #1f77b4;'>{obra_atual}</h3>", unsafe_allow_html=True)
            st.markdown(f"<p style='text-align: center;'>Mostrando obra <b>{st.session_state.slide_index + 1}</b> de <b>{len(obras_selecionadas)}</b></p>", unsafe_allow_html=True)
            
        with col_next:
            if st.button("Próxima Obra ➡️", use_container_width=True):
                # O módulo (%) faz o carrossel dar a volta quando chega no fim
                st.session_state.slide_index = (st.session_state.slide_index + 1) % len(obras_selecionadas)

        st.markdown("---")

        # 3. RENDERIZAÇÃO DO GRÁFICO (Filtrado apenas para a obra atual)
        df_obra_chart = df_calculado[df_calculado["Obra"] == obra_atual]
        
        if not df_obra_chart.empty:
            df_melt = df_obra_chart.melt(
                id_vars=["Obra", "Semana_Display", "Semana"], 
                value_vars=[c for c in ["Projetado %", "Projeto Previsto %", "Fabricado %", "Fabricação Prevista %", "Montado %", "Montagem Prevista %"] if c in df_obra_chart.columns],
                var_name="Métrica", 
                value_name="Porcentagem"
            )
            chart = alt.Chart(df_melt).mark_line(point=True, strokeWidth=3).encode(
                x=alt.X('Semana_Display:N', sort=alt.SortField(field="Semana", order='ascending'), title='Semana'),
                y=alt.Y('Porcentagem:Q', title='Avanço (%)'), 
                color=alt.Color('Métrica:N', scale=alt.Scale(scheme='category10')), 
                strokeDash=alt.condition(
                    alt.datum.Métrica.contains('Previst'), 
                    alt.value([5, 5]), # Linha tracejada para previsões
                    alt.value([0])     # Linha contínua para o realizado
                ),
                tooltip=['Obra', 'Semana_Display', 'Métrica', alt.Tooltip('Porcentagem', format='.1f')]
            ).properties(height=350).interactive()
            
            st.altair_chart(chart, use_container_width=True)

        # 4. RENDERIZAÇÃO DA TABELA GERAL (Específica da obra atual)
        st.subheader("📋 Resumo Consolidado da Obra")
        
        # Como as funções de banco de dados tem cache (@st.cache_data), não há problema em chamar de novo
        df_geral_slide = carregar_dados_gerais()
        df_orc_slide = st.session_state['orcamentos'].drop_duplicates(subset=['Obra'], keep='first')
        
        # Filtra apenas a obra atual ANTES de fazer os cálculos pesados
        df_geral_slide = df_geral_slide[df_geral_slide["Obra"] == obra_atual].copy()
        
        if not df_geral_slide.empty:
            df_geral_slide = df_geral_slide.merge(df_orc_slide, on="Obra", how="left")
            
            # Repete a lógica de cálculos da tabela geral para esta linha
            cols_num = ["Orcamento", "Orcamento Lajes", "Projetado", "Fabricado", "Acabado", "Expedido", "Montado"]
            for col in cols_num: 
                if col in df_geral_slide.columns: df_geral_slide[col] = df_geral_slide[col].fillna(0.0)

            for etapa in ["Projetado", "Fabricado", "Acabado", "Expedido", "Montado"]:
                df_geral_slide[f"{etapa} %"] = df_geral_slide.apply(lambda r: (r[etapa]/r["Orcamento"]*100) if r.get("Orcamento", 0) > 0 else 0, axis=1)

            hoje = pd.to_datetime(datetime.date.today())
            for col in ["Fim Projeto", "Fim Fabricacao", "Fim Montagem"]:
                if col not in df_geral_slide.columns: df_geral_slide[col] = None
                df_geral_slide[col] = pd.to_datetime(df_geral_slide[col], errors='coerce')

            df_geral_slide['Saldo Proj'] = df_geral_slide.apply(lambda r: (r['Fim Projeto'] - hoje).days if pd.notna(r['Fim Projeto']) else None, axis=1)
            df_geral_slide['Saldo Fab']  = df_geral_slide.apply(lambda r: (r['Fim Fabricacao'] - hoje).days if pd.notna(r['Fim Fabricacao']) else None, axis=1)
            df_geral_slide['Saldo Mont'] = df_geral_slide.apply(lambda r: (r['Fim Montagem'] - hoje).days if pd.notna(r['Fim Montagem']) else None, axis=1)

            cols_final = [c for c in [
                "Obra", "Orcamento", "Orcamento Lajes", "Projetado", "Projetado %", "Saldo Proj",
                "Taxa de Aço", "Fabricado", "Fabricado %", "Saldo Fab", "Acabado", "Acabado %",
                "Expedido", "Expedido %", "Montado", "Montado %", "Saldo Mont"
            ] if c in df_geral_slide.columns]

            # Reutiliza o mesmo column_config da Aba 4
            st.dataframe(
                df_geral_slide[cols_final], use_container_width=True, hide_index=True,
                column_config={
                    "Orcamento": st.column_config.NumberColumn("Orçamento", format="%.2f"),
                    "Orcamento Lajes": st.column_config.NumberColumn("Orç. Lajes", format="%.2f"),
                    "Taxa de Aço": st.column_config.NumberColumn("Aço (kg/m³)", format="%.2f"),
                    "Projetado": st.column_config.NumberColumn("Vol. Proj.", format="%.2f"),
                    "Projetado %": st.column_config.NumberColumn("Proj. %", format="%.1f%%"), 
                    "Saldo Proj": st.column_config.NumberColumn("⏳ Dias", format="%d d"),
                    "Fabricado": st.column_config.NumberColumn("Vol. Fab.", format="%.2f"),
                    "Fabricado %": st.column_config.NumberColumn("Fab. %", format="%.1f%%"),
                    "Saldo Fab": st.column_config.NumberColumn("⏳ Dias", format="%d d"),
                    "Montado": st.column_config.NumberColumn("Vol. Mont.", format="%.2f"),
                    "Montado %": st.column_config.NumberColumn("Mont. %", format="%.1f%%"),
                    "Saldo Mont": st.column_config.NumberColumn("⏳ Dias", format="%d d"),
                    "Acabado": st.column_config.NumberColumn("Vol. Acab.", format="%.2f"),
                    "Acabado %": st.column_config.NumberColumn("Acab. %", format="%.1f%%"),
                    "Expedido": st.column_config.NumberColumn("Vol. Exp.", format="%.2f"),
                    "Expedido %": st.column_config.NumberColumn("Exp. %", format="%.1f%%"),
                }
            )
# --- ABA 4: TABELA GERAL (VISÃO DETALHADA + SALDO DIAS) ---
with tab_geral:
    st.subheader("🏗️ Resumo Geral Detalhado")
    try:
        df_geral = carregar_dados_gerais()
        df_orc_clean = st.session_state['orcamentos'].drop_duplicates(subset=['Obra'], keep='first')
        df_geral = df_geral.merge(df_orc_clean, on="Obra", how="left")

        # Cálculos Numéricos
        cols_num = ["Orcamento", "Orcamento Lajes", "Projetado", "Fabricado", "Acabado", "Expedido", "Montado"]
        for col in cols_num: 
            if col in df_geral.columns: df_geral[col] = df_geral[col].fillna(0.0)

        for etapa in ["Projetado", "Fabricado", "Acabado", "Expedido", "Montado"]:
            df_geral[f"{etapa} %"] = df_geral.apply(lambda r: (r[etapa]/r["Orcamento"]*100) if r["Orcamento"]>0 else 0, axis=1)

        # Cálculo Saldo de Dias
        hoje = pd.to_datetime(datetime.date.today())
        cols_fim = ["Fim Projeto", "Fim Fabricacao", "Fim Montagem"]
        
        for col in cols_fim:
            if col not in df_geral.columns: df_geral[col] = None
            df_geral[col] = pd.to_datetime(df_geral[col], errors='coerce')

        def calc_saldo(row, col_prazo):
            if pd.isna(row[col_prazo]): return None
            return (row[col_prazo] - hoje).days

        df_geral['Saldo Proj'] = df_geral.apply(lambda r: calc_saldo(r, 'Fim Projeto'), axis=1)
        df_geral['Saldo Fab'] = df_geral.apply(lambda r: calc_saldo(r, 'Fim Fabricacao'), axis=1)
        df_geral['Saldo Mont'] = df_geral.apply(lambda r: calc_saldo(r, 'Fim Montagem'), axis=1)

        # --- ORDEM FINAL (Conforme solicitado) ---
        colunas_ordenadas = [
            "Obra", "Orcamento", "Orcamento Lajes",
            
            "Projetado", "Projetado %", "Saldo Proj",
            "Taxa de Aço",
            
            "Fabricado", "Fabricado %", "Saldo Fab",
            
            "Acabado", "Acabado %",
            "Expedido", "Expedido %",
            
            "Montado", "Montado %", "Saldo Mont"
        ]
        
        cols_final = [c for c in colunas_ordenadas if c in df_geral.columns]

        st.dataframe(
            df_geral[cols_final], use_container_width=True, hide_index=True,
            column_config={
                "Orcamento": st.column_config.NumberColumn("Orçamento", format="%.2f"),
                "Orcamento Lajes": st.column_config.NumberColumn("Orç. Lajes", format="%.2f"),
                "Taxa de Aço": st.column_config.NumberColumn("Aço (kg/m³)", format="%.2f"),
                
                # Etapas com Saldo
                "Projetado": st.column_config.NumberColumn("Vol. Proj.", format="%.2f"),
                "Projetado %": st.column_config.NumberColumn("Proj. %", format="%.1f%%"), 
                "Saldo Proj": st.column_config.NumberColumn("⏳ Dias", format="%d d"),

                "Fabricado": st.column_config.NumberColumn("Vol. Fab.", format="%.2f"),
                "Fabricado %": st.column_config.NumberColumn("Fab. %", format="%.1f%%"),
                "Saldo Fab": st.column_config.NumberColumn("⏳ Dias", format="%d d"),

                "Montado": st.column_config.NumberColumn("Vol. Mont.", format="%.2f"),
                "Montado %": st.column_config.NumberColumn("Mont. %", format="%.1f%%"),
                "Saldo Mont": st.column_config.NumberColumn("⏳ Dias", format="%d d"),

                # Etapas sem saldo
                "Acabado": st.column_config.NumberColumn("Vol. Acab.", format="%.2f"),
                "Acabado %": st.column_config.NumberColumn("Acab. %", format="%.1f%%"),
                "Expedido": st.column_config.NumberColumn("Vol. Exp.", format="%.2f"),
                "Expedido %": st.column_config.NumberColumn("Exp. %", format="%.1f%%"),
            }
        )
        st.markdown('---')
        st.subheader('📅 War Room Semanal')
        try:
            data_week = carregar_war_room_week()
            if data_week:
                df_week = pd.DataFrame(data_week)
                needed = {'inicio','fim','setor','total_programado','total_realizado'}
                if needed.issubset(df_week.columns):
                    df_week['Datas'] = pd.to_datetime(df_week['inicio']).dt.strftime('%d/%m') + ' a ' + pd.to_datetime(df_week['fim']).dt.strftime('%d/%m')
                    df_week = df_week[['Datas','setor','total_programado','total_realizado']].rename(columns={
                        'setor': 'Setor',
                        'total_programado': 'Total Programado',
                        'total_realizado': 'Total Realizado',
                    })
                st.dataframe(df_week, use_container_width=True, hide_index=True)
            else:
                st.info('Sem dados semanais.')
        except Exception as e:
            st.error(f'Erro ao carregar War Room Semanal: {e}')

    except Exception as e:
        st.error(f"Erro ao gerar tabela: {e}")

# --- ABA 5: PLANEJADOR (RESTAURADA) ---
with tab_planejador:
    st.subheader("📅 Planejador de Obra")
    st.info("Simule uma nova obra usando a estrutura de datas de uma obra existente OU a média geral.")

    col_plan1, col_plan2, col_plan3 = st.columns([1, 1, 1])
    opcoes_referencia = ["Média Geral (Todas as Obras)"] + sorted(todas_obras_lista)
    with col_plan1: obra_referencia = st.selectbox("Base de Referência:", options=opcoes_referencia)
    with col_plan2: data_inicio_simulacao = st.date_input("Início da Simulação:", value=datetime.date.today())
    with col_plan3:
        try:
            familias_unicas = carregar_dados_familias()['Familia'].unique().tolist()
            df_input_familias = pd.DataFrame({'Familia': familias_unicas, 'Quantidade': 0, 'Volume': 0.0})
        except:
            df_input_familias = pd.DataFrame(columns=['Familia', 'Quantidade', 'Volume'])

    st.markdown("---")
    st.write("**Defina os totais da nova obra:**")
    df_familias_input = st.data_editor(
        df_input_familias, hide_index=True, use_container_width=True,
        column_config={
            "Familia": st.column_config.TextColumn("Família", disabled=True),
            "Quantidade": st.column_config.NumberColumn("Qtd (Pçs)", min_value=0, step=1),
            "Volume": st.column_config.NumberColumn("Volume (m³)", min_value=0.0, format="%.2f")
        }
    )
    
    total_qtd_input = df_familias_input['Quantidade'].sum()
    total_vol_input = df_familias_input['Volume'].sum()
    st.metric("Volume Total Planejado", f"{total_vol_input:.2f} m³")

    st.markdown("---")
    
    if st.button("Gerar Projeção de Cronograma", type="primary"):
        try:
            datas = None
            if obra_referencia == "Média Geral (Todas as Obras)":
                df_medias = calcular_medias_cronograma()
                if not df_medias.empty:
                    media = df_medias.iloc[0]
                    ini_p = pd.to_datetime(data_inicio_simulacao)
                    # Exemplo simples de projeção linear baseado nas médias
                    fim_p = ini_p + pd.Timedelta(days=media['dias_duracao_proj'])
                    ini_f = ini_p + pd.Timedelta(days=media['dias_lag_fab'])
                    fim_f = ini_f + pd.Timedelta(days=media['dias_duracao_fab'])
                    ini_m = ini_p + pd.Timedelta(days=media['dias_lag_mont'])
                    fim_m = ini_m + pd.Timedelta(days=media['dias_duracao_mont'])
                    datas = {'ini_proj': ini_p, 'fim_proj': fim_p, 'ini_fab': ini_f, 'fim_fab': fim_f, 'ini_mont': ini_m, 'fim_mont': fim_m}
                else: st.error("Dados insuficientes.")
            else:
                df_datas = carregar_datas_limite_etapas(obra_referencia)
                if not df_datas.empty and not df_datas.iloc[0].isnull().all():
                    raw = df_datas.iloc[0]
                    # Calcula durações da obra referência e aplica na nova data de início
                    dur_p = (raw['fim_proj'] - raw['ini_proj']).days
                    lag_f = (raw['ini_fab'] - raw['ini_proj']).days
                    dur_f = (raw['fim_fab'] - raw['ini_fab']).days
                    lag_m = (raw['ini_mont'] - raw['ini_proj']).days
                    dur_m = (raw['fim_mont'] - raw['ini_mont']).days
                    
                    ini_p = pd.to_datetime(data_inicio_simulacao)
                    datas = {
                        'ini_proj': ini_p, 'fim_proj': ini_p + pd.Timedelta(days=dur_p),
                        'ini_fab': ini_p + pd.Timedelta(days=lag_f), 'fim_fab': (ini_p + pd.Timedelta(days=lag_f)) + pd.Timedelta(days=dur_f),
                        'ini_mont': ini_p + pd.Timedelta(days=lag_m), 'fim_mont': (ini_p + pd.Timedelta(days=lag_m)) + pd.Timedelta(days=dur_m)
                    }
            
            if datas:
                def gerar_semanas(inicio, fim):
                    if pd.isna(inicio) or pd.isna(fim): return []
                    start = pd.to_datetime(inicio) - pd.Timedelta(days=pd.to_datetime(inicio).weekday())
                    end = pd.to_datetime(fim)
                    weeks = []
                    while start <= end:
                        weeks.append(start)
                        start += pd.Timedelta(days=7)
                    return weeks

                semanas_proj = gerar_semanas(datas['ini_proj'], datas['fim_proj'])
                semanas_fab = gerar_semanas(datas['ini_fab'], datas['fim_fab'])
                semanas_mont = gerar_semanas(datas['ini_mont'], datas['fim_mont'])
                
                todas_semanas = sorted(list(set(semanas_proj + semanas_fab + semanas_mont)))
                df_plan = pd.DataFrame({'Semana': todas_semanas})
                df_plan['Semana Display'] = df_plan['Semana'].apply(formatar_semana)
                
                # Distribuição Linear Simples (Volume Total / Numero de Semanas)
                vp = total_vol_input / len(semanas_proj) if semanas_proj else 0
                vf = total_vol_input / len(semanas_fab) if semanas_fab else 0
                vm = total_vol_input / len(semanas_mont) if semanas_mont else 0
                
                qp = total_qtd_input / len(semanas_proj) if semanas_proj else 0
                qf = total_qtd_input / len(semanas_fab) if semanas_fab else 0
                qm = total_qtd_input / len(semanas_mont) if semanas_mont else 0
                
                df_plan['Projeto (Vol)'] = df_plan['Semana'].apply(lambda x: vp if x in semanas_proj else 0).cumsum()
                df_plan['Fabricação (Vol)'] = df_plan['Semana'].apply(lambda x: vf if x in semanas_fab else 0).cumsum()
                df_plan['Montagem (Vol)'] = df_plan['Semana'].apply(lambda x: vm if x in semanas_mont else 0).cumsum()
                
                st.subheader("Simulação de Avanço Acumulado")
                st.dataframe(df_plan, use_container_width=True, hide_index=True)
            else:
                st.warning("Não foi possível gerar cronograma.")
        except Exception as e:
            st.error(f"Erro: {e}")

# --- ABA 6: WAR ROOM ---
with tab_warroom:
    st.subheader("🏗️ War Room Produção")
    st.caption(f"Data: {datetime.date.today().strftime('%d/%m/%Y')} | Fonte: API War Room")

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("🔄 Atualizar agora", key="btn_war_room_refresh"):
            carregar_war_room.clear()
            st.experimental_rerun()
    with c2:
        st.write("")

    try:
        data_wr = carregar_war_room()
        now = datetime.datetime.now()
        current_hour = now.hour

        def fmt(val, qtd, unit):
            if (val == 0 and qtd == 0) or val is None:
                return "-"
            n = f"{val:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
            if unit == "kg": return f"{n} kg"
            if unit == "carga": return f"{qtd} cgs"
            if unit == "kg_pc": return f"{n} kg | {qtd} pç"
            if unit == "vol_pc": return f"{n} m³ | {qtd} pç"
            return f"{n}"

        def get_active_column():
            if current_hour < 8: return "Hoje 8h"
            if 8 <= current_hour < 13: return "Hoje 13h"
            return "Hoje 18h"

        active_col = get_active_column()

        rows = []
        meta_batida = []
        for r in data_wr:
            rows.append({
                "Setor": r.get("setor", ""),
                "Hoje Prog": fmt(r.get("progHoje", 0), r.get("qProgHoje", 0), r.get("unidade", "")),
                "Hoje 8h": fmt(r.get("realHoje8h", 0), r.get("qReal8h", 0), r.get("unidade", "")),
                "Hoje 13h": fmt(r.get("realHoje13h", 0), r.get("qReal13h", 0), r.get("unidade", "")),
                "Hoje 18h": fmt(r.get("realHoje18h", 0), r.get("qReal18h", 0), r.get("unidade", "")),
                "Ontem Prog": fmt(r.get("progOntem", 0), r.get("qProgOntem", 0), r.get("unidade", "")),
                "Ontem Real": fmt(r.get("realOntem", 0), r.get("qRealOntem", 0), r.get("unidade", "")),
                "Amanhã Prog": fmt(r.get("progAmanha", 0), r.get("qProgAmanha", 0), r.get("unidade", "")),
            })
            meta_batida.append((r.get("realOntem", 0) or 0) >= (r.get("progOntem", 0) or 0))

        df_wr = pd.DataFrame(rows)

        def style_row(row):
            styles = [""] * len(df_wr.columns)
            # Coluna ativa (hora)
            if active_col in df_wr.columns:
                idx = df_wr.columns.get_loc(active_col)
                styles[idx] = "background-color:#fee2e2; color:#b91c1c; font-weight:700"
            # Ontem Real (meta batida)
            idx_ontem = df_wr.columns.get_loc("Ontem Real")
            styles[idx_ontem] = "color:#16a34a; font-weight:700" if meta_batida[row.name] else "color:#dc2626; font-weight:700"
            return styles

        st.dataframe(df_wr.style.apply(style_row, axis=1), use_container_width=True, hide_index=True)
        st.caption(f"Atualizado em: {now.strftime('%H:%M:%S')}")
    except Exception as e:
        st.error(f"Erro ao carregar War Room: {e}")
