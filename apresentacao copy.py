import streamlit as st
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import altair as alt
import datetime 

# ========================================================
#          CONFIGURAÇÕES DO BANCO DE DADOS (MODIFICADO)
# ========================================================
# O st.secrets vai ler as configurações do ambiente seguro do Streamlit
DB_CONFIG = {
    "host": st.secrets["db_host"],
    "user": st.secrets["db_user"],
    "password": st.secrets["db_password"],
    "database": st.secrets["db_name"],
    "port": 3306
}

# Ajuste da URL para o SQLAlchemy usar os segredos também
DB_URL = f"mysql+mysqlconnector://{st.secrets['db_user']}:{st.secrets['db_password']}@{st.secrets['db_host']}:3306/{st.secrets['db_name']}"

# ========================================================
#     FUNÇÃO PARA LER DADOS (POR SEMANA) - MODIFICADA
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

    # --- LÓGICA DE UNIFICAÇÃO (MALL SILVIO SILVEIRA) ---
    # Renomeia "LOJAS" para "POA"
    df.loc[df['Obra'] == 'MALL SILVIO SILVEIRA - LOJAS', 'Obra'] = 'MALL SILVIO SILVEIRA - POA'
    
    # Agrupa novamente por Obra e Semana para somar os volumes das duas que agora têm o mesmo nome
    df = df.groupby(['Obra', 'Semana'], as_index=False)[['Volume_Projetado', 'Volume_Fabricado', 'Volume_Montado']].sum()
    # ---------------------------------------------------

    return df

# ========================================================
# FUNÇÃO PARA LER DADOS (TOTAIS POR OBRA) - MODIFICADA
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

    # --- LÓGICA DE UNIFICAÇÃO (MALL SILVIO SILVEIRA) ---
    df_geral.loc[df_geral['Obra'] == 'MALL SILVIO SILVEIRA - LOJAS', 'Obra'] = 'MALL SILVIO SILVEIRA - POA'
    
    # Agrupa somando os volumes e fazendo a média da taxa de aço
    df_geral = df_geral.groupby('Obra', as_index=False).agg({
        'Projetado': 'sum',
        'Fabricado': 'sum',
        'Acabado': 'sum',
        'Expedido': 'sum',
        'Montado': 'sum',
        'Taxa de Aço': 'mean' # Média das taxas das duas obras
    })
    # ---------------------------------------------------

    return df_geral

# ========================================================
# FUNÇÃO PARA LER DADOS (POR FAMÍLIA) - MODIFICADA
# ========================================================
@st.cache_data(ttl=300)
def carregar_dados_familias():
    conn = mysql.connector.connect(**DB_CONFIG)
    query = """
        SELECT
            nomeObra AS Obra,
            familia AS Familia,
            COUNT(nomePeca) AS unidade,
            SUM(volumeReal) AS Volume
        FROM `plannix-db`.`plannix`
        WHERE 
            familia IS NOT NULL 
            AND nomePeca IS NOT NULL 
            AND volumeReal IS NOT NULL
        GROUP BY 
            nomeObra, 
            familia
        ORDER BY 
            Obra, 
            Familia;
    """
    df_familias = pd.read_sql(query, conn)
    conn.close()

    # --- LÓGICA DE UNIFICAÇÃO (MALL SILVIO SILVEIRA) ---
    df_familias.loc[df_familias['Obra'] == 'MALL SILVIO SILVEIRA - LOJAS', 'Obra'] = 'MALL SILVIO SILVEIRA - POA'
    
    # Agrupa por Obra e Família, somando quantidades e volumes
    df_familias = df_familias.groupby(['Obra', 'Familia'], as_index=False).sum()
    # ---------------------------------------------------

    return df_familias

# ========================================================
# FUNÇÃO PARA BUSCAR DATAS LIMITE DA OBRA (ESPECÍFICA)
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

# ========================================================
# FUNÇÃO PARA CALCULAR MÉDIAS GERAIS DE DURAÇÃO
# ========================================================
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
    except Exception as e:
        st.info("Nota: Tabela 'orcamentos_usuario' não encontrada.")
    
    try:
        df_previsoes_salvas = pd.read_sql("SELECT * FROM previsoes_usuario", con=engine)
        if not df_previsoes_salvas.empty:
            df_previsoes_salvas['Semana'] = pd.to_datetime(df_previsoes_salvas['Semana'])
    except Exception as e:
        st.info("Nota: Tabela 'previsoes_usuario' não encontrada.")

    engine.dispose()
    return df_orcamentos_salvos, df_previsoes_salvas

# ========================================================
# FUNÇÃO HELPER PARA FORMATAR A SEMANA (SEG-DOM)
# ========================================================
def formatar_semana(date):
    if pd.isna(date):
        return None
    if isinstance(date, str):
        try:
            date = pd.to_datetime(date)
        except:
            return date
    start_date = date 
    end_date = start_date + pd.Timedelta(days=6)
    start_str = start_date.strftime('%d/%m')
    end_str = end_date.strftime('%d/%m')
    year_str = start_date.strftime('%Y')
    return f"{start_str} á {end_str} ({year_str})"

# ========================================================
# FUNÇÃO PARA SALVAR DADOS NO MYSQL
# ========================================================
def salvar_dados_usuario(df_previsoes, df_orcamentos):
    engine = create_engine(DB_URL)
    try:
        df_previsoes_limpo = df_previsoes.dropna(subset=['Obra', 'Semana'])
        df_save_previsoes = df_previsoes_limpo[[
            "Obra", "Semana", "Projeto Previsto %", 
            "Fabricação Prevista %", "Montagem Prevista %"
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

# --- 2. PROCESSAMENTO DE DATAS ---
df_base['Semana'] = pd.to_datetime(df_base['Semana'])

# --- 2.5 INICIALIZAÇÃO DO SESSION STATE ---
todas_obras_lista = df_base["Obra"].unique().tolist()

if 'orcamentos' not in st.session_state:
    df_orcamentos_base = pd.DataFrame({"Obra": todas_obras_lista})
    
    # Faz o merge com o que veio do banco de dados
    df_orcamentos_para_editor = df_orcamentos_base.merge(
        df_orcamentos_salvos, on="Obra", how="left"
    )
    
    # --- PADRONIZAÇÃO DE VALORES PADRÃO (EXISTENTES E NOVOS) ---
    defaults = {
        'Orcamento': 100.0,
        'Orcamento Lajes': 0.0,
        'Prazo Projeto': 0,
        'Prazo Fabricacao': 0,
        'Prazo Montagem': 0
    }
    
    for col, val in defaults.items():
        if col not in df_orcamentos_para_editor.columns:
            df_orcamentos_para_editor[col] = val
        else:
            df_orcamentos_para_editor[col] = df_orcamentos_para_editor[col].fillna(val)

    # Tratamento especial para data (Data Inicio)
    if 'Data Inicio' not in df_orcamentos_para_editor.columns:
        df_orcamentos_para_editor['Data Inicio'] = None
    else:
        # Garante formato datetime
        df_orcamentos_para_editor['Data Inicio'] = pd.to_datetime(df_orcamentos_para_editor['Data Inicio'])

    st.session_state['orcamentos'] = df_orcamentos_para_editor

# --- 3. FILTRO GLOBAL (FORA DAS ABAS) ---
st.subheader("⚙️ Filtros Globais")

min_date = df_base['Semana'].min()
max_date = df_base['Semana'].max()
filtro_data_inicio_default = min_date - pd.Timedelta(weeks=10)

col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    obras_selecionadas = st.multiselect(
        "Selecione as Obras:",
        options=todas_obras_lista,
        default=todas_obras_lista
    )
with col2:
    data_inicio = st.date_input("Data de Início:", 
                                value=filtro_data_inicio_default,
                                min_value=None, max_value=None)
with col3:
    data_fim = st.date_input("Data Final:", 
                             value=max_date, 
                             min_value=None, max_value=None)

data_inicio = pd.to_datetime(data_inicio)
data_fim = pd.to_datetime(data_fim)
    
# --- 4. PREPARAÇÃO DOS DADOS (PREENCHIMENTO DE LACUNAS + CUMSUM) ---
df_para_cumsum = df_base[
    (df_base["Obra"].isin(obras_selecionadas))
].copy()

# 1. Adicionar as 10 semanas ANTES e DEPOIS (sua lógica anterior + a nova)
zero_rows = []
weeks_to_add = 10 

for obra in obras_selecionadas:
    obra_df = df_para_cumsum[df_para_cumsum['Obra'] == obra]
    if not obra_df.empty:
        # 10 Semanas ANTES
        min_obra_date = obra_df['Semana'].min()
        current_date = min_obra_date
        for _ in range(weeks_to_add):
            current_date = current_date - pd.Timedelta(days=7)
            zero_rows.append({
                'Obra': obra,
                'Semana': current_date,
                'Volume_Projetado': 0, 'Volume_Fabricado': 0, 'Volume_Montado': 0
            })
            
        # 10 Semanas DEPOIS
        max_obra_date = obra_df['Semana'].max()
        current_date_future = max_obra_date
        for _ in range(weeks_to_add):
            current_date_future = current_date_future + pd.Timedelta(days=7)
            zero_rows.append({
                'Obra': obra,
                'Semana': current_date_future,
                'Volume_Projetado': 0, 'Volume_Fabricado': 0, 'Volume_Montado': 0
            })

if zero_rows:
    df_zero = pd.DataFrame(zero_rows)
    df_para_cumsum = pd.concat([df_zero, df_para_cumsum], ignore_index=True)

# 2. PREENCHER LACUNAS NO MEIO (Reindexar datas)
# Isso garante que se houver um buraco entre a semana 1 e 3, a semana 2 é criada.
dfs_preenchidos = []

if not df_para_cumsum.empty:
    for obra, dados in df_para_cumsum.groupby("Obra"):
        # Remove duplicatas de data se houver (segurança) e define index
        dados = dados.groupby("Semana").sum(numeric_only=True).reset_index()
        dados = dados.set_index("Semana").sort_index()
        
        # Cria um range completo de datas (semana a semana) do início ao fim desta obra
        idx_completo = pd.date_range(start=dados.index.min(), end=dados.index.max(), freq='7D')
        
        # Reindexa: cria as linhas vazias e preenche volumes com 0
        dados_reindex = dados.reindex(idx_completo)
        dados_reindex['Obra'] = obra
        dados_reindex[['Volume_Projetado', 'Volume_Fabricado', 'Volume_Montado']] = \
            dados_reindex[['Volume_Projetado', 'Volume_Fabricado', 'Volume_Montado']].fillna(0)
        
        dfs_preenchidos.append(dados_reindex)
    
    # Recria o df principal com as lacunas preenchidas
    df_para_cumsum = pd.concat(dfs_preenchidos).reset_index().rename(columns={'index': 'Semana'})

# 3. CÁLCULO DO ACUMULADO (CUMSUM)
# Agora que temos 0 nas semanas vazias, o cumsum vai repetir o valor da semana anterior
df_para_cumsum = df_para_cumsum.sort_values(["Obra", "Semana"]) 
df_para_cumsum["Volume_Projetado"] = df_para_cumsum.groupby("Obra")["Volume_Projetado"].cumsum()
df_para_cumsum["Volume_Fabricado"] = df_para_cumsum.groupby("Obra")["Volume_Fabricado"].cumsum()
df_para_cumsum["Volume_Montado"] = df_para_cumsum.groupby("Obra")["Volume_Montado"].cumsum()

# Aplica o filtro de visualização (Datas globais selecionadas)
df = df_para_cumsum[
    (df_para_cumsum["Semana"] >= data_inicio) & 
    (df_para_cumsum["Semana"] <= data_fim)
].copy()

if df.empty and not obras_selecionadas:
    st.warning("Nenhuma obra encontrada.")
    st.stop()

df['Semana_Display'] = df['Semana'].apply(formatar_semana)
    
# --- 5. DEFINIÇÃO DAS ABAS ---
tab_cadastro, tab_tabelas, tab_graficos, tab_geral, tab_planejador = st.tabs([
    "📁 Cadastro", 
    "📊 Tabelas", 
    "📈 Gráficos",
    "🌍 Tabela Geral",
    "📅 Planejador (Em Desenvolvimento)"
])

# --- ABA 1: CADASTRO ---
with tab_cadastro:
    st.subheader("💰 1. Orçamento e Prazos da Obra")
    st.info("Cadastre o orçamento e a Data de Início + Duração (em dias) de cada etapa.")
    
    orcamentos_filtrado = st.session_state['orcamentos'][st.session_state['orcamentos']['Obra'].isin(obras_selecionadas)]
    
    df_orcamentos_editado = st.data_editor(
        orcamentos_filtrado, 
        key="orcamento_editor", 
        hide_index=True, 
        width="stretch", 
        disabled=["Obra"], 
        column_config={
            "Orcamento": st.column_config.NumberColumn("Orçamento (Volume)", min_value=0.01, format="%.2f"),
            "Orcamento Lajes": st.column_config.NumberColumn("Orcamento Lajes", min_value=0.00, format="%.2f"),
            # --- NOVAS COLUNAS ---
            "Data Inicio": st.column_config.DateColumn("Data Início", format="DD/MM/YYYY"),
            "Prazo Projeto": st.column_config.NumberColumn("Dias Projeto", min_value=0, step=1, help="Duração em dias corridos"),
            "Prazo Fabricacao": st.column_config.NumberColumn("Dias Fabricação", min_value=0, step=1, help="Duração em dias corridos"),
            "Prazo Montagem": st.column_config.NumberColumn("Dias Montagem", min_value=0, step=1, help="Duração em dias corridos"),
        }
    )
    st.session_state['orcamentos'].update(df_orcamentos_editado)


# --- 6. MERGE E PREPARAÇÃO ---
df_orcamentos_atual = st.session_state['orcamentos']
df = df.merge(df_orcamentos_atual, on="Obra", how="left")
df["Projetado %"] = (df["Volume_Projetado"] / df["Orcamento"]) * 100
df["Fabricado %"] = (df["Volume_Fabricado"] / df["Orcamento"]) * 100
df["Montado %"] = (df["Volume_Montado"] / df["Orcamento"]) * 100

if not df_previsoes_salvas.empty:
    df = df.merge(df_previsoes_salvas, on=["Obra", "Semana"], how="left")

df["Projeto Previsto %"] = df["Projeto Previsto %"].fillna(0.0)
df["Fabricação Prevista %"] = df["Fabricação Prevista %"].fillna(0.0)
df["Montagem Prevista %"] = df["Montagem Prevista %"].fillna(0.0)
df_para_edicao = df.copy() 

# --- ABA 2: TABELAS ---
with tab_tabelas:
    st.subheader("Controles de Visualização")
    col_vis1, col_vis2 = st.columns(2)
    with col_vis1: show_editor = st.checkbox("Mostrar Tabela de Entrada de Previsões", value=True)
    with col_vis2: show_result_table = st.checkbox("Mostrar Tabela de Resultado Completa", value=True)
    
    df_editado = df_para_edicao 
    if show_editor:
        st.markdown("---")
        st.subheader("✏️ 2. Edite as Porcentagens de Previsão")
        st.info("As 10 semanas anteriores ao início da obra foram adicionadas automaticamente.")
        colunas_desabilitadas = ["Obra", "Semana", "Semana_Display", "Volume_Projetado", "Projetado %", "Volume_Fabricado", "Fabricado %", "Volume_Montado", "Montado %", "Orcamento", "Orcamento Lajes"]
        df_editado = st.data_editor(
            df_para_edicao, key="dados_editor", width="stretch", hide_index=True, disabled=colunas_desabilitadas,
            column_config={
                "Semana_Display": "Semana", 
                "Projetado %": st.column_config.NumberColumn(format="%.0f%%"),
                "Fabricado %": st.column_config.NumberColumn(format="%.0f%%"),
                "Montado %": st.column_config.NumberColumn(format="%.0f%%"),
                "Projeto Previsto %": st.column_config.NumberColumn(format="%.0f%%", min_value=0, max_value=200),
                "Fabricação Prevista %": st.column_config.NumberColumn(format="%.0f%%", min_value=0, max_value=200),
                "Montagem Prevista %": st.column_config.NumberColumn(format="%.0f%%", min_value=0, max_value=200),
                "Volume_Projetado": None, "Volume_Fabricado": None, "Volume_Montado": None, "Orcamento": None, "Orcamento Lajes": None, "Semana": None
            }
        )
        st.markdown("---")

    # ==============================================================================
    #  CORREÇÃO: LÓGICA DE CORTE APÓS 100%
    # ==============================================================================
    import numpy as np 
    
    df_calculado = df_editado.copy().sort_values(['Obra', 'Semana'])
    cols_previstas = ["Projeto Previsto %", "Fabricação Prevista %", "Montagem Prevista %"]
    
    for col in cols_previstas:
        # 1. Transforma 0.0 em NaN para permitir o preenchimento (ffill)
        df_calculado[col] = df_calculado[col].replace(0.0, np.nan)
        
        # 2. Preenche buracos com o valor anterior (para evitar quedas no meio do gráfico)
        df_calculado[col] = df_calculado.groupby('Obra')[col].ffill()
        
        # 3. Preenche APENAS os NaNs iniciais (antes do projeto começar) com 0
        #    (Se não fizermos isso agora, o passo 4 pode falhar ou deixar buracos no início)
        df_calculado[col] = df_calculado[col].fillna(0.0)

        # 4. LÓGICA DE CORTE: Se a semana ANTERIOR já era >= 100%, a atual vira NaN.
        #    Isso faz o gráfico parar de desenhar a linha.
        prev_vals = df_calculado.groupby('Obra')[col].shift(1)
        mask_concluido = prev_vals >= 100.0
        
        # Aplica NaN onde o projeto já estava concluído na semana anterior
        df_calculado.loc[mask_concluido, col] = np.nan

    # ==============================================================================
    #  FIM DA CORREÇÃO
    # ==============================================================================

    df_calculado["Volume Projetado Previsto"] = (df_calculado["Orcamento"] * (df_calculado["Projeto Previsto %"] / 100))
    df_calculado["Volume Fabricado Previsto"] = (df_calculado["Orcamento"] * (df_calculado["Fabricação Prevista %"] / 100))
    df_calculado["Volume Montado Previsto"] = (df_calculado["Orcamento"] * (df_calculado["Montagem Prevista %"] / 100))

    colunas_para_exibir = ["Obra", "Semana_Display", "Volume_Projetado", "Projetado %", "Projeto Previsto %", "Volume Projetado Previsto", "Volume_Fabricado", "Fabricado %", "Fabricação Prevista %", "Volume Fabricado Previsto", "Volume_Montado", "Montado %", "Montagem Prevista %", "Volume Montado Previsto", "Orcamento", "Orcamento Lajes"]
    cols_existentes = [c for c in colunas_para_exibir if c in df_calculado.columns]
    df_final_display = df_calculado[cols_existentes]

    st.markdown("---")
    if st.button("💾 Salvar Alterações no Banco de Dados", type="primary"):
        # Salva o df_editado (input original do usuário), sem as manipulações visuais
        salvar_dados_usuario(df_editado, st.session_state['orcamentos'])

    if show_result_table:
        st.subheader("✅ 3. Tabela de Resultado Completa")
        st.dataframe(df_final_display, width="stretch", column_config={"Semana_Display": "Semana", "Projetado %": st.column_config.NumberColumn(format="%.0f%%"), "Fabricado %": st.column_config.NumberColumn(format="%.0f%%"), "Montado %": st.column_config.NumberColumn(format="%.0f%%"), "Orcamento": st.column_config.NumberColumn(format="%.2f"), "Orcamento Lajes": st.column_config.NumberColumn(format="%.2f"), "Volume Projetado Previsto": st.column_config.NumberColumn(format="%.2f"), "Volume Fabricado Previsto": st.column_config.NumberColumn(format="%.2f"), "Volume Montado Previsto": st.column_config.NumberColumn(format="%.2f")})
# --- ABA 3: GRÁFICOS ---
with tab_graficos:
    st.subheader("📈 4. Gráfico de Tendências de Porcentagens")
    id_vars = ["Obra", "Semana", "Semana_Display"]
    value_vars = ["Projetado %", "Projeto Previsto %", "Fabricado %", "Fabricação Prevista %", "Montado %", "Montagem Prevista %"]
    
    if not df_calculado.empty:
        df_grafico = df_calculado.copy() 
        for col in value_vars:
            if col in df_grafico.columns: df_grafico[col] = pd.to_numeric(df_grafico[col], errors='coerce')
        value_vars_existentes = [c for c in value_vars if c in df_grafico.columns]
        df_chart = df_grafico.melt(id_vars=id_vars, value_vars=value_vars_existentes, var_name="Tipo_Metrica", value_name="Porcentagem")
        chart = alt.Chart(df_chart).mark_line(point=True).encode(
            x=alt.X('Semana_Display:N', title='Semana', sort=alt.SortField(field="Semana", order='ascending')),
            y=alt.Y('Porcentagem:Q', title='Porcentagem Acumulada (%)'), 
            color=alt.Color('Tipo_Metrica:N', title="Métrica"),
            strokeDash=alt.StrokeDash('Obra:N', title="Obra"),
            tooltip=['Obra', 'Semana_Display', 'Tipo_Metrica', alt.Tooltip('Porcentagem', format='.1f')]
        ).properties(title="Tendências de Metas vs. Realizado").interactive()
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Gráfico não disponível.")

# --- ABA 4: TABELA GERAL ---
with tab_geral:
    st.subheader("🏗️ Resumo Geral e Prazos")
    try:
        df_geral = carregar_dados_gerais()
        
        # 1. PEGAR ORÇAMENTOS E GARANTIR QUE NÃO HAJA DUPLICATAS
        # Isso evita o erro de criar listas dentro das células
        df_orcamentos_clean = st.session_state['orcamentos'].drop_duplicates(subset=['Obra'], keep='first')
        
        # Merge com os dados de orçamento e prazos
        df_geral = df_geral.merge(df_orcamentos_clean, on="Obra", how="left")

        # 2. CÁLCULO DE PORCENTAGENS
        cols_calc = ["Projetado", "Fabricado", "Acabado", "Expedido", "Montado"]
        for col in cols_calc:
            df_geral[col] = df_geral[col].fillna(0)
            df_geral[f"{col} %"] = (df_geral[col] / df_geral["Orcamento"]) * 100

        # 3. CORREÇÃO ROBUSTA DE DATA (RESOLVE O ERRO <class 'list'>)
        # Força conversão para string, limpa caracteres de lista e converte para data
        df_geral['Data Inicio'] = df_geral['Data Inicio'].astype(str).str.replace(r'[\[\]\']', '', regex=True)
        df_geral['Data Inicio'] = pd.to_datetime(df_geral['Data Inicio'], errors='coerce')

        # 4. CÁLCULO DE DIAS RESTANTES
        hoje = pd.to_datetime(datetime.date.today())

        def calcular_restante(row, coluna_prazo):
            # Se não tiver data ou prazo, retorna vazio
            if pd.isna(row['Data Inicio']) or pd.isna(row[coluna_prazo]) or row[coluna_prazo] == 0:
                return None
            try:
                # Data estimada de fim = Inicio + Prazo em dias
                data_fim = row['Data Inicio'] + pd.Timedelta(days=int(row[coluna_prazo]))
                return (data_fim - hoje).days
            except:
                return None

        df_geral['Restante Proj'] = df_geral.apply(lambda row: calcular_restante(row, 'Prazo Projeto'), axis=1)
        df_geral['Restante Fab'] = df_geral.apply(lambda row: calcular_restante(row, 'Prazo Fabricacao'), axis=1)
        df_geral['Restante Mont'] = df_geral.apply(lambda row: calcular_restante(row, 'Prazo Montagem'), axis=1)

        # 5. ORGANIZAÇÃO DAS COLUNAS (LADO A LADO)
        # Aqui definimos a ordem exata de exibição
        colunas_ordenadas = [
            "Obra", 
            "Orcamento", 
            "Data Inicio",
            # Setor Projeto
            "Projetado %", "Restante Proj",
            # Setor Fabricação
            "Fabricado %", "Restante Fab",
            # Setor Montagem
            "Montado %", "Restante Mont",
            # Outros
            "Taxa de Aço"
        ]
        
        # Filtra apenas as que existem no dataframe para evitar erro de coluna inexistente
        cols_final = [c for c in colunas_ordenadas if c in df_geral.columns]

        st.dataframe(
            df_geral[cols_final], 
            width="stretch", 
            hide_index=True, 
            column_config={
                "Orcamento": st.column_config.NumberColumn(format="%.2f"),
                "Data Inicio": st.column_config.DateColumn("Início", format="DD/MM/YYYY"),
                
                # Configuração Visual: Barra de Progresso + Número de Dias
                "Projetado %": st.column_config.ProgressColumn("Proj. %", format="%.0f%%", min_value=0, max_value=100),
                "Restante Proj": st.column_config.NumberColumn("Faltam (Dias)", format="%d d"),
                
                "Fabricado %": st.column_config.ProgressColumn("Fab. %", format="%.0f%%", min_value=0, max_value=100),
                "Restante Fab": st.column_config.NumberColumn("Faltam (Dias)", format="%d d"),
                
                "Montado %": st.column_config.ProgressColumn("Mont. %", format="%.0f%%", min_value=0, max_value=100),
                "Restante Mont": st.column_config.NumberColumn("Faltam (Dias)", format="%d d"),
                
                "Taxa de Aço": st.column_config.NumberColumn("Aço (kg/m³)", format="%.2f")
            }
        )
    except Exception as e:
        st.error(f"Erro ao gerar tabela geral: {e}")
       

# --- ABA 5: PLANEJADOR ---
with tab_planejador:
    st.subheader("📅 Planejador de Obra")
    st.info("Simule uma nova obra usando a estrutura de datas de uma obra existente OU a média geral de todas as obras.")

    col_plan1, col_plan2, col_plan3 = st.columns([1, 1, 1])
    
    opcoes_referencia = ["Média Geral (Todas as Obras)"] + sorted(todas_obras_lista)
    
    with col_plan1:
        obra_referencia = st.selectbox("Base de Referência (Cronograma):", options=opcoes_referencia)
    
    with col_plan2:
        data_inicio_simulacao = st.date_input("Data de Início do Projeto (Simulação):", value=datetime.date.today())

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
            "Quantidade": st.column_config.NumberColumn("Quantidade (Pçs)", min_value=0, step=1),
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
                    fim_p = ini_p + pd.Timedelta(days=media['dias_duracao_proj'])
                    ini_f = ini_p + pd.Timedelta(days=media['dias_lag_fab'])
                    fim_f = ini_f + pd.Timedelta(days=media['dias_duracao_fab'])
                    ini_m = ini_p + pd.Timedelta(days=media['dias_lag_mont'])
                    fim_m = ini_m + pd.Timedelta(days=media['dias_duracao_mont'])
                    datas = {'ini_proj': ini_p, 'fim_proj': fim_p, 'ini_fab': ini_f, 'fim_fab': fim_f, 'ini_mont': ini_m, 'fim_mont': fim_m}
                else: st.error("Dados insuficientes para média.")
            else:
                df_datas = carregar_datas_limite_etapas(obra_referencia)
                if not df_datas.empty and not df_datas.iloc[0].isnull().all():
                    raw_datas = df_datas.iloc[0]
                    dur_p = (raw_datas['fim_proj'] - raw_datas['ini_proj']).days
                    lag_f = (raw_datas['ini_fab'] - raw_datas['ini_proj']).days
                    dur_f = (raw_datas['fim_fab'] - raw_datas['ini_fab']).days
                    lag_m = (raw_datas['ini_mont'] - raw_datas['ini_proj']).days
                    dur_m = (raw_datas['fim_mont'] - raw_datas['ini_mont']).days
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
                df_planejamento = pd.DataFrame({'Semana': todas_semanas})
                df_planejamento['Semana Display'] = df_planejamento['Semana'].apply(formatar_semana)
                
                vol_por_sem_proj = total_vol_input / len(semanas_proj) if semanas_proj else 0
                vol_por_sem_fab = total_vol_input / len(semanas_fab) if semanas_fab else 0
                vol_por_sem_mont = total_vol_input / len(semanas_mont) if semanas_mont else 0
                
                qtd_por_sem_proj = total_qtd_input / len(semanas_proj) if semanas_proj else 0
                qtd_por_sem_fab = total_qtd_input / len(semanas_fab) if semanas_fab else 0
                qtd_por_sem_mont = total_qtd_input / len(semanas_mont) if semanas_mont else 0
                
                def get_val(semana, lista, valor): return valor if semana in lista else 0.0
                
                # Calcula os valores incrementais
                df_planejamento['Projeto (Vol)'] = df_planejamento['Semana'].apply(lambda x: get_val(x, semanas_proj, vol_por_sem_proj))
                df_planejamento['Projeto (Qtd)'] = df_planejamento['Semana'].apply(lambda x: get_val(x, semanas_proj, qtd_por_sem_proj))
                df_planejamento['Fabricação (Vol)'] = df_planejamento['Semana'].apply(lambda x: get_val(x, semanas_fab, vol_por_sem_fab))
                df_planejamento['Fabricação (Qtd)'] = df_planejamento['Semana'].apply(lambda x: get_val(x, semanas_fab, qtd_por_sem_fab))
                df_planejamento['Montagem (Vol)'] = df_planejamento['Semana'].apply(lambda x: get_val(x, semanas_mont, vol_por_sem_mont))
                df_planejamento['Montagem (Qtd)'] = df_planejamento['Semana'].apply(lambda x: get_val(x, semanas_mont, qtd_por_sem_mont))

                # (NOVO) Aplica CUMSUM para mostrar acumulado
                df_planejamento['Projeto (Vol)'] = df_planejamento['Projeto (Vol)'].cumsum()
                df_planejamento['Projeto (Qtd)'] = df_planejamento['Projeto (Qtd)'].cumsum()
                df_planejamento['Fabricação (Vol)'] = df_planejamento['Fabricação (Vol)'].cumsum()
                df_planejamento['Fabricação (Qtd)'] = df_planejamento['Fabricação (Qtd)'].cumsum()
                df_planejamento['Montagem (Vol)'] = df_planejamento['Montagem (Vol)'].cumsum()
                df_planejamento['Montagem (Qtd)'] = df_planejamento['Montagem (Qtd)'].cumsum()
                
                st.subheader("Matriz de Planejamento Semanal (Acumulado)")
                st.dataframe(
                    df_planejamento, hide_index=True, width="stretch",
                    column_config={
                        "Semana": None,
                        "Projeto (Vol)": st.column_config.NumberColumn(format="%.2f"),
                        "Fabricação (Vol)": st.column_config.NumberColumn(format="%.2f"),
                        "Montagem (Vol)": st.column_config.NumberColumn(format="%.2f"),
                        "Projeto (Qtd)": st.column_config.NumberColumn(format="%.0f"),
                        "Fabricação (Qtd)": st.column_config.NumberColumn(format="%.0f"),
                        "Montagem (Qtd)": st.column_config.NumberColumn(format="%.0f"),
                    }
                )
            else:
                st.warning("Não foi possível gerar o cronograma com os dados disponíveis.")
        except Exception as e:
            st.error(f"Erro ao gerar planejamento: {e}")
