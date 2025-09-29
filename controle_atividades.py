import streamlit as st
import pandas as pd
from datetime import datetime
import psycopg2
import plotly.express as px

# ==============================
# 1. Lendo credenciais do st.secrets
# ==============================
DB_PARAMS = {
    "host": st.secrets["postgresql"]["host"],
    "port": st.secrets["postgresql"]["port"],
    "database": st.secrets["postgresql"]["database"],
    "user": st.secrets["postgresql"]["user"],
    "password": st.secrets["postgresql"]["password"],
    "sslmode": st.secrets["postgresql"]["sslmode"],
}

# ==============================
# 2. Conexão com PostgreSQL
# ==============================
def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        st.error(f"❌ Erro ao conectar ao banco de dados: {e}")
        return None

# ==============================
# 3. Setup do Banco (criação de tabelas)
# ==============================
def setup_db():
    conn = get_db_connection()
    if conn is None: return

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS usuarios (
                    usuario VARCHAR(50) PRIMARY KEY,
                    senha VARCHAR(50) NOT NULL,
                    admin BOOLEAN DEFAULT FALSE
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS atividades (
                    id SERIAL PRIMARY KEY,
                    usuario VARCHAR(50) REFERENCES usuarios(usuario),
                    data DATE NOT NULL,
                    mes INTEGER NOT NULL,
                    ano INTEGER NOT NULL,
                    descricao VARCHAR(255) NOT NULL,
                    projeto VARCHAR(255) NOT NULL,
                    porcentagem INTEGER NOT NULL,
                    observacao TEXT
                );
            """)
            conn.commit()
    except Exception as e:
        st.error(f"Erro ao criar/verificar tabelas: {e}")
    finally:
        conn.close()

setup_db()

# ==============================
# 4. CRUD
# ==============================
def salvar_usuario(usuario, senha, admin=False):
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO usuarios (usuario, senha, admin)
                VALUES (%s, %s, %s)
                ON CONFLICT (usuario) DO NOTHING;
            """, (usuario, senha, admin))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar usuário: {e}")
        return False
    finally:
        conn.close()

def validar_login(usuario, senha):
    conn = get_db_connection()
    if conn is None: return False, False
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT senha, admin FROM usuarios WHERE usuario = %s;", (usuario,))
            result = cursor.fetchone()
            if result and result[0] == senha:
                return True, result[1]
            return False, False
    except Exception as e:
        st.error(f"Erro ao validar login: {e}")
        return False, False
    finally:
        conn.close()

def salvar_atividade(usuario, data, descricao, projeto, porcentagem, observacao):
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            mes, ano = data.month, data.year
            cursor.execute("""
                INSERT INTO atividades (usuario, data, mes, ano, descricao, projeto, porcentagem, observacao)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (usuario, data, mes, ano, descricao, projeto, porcentagem, observacao))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar atividade: {e}")
        return False
    finally:
        conn.close()

def deletar_atividade(id):
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM atividades WHERE id=%s;", (id,))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao deletar atividade: {e}")
        return False
    finally:
        conn.close()

def carregar_dados():
    conn = get_db_connection()
    if conn is None: 
        return pd.DataFrame(), pd.DataFrame()
    try:
        usuarios_df = pd.read_sql("SELECT usuario, admin FROM usuarios;", conn)
        atividades_df = pd.read_sql("""
            SELECT id, usuario, data, mes, ano, descricao, projeto, porcentagem, observacao 
            FROM atividades ORDER BY data DESC;
        """, conn)
        return usuarios_df, atividades_df
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame(), pd.DataFrame()
    finally:
        conn.close()

# ==============================
# 5. Interface Streamlit
# ==============================
# [Inclua aqui todos os DESCRICOES e PROJETOS como antes]
# Para manter o código enxuto aqui, estou assumindo que DESCRICOES e PROJETOS já estão definidos.

if "usuario" not in st.session_state:
    st.session_state["usuario"] = None
    st.session_state["admin"] = False

usuarios_df, atividades_df = carregar_dados()

if st.session_state["usuario"] is None:
    st.title("🔐 Login")
    usuario = st.text_input("Usuário")
    senha = st.text_input("Senha", type="password")
    if st.button("Entrar"):
        ok, admin = validar_login(usuario, senha)
        if ok:
            st.session_state["usuario"] = usuario
            st.session_state["admin"] = admin
            st.experimental_rerun()
        else:
            st.error("Usuário ou senha incorretos")
else:
    st.sidebar.markdown(f"**Usuário:** {st.session_state['usuario']}")
    if st.sidebar.button("Sair"):
        st.session_state["usuario"] = None
        st.session_state["admin"] = False
        st.experimental_rerun()

    abas = ["Lançar Atividade", "Minhas Atividades", "Validação"]
    if st.session_state["admin"]:
        abas += ["Gerenciar Usuários", "Consolidado"]

    aba = st.sidebar.radio("Menu", abas)

    if aba == "Gerenciar Usuários" and st.session_state["admin"]:
        st.header("👥 Gerenciar Usuários")
        with st.form("form_add_user"):
            novo_usuario = st.text_input("Usuário")
            nova_senha = st.text_input("Senha", type="password")
            admin_check = st.checkbox("Admin")
            if st.form_submit_button("Adicionar"):
                if salvar_usuario(novo_usuario, nova_senha, admin_check):
                    st.success("Usuário adicionado!")
                    st.experimental_rerun()
        st.dataframe(usuarios_df, use_container_width=True)

    elif aba == "Lançar Atividade":
        st.header("📝 Lançar Atividade")
        with st.form("form_atividade"):
            data = st.date_input("Data", datetime.today())
            descricao = st.selectbox("Descrição", DESCRICOES)
            projeto = st.selectbox("Projeto", PROJETOS)
            porcentagem = st.slider("Porcentagem", 0, 100, 100)
            observacao = st.text_area("Observação")
            if st.form_submit_button("Salvar"):
                if observacao.strip():
                    if salvar_atividade(st.session_state["usuario"], data, descricao, projeto, porcentagem, observacao):
                        st.success("Atividade salva!")
                        st.experimental_rerun()
                else:
                    st.error("A observação é obrigatória.")

    elif aba == "Minhas Atividades":
        st.header("📊 Minhas Atividades")
        minhas = atividades_df[atividades_df["usuario"] == st.session_state["usuario"]]
        if minhas.empty:
            st.info("Nenhuma atividade encontrada.")
        else:
            for idx, row in minhas.iterrows():
                st.markdown(f"**Data:** {row['data'].strftime('%Y-%m-%d')}, **Projeto:** {row['projeto']}, **Descrição:** {row['descricao']}, **%:** {row['porcentagem']}")
                st.write(f"Observação: {row['observacao']}")
                if st.button(f"Apagar {row['id']}", key=row['id']):
                    if deletar_atividade(row['id']):
                        st.success("Atividade deletada!")
                        st.experimental_rerun()

    elif aba == "Validação":
        st.header("📈 Validação por Mês")
        df_val = atividades_df.copy()
        if not st.session_state["admin"]:
            df_val = df_val[df_val["usuario"] == st.session_state["usuario"]]
        # Selecionar mês e ano
        anos = df_val["ano"].unique()
        anos.sort()
        ano_selec = st.selectbox("Ano", anos)
        meses = df_val[df_val["ano"] == ano_selec]["mes"].unique()
        meses.sort()
        mes_selec = st.selectbox("Mês", meses)
        df_val = df_val[(df_val["ano"]==ano_selec) & (df_val["mes"]==mes_selec)]
        if df_val.empty:
            st.info("Nenhuma atividade encontrada para este mês.")
        else:
            df_graf = df_val.groupby("descricao")["porcentagem"].sum().reset_index()
            fig = px.pie(df_graf, names="descricao", values="porcentagem", title=f"Distribuição de atividades - {mes_selec}/{ano_selec}")
            st.plotly_chart(fig, use_container_width=True)

    elif aba == "Consolidado" and st.session_state["admin"]:
        st.header("📑 Consolidado")
        st.dataframe(atividades_df, use_container_width=True)
