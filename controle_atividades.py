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

def deletar_atividade(atividade_id):
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM atividades WHERE id = %s;", (atividade_id,))
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
DESCRICOES = ["1.001 - Gestão", "1.002 - Geral", "2.001 - Gestão do administrativo"]
PROJETOS = ["101-0 Diretoria Executiva", "102-0 Diretoria Administrativa"]

if "usuario" not in st.session_state:
    st.session_state["usuario"] = None
    st.session_state["admin"] = False

usuarios_df, atividades_df = carregar_dados()

# ------------------------------
# Login
# ------------------------------
if st.session_state["usuario"] is None:
    st.title("🔐 Login")
    usuario = st.text_input("Usuário")
    senha = st.text_input("Senha", type="password")
    if st.button("Entrar"):
        ok, admin = validar_login(usuario, senha)
        if ok:
            st.session_state["usuario"] = usuario
            st.session_state["admin"] = admin
            st.rerun()
        else:
            st.error("Usuário ou senha incorretos")
else:
    st.sidebar.markdown(f"**Usuário:** {st.session_state['usuario']}")
    if st.sidebar.button("Sair"):
        st.session_state["usuario"] = None
        st.session_state["admin"] = False
        st.rerun()

    abas = ["Lançar Atividade", "Minhas Atividades", "Validação"]
    if st.session_state["admin"]:
        abas += ["Gerenciar Usuários", "Consolidado"]

    aba = st.sidebar.radio("Menu", abas)

    # ------------------------------
    # Gerenciar Usuários
    # ------------------------------
    if aba == "Gerenciar Usuários" and st.session_state["admin"]:
        st.header("👥 Gerenciar Usuários")
        with st.form("form_add_user"):
            novo_usuario = st.text_input("Usuário")
            nova_senha = st.text_input("Senha", type="password")
            admin_check = st.checkbox("Admin")
            if st.form_submit_button("Adicionar"):
                if salvar_usuario(novo_usuario, nova_senha, admin_check):
                    st.success("Usuário adicionado!")
                    st.rerun()
        st.dataframe(usuarios_df, use_container_width=True)

    # ------------------------------
    # Lançar Atividade
    # ------------------------------
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
                        st.rerun()
                else:
                    st.error("A observação é obrigatória.")

    # ------------------------------
    # Minhas Atividades
    # ------------------------------
    elif aba == "Minhas Atividades":
        st.header("📊 Minhas Atividades")
        minhas = atividades_df[atividades_df["usuario"] == st.session_state["usuario"]]
        if minhas.empty:
            st.info("Nenhuma atividade encontrada.")
        else:
            st.dataframe(minhas, use_container_width=True)

    # ------------------------------
    # Validação
    # ------------------------------
    elif aba == "Validação":
        st.header("📊 Validação de Lançamentos")
        df_val = atividades_df.copy() if st.session_state["admin"] else atividades_df[atividades_df["usuario"] == st.session_state["usuario"]]

        if df_val.empty:
            st.info("Nenhuma atividade encontrada.")
        else:
            # Filtros
            projeto_filter = st.selectbox("Filtrar por Projeto", options=["Todos"] + sorted(df_val["projeto"].unique()))
            descricao_filter = st.selectbox("Filtrar por Descrição", options=["Todos"] + sorted(df_val["descricao"].unique()))

            if projeto_filter != "Todos":
                df_val = df_val[df_val["projeto"] == projeto_filter]
            if descricao_filter != "Todos":
                df_val = df_val[df_val["descricao"] == descricao_filter]

            # Agrupar por mês e calcular média %
            resumo = df_val.groupby(['ano', 'mes']).porcentagem.mean().reset_index()
            resumo['ano_mes'] = resumo['ano'].astype(str) + '-' + resumo['mes'].astype(str)
            st.write("✅ Percentual médio de lançamentos por mês:")
            st.dataframe(resumo[['ano_mes', 'porcentagem']], use_container_width=True)

            # Gráfico
            fig = px.bar(resumo, x='ano_mes', y='porcentagem', text='porcentagem', labels={'ano_mes': 'Mês', 'porcentagem': '% Lançado'})
            st.plotly_chart(fig, use_container_width=True)

            # Botão apagar atividades
            for _, row in df_val.iterrows():
                st.write(f"**{row['data']} - {row['descricao']} ({row['porcentagem']}%)**")
                st.write(f"Projeto: {row['projeto']}")
                st.write(f"Observação: {row['observacao']}")
                if st.session_state["admin"] or row["usuario"] == st.session_state["usuario"]:
                    if st.button(f"Deletar {row['id']}", key=f"del_{row['id']}"):
                        deletar_atividade(row['id'])
                        st.success("Atividade deletada!")
                        st.experimental_rerun()

    # ------------------------------
    # Consolidado
    # ------------------------------
    elif aba == "Consolidado" and st.session_state["admin"]:
        st.header("📑 Consolidado")
        st.dataframe(atividades_df, use_container_width=True)
