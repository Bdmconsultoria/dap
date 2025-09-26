import streamlit as st
import pandas as pd
from datetime import datetime
import psycopg2
import socket

# ==============================
# 0. Forçar IPv4 (Ignorar IPv6)
# ==============================
original_getaddrinfo = socket.getaddrinfo

def getaddrinfo_ipv4(host, port, *args, **kwargs):
    # Retorna apenas endereços IPv4
    infos = original_getaddrinfo(host, port, *args, **kwargs)
    return [i for i in infos if i[0] == socket.AF_INET]

socket.getaddrinfo = getaddrinfo_ipv4

# ==============================
# 1. Configurações do Banco de Dados PostgreSQL (Supabase)
# ==============================
DB_PARAMS = {
    "host": "db.urytjzupeorabraufjef.supabase.co",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "Bp@20081993",
    "sslmode": "require"
}

# ==============================
# 2. Conexão PostgreSQL com cache
# ==============================
@st.cache_resource
def get_db_connection():
    """Conexão PostgreSQL única por sessão."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        st.error(f"❌ Erro ao conectar ao banco de dados: {e}")
        return None

# ==============================
# 3. Setup do banco
# ==============================
def setup_db():
    conn = get_db_connection()
    if conn is None: st.stop()
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

setup_db()

# ==============================
# 4. Funções CRUD
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

@st.cache_data(show_spinner="🔄 Carregando dados do PostgreSQL...")
def load_data_from_db(reload_flag):
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame(), pd.DataFrame()
    try:
        usuarios_df = pd.read_sql("SELECT usuario, senha, admin FROM usuarios;", conn)
        atividades_df = pd.read_sql("SELECT id, usuario, data, mes, ano, descricao, projeto, porcentagem, observacao FROM atividades ORDER BY data DESC;", conn)
        return usuarios_df, atividades_df
    except Exception as e:
        st.error(f"Erro ao carregar dados do DB: {e}")
        return pd.DataFrame(), pd.DataFrame()

# Inicializa flag de recarga
if "reload_flag" not in st.session_state:
    st.session_state.reload_flag = 0

usuarios_df, atividades_df = load_data_from_db(st.session_state.reload_flag)

# ==============================
# 5. Listas fixas
# ==============================
DESCRICOES = [
    "1.001 - Gestão", "1.002 - Geral", "1.003 - Conselho", "1.004 - Treinamento e Desenvolvimento",
    "2.001 - Gestão do administrativo", "2.002 - Administrativa", "2.003 - Jurídica", "2.004 - Financeira",
    "2.006 - Fiscal", "2.007 - Infraestrutura TI", "2.008 - Treinamento interno", "2.011 - Análise de dados",
    "2.012 - Logística de viagens", "2.013 - Prestação de contas", "3.001 - Prospecção de oportunidades",
    "3.002 - Prospecção de temas", "3.003 - Administração comercial", "3.004 - Marketing Digital",
    "3.005 - Materiais de apoio", "3.006 - Grupos de Estudo", "3.007 - Elaboração de POC/Piloto",
    "3.008 - Elaboração e apresentação de proposta", "3.009 - Acompanhamento de proposta",
    "3.010 - Reunião de acompanhamento de funil", "3.011 - Planejamento Estratégico/Comercial",
    "3.012 - Sucesso do Cliente", "3.013 - Participação em eventos", "4.001 - Planejamento de projeto",
    "4.002 - Gestão de projeto", "4.003 - Reuniões internas de trabalho", "4.004 - Reuniões externas de trabalho",
    "4.005 - Pesquisa", "4.006 - Especificação de software", "4.007 - Desenvolvimento de software/rotinas",
    "4.008 - Coleta e preparação de dados", "4.009 - Elaboração de estudos e modelos",
    "4.010 - Confecção de relatórios técnicos", "4.011 - Confecção de apresentações técnicas",
    "4.012 - Confecção de artigos técnicos", "4.013 - Difusão de resultados", "4.014 - Elaboração de documentação final",
    "4.015 - Finalização do projeto", "5.001 - Gestão de desenvolvimento", "5.002 - Planejamento de projeto",
    "5.003 - Gestão de projeto", "5.004 - Reuniões internas de trabalho", "5.005 - Reuniões externa de trabalho",
    "5.006 - Pesquisa", "5.007 - Coleta e preparação de dados", "5.008 - Modelagem", "5.009 - Análise de tarefa",
    "5.010 - Especificação de tarefa", "5.011 - Correção de bug", "5.012 - Desenvolvimento de melhorias",
    "5.013 - Desenvolvimento de novas funcionalidades", "5.014 - Desenvolvimento de integrações",
    "5.015 - Treinamento interno", "5.016 - Documentação", "5.017 - Atividades gerenciais", "5.018 - Estudos"
]

PROJETOS = [
    "101-0 (Interno) Diretoria Executiva", "102-0 (Interno) Diretoria Administrativa",
    "103-0 (Interno) Diretoria de Engenharia", "104-0 (Interno) Diretoria de Negócios",
    "105-0 (Interno) Diretoria de Produtos", "106-0 (Interno) Diretoria de Tecnologia",
    "107-0 (Interno) Departamento Administrativo", "108-0 (Interno) Departamento de Gente e Cultura",
    "109-0 (Interno) Departamento de Infraestrutura", "110-0 (Interno) Departamento de Marketing",
    "111-0 (Interno) Departamento de Operação", "112-0 (Interno) Departamento de Sucesso do Cliente",
    "113-0 (Interno) Produto ARIES", "114-0 (Interno) Produto ActionWise", "115-0 (Interno) Produto Carga Base"
]

# ==============================
# 6. Interface Streamlit
# ==============================
if "usuario" not in st.session_state:
    st.session_state["usuario"] = None
    st.session_state["admin"] = False

# --- Tela de Login ---
if st.session_state["usuario"] is None:
    st.title("🔐 Login (PostgreSQL)")
    usuario = st.text_input("Usuário")
    senha = st.text_input("Senha", type="password")
    
    if st.button("Entrar"):
        usuarios_df, atividades_df = load_data_from_db(st.session_state.reload_flag)
        ok, admin = validar_login(usuario, senha)
        if ok:
            st.session_state["usuario"] = usuario
            st.session_state["admin"] = admin
            st.experimental_rerun()
        else:
            st.error("Usuário ou senha incorretos")

# --- App logado ---
else:
    st.sidebar.markdown(f"**Usuário:** {st.session_state['usuario']}")
    if st.sidebar.button("Sair"):
        st.session_state["usuario"] = None
        st.session_state["admin"] = False
