import streamlit as st
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2 import sql

# ==============================
# 1. Configurações do Banco de Dados PostgreSQL (Supabase)
# O host foi revertido para o NOME DE DOMÍNIO padrão.
# Se o erro de conexão persistir, verifique a configuração de rede/firewall do seu projeto Supabase.
# ==============================
DB_PARAMS = {
    # Usando o nome de domínio para a conexão em nuvem.
    "host": "db.urytjzupeorabraufjef.supabase.co", 
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "Bp@20081993",
    # Supabase requer SSL para conexões externas.
    "sslmode": "require" 
}

# ==============================
# Configurações iniciais do Streamlit
# ==============================
st.set_page_config(page_title="Controle de Atividades (PostgreSQL)", layout="wide")

# ==============================
# 2. Funções de Conexão e Setup do DB
# ==============================

def get_db_connection():
    """Tenta estabelecer a conexão com o banco de dados PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        # st.error é usado aqui para mostrar o erro de forma clara no app
        st.error(f"❌ Erro ao conectar ao banco de dados. Verifique suas credenciais e a disponibilidade do serviço: {e}")
        return None

def setup_db():
    """Cria as tabelas 'usuarios' e 'atividades' se não existirem."""
    conn = get_db_connection()
    if conn is None: return

    try:
        with conn.cursor() as cursor:
            # Tabela de Usuários (necessária para sistema multiusuário)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS usuarios (
                    usuario VARCHAR(50) PRIMARY KEY,
                    senha VARCHAR(50) NOT NULL,
                    admin BOOLEAN DEFAULT FALSE
                );
            """)

            # Tabela de Atividades
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
        st.error(f"Erro na criação/verificação das tabelas: {e}")
    finally:
        conn.close()

# Executa o setup do DB ao iniciar o script
setup_db()


# ==============================
# 3. Funções de Carregamento de Dados (Cache)
# ==============================

# Usa st.cache_data para carregar dados do DB e evitar recargas desnecessárias
# O cache será invalidado quando 'reload_flag' mudar (após uma inserção ou update)
@st.cache_data(show_spinner="🔄 Carregando dados do PostgreSQL...")
def load_data_from_db(reload_flag):
    """Carrega dados das tabelas 'usuarios' e 'atividades' para DataFrames."""
    conn = get_db_connection()
    if conn is None:
        # Retorna DataFrames vazios em caso de falha na conexão
        return pd.DataFrame(), pd.DataFrame()

    try:
        # Carrega usuários
        usuarios_query = "SELECT usuario, senha, admin FROM usuarios;"
        usuarios_df = pd.read_sql(usuarios_query, conn)

        # Carrega atividades
        atividades_query = "SELECT id, usuario, data, mes, ano, descricao, projeto, porcentagem, observacao FROM atividades ORDER BY data DESC;"
        atividades_df = pd.read_sql(atividades_query, conn)

        return usuarios_df, atividades_df
    except Exception as e:
        st.error(f"Erro ao carregar dados do DB: {e}")
        return pd.DataFrame(), pd.DataFrame()
    finally:
        conn.close()

# Inicializa o flag de recarga na sessão
if "reload_flag" not in st.session_state:
    st.session_state.reload_flag = 0

# Carrega dados do PostgreSQL (agora usando a função com cache)
usuarios_df, atividades_df = load_data_from_db(st.session_state.reload_flag)

# ==============================
# 4. Funções auxiliares (CRUD)
# ==============================

def salvar_usuario(usuario, senha, admin=False):
    """Insere um novo usuário no DB. Retorna True em caso de sucesso."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            # Usa INSERT INTO ON CONFLICT DO NOTHING para evitar duplicatas e erro
            cursor.execute(
                """
                INSERT INTO usuarios (usuario, senha, admin) 
                VALUES (%s, %s, %s) 
                ON CONFLICT (usuario) 
                DO NOTHING;
                """,
                (usuario, senha, admin)
            )
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar usuário no DB: {e}")
        return False
    finally:
        conn.close()

def validar_login(usuario, senha):
    """Verifica se o usuário e senha são válidos no DB. Retorna (True/False, admin_status)."""
    conn = get_db_connection()
    if conn is None: return False, False
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT senha, admin FROM usuarios WHERE usuario = %s;",
                (usuario,)
            )
            result = cursor.fetchone()
            
            if result and result[0] == senha:
                return True, result[1] # result[0] é a senha, result[1] é o status admin
            return False, False
    except Exception as e:
        st.error(f"Erro ao validar login no DB: {e}")
        return False, False
    finally:
        conn.close()

def salvar_atividade(usuario, data, descricao, projeto, porcentagem, observacao):
    """Insere uma nova atividade no DB. Retorna True em caso de sucesso."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            mes = data.month
            ano = data.year
            
            cursor.execute(
                """
                INSERT INTO atividades (usuario, data, mes, ano, descricao, projeto, porcentagem, observacao) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (usuario, data, mes, ano, descricao, projeto, porcentagem, observacao)
            )
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar atividade no DB: {e}")
        return False
    finally:
        conn.close()

# ==============================
# Listas fixas
# ==============================
# Manutenção das listas fixas em português
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
# 5. Interface Streamlit
# ==============================
if "usuario" not in st.session_state:
    st.session_state["usuario"] = None
    st.session_state["admin"] = False

if st.session_state["usuario"] is None:
    # --- Tela de Login ---
    st.title("🔐 Login (PostgreSQL)")
    usuario = st.text_input("Usuário")
    senha = st.text_input("Senha", type="password")
    
    if st.button("Entrar"):
        # Garante que os DataFrames estejam frescos para validar o login
        usuarios_df, atividades_df = load_data_from_db(st.session_state.reload_flag)
        
        ok, admin = validar_login(usuario, senha)
        if ok:
            st.session_state["usuario"] = usuario
            st.session_state["admin"] = admin
            st.rerun()
        else:
            st.error("Usuário ou senha incorretos")
else:
    # --- App Logado ---
    st.sidebar.markdown(f"**Usuário:** {st.session_state['usuario']}")
    
    if st.sidebar.button("Sair"):
        st.session_state["usuario"] = None
        st.session_state["admin"] = False
        st.session_state.reload_flag += 1 # Opcional: força recarga ao sair
        st.rerun()

    abas = ["Lançar Atividade", "Minhas Atividades"]
    if st.session_state["admin"]:
        abas += ["Gerenciar Usuários", "Consolidado"]

    aba = st.sidebar.radio("Menu", abas)
    st.sidebar.divider()

    # --- Gerenciar Usuários (Admin) ---
    if aba == "Gerenciar Usuários" and st.session_state["admin"]:
        st.header("👥 Gerenciar Usuários")
        
        with st.form("form_add_user"):
            st.subheader("Adicionar Novo Usuário")
            col1, col2, col3 = st.columns([3, 3, 1])
            novo_usuario = col1.text_input("Nome de Usuário", key="new_user")
            nova_senha = col2.text_input("Senha", type="password", key="new_pass")
            admin_check = col3.checkbox("Admin", key="new_admin")
            submitted = st.form_submit_button("Adicionar Usuário")

            if submitted:
                if salvar_usuario(novo_usuario, nova_senha, admin_check):
                    st.success("Usuário adicionado com sucesso ao PostgreSQL!")
                    # Incrementa o flag para forçar a recarga dos dados
                    st.session_state.reload_flag += 1
                    st.rerun()
                else:
                    st.error("Falha ao adicionar usuário.")

        st.subheader("Usuários Cadastrados")
        st.dataframe(usuarios_df.drop(columns=['senha']), use_container_width=True)

    # --- Lançar Atividade ---
    elif aba == "Lançar Atividade":
        st.header("📝 Lançamento de Atividade (DAP)")
        
        with st.form("form_atividade"):
            data = st.date_input("Data da Atividade", datetime.today())
            descricao = st.selectbox("Descrição da Atividade (Código)", DESCRICOES)
            projeto = st.selectbox("Projeto", PROJETOS)
            porcentagem = st.slider("Porcentagem de Dedicação", 0, 100, 100)
            observacao = st.text_area("Observação / Detalhamento da Tarefa (Obrigatório)", height=150)
            
            submitted = st.form_submit_button("Salvar Atividade no DB")
            
            if submitted:
                if not observacao.strip():
                    st.error("A Observação/Detalhamento é obrigatória.")
                else:
                    if salvar_atividade(st.session_state["usuario"], data, descricao, projeto, porcentagem, observacao):
                        st.success("Atividade salva com sucesso no PostgreSQL!")
                        # Incrementa o flag para forçar a recarga dos dados
                        st.session_state.reload_flag += 1
                        st.rerun()
                    else:
                        st.error("Falha ao salvar atividade.")

    # --- Minhas Atividades ---
    elif aba == "Minhas Atividades":
        st.header("📊 Minhas Atividades Registradas")
        
        minhas = atividades_df[atividades_df["usuario"] == st.session_state["usuario"]]
        
        if minhas.empty:
            st.info("Nenhuma atividade encontrada para o seu usuário.")
        else:
            # Seleciona colunas relevantes para exibição
            display_df = minhas[['data', 'descricao', 'projeto', 'porcentagem', 'observacao']]
            st.dataframe(display_df, use_container_width=True)
            
            st.download_button(
                "📥 Exportar CSV", 
                minhas.to_csv(index=False).encode('utf-8'), 
                f"atividades_{st.session_state['usuario']}.csv",
                mime="text/csv"
            )

    # --- Consolidado (Admin) ---
    elif aba == "Consolidado" and st.session_state["admin"]:
        st.header("📑 Consolidado Geral de Atividades")
        
        if atividades_df.empty:
            st.info("Nenhum registro de atividade no banco de dados.")
        else:
            st.dataframe(atividades_df.drop(columns=['id']), use_container_width=True)
            
            st.download_button(
                "📥 Exportar Consolidado CSV", 
                atividades_df.to_csv(index=False).encode('utf-8'), 
                "consolidado_geral.csv",
                mime="text/csv"
            )

                mime="text/csv"
            )

