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
# 5. Listas de Descrições e Projetos (completo)
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
    "4.008 - Coleta e preparação de dados", "4.009 - Elaboração de estudos e modelos", "4.010 - Confecção de relatórios técnicos",
    "4.011 - Confecção de apresentações técnicas", "4.012 - Confecção de artigos técnicos", "4.013 - Difusão de resultados",
    "4.014 - Elaboração de documentação final", "4.015 - Finalização do projeto", "5.001 - Gestão de desenvolvimento",
    "5.002 - Planejamento de projeto", "5.003 - Gestão de projeto", "5.004 - Reuniões internas de trabalho",
    "5.005 - Reuniões externa de trabalho", "5.006 - Pesquisa", "5.007 - Coleta e preparação de dados",
    "5.008 - Modelagem", "5.009 - Análise de tarefa", "5.010 - Especificação de tarefa", "5.011 - Correção de bug",
    "5.012 - Desenvolvimento de melhorias", "5.013 - Desenvolvimento de novas funcionalidades",
    "5.014 - Desenvolvimento de integrações", "5.015 - Treinamento interno", "5.016 - Documentação",
    "5.017 - Atividades gerenciais", "5.018 - Estudos", "6.001 - Gestão de equipe", "6.002 - Pesquisa",
    "6.003 - Especificação de testes", "6.004 - Desenvolvimento de automações", "6.005 - Realização de testes",
    "6.006 - Reuniões internas de trabalho", "6.007 - Treinamento interno", "6.008 - Elaboração de material",
    "7.001 - Gestão de equipe", "7.002 - Pesquisa e estudos", "7.003 - Análise de ticket",
    "7.004 - Reuniões internas de trabalho", "7.005 - Reuniões externas de trabalho", "7.006 - Preparação de treinamento externo",
    "7.007 - Realização de treinamento externo", "7.008 - Documentação de treinamento", "7.009 - Treinamento interno",
    "7.010 - Criação de tarefa", "9.001 - Gestão do RH", "9.002 - Recrutamento e seleção", "9.003 - Participação em eventos",
    "9.004 - Pesquisa e estratégia", "9.005 - Treinamento e desenvolvimento", "9.006 - Registro de feedback",
    "9.007 - Avaliação de RH", "9.008 - Elaboração de conteúdo", "9.009 - Comunicação interna",
    "9.010 - Reuniões internas de trabalho", "9.011 - Reunião externa", "9.012 - Apoio contábil e financeiro",
    "10.001 - Planejamento de operação", "10.002 - Gestão de operação", "10.003 - Reuniões internas de trabalho",
    "10.004 - Reuniões externas de trabalho", "10.005 - Especificação de melhoria ou correção de software",
    "10.006 - Desenvolvimento de automações", "10.007 - Coleta e preparação de dados", "10.008 - Elaboração de estudos e modelos",
    "10.009 - Confecção de relatórios técnicos", "10.010 - Confecção de apresentações técnicas",
    "10.011 - Confecção de artigos técnicos", "10.012 - Difusão de resultados", "10.013 - Preparação de treinamento externo",
    "10.014 - Realização de treinamento externo", "10.015 - Mapeamento de Integrações"
]

PROJETOS = [
    "101-0 (Interno) Diretoria Executiva", "102-0 (Interno) Diretoria Administrativa", 
    "103-0 (Interno) Diretoria de Engenharia", "104-0 (Interno) Diretoria de Negócios",
    "105-0 (Interno) Diretoria de Produtos", "106-0 (Interno) Diretoria de Tecnologia",
    "107-0 (Interno) Departamento Administrativo", "108-0 (Interno) Departamento de Gente e Cultura",
    "109-0 (Interno) Departamento de Infraestrutura", "110-0 (Interno) Departamento de Marketing",
    "111-0 (Interno) Departamento de Operação", "112-0 (Interno) Departamento de Sucesso do Cliente",
    "113-0 (Interno) Produto ARIES", "114-0 (Interno) Produto ActionWise", "115-0 (Interno) Produto Carga Base",
    "116-0 (Interno) Produto Godel Perdas", "117-0 (Interno) Produto Godel Conecta", "118-0 (Interno) Produto SIGPerdas",
    "119-0 (Interno) Produto SINAPgrid", "120-0 (Interno) Produto SINAP4.0", "121-0 (Interno) SINAPgrid Acadêmico",
    "122-0 (Interno) Produto SINAPgateway (BAGRE)", "123-0 (Interno) Produto SINAPautomação e diagnóstico (autobatch)",
    "302-0 (SENSE - Equatorial) Virtus", "402-0 (SOFTEX - Copel) Renovação de Ativos Continuação",
    "573-1 (ENEL) Suporte SINAPgrid", "573-2 (ENEL) Re-configuração", "575-0 (Amazonas) Suporte SINAPgrid",
    "578-1 (Copel) Suporte SINAPgrid", "578-2 (Copel) Suporte Godel Conecta", "578-3 (Copel) Suporte GDIS",
    "581-0 (CERILUZ) Suporte SINAPgrid", "583-0 (CERTAJA) Suporte SINAPgrid", "584-0 (CERTEL) Suporte SINAPgrid",
    "585-0 (COOPERLUZ) Suporte SINAPgrid", "587-0 (COPREL) Suporte SINAPgrid", "606-0 (Roraima) Suporte SINAPgrid",
    "615-0 (Energisa) Suporte SIGPerdas", "620-1 (CPFL) Suporte SINAPgrid", "638-1 (Amazonas) Suporte SIGPerdas",
    "638-2 (Roraima) Suporte SIGPerdas", "640-0 (SENAI - CTG) Hidrogênio Verde", "647-0 (Energisa) Consultoria de Estudos Elétricos",
    "648-0 (Neoenergia) Suporte SINAPgrid", "649-0 (Neoenergia) Godel PCom e Godel Analytics", "653-0 (Roraima) Projeto Gestor GDIS",
    "655-0 (CELESC) Sistema Integrável de Matchmaking", "658-0 (Copel) Planauto Continuação", "659-0 (Copel) Cálculo de Benefícios de Investimentos",
    "660-0 (CERFOX) Suporte SINAPgrid", "661-0 (ENEL SP, RJ e CE) Consultoria técnica BDGD", "663-0 (Banco Mundial) Eletromobilidade em São Paulo",
    "666-0 (Energisa) Análise MM GD", "667-0 (Energisa) Planejamento Decenal MT", "668-0 (Energisa) Critérios de Planejamento de SEs",
    "669-0 (Desenvolve SP) Hub de Dados", "670-0 (CPFL) Proteção", "671-0 (Equatorial) Godel Perdas",
    "672-0 (ENEL SP) URD Subterrâneo", "673-0 (Equatorial) PDD", "674-0 (Energisa PB) Planejamento Decenal 2025",
    "675-0 (EDEMSA) Godel Perdas Suporte Técnico Bromteck", "676-0 (Equatorial) PoC Resiliência", "677-0 (Neoenergia) Suporte Godel Perdas",
    "678-0 (CPFL) AMBAR", "679-0 (ENEL) Godel Conecta", "680-0 (CESI) Angola Banco Mundial",
    "681-0 (CEMACON) Suporte SINAPgrid", "682-0 (FECOERGS) Treinamento SINAPgrid"
]

# ==============================
# 6. Interface Streamlit
# ==============================
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

    elif aba == "Minhas Atividades":
        st.header("📊 Minhas Atividades")
        minhas = atividades_df[atividades_df["usuario"] == st.session_state["usuario"]]
        if minhas.empty:
            st.info("Nenhuma atividade encontrada.")
        else:
            for _, row in minhas.iterrows():
                st.write(f"**{row['data']} - {row['descricao']}** ({row['porcentagem']}%)")
                st.write(f"Projeto: {row['projeto']}")
                st.write(f"Observação: {row['observacao']}")
                if st.button(f"Deletar {row['id']}"):
                    deletar_atividade(row['id'])
                    st.success("Atividade deletada!")
                    st.experimental_rerun()

    elif aba == "Validação":
        st.header("📊 Validação de Lançamentos")
        if st.session_state["admin"]:
            df_val =







