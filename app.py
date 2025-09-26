import streamlit as st
import pandas as pd
from datetime import datetime
import os

# ==============================
# Configurações iniciais
# ==============================
st.set_page_config(page_title="Controle de Atividades", layout="wide")

# Arquivo para salvar usuários e atividades
USERS_FILE = "usuarios.csv"
ATIVIDADES_FILE = "atividades.csv"

# Inicializa os arquivos caso não existam
if not os.path.exists(USERS_FILE):
    pd.DataFrame(columns=["usuario", "senha", "admin"]).to_csv(USERS_FILE, index=False)
if not os.path.exists(ATIVIDADES_FILE):
    pd.DataFrame(columns=["usuario", "data", "mes", "ano", "descricao", "projeto", "porcentagem", "observacao"]).to_csv(ATIVIDADES_FILE, index=False)

# Carrega dados
usuarios_df = pd.read_csv(USERS_FILE)
atividades_df = pd.read_csv(ATIVIDADES_FILE)

# ==============================
# Funções auxiliares
# ==============================
def salvar_usuario(usuario, senha, admin=False):
    global usuarios_df
    if usuario not in usuarios_df["usuario"].values:
        novo = pd.DataFrame([[usuario, senha, admin]], columns=["usuario", "senha", "admin"])
        usuarios_df = pd.concat([usuarios_df, novo], ignore_index=True)
        usuarios_df.to_csv(USERS_FILE, index=False)

def validar_login(usuario, senha):
    if usuario in usuarios_df["usuario"].values:
        row = usuarios_df.loc[usuarios_df["usuario"] == usuario].iloc[0]
        if row["senha"] == senha:
            return True, bool(row["admin"])
    return False, False

def salvar_atividade(usuario, data, descricao, projeto, porcentagem, observacao):
    global atividades_df
    mes = data.month
    ano = data.year
    nova = pd.DataFrame([[usuario, data.strftime("%d/%m/%Y"), mes, ano, descricao, projeto, porcentagem, observacao]],
                        columns=["usuario", "data", "mes", "ano", "descricao", "projeto", "porcentagem", "observacao"])
    atividades_df = pd.concat([atividades_df, nova], ignore_index=True)
    atividades_df.to_csv(ATIVIDADES_FILE, index=False)

# ==============================
# Listas fixas
# ==============================
DESCRICOES = [
    "1.001 - Gestão",
    "1.002 - Geral",
    "1.003 - Conselho",
    "1.004 - Treinamento e Desenvolvimento",
    "2.001 - Gestão do administrativo",
    "2.002 - Administrativa",
    "2.003 - Jurídica",
    "2.004 - Financeira",
    "2.006 - Fiscal",
    "2.007 - Infraestrutura TI",
    "2.008 - Treinamento interno",
    "2.011 - Análise de dados",
    "2.012 - Logística de viagens",
    "2.013 - Prestação de contas",
    "3.001 - Prospecção de oportunidades",
    "3.002 - Prospecção de temas",
    "3.003 - Administração comercial",
    "3.004 - Marketing Digital",
    "3.005 - Materiais de apoio",
    "3.006 - Grupos de Estudo",
    "3.007 - Elaboração de POC/Piloto",
    "3.008 - Elaboração e apresentação de proposta",
    "3.009 - Acompanhamento de proposta",
    "3.010 - Reunião de acompanhamento de funil",
    "3.011 - Planejamento Estratégico/Comercial",
    "3.012 - Sucesso do Cliente",
    "3.013 - Participação em eventos",
    "4.001 - Planejamento de projeto",
    "4.002 - Gestão de projeto",
    "4.003 - Reuniões internas de trabalho",
    "4.004 - Reuniões externas de trabalho",
    "4.005 - Pesquisa",
    "4.006 - Especificação de software",
    "4.007 - Desenvolvimento de software/rotinas",
    "4.008 - Coleta e preparação de dados",
    "4.009 - Elaboração de estudos e modelos",
    "4.010 - Confecção de relatórios técnicos",
    "4.011 - Confecção de apresentações técnicas",
    "4.012 - Confecção de artigos técnicos",
    "4.013 - Difusão de resultados",
    "4.014 - Elaboração de documentação final",
    "4.015 - Finalização do projeto",
    "5.001 - Gestão de desenvolvimento",
    "5.002 - Planejamento de projeto",
    "5.003 - Gestão de projeto",
    "5.004 - Reuniões internas de trabalho",
    "5.005 - Reuniões externa de trabalho",
    "5.006 - Pesquisa",
    "5.007 - Coleta e preparação de dados",
    "5.008 - Modelagem",
    "5.009 - Análise de tarefa",
    "5.010 - Especificação de tarefa",
    "5.011 - Correção de bug",
    "5.012 - Desenvolvimento de melhorias",
    "5.013 - Desenvolvimento de novas funcionalidades",
    "5.014 - Desenvolvimento de integrações",
    "5.015 - Treinamento interno",
    "5.016 - Documentação",
    "5.017 - Atividades gerenciais",
    "5.018 - Estudos"
]

PROJETOS = [
    "101-0 (Interno) Diretoria Executiva",
    "102-0 (Interno) Diretoria Administrativa",
    "103-0 (Interno) Diretoria de Engenharia",
    "104-0 (Interno) Diretoria de Negócios",
    "105-0 (Interno) Diretoria de Produtos",
    "106-0 (Interno) Diretoria de Tecnologia",
    "107-0 (Interno) Departamento Administrativo",
    "108-0 (Interno) Departamento de Gente e Cultura",
    "109-0 (Interno) Departamento de Infraestrutura",
    "110-0 (Interno) Departamento de Marketing",
    "111-0 (Interno) Departamento de Operação",
    "112-0 (Interno) Departamento de Sucesso do Cliente",
    "113-0 (Interno) Produto ARIES",
    "114-0 (Interno) Produto ActionWise",
    "115-0 (Interno) Produto Carga Base"
]

# ==============================
# Login
# ==============================
if "usuario" not in st.session_state:
    st.session_state["usuario"] = None
    st.session_state["admin"] = False

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
    st.sidebar.write(f"👤 Logado como: {st.session_state['usuario']}")
    if st.sidebar.button("Sair"):
        st.session_state["usuario"] = None
        st.session_state["admin"] = False
        st.rerun()

    abas = ["Lançar Atividade", "Minhas Atividades"]
    if st.session_state["admin"]:
        abas += ["Gerenciar Usuários", "Consolidado"]

    aba = st.sidebar.radio("Menu", abas)

    if aba == "Gerenciar Usuários" and st.session_state["admin"]:
        st.header("👥 Gerenciar Usuários")
        novo_usuario = st.text_input("Novo usuário")
        nova_senha = st.text_input("Senha", type="password")
        admin = st.checkbox("Administrador")
        if st.button("Adicionar Usuário"):
            salvar_usuario(novo_usuario, nova_senha, admin)
            st.success("Usuário adicionado!")

        st.subheader("Usuários cadastrados")
        st.dataframe(usuarios_df)

    elif aba == "Lançar Atividade":
        st.header("📝 DAP Completa")
        data = st.date_input("Data", datetime.today())
        descricao = st.selectbox("Descrição", DESCRICOES)
        projeto = st.selectbox("Projeto", PROJETOS)
        porcentagem = st.slider("Porcentagem", 0, 100, 100)
        observacao = st.text_area("Observação")
        if st.button("Salvar"):
            salvar_atividade(st.session_state["usuario"], data, descricao, projeto, porcentagem, observacao)
            st.success("Atividade salva!")

    elif aba == "Minhas Atividades":
        st.header("📊 Minhas Atividades")
        minhas = atividades_df[atividades_df["usuario"] == st.session_state["usuario"]]
        st.dataframe(minhas)
        st.download_button("📥 Exportar CSV", minhas.to_csv(index=False), "atividades.csv")

    elif aba == "Consolidado" and st.session_state["admin"]:
        st.header("📑 Consolidado de Atividades")
        st.dataframe(atividades_df)
        st.download_button("📥 Exportar Consolidado CSV", atividades_df.to_csv(index=False), "consolidado.csv")