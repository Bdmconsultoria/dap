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
    # Cria com colunas: usuario, senha, admin (booleano)
    pd.DataFrame(columns=["usuario", "senha", "admin"]).to_csv(USERS_FILE, index=False)
if not os.path.exists(ATIVIDADES_FILE):
    # Cria com colunas: usuario, data, mes, ano, descricao, projeto, porcentagem, observacao
    pd.DataFrame(columns=["usuario", "data", "mes", "ano", "descricao", "projeto", "porcentagem", "observacao"]).to_csv(ATIVIDADES_FILE, index=False)

# Carrega dados
# Tenta carregar os DataFrames, tratando erros se os arquivos estiverem vazios ou corrompidos
try:
    usuarios_df = pd.read_csv(USERS_FILE)
except pd.errors.EmptyDataError:
    usuarios_df = pd.DataFrame(columns=["usuario", "senha", "admin"])

try:
    atividades_df = pd.read_csv(ATIVIDADES_FILE)
except pd.errors.EmptyDataError:
    atividades_df = pd.DataFrame(columns=["usuario", "data", "mes", "ano", "descricao", "projeto", "porcentagem", "observacao"])


# ==============================
# Funções auxiliares
# ==============================
def salvar_usuario(usuario, senha, admin=False):
    global usuarios_df
    # Verifica se o usuário já existe (case-sensitive)
    if usuario not in usuarios_df["usuario"].values:
        novo = pd.DataFrame([[usuario, senha, admin]], columns=["usuario", "senha", "admin"])
        usuarios_df = pd.concat([usuarios_df, novo], ignore_index=True)
        usuarios_df.to_csv(USERS_FILE, index=False)
        return True
    return False

def validar_login(usuario, senha):
    if usuario in usuarios_df["usuario"].values:
        row = usuarios_df.loc[usuarios_df["usuario"] == usuario].iloc[0]
        # Garante que 'admin' é tratado como booleano (True/1 ou False/0)
        is_admin = str(row.get("admin", False)).lower() in ('true', '1', 'True')
        if row["senha"] == senha:
            return True, is_admin
    return False, False

def salvar_atividade(usuario, data, descricao, projeto, porcentagem, observacao):
    global atividades_df
    mes = data.month
    ano = data.year
    # data formatada para exibição (ex: 26/09/2025)
    data_formatada = data.strftime("%d/%m/%Y")
    nova = pd.DataFrame([[usuario, data_formatada, mes, ano, descricao, projeto, porcentagem, observacao]],
                        columns=["usuario", "data", "mes", "ano", "descricao", "projeto", "porcentagem", "observacao"])
    atividades_df = pd.concat([atividades_df, nova], ignore_index=True)
    atividades_df.to_csv(ATIVIDADES_FILE, index=False)

# ==============================
# Listas fixas (Manter em Português)
# ==============================
DESCRICOES = [
    "1.001 - Gestão", "1.002 - Geral", "1.003 - Conselho", "1.004 - Treinamento e Desenvolvimento",
    "2.001 - Gestão do administrativo", "2.002 - Administrativa", "2.003 - Jurídica", "2.004 - Financeira",
    "2.006 - Fiscal", "2.007 - Infraestrutura TI", "2.008 - Treinamento interno", "2.011 - Análise de dados",
    "2.012 - Logística de viagens", "2.013 - Prestação de contas",
    "3.001 - Prospecção de oportunidades", "3.002 - Prospecção de temas", "3.003 - Administração comercial",
    "3.004 - Marketing Digital", "3.005 - Materiais de apoio", "3.006 - Grupos de Estudo", "3.007 - Elaboração de POC/Piloto",
    "3.008 - Elaboração e apresentação de proposta", "3.009 - Acompanhamento de proposta", "3.010 - Reunião de acompanhamento de funil",
    "3.011 - Planejamento Estratégico/Comercial", "3.012 - Sucesso do Cliente", "3.013 - Participação em eventos",
    "4.001 - Planejamento de projeto", "4.002 - Gestão de projeto", "4.003 - Reuniões internas de trabalho",
    "4.004 - Reuniões externas de trabalho", "4.005 - Pesquisa", "4.006 - Especificação de software",
    "4.007 - Desenvolvimento de software/rotinas", "4.008 - Coleta e preparação de dados", "4.009 - Elaboração de estudos e modelos",
    "4.010 - Confecção de relatórios técnicos", "4.011 - Confecção de apresentações técnicas", "4.012 - Confecção de artigos técnicos",
    "4.013 - Difusão de resultados", "4.014 - Elaboração de documentação final", "4.015 - Finalização do projeto",
    "5.001 - Gestão de desenvolvimento", "5.002 - Planejamento de projeto", "5.003 - Gestão de projeto",
    "5.004 - Reuniões internas de trabalho", "5.005 - Reuniões externa de trabalho", "5.006 - Pesquisa",
    "5.007 - Coleta e preparação de dados", "5.008 - Modelagem", "5.009 - Análise de tarefa", "5.010 - Especificação de tarefa",
    "5.011 - Correção de bug", "5.012 - Desenvolvimento de melhorias", "5.013 - Desenvolvimento de novas funcionalidades",
    "5.014 - Desenvolvimento de integrações", "5.015 - Treinamento interno", "5.016 - Documentação",
    "5.017 - Atividades gerenciais", "5.018 - Estudos"
]

PROJETOS = [
    "101-0 (Interno) Diretoria Executiva", "102-0 (Interno) Diretoria Administrativa",
    "103-0 (Interno) Diretoria de Engenharia", "104-0 (Interno) Diretoria de Negócios",
    "105-0 (Interno) Diretoria de Produtos", "106-0 (Interno) Diretoria de Tecnologia",
    "107-0 (Interno) Departamento Administrativo", "108-0 (Interno) Departamento de Gente e Cultura",
    "109-0 (Interno) Departamento de Infraestrutura", "110-0 (Interno) Departamento de Marketing",
    "111-0 (Interno) Departamento de Operação", "112-0 (Interno) Departamento de Sucesso do Cliente",
    "113-0 (Interno) Produto ARIES", "114-0 (Interno) Produto ActionWise",
    "115-0 (Interno) Produto Carga Base"
]

# ==============================
# Login e Navegação
# ==============================
if "usuario" not in st.session_state:
    st.session_state["usuario"] = None
    st.session_state["admin"] = False

if st.session_state["usuario"] is None:
    # -----------------------------
    # Tela de Login
    # -----------------------------
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
    # -----------------------------
    # Menu Principal
    # -----------------------------
    st.sidebar.write(f"👤 Logado como: {st.session_state['usuario']}")
    if st.sidebar.button("Sair"):
        st.session_state["usuario"] = None
        st.session_state["admin"] = False
        st.rerun()

    # Define as abas. 'Validação' agora é acessível a todos.
    abas = ["Lançar Atividade", "Minhas Atividades", "Validação"]
    
    # Adiciona abas de administração
    if st.session_state["admin"]:
        abas += ["Gerenciar Usuários", "Consolidado"]

    aba = st.sidebar.radio("Menu", abas)

    # -----------------------------
    # Gerenciar Usuários (Admin)
    # -----------------------------
    if aba == "Gerenciar Usuários" and st.session_state["admin"]:
        st.header("👥 Gerenciar Usuários")
        
        with st.form("form_novo_usuario"):
            novo_usuario = st.text_input("Novo usuário")
            nova_senha = st.text_input("Senha", type="password")
            admin_check = st.checkbox("Administrador")
            if st.form_submit_button("Adicionar Usuário"):
                if salvar_usuario(novo_usuario, nova_senha, admin_check):
                    st.success("Usuário adicionado!")
                else:
                    st.warning("Usuário já existe.")
                # Usa rerun para forçar o dataframe de usuários a ser recarregado no topo do script
                st.rerun() 

        st.subheader("Usuários cadastrados")
        st.dataframe(usuarios_df, hide_index=True)

    # -----------------------------
    # Lançar Atividade
    # -----------------------------
    elif aba == "Lançar Atividade":
        st.header("📝 Lançamento de Atividade (DAP Completa)")
        with st.form("form_lancamento"):
            data = st.date_input("Data", datetime.today())
            descricao = st.selectbox("Descrição da Atividade (Código - Título)", DESCRICOES)
            projeto = st.selectbox("Projeto/Alocação", PROJETOS)
            
            # Garante que a porcentagem é um valor entre 0 e 100
            porcentagem = st.slider("Porcentagem de Dedicação do Dia (0 a 100)", 0, 100, 100)
            
            observacao = st.text_area("Observação / Detalhamento da Atividade")
            if st.form_submit_button("Salvar Atividade"):
                salvar_atividade(st.session_state["usuario"], data, descricao, projeto, porcentagem, observacao)
                st.success("Atividade salva com sucesso!")
                # FIX: Remove o bloco 'global atividades_df' e usa st.rerun() para recarregar o script e o DataFrame
                st.rerun() 


    # -----------------------------
    # Minhas Atividades
    # -----------------------------
    elif aba == "Minhas Atividades":
        st.header("📊 Minhas Atividades Lançadas")
        minhas = atividades_df[atividades_df["usuario"] == st.session_state["usuario"]]
        
        if minhas.empty:
            st.info("Você ainda não lançou nenhuma atividade.")
        else:
            # Seleciona e renomeia as colunas para melhor visualização
            colunas_exibicao = minhas[['data', 'descricao', 'projeto', 'porcentagem', 'observacao']]
            colunas_exibicao.columns = ['Data', 'Descrição', 'Projeto', 'Percentual (%)', 'Observação']
            st.dataframe(colunas_exibicao, hide_index=True)
            
            st.download_button(
                "📥 Exportar Minhas Atividades CSV", 
                minhas.to_csv(index=False).encode('utf-8'), 
                "minhas_atividades.csv",
                mime="text/csv"
            )

    # -----------------------------
    # Consolidado (Admin)
    # -----------------------------
    elif aba == "Consolidado" and st.session_state["admin"]:
        st.header("📑 Consolidado Geral de Atividades")
        if atividades_df.empty:
            st.info("Ainda não há atividades lançadas na base de dados.")
        else:
            st.dataframe(atividades_df, hide_index=True)
            st.download_button(
                "📥 Exportar Consolidado CSV", 
                atividades_df.to_csv(index=False).encode('utf-8'), 
                "consolidado_geral.csv",
                mime="text/csv"
            )

    # -----------------------------
    # Validação (Acessível a Todos com Filtro de Visão)
    # -----------------------------
    elif aba == "Validação":
        
        if atividades_df.empty:
            st.warning("Não há atividades lançadas para realizar a validação.")
            return

        # 1. Definir o DataFrame a ser validado (Admin vê tudo, comum vê apenas o seu)
        if st.session_state["admin"]:
            st.header("✅ Validação de Porcentagem Mensal por Usuário (Visão Global)")
            st.info("Visão Administrativa: Mostra a **soma da porcentagem de atividades lançadas** por todos os usuários, agrupadas por Mês e Ano. O ideal é que a dedicação total do colaborador seja de **100%** em cada mês.")
            df_to_validate = atividades_df
            nome_export = "validacao_mensal_global.csv"
        else:
            st.header(f"✅ Validação de Suas Horas Mensais ({st.session_state['usuario']})")
            st.info("Esta tabela mostra a **soma da porcentagem de atividades lançadas** em seu nome, agrupadas por Mês e Ano. O ideal é que a dedicação total seja de **100%** em cada mês.")
            df_to_validate = atividades_df[atividades_df['usuario'] == st.session_state["usuario"]]
            nome_export = "validacao_mensal_pessoal.csv"
            
            if df_to_validate.empty:
                st.warning("Você ainda não lançou atividades suficientes para esta validação.")
                return 

        # 2. Preparar e agrupar os dados
        validacao_df = df_to_validate[['usuario', 'ano', 'mes', 'porcentagem']].copy()
        # Garante que porcentagem é numérica, tratando possíveis erros
        validacao_df['porcentagem'] = pd.to_numeric(validacao_df['porcentagem'], errors='coerce').fillna(0)


        # 3. Calcular o total de porcentagem por usuário e mês
        total_por_mes = validacao_df.groupby(['usuario', 'ano', 'mes'])['porcentagem'].sum().reset_index()
        total_por_mes.rename(columns={'porcentagem': 'Total_Porcentagem_Lancada'}, inplace=True)

        # 4. Formatar para exibição
        # Cria a coluna Mês/Ano e ordena
        total_por_mes['mes_ano'] = total_por_mes['mes'].astype(str).str.zfill(2) + '/' + total_por_mes['ano'].astype(str)
        total_por_mes = total_por_mes.sort_values(by=['ano', 'mes', 'usuario'], ascending=[False, False, True]) # Ordena do mais recente para o mais antigo
        
        
        # 5. Configurar a tabela de exibição
        if st.session_state["admin"]:
            tabela_final = total_por_mes[['usuario', 'mes_ano', 'Total_Porcentagem_Lancada']]
            tabela_final.columns = ['Usuário', 'Mês/Ano', 'Porcentagem Lançada']
        else:
            # Usuário comum só vê suas colunas relevantes
            tabela_final = total_por_mes[['mes_ano', 'Total_Porcentagem_Lancada']]
            tabela_final.columns = ['Mês/Ano', 'Porcentagem Lançada']


        # 6. Exibir o resultado com ProgressColumn para visualização
        column_config_dict = {
            "Porcentagem Lançada": st.column_config.ProgressColumn(
                "Porcentagem Lançada",
                help="Soma de todas as porcentagens de atividades no mês. O valor de referência é 100%.",
                format="%d%%",
                min_value=0,
                max_value=100,
                width="medium"
            ),
            "Mês/Ano": "Mês/Ano"
        }
        
        if st.session_state["admin"]:
             column_config_dict["Usuário"] = "Usuário"

        st.dataframe(
            tabela_final,
            hide_index=True,
            column_config=column_config_dict
        )

        st.download_button(
            "📥 Exportar Validação Mensal CSV",
            total_por_mes.to_csv(index=False).encode('utf-8'),
            nome_export,
            mime="text/csv"
        )
