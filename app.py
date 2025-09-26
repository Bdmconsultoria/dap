import streamlit as st
import pandas as pd
from datetime import datetime
import os
import uuid
import numpy as np

# ==============================
# Configurações iniciais
# ==============================
st.set_page_config(page_title="Controle de Atividades (CSV Local)", layout="wide")

# Arquivo para salvar usuários e atividades
USERS_FILE = "usuarios.csv"
ATIVIDADES_FILE = "atividades.csv"

# ==============================
# Funções de inicialização e carregamento
# ==============================

def initialize_files():
    """Inicializa os arquivos CSV se não existirem."""
    if not os.path.exists(USERS_FILE):
        df_users = pd.DataFrame(columns=["usuario", "senha", "admin"])
        df_users.to_csv(USERS_FILE, index=False)
        # Adiciona um usuário admin padrão para a primeira inicialização
        salvar_usuario_csv('admin', '123', True)
    
    if not os.path.exists(ATIVIDADES_FILE):
        # Adiciona a coluna 'id' para rastrear e permitir a exclusão
        df_activities = pd.DataFrame(columns=["id", "usuario", "data", "mes", "ano", "descricao", "projeto", "porcentagem", "observacao"])
        df_activities.to_csv(ATIVIDADES_FILE, index=False)

@st.cache_data(show_spinner=False)
def load_data():
    """Carrega os dados dos arquivos CSV."""
    try:
        usuarios_df = pd.read_csv(USERS_FILE)
        atividades_df = pd.read_csv(ATIVIDADES_FILE)
        # Garante que 'id' é um GUID para exclusão
        atividades_df['id'] = atividades_df['id'].astype(str)
    except Exception as e:
        st.error(f"Erro ao carregar dados CSV. Certifique-se de que os arquivos não estão corrompidos. {e}")
        usuarios_df = pd.DataFrame(columns=["usuario", "senha", "admin"])
        atividades_df = pd.DataFrame(columns=["id", "usuario", "data", "mes", "ano", "descricao", "projeto", "porcentagem", "observacao"])
    return usuarios_df, atividades_df

# Inicializa arquivos na primeira execução
initialize_files()

# Carrega DataFrames (usados globalmente na sessão)
usuarios_df, atividades_df = load_data()


# ==============================
# Funções auxiliares (CSV WRITE/VALIDATE)
# ==============================
def salvar_usuario_csv(usuario, senha, admin=False):
    """Salva um novo usuário no CSV."""
    global usuarios_df
    
    # Verifica se o usuário já existe
    if usuario in usuarios_df["usuario"].values:
        return False
        
    novo = pd.DataFrame([[usuario, senha, admin]], columns=["usuario", "senha", "admin"])
    usuarios_df = pd.concat([usuarios_df, novo], ignore_index=True)
    usuarios_df.to_csv(USERS_FILE, index=False)
    # Limpa o cache para o próximo carregamento
    load_data.clear()
    return True

def validar_login(usuario, senha):
    """Valida o login."""
    if usuario in usuarios_df["usuario"].values:
        row = usuarios_df.loc[usuarios_df["usuario"] == usuario].iloc[0]
        # Garante que 'admin' é tratado como booleano
        is_admin = str(row.get("admin", False)).lower() in ('true', '1', 'True')
        if row["senha"] == senha:
            return True, is_admin
    return False, False

def salvar_atividade_csv(usuario, data, descricao, projeto, porcentagem, observacao):
    """Salva uma nova atividade no CSV."""
    global atividades_df
    
    mes = data.month
    ano = data.year
    
    # Gera um ID único para a atividade
    new_id = str(uuid.uuid4())
    
    nova = pd.DataFrame(
        [[new_id, usuario, data.strftime("%d/%m/%Y"), mes, ano, descricao, projeto, porcentagem, observacao]],
        columns=["id", "usuario", "data", "mes", "ano", "descricao", "projeto", "porcentagem", "observacao"]
    )
    atividades_df = pd.concat([atividades_df, nova], ignore_index=True)
    atividades_df.to_csv(ATIVIDADES_FILE, index=False)
    # Limpa o cache para o próximo carregamento
    load_data.clear()

def deletar_atividade_csv(activity_id):
    """Deleta uma atividade do CSV usando o ID."""
    global atividades_df
    
    # Filtra o DataFrame para remover a linha com o ID correspondente
    # Garante que a coluna 'id' no DataFrame e o activity_id de entrada são strings
    atividades_df = atividades_df[atividades_df['id'].astype(str) != str(activity_id)]
    
    # Salva o DataFrame modificado de volta no arquivo
    atividades_df.to_csv(ATIVIDADES_FILE, index=False)
    # Limpa o cache para o próximo carregamento
    load_data.clear()


# ==============================
# Listas fixas
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
    st.info("Dados salvos localmente em arquivos CSV. Usuário padrão: **admin** / **123**.")
    usuario_input = st.text_input("Usuário")
    senha_input = st.text_input("Senha", type="password")
    
    if st.button("Entrar"):
        ok, admin = validar_login(usuario_input, senha_input)
        if ok:
            st.session_state["usuario"] = usuario_input
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

    abas = ["Lançar Atividade", "Minhas Atividades", "Validação"]
    
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
                if salvar_usuario_csv(novo_usuario, nova_senha, admin_check):
                    st.success("Usuário adicionado!")
                else:
                    st.warning("Usuário já existe.")
                st.rerun() 

        st.subheader("Usuários cadastrados")
        st.dataframe(usuarios_df, hide_index=True)

    # -----------------------------
    # Lançar Atividade
    # -----------------------------
    elif aba == "Lançar Atividade":
        st.header("📝 Lançamento de Atividade (DAP Completa)")
        with st.form("form_lancamento"):
            data_input = st.date_input("Data", datetime.today())
            descricao = st.selectbox("Descrição da Atividade (Código - Título)", DESCRICOES)
            projeto = st.selectbox("Projeto/Alocação", PROJETOS)
            
            porcentagem = st.slider("Porcentagem de Dedicação do Dia (0 a 100)", 0, 100, 100)
            
            observacao = st.text_area("Observação / Detalhamento da Atividade")
            if st.form_submit_button("Salvar Atividade"):
                salvar_atividade_csv(st.session_state["usuario"], data_input, descricao, projeto, porcentagem, observacao)
                st.success("Atividade salva!")
                st.rerun() 


    # -----------------------------
    # Minhas Atividades (Com Exclusão)
    # -----------------------------
    elif aba == "Minhas Atividades":
        st.header("📊 Minhas Atividades Lançadas")
        
        minhas = atividades_df[atividades_df["usuario"] == st.session_state["usuario"]].copy()

        if minhas.empty:
            st.info("Você ainda não lançou nenhuma atividade.")
        else:
            # Seleciona e renomeia as colunas para melhor visualização
            minhas = minhas.sort_values(by='data', ascending=False)
            
            st.markdown("---")
            st.subheader("Atividades para Exclusão:")
            st.warning("Para apagar, clique no botão '🗑️ Apagar' ao lado da atividade.")

            # Itera sobre as atividades para criar botões de exclusão
            for index, row in minhas.iterrows():
                cols = st.columns([0.1, 0.2, 0.4, 0.15, 0.15])
                
                # Exibe a data, descrição, projeto e porcentagem em colunas
                cols[0].write(row['data'])
                cols[1].write(row['descricao'].split(" - ")[-1]) # Exibe só a descrição
                cols[2].write(row['projeto'])
                cols[3].write(f"{row['porcentagem']}%")
                
                # Botão de Exclusão
                delete_button_key = f"delete_{row['id']}"
                if cols[4].button("🗑️ Apagar", key=delete_button_key):
                    deletar_atividade_csv(row['id'])
                    st.success(f"Atividade de {row['data']} apagada.")
                    st.rerun() # Recarrega a página para atualizar a lista
                
                # Expander para Observação
                with st.expander("Ver Observação Completa", expanded=False):
                    st.text(row['observacao'])
                
                st.markdown("---")

            # Botão de exportação no final da lista
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
        df_consolidado = atividades_df.drop(columns=['id'], errors='ignore')

        if df_consolidado.empty:
            st.info("Ainda não há atividades lançadas na base de dados.")
        else:
            st.dataframe(df_consolidado, hide_index=True)
            st.download_button(
                "📥 Exportar Consolidado CSV", 
                df_consolidado.to_csv(index=False).encode('utf-8'), 
                "consolidado_geral.csv",
                mime="text/csv"
            )

    # -----------------------------
    # Validação (Acessível a Todos com Filtro de Visão)
    # -----------------------------
    elif aba == "Validação":
        
        df_base = atividades_df.copy()
        
        if df_base.empty:
            st.warning("Não há atividades lançadas para realizar a validação.")
            st.stop() 

        # 1. Definir o DataFrame a ser validado (Admin vê tudo, comum vê apenas o seu)
        if st.session_state["admin"]:
            st.header("✅ Validação de Porcentagem Mensal por Usuário (Visão Global)")
            st.info("Visão Administrativa: Mostra a **soma da porcentagem de atividades lançadas** por todos os usuários, agrupadas por Mês e Ano. O ideal é que a dedicação total do colaborador seja de **100%** em cada mês.")
            df_to_validate = df_base
            nome_export = "validacao_mensal_global.csv"
        else:
            st.header(f"✅ Validação de Suas Horas Mensais ({st.session_state['usuario']})")
            st.info("Esta tabela mostra a **soma da porcentagem de atividades lançadas** em seu nome, agrupadas por Mês e Ano. O ideal é que a dedicação total seja de **100%** em cada mês.")
            df_to_validate = df_base[df_base['usuario'] == st.session_state["usuario"]]
            nome_export = "validacao_mensal_pessoal.csv"
            
            if df_to_validate.empty:
                st.warning("Você ainda não lançou atividades suficientes para esta validação.")
                st.stop() # Interrompe a execução para usuários sem dados

        # 2. Preparar e agrupar os dados
        validacao_df = df_to_validate[['usuario', 'ano', 'mes', 'porcentagem']].copy()
        validacao_df['porcentagem'] = pd.to_numeric(validacao_df['porcentagem'], errors='coerce').fillna(0)


        # 3. Calcular o total de porcentagem por usuário e mês
        total_por_mes = validacao_df.groupby(['usuario', 'ano', 'mes'])['porcentagem'].sum().reset_index()
        total_por_mes.rename(columns={'porcentagem': 'Total_Porcentagem_Lancada'}, inplace=True)

        # 4. Formatar para exibição
        total_por_mes['mes_ano'] = total_por_mes['mes'].astype(str).str.zfill(2) + '/' + total_por_mes['ano'].astype(str)
        total_por_mes = total_por_mes.sort_values(by=['ano', 'mes', 'usuario'], ascending=[False, False, True])
        
        
        # 5. Configurar a tabela de exibição
        if st.session_state["admin"]:
            tabela_final = total_por_mes[['usuario', 'mes_ano', 'Total_Porcentagem_Lancada']]
            tabela_final.columns = ['Usuário', 'Mês/Ano', 'Porcentagem Lançada']
        else:
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
