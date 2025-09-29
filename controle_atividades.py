import streamlit as st
import pandas as pd
from datetime import datetime
import psycopg2
import psycopg2.extras # Importação necessária para inserção em massa
import plotly.express as px
import io # Importação necessária para ler arquivos carregados

# ==============================
# 1. Credenciais PostgreSQL
# ==============================
# Nota: st.secrets deve estar configurado no seu ambiente Streamlit
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
    """Tenta estabelecer a conexão com o banco de dados e retorna o objeto de conexão."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        # st.error(f"❌ Erro ao conectar ao banco de dados: {e}")
        return None

# ==============================
# 3. Setup do Banco (criação de tabelas)
# ==============================
def setup_db():
    """Cria as tabelas 'usuarios' e 'atividades' se elas não existirem."""
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
# 4. CRUD e Consultas
# ==============================
def salvar_usuario(usuario, senha, admin=False):
    """Salva um novo usuário (ou ignora se já existir)."""
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
    """Verifica as credenciais de login e retorna status e privilégio de admin."""
    conn = get_db_connection()
    if conn is None: return False, False
    try:
        with conn.cursor() as cursor:
            # ATENÇÃO: Em um ambiente de produção, a senha NÃO deve ser armazenada em texto puro.
            # Use hashing (ex: bcrypt) para segurança.
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

def calcular_porcentagem_existente(usuario, data):
    """Calcula a soma das porcentagens de atividades já registradas para o usuário na data."""
    conn = get_db_connection()
    if conn is None:
        # Se falhar, retorna um valor alto para impedir o lançamento e forçar o erro no UI.
        return 101 
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COALESCE(SUM(porcentagem), 0)
                FROM atividades
                WHERE usuario = %s AND data = %s;
            """, (usuario, data))
            result = cursor.fetchone()
            # COALESCE garante que se não houver atividades, o resultado será 0.
            return result[0] if result else 0 
    except Exception as e:
        st.error(f"Erro ao calcular porcentagem existente: {e}")
        return 101 # Retorna 101 em caso de erro no DB para impedir lançamento
    finally:
        if conn:
            conn.close()

def salvar_atividade(usuario, data, descricao, projeto, porcentagem, observacao):
    """Salva uma nova atividade no banco de dados."""
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

def apagar_atividade(atividade_id):
    """Apaga uma atividade específica pelo ID."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM atividades WHERE id = %s;", (atividade_id,))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao apagar atividade: {e}")
        return False
    finally:
        conn.close()

def carregar_dados():
    """Carrega todos os usuários e atividades do banco de dados para DataFrames."""
    conn = get_db_connection()
    if conn is None: 
        return pd.DataFrame(), pd.DataFrame()
    try:
        usuarios_df = pd.read_sql("SELECT usuario, admin FROM usuarios;", conn)
        atividades_df = pd.read_sql("""
            SELECT id, usuario, data, mes, ano, descricao, projeto, porcentagem, observacao 
            FROM atividades ORDER BY data DESC;
        """, conn)
        
        # CORREÇÃO: Converter a coluna 'data' para datetime para permitir o uso do acessor .dt no Pandas
        if not atividades_df.empty:
            atividades_df['data'] = pd.to_datetime(atividades_df['data'])
            
        return usuarios_df, atividades_df
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame(), pd.DataFrame()
    finally:
        conn.close()

def bulk_insert_usuarios(user_list):
    """Insere usuários inexistentes no banco de dados. Senha padrão: '123'."""
    conn = get_db_connection()
    if conn is None:
        return 0, "❌ Falha na conexão com o banco de dados."
    
    # Preparar a lista de tuplas (usuario, senha padrão, admin=False)
    # Todos os usuários importados terão a senha '123' e não serão administradores por padrão.
    data_list = [(user, '123', False) for user in user_list]

    query = """
        INSERT INTO usuarios (usuario, senha, admin)
        VALUES (%s, %s, %s)
        ON CONFLICT (usuario) DO NOTHING
    """
    
    try:
        with conn.cursor() as cursor:
            # Usar execute_batch para inserção eficiente
            psycopg2.extras.execute_batch(cursor, query, data_list)
            # rowcount retorna o número de linhas realmente afetadas (inseridas)
            inserted_count = cursor.rowcount
            conn.commit()
            return inserted_count, "✅ Sucesso! Usuários pré-cadastrados com êxito."
    except Exception as e:
        conn.rollback()
        return 0, f"❌ Erro durante o pré-cadastro de usuários: {e}"
    finally:
        conn.close()


def bulk_insert_atividades(df_to_insert):
    """Insere atividades em massa no banco de dados usando psycopg2.extras.execute_batch."""
    conn = get_db_connection()
    if conn is None:
        return 0, "❌ Falha na conexão com o banco de dados."
    
    # 1. Preparar os dados para inserção
    # O DataFrame deve ter as colunas na ordem correta: (usuario, data, mes, ano, descricao, projeto, porcentagem, observacao)
    data_list = [tuple(row) for row in df_to_insert[[
        'usuario', 'data', 'mes', 'ano', 'descricao', 'projeto', 'porcentagem', 'observacao'
    ]].values]

    query = """
        INSERT INTO atividades (usuario, data, mes, ano, descricao, projeto, porcentagem, observacao)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        with conn.cursor() as cursor:
            # Usar execute_batch para inserção eficiente
            psycopg2.extras.execute_batch(cursor, query, data_list)
            conn.commit()
            return len(data_list), "✅ Sucesso! Dados importados com êxito."
    except Exception as e:
        conn.rollback()
        return 0, f"❌ Erro durante a importação em massa: {e}"
    finally:
        conn.close()


# ==============================
# 5. Dados fixos
# ==============================
DESCRICOES = ["1.001 - Gestão","1.002 - Geral","1.003 - Conselho","1.004 - Treinamento e Desenvolvimento",
              "2.001 - Gestão do administrativo","2.002 - Administrativa","2.003 - Jurídica","2.004 - Financeira",
              "2.006 - Fiscal","2.007 - Infraestrutura TI","2.008 - Treinamento interno","2.011 - Análise de dados",
              "2.012 - Logística de viagens","2.013 - Prestação de contas","3.001 - Prospecção de oportunidades",
              "3.002 - Prospecção de temas","3.003 - Administração comercial","3.004 - Marketing Digital",
              "3.005 - Materiais de apoio","3.006 - Grupos de Estudo","3.007 - Elaboração de POC/Piloto",
              "3.008 - Elaboração e apresentação de proposta","3.009 - Acompanhamento de proposta",
              "3.010 - Reunião de acompanhamento de funil","3.011 - Planejamento Estratégico/Comercial",
              "3.012 - Sucesso do Cliente","3.013 - Participação em eventos","4.001 - Planejamento de projeto",
              "4.002 - Gestão de projeto","4.003 - Reuniões internas de trabalho","4.004 - Reuniões externas de trabalho",
              "4.005 - Pesquisa","4.006 - Especificação de software","4.007 - Desenvolvimento de software/rotinas",
              "4.008 - Coleta e preparação de dados","4.009 - Elaboração de estudos e modelos","4.010 - Confecção de relatórios técnicos",
              "4.011 - Confecção de apresentações técnicas","4.012 - Confecção de artigos técnicos","4.013 - Difusão de resultados",
              "4.014 - Elaboração de documentação final","4.015 - Finalização do projeto","5.001 - Gestão de desenvolvimento",
              "5.002 - Planejamento de projeto","5.003 - Gestão de projeto","5.004 - Reuniões internas de trabalho",
              "5.005 - Reuniões externa de trabalho","5.006 - Pesquisa","5.007 - Coleta e preparação de dados",
              "5.008 - Modelagem","5.009 - Análise de tarefa","5.010 - Especificação de tarefa","5.011 - Correção de bug",
              "5.012 - Desenvolvimento de melhorias","5.013 - Desenvolvimento de novas funcionalidades",
              "5.014 - Desenvolvimento de integrações","5.015 - Treinamento interno","5.016 - Documentação",
              "5.017 - Atividades gerenciais","5.018 - Estudos","6.001 - Gestão de equipe","6.002 - Pesquisa",
              "6.003 - Especificação de testes","6.004 - Desenvolvimento de automações","6.005 - Realização de testes",
              "6.006 - Reuniões internas de trabalho","6.007 - Treinamento interno","6.008 - Elaboração de material",
              "7.001 - Gestão de equipe","7.002 - Pesquisa e estudos","7.003 - Análise de ticket","7.004 - Reuniões internas de trabalho",
              "7.005 - Reuniões externas de trabalho","7.006 - Preparação de treinamento externo","7.007 - Realização de treinamento externo",
              "7.008 - Documentação de treinamento","7.009 - Treinamento interno","7.010 - Criação de tarefa","9.001 - Gestão do RH",
              "9.002 - Recrutamento e seleção","9.003 - Participação em eventos","9.004 - Pesquisa e estratégia","9.005 - Treinamento e desenvolvimento",
              "9.006 - Registro de feedback","9.007 - Avaliação de RH","9.008 - Elaboração de conteúdo","9.009 - Comunicação interna",
              "9.010 - Reuniões internas de trabalho","9.011 - Reunião externa","9.012 - Apoio contábil e financeiro","10.001 - Planejamento de operação",
              "10.002 - Gestão de operação","10.003 - Reuniões internas de trabalho","10.004 - Reuniões externas de trabalho",
              "10.005 - Especificação de melhoria ou correção de software","10.006 - Desenvolvimento de automações",
              "10.007 - Coleta e preparação de dados","10.008 - Elaboração de estudos e modelos","10.009 - Confecção de relatórios técnicos",
              "10.010 - Confecção de apresentações técnicas","10.011 - Confecção de artigos técnicos","10.012 - Difusão de resultados",
              "10.013 - Preparação de treinamento externo","10.014 - Realização de treinamento externo","10.015 - Mapeamento de Integrações"]

PROJETOS = ["101-0 (Interno) Diretoria Executiva","102-0 (Interno) Diretoria Administrativa","103-0 (Interno) Diretoria de Engenharia",
            "104-0 (Interno) Diretoria de Negócios","105-0 (Interno) Diretoria de Produtos","106-0 (Interno) Diretoria de Tecnologia",
            "107-0 (Interno) Departamento Administrativo","108-0 (Interno) Departamento de Gente e Cultura","109-0 (Interno) Departamento de Infraestrutura",
            "110-0 (Interno) Departamento de Marketing","111-0 (Interno) Departamento de Operação","112-0 (Interno) Departamento de Sucesso do Cliente",
            "113-0 (Interno) Produto ARIES","114-0 (Interno) Produto ActionWise","115-0 (Interno) Produto Carga Base","116-0 (Interno) Produto Godel Perdas",
            "117-0 (Interno) Produto Godel Conecta","118-0 (Interno) Produto SIGPerdas","119-0 (Interno) Produto SINAPgrid","120-0 (Interno) Produto SINAP4.0",
            "121-0 (Interno) SINAPgrid Acadêmico","122-0 (Interno) Produto SINAPgateway (BAGRE)","123-0 (Interno) Produto SINAPautomação e diagnóstico (autobatch)",
            "302-0 (SENSE - Equatorial) Virtus","402-0 (SOFTEX - Copel) Renovação de Ativos Continuação","573-1 (ENEL) Suporte SINAPgrid",
            "573-2 (ENEL) Re-configuração","575-0 (Amazonas) Suporte SINAPgrid","578-1 (Copel) Suporte SINAPgrid","578-2 (Copel) Suporte Godel Conecta",
            "578-3 (Copel) Suporte GDIS","581-0 (CERILUZ) Suporte SINAPgrid","583-0 (CERTAJA) Suporte SINAPgrid","584-0 (CERTEL) Suporte SINAPgrid",
            "585-0 (COOPERLUZ) Suporte SINAPgrid","587-0 (COPREL) Suporte SINAPgrid","606-0 (Roraima) Suporte SINAPgrid","615-0 (Energisa) Suporte SIGPerdas",
            "620-1 (CPFL) Suporte SINAPgrid","638-1 (Amazonas) Suporte SIGPerdas","638-2 (Roraima) Suporte SIGPerdas","640-0 (SENAI - CTG) Hidrogênio Verde",
            "647-0 (Energisa) Consultoria de Estudos Elétricos","648-0 (Neoenergia) Suporte SINAPgrid","649-0 (Neoenergia) Godel PCom e Godel Analytics",
            "653-0 (Roraima) Projeto Gestor GDIS","655-0 (CELESC) Sistema Integrável de Matchmaking","658-0 (Copel) Planauto Continuação",
            "659-0 (Copel) Cálculo de Benefícios de Investimentos","660-0 (CERFOX) Suporte SINAPgrid","661-0 (ENEL SP, RJ e CE) Consultoria técnica BDGD",
            "663-0 (Banco Mundial) Eletromobilidade em São Paulo","666-0 (Energisa) Análise MM GD","667-0 (Energisa) Planejamento Decenal MT",
            "668-0 (Energisa) Critérios de Planejamento de SEs","669-0 (Desenvolve SP) Hub de Dados","670-0 (CPFL) Proteção","671-0 (Equatorial) Godel Perdas",
            "672-0 (ENEL SP) URD Subterrâneo","673-0 (Equatorial) PDD","674-0 (Energisa PB) Planejamento Decenal 2025","675-0 (EDEMSA) Godel Perdas Suporte Técnico Bromteck",
            "676-0 (Equatorial) PoC Resiliência","677-0 (Neoenergia) Suporte Godel Perdas","678-0 (CPFL) AMBAR","679-0 (ENEL) Godel Conecta",
            "680-0 (CESI) Angola Banco Mundial","681-0 (CEMACON) Suporte SINAPgrid","682-0 (FECOERGS) Treinamento SINAPgrid"]

# ==============================
# 6. Sessão
# ==============================
if "usuario" not in st.session_state:
    st.session_state["usuario"] = None
    st.session_state["admin"] = False

# Carrega os dados sempre que o estado de sessão muda ou a página recarrega
usuarios_df, atividades_df = carregar_dados()

# ==============================
# 7. Login e Navegação
# ==============================
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

    abas = ["Lançar Atividade", "Minhas Atividades"]
    if st.session_state["admin"]:
        # Adiciona a aba de importação de dados
        abas += ["Gerenciar Usuários", "Consolidado", "Importar Dados"]

    aba = st.sidebar.radio("Menu", abas)

    # ==============================
    # Gerenciar Usuários
    # ==============================
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

    # ==============================
    # Lançar Atividade (Com Validação de 100%)
    # ==============================
    elif aba == "Lançar Atividade":
        st.header("📝 Lançar Atividade")
        with st.form("form_atividade"):
            data = st.date_input("Data", datetime.today())
            descricao = st.selectbox("Descrição", DESCRICOES)
            projeto = st.selectbox("Projeto", PROJETOS)
            # A porcentagem mínima deve ser 1 para evitar lançamentos vazios
            porcentagem = st.slider("Porcentagem", 1, 100, 100) 
            observacao = st.text_area("Observação")
            
            if st.form_submit_button("Salvar"):
                if observacao.strip():
                    
                    # --- VALIDAÇÃO DE 100% DIÁRIO ---
                    # 1. Obter a soma das porcentagens já lançadas para o dia e usuário
                    total_existente = calcular_porcentagem_existente(st.session_state["usuario"], data)
                    novo_total = total_existente + porcentagem

                    # 2. Verificar se o novo total excede 100%
                    if novo_total > 100:
                        st.error(
                            f"⚠️ **Alocação Excedida!** O total de porcentagem lançado para **{data.strftime('%d/%m/%Y')}** "
                            f"é de **{total_existente}%**. A nova atividade de **{porcentagem}%** faria o total ser **{novo_total}%**, "
                            f"que excede o limite de 100%."
                        )
                    else:
                        # 3. Salvar se a validação passar
                        if salvar_atividade(st.session_state["usuario"], data, descricao, projeto, porcentagem, observacao):
                            # Se for 100%, mostra uma mensagem especial.
                            if novo_total == 100:
                                st.balloons()
                                st.success("🎉 Atividade salva! Você completou a alocação de 100% para este dia.")
                            else:
                                st.success(f"Atividade salva! Total alocado no dia: {novo_total}%.")
                            st.experimental_rerun()
                            
                else:
                    st.error("A observação é obrigatória.")

    # ==============================
    # Minhas Atividades
    # ==============================
    elif aba == "Minhas Atividades":
        st.header("📊 Minhas Atividades")
        minhas = atividades_df[atividades_df["usuario"] == st.session_state["usuario"]]
        if minhas.empty:
            st.info("Nenhuma atividade encontrada.")
        else:
            # Filtro por Mês
            # 'data' agora é um tipo datetime, então o .dt funciona
            minhas['data_mes'] = minhas['data'].dt.strftime('%Y-%m')
            meses_disponiveis = minhas['data_mes'].unique()
            mes_selecionado = st.selectbox("Filtrar por mês/ano", sorted(meses_disponiveis, reverse=True))
            df_filtro = minhas[minhas['data_mes'] == mes_selecionado].sort_values(by='data', ascending=False)
            
            st.markdown("---")

            # Lista de Atividades
            for idx, row in df_filtro.iterrows():
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.markdown(f"📅 **{row['data'].strftime('%d/%m/%Y')}** - **{row['descricao']}** ({row['porcentagem']}%)")
                    st.markdown(f"**Projeto:** *{row['projeto']}*")
                    st.markdown(f"**Obs:** {row['observacao']}")
                with col2:
                    if col2.button("🗑️ Apagar", key=f"del_{row['id']}"):
                        if apagar_atividade(row['id']):
                            st.success("Atividade apagada!")
                            st.experimental_rerun()
                st.markdown("---")

            # Gráfico de pizza
            st.subheader(f"Distribuição de Projetos - {mes_selecionado}")
            df_agrupado_projeto = df_filtro.groupby('projeto')['porcentagem'].sum().reset_index()
            fig_projeto = px.pie(
                df_agrupado_projeto, 
                names='projeto', 
                values='porcentagem', 
                title='Alocação por Projeto no Mês',
                hole=.3,
            )
            st.plotly_chart(fig_projeto, use_container_width=True)

    # ==============================
    # Consolidado para Admin
    # ==============================
    elif aba == "Consolidado" and st.session_state["admin"]:
        st.header("📑 Consolidado Geral de Atividades")
        
        if atividades_df.empty:
            st.info("Nenhuma atividade lançada no sistema.")
        else:
            # Filtros Admin
            col_admin1, col_admin2, col_admin3 = st.columns(3)
            
            usuarios_unicos = sorted(atividades_df['usuario'].unique())
            usuario_selecionado = col_admin1.selectbox("Filtrar por Usuário", ["Todos"] + usuarios_unicos)
            
            # 'data' agora é um tipo datetime, então o .dt funciona
            atividades_df['data_mes'] = atividades_df['data'].dt.strftime('%Y-%m')
            meses_unicos = sorted(atividades_df['data_mes'].unique(), reverse=True)
            mes_selecionado_admin = col_admin2.selectbox("Filtrar por Mês/Ano", ["Todos"] + meses_unicos)
            
            df_consolidado = atividades_df.copy()

            if usuario_selecionado != "Todos":
                df_consolidado = df_consolidado[df_consolidado['usuario'] == usuario_selecionado]
            
            if mes_selecionado_admin != "Todos":
                df_consolidado = df_consolidado[df_consolidado['data_mes'] == mes_selecionado_admin]

            st.markdown("---")
            
            if not df_consolidado.empty:
                st.subheader("Visualização dos Dados Filtrados")
                
                # Gráfico de Barras: % alocada por dia para o usuário/mês filtrado
                df_diario = df_consolidado.groupby(['data'])['porcentagem'].sum().reset_index()
                df_diario.columns = ['Data', 'Total Alocado (%)']
                
                fig_diario = px.bar(
                    df_diario, 
                    x='Data', 
                    y='Total Alocado (%)', 
                    title=f"Total de Porcentagem Alocada por Dia",
                    color='Total Alocado (%)',
                    color_continuous_scale=px.colors.sequential.Plotly3,
                    height=400
                )
                fig_diario.add_hline(y=100, line_dash="dash", line_color="red", annotation_text="100% Ideal", annotation_position="top left")
                st.plotly_chart(fig_diario, use_container_width=True)
                
                # Tabela de dados detalhada
                st.subheader("Tabela de Dados Detalhada")
                st.dataframe(df_consolidado.drop(columns=['data_mes']), use_container_width=True)

            else:
                st.info("Nenhum dado encontrado com os filtros selecionados.")
    
    # ==============================
    # Importar Dados (Admin)
    # ==============================
    elif aba == "Importar Dados" and st.session_state["admin"]:
        st.header("⬆️ Importação de Dados em Massa (Admin)")
        st.warning(
            "⚠️ **Aviso de Data:** Seu arquivo CSV contém apenas Mês e Ano. O sistema usará o **dia 1** "
            "do mês para preencher o campo `data` (data) no banco de dados. "
            "A porcentagem será multiplicada por 100 (ex: 0.25 -> 25%)."
        )
        
        uploaded_file = st.file_uploader("Carregar arquivo CSV ou XLSX com lançamentos", type=["csv", "xlsx"])
        
        if uploaded_file:
            try:
                # 1. Leitura do Arquivo
                if uploaded_file.name.endswith('.csv'):
                    df_import = pd.read_csv(uploaded_file, sep=None, engine='python', encoding='utf-8')
                elif uploaded_file.name.endswith('.xlsx'):
                    # Tenta ler como CSV com delimitador comum se o arquivo for renomeado
                     df_import = pd.read_csv(uploaded_file, sep=',', encoding='utf-8')

                st.subheader("Pré-visualização dos Dados Carregados")
                st.dataframe(df_import.head())
                
                # 2. Renomear e Mapear Colunas
                colunas_mapeamento = {
                    'Nome': 'usuario',
                    'Mês': 'mes',
                    'Ano': 'ano',
                    'Descrição': 'descricao',
                    'Projeto': 'projeto',
                    'Porcentagem': 'porcentagem',
                    'Observação (Opcional)': 'observacao'
                }
                
                df_import.rename(columns=colunas_mapeamento, inplace=True)
                
                # Garantir que a coluna 'usuario' exista após a renomeação
                if 'usuario' not in df_import.columns:
                    raise KeyError("'Nome' ou 'usuario' não encontrada no arquivo após renomeação.")

                # 3. PRÉ-CADASTRO DE USUÁRIOS
                usuarios_csv = df_import['usuario'].unique().tolist()
                
                with st.spinner(f"Verificando e pré-cadastrando {len(usuarios_csv)} usuários..."):
                    
                    # Filtra usuários que já existem no banco para não tentar inserí-los
                    usuarios_existentes_db = usuarios_df['usuario'].tolist()
                    usuarios_para_inserir = [u for u in usuarios_csv if u not in usuarios_existentes_db]

                    if usuarios_para_inserir:
                        inserted_count, user_msg = bulk_insert_usuarios(usuarios_para_inserir)
                        st.info(f"Usuários encontrados no arquivo: **{len(usuarios_csv)}**. Novos usuários cadastrados: **{inserted_count}** (senha padrão: '123').")
                    else:
                        st.info(f"Todos os {len(usuarios_csv)} usuários do arquivo já estão cadastrados no sistema.")
                
                # 4. Limpeza e Transformação dos Dados de Atividade
                
                # a) Criar a coluna 'data'
                df_import['data'] = pd.to_datetime(df_import[['ano', 'mes']].assign(dia=1))
                
                # b) Converter 'porcentagem' (float decimal) para INT (0-100)
                df_import['porcentagem'] = (df_import['porcentagem'] * 100).round().astype(int)
                
                # c) Tratar observações nulas (NaN)
                df_import['observacao'].fillna('', inplace=True)

                # d) Garantir que apenas colunas necessárias e transformadas existam
                colunas_finais = ['usuario', 'data', 'mes', 'ano', 'descricao', 'projeto', 'porcentagem', 'observacao']
                df_para_inserir = df_import[colunas_finais]

                st.success(f"Pronto para importar **{len(df_para_inserir)}** registros de atividades.")
                
                # 5. Botão de Confirmação para Inserção das Atividades
                if st.button("Confirmar Importação de ATIVIDADES para o Banco de Dados", key="btn_import_final"):
                    with st.spinner('Importando dados de atividades em massa...'):
                        linhas_inseridas, mensagem = bulk_insert_atividades(df_para_inserir)
                    
                    if linhas_inseridas > 0:
                        st.success(f"🎉 {linhas_inseridas} registros de atividades importados com sucesso!")
                    else:
                        st.error(mensagem)
                    
                    # Recarrega os dados globais e o Streamlit
                    st.experimental_rerun()
                    
            except KeyError as e:
                st.error(f"❌ Erro: Uma coluna esperada não foi encontrada no arquivo. Verifique se as colunas estão corretas. Coluna ausente: **{e}**")
            except Exception as e:
                st.error(f"❌ Erro ao processar ou ler o arquivo: {e}")
