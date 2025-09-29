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

def calcular_porcentagem_existente(usuario, mes, ano, excluido_id=None):
    """
    Calcula a soma das porcentagens de atividades já registradas para o usuário no MÊS/ANO,
    excluindo opcionalmente uma atividade (usado na edição).
    """
    conn = get_db_connection()
    if conn is None:
        return 101 
    try:
        with conn.cursor() as cursor:
            query = """
                SELECT COALESCE(SUM(porcentagem), 0)
                FROM atividades
                WHERE usuario = %s AND mes = %s AND ano = %s
            """
            params = [usuario, mes, ano]
            
            if excluido_id is not None:
                query += " AND id != %s"
                params.append(excluido_id)
            
            cursor.execute(query + ";", params)
            result = cursor.fetchone()
            return result[0] if result else 0 
    except Exception as e:
        st.error(f"Erro ao calcular porcentagem existente: {e}")
        return 101 
    finally:
        if conn:
            conn.close()

def salvar_atividade(usuario, mes, ano, descricao, projeto, porcentagem, observacao, atividade_id=None):
    """Salva uma nova atividade ou atualiza uma existente (se atividade_id for fornecido)."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            # A data é o primeiro dia do mês para satisfazer o requisito DATE do PostgreSQL
            data_db = datetime(year=ano, month=mes, day=1).date()
            
            if atividade_id is None:
                # Inserir Nova Atividade
                query = """
                    INSERT INTO atividades (usuario, data, mes, ano, descricao, projeto, porcentagem, observacao)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
                params = (usuario, data_db, mes, ano, descricao, projeto, porcentagem, observacao)
            else:
                # Atualizar Atividade Existente
                query = """
                    UPDATE atividades
                    SET data = %s, mes = %s, ano = %s, descricao = %s, projeto = %s, porcentagem = %s, observacao = %s
                    WHERE id = %s;
                """
                params = (data_db, mes, ano, descricao, projeto, porcentagem, observacao, atividade_id)
            
            cursor.execute(query, params)
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar/editar atividade: {e}")
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

@st.cache_data(ttl=600)
def carregar_dados():
    """Carrega todos os usuários e atividades do banco de dados para DataFrames."""
    conn = get_db_connection()
    if conn is None: 
        return pd.DataFrame(), pd.DataFrame()
    try:
        usuarios_df = pd.read_sql("SELECT usuario, admin FROM usuarios;", conn)
        atividades_df = pd.read_sql("""
            SELECT id, usuario, data, mes, ano, descricao, projeto, porcentagem, observacao 
            FROM atividades ORDER BY ano DESC, mes DESC, data DESC;
        """, conn)
        
        # Converter a coluna 'data' para datetime para permitir o uso do acessor .dt no Pandas
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
    # A limpeza (strip) deve ser feita antes de chamar esta função.
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

def limpar_nomes_usuarios_db():
    """
    Executa comandos SQL para remover espaços em branco iniciais/finais
    nas colunas 'usuario' em ambas as tabelas (usuarios e atividades).
    """
    conn = get_db_connection()
    if conn is None: return False, "Falha na conexão com o banco de dados."
    
    try:
        with conn.cursor() as cursor:
            
            # 1. Atualiza a tabela ATIVIDADES para remover espaços na chave estrangeira
            cursor.execute("""
                UPDATE atividades
                SET usuario = TRIM(usuario);
            """)
            atividades_afetadas = cursor.rowcount
            
            # 2. Re-insere os usuários (agora limpos) na tabela USUARIOS
            # Esta é a parte complexa: o PostgreSQL não permite UPDATE na chave primária
            # que resulte em duplicidade. A melhor abordagem é recriar a lista.

            # Passo 2a: Coletar todos os nomes de usuários únicos e limpos da tabela atividades
            cursor.execute("""
                SELECT DISTINCT TRIM(usuario) FROM atividades;
            """)
            usuarios_limpos = [row[0] for row in cursor.fetchall()]
            
            # Passo 2b: Limpar a tabela usuarios dos nomes antigos com espaço
            # (Vamos apagar e reinserir para garantir que a PK seja limpa)
            cursor.execute("TRUNCATE TABLE usuarios CASCADE;") # Apaga todos os usuários e referências
            
            # Passo 2c: Reinserir todos os usuários limpos com a senha padrão '123'
            usuarios_para_reinserir = [(user, '123', False) for user in usuarios_limpos]
            
            if usuarios_para_reinserir:
                query_insert_users = """
                    INSERT INTO usuarios (usuario, senha, admin)
                    VALUES (%s, %s, %s)
                """
                psycopg2.extras.execute_batch(cursor, query_insert_users, usuarios_para_reinserir)
                usuarios_reinseridos = cursor.rowcount
            else:
                usuarios_reinseridos = 0


            conn.commit()
            return True, (
                f"✅ Sucesso! Limpeza concluída. "
                f"{atividades_afetadas} atividades e {usuarios_reinseridos} usuários foram corrigidos."
            )
            
    except Exception as e:
        conn.rollback()
        return False, f"❌ Erro ao limpar nomes no DB: {e}"
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

# Adiciona a opção vazia no início das listas para uso no selectbox
DESCRICOES_SELECT = ["--- Selecione ---"] + DESCRICOES
PROJETOS_SELECT = ["--- Selecione ---"] + PROJETOS

# Dados para Seleção de Mês e Ano (para o formulário)
MESES = {
    1: "01 - Janeiro", 2: "02 - Fevereiro", 3: "03 - Março", 4: "04 - Abril",
    5: "05 - Maio", 6: "06 - Junho", 7: "07 - Julho", 8: "08 - Agosto",
    9: "09 - Setembro", 10: "10 - Outubro", 11: "11 - Novembro", 12: "12 - Dezembro"
}
MESES_SELECT = ["--- Selecione ---"] + list(MESES.values())
ANOS = list(range(datetime.today().year - 2, datetime.today().year + 3)) # De 2 anos atrás a 2 anos adiante

# ==============================
# 6. Sessão
# ==============================
if "usuario" not in st.session_state:
    st.session_state["usuario"] = None
    st.session_state["admin"] = False
# Adiciona o estado para controlar a edição
if 'edit_id' not in st.session_state:
    st.session_state['edit_id'] = None

# Carrega os dados sempre que o estado de sessão muda ou a página recarrega
# Isso é essencial para que o Streamlit funcione de forma reativa.
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
            # Garante que o nome de login também é limpo, caso o usuário digite com espaço
            st.session_state["usuario"] = usuario.strip()
            st.session_state["admin"] = admin
            st.rerun() # SUBSTITUÍDO: st.experimental_rerun() -> st.rerun()
        else:
            st.error("Usuário ou senha incorretos")
else:
    st.sidebar.markdown(f"**Usuário:** {st.session_state['usuario']}")
    if st.sidebar.button("Sair"):
        st.session_state["usuario"] = None
        st.session_state["admin"] = False
        st.rerun() # SUBSTITUÍDO: st.experimental_rerun() -> st.rerun()

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
        
        # --- Ferramenta de Limpeza de Nomes (Admin) ---
        st.subheader("Ferramenta de Manutenção (Limpar Espaços)")
        st.warning(
            "Esta ação **REMOVE ESPAÇOS em branco iniciais/finais** de TODOS os nomes de usuários no DB, "
            "corrigindo problemas de login e de chaves estrangeiras. **Todos os usuários terão a senha redefinida para '123'.**"
        )
        if st.button("Executar Limpeza de Nomes de Usuário (TRIM)", key="btn_limpeza_db"):
            with st.spinner("Executando limpeza no banco de dados..."):
                sucesso, mensagem = limpar_nomes_usuarios_db()
            
            # Limpa o cache e recarrega para mostrar a tabela correta
            carregar_dados.clear()
            
            if sucesso:
                st.success(mensagem)
            else:
                st.error(mensagem)
            
            # Recarrega o Streamlit para forçar novo login se necessário
            st.rerun()


        # --- Formulário de Adição de Usuário ---
        st.subheader("Adicionar Novo Usuário")
        with st.form("form_add_user"):
            novo_usuario = st.text_input("Usuário")
            nova_senha = st.text_input("Senha", type="password")
            admin_check = st.checkbox("Admin")
            if st.form_submit_button("Adicionar"):
                # Aplica o strip na hora de adicionar o usuário
                if salvar_usuario(novo_usuario.strip(), nova_senha, admin_check):
                    st.success("Usuário adicionado!")
                    st.rerun() # SUBSTITUÍDO: st.experimental_rerun() -> st.rerun()
        
        # Tabela de Usuários
        usuarios_df_reloaded, _ = carregar_dados() # Recarrega para mostrar o estado atualizado
        st.subheader("Tabela de Usuários")
        st.dataframe(usuarios_df_reloaded, use_container_width=True)

    # ==============================
    # Lançar Atividade (Mensal)
    # ==============================
    elif aba == "Lançar Atividade":
        st.header("📝 Lançar Atividade (Mensal)")
        with st.form("form_atividade"):
            
            col_mes, col_ano = st.columns(2)
            
            # Mês e Ano Seletores
            mes_select = col_mes.selectbox("Mês", MESES_SELECT, index=list(MESES.values()).index(MESES[datetime.today().month]) + 1)
            ano_select = col_ano.selectbox("Ano", ANOS, index=ANOS.index(datetime.today().year))
            
            descricao = st.selectbox("Descrição", DESCRICOES_SELECT)
            projeto = st.selectbox("Projeto", PROJETOS_SELECT)
            # A porcentagem mínima deve ser 1 para evitar lançamentos vazios
            porcentagem = st.slider("Porcentagem (%)", 1, 100, 100) 
            observacao = st.text_area("Observação")
            
            if st.form_submit_button("Salvar Lançamento"):
                
                # Mapeia Mês de volta para o número
                mes_num = next((k for k, v in MESES.items() if v == mes_select), None)

                # 1. Validação de Seleção Vazias
                if mes_num is None or descricao == "--- Selecione ---" or projeto == "--- Selecione ---":
                    st.error("Por favor, selecione um Mês, Descrição e Projeto válidos.")
                    st.stop()
                
                # 2. Validação de Observação
                if not observacao.strip():
                    st.error("A observação é obrigatória.")
                    st.stop()
                    
                # --- VALIDAÇÃO DE 100% MENSAL ---
                # 3. Obter a soma das porcentagens já lançadas para o MÊS/ANO e usuário
                total_existente = calcular_porcentagem_existente(st.session_state["usuario"], mes_num, ano_select)
                novo_total = total_existente + porcentagem

                # 4. Verificar se o novo total excede 100%
                if novo_total > 100:
                    st.error(
                        f"⚠️ **Alocação Excedida!** O total de porcentagem lançado para **{mes_select}/{ano_select}** "
                        f"é de **{total_existente}%**. A nova atividade de **{porcentagem}%** faria o total ser **{novo_total}%**, "
                        f"que excede o limite de 100%."
                    )
                else:
                    # 5. Salvar se a validação passar
                    if salvar_atividade(st.session_state["usuario"], mes_num, ano_select, descricao, projeto, porcentagem, observacao):
                        # Invalida o cache para forçar a recarga dos dados na próxima execução
                        carregar_dados.clear()
                        
                        # Se for 100%, mostra uma mensagem especial.
                        if novo_total == 100:
                            st.balloons()
                            st.success(f"🎉 Atividade salva! Você completou a alocação de 100% para {mes_select}/{ano_select}.")
                        else:
                            st.success(f"Atividade salva! Total alocado em {mes_select}/{ano_select}: {novo_total}%.")
                        
                        st.rerun()
                            

    # ==============================
    # Minhas Atividades (com Edição e 2 Gráficos)
    # ==============================
    elif aba == "Minhas Atividades":
        st.header("📊 Minhas Atividades")
        minhas = atividades_df[atividades_df["usuario"] == st.session_state["usuario"]]
        
        if st.session_state['edit_id']:
            # --- MODAL DE EDIÇÃO ---
            st.subheader("✍️ Editar Atividade")
            
            # Pega os dados da atividade sendo editada
            atividade_edit = minhas[minhas['id'] == st.session_state['edit_id']].iloc[0]
            
            with st.form("form_edicao"):
                
                col_mes_edit, col_ano_edit = st.columns(2)
                
                # Encontra o índice da Descrição, Projeto, Mês e Ano para pré-selecionar no selectbox
                default_desc_idx = DESCRICOES_SELECT.index(atividade_edit['descricao'])
                default_proj_idx = PROJETOS_SELECT.index(atividade_edit['projeto'])
                default_mes_idx = MESES_SELECT.index(MESES[atividade_edit['mes']])
                default_ano_idx = ANOS.index(atividade_edit['ano'])
                
                mes_edit = col_mes_edit.selectbox("Mês", MESES_SELECT, index=default_mes_idx)
                ano_edit = col_ano_edit.selectbox("Ano", ANOS, index=default_ano_idx)
                
                descricao_edit = st.selectbox("Descrição", DESCRICOES_SELECT, index=default_desc_idx)
                projeto_edit = st.selectbox("Projeto", PROJETOS_SELECT, index=default_proj_idx)
                porcentagem_edit = st.slider("Porcentagem (%)", 1, 100, atividade_edit['porcentagem']) 
                observacao_edit = st.text_area("Observação", atividade_edit['observacao'])
                
                col_save, col_cancel = st.columns(2)
                
                if col_save.form_submit_button("Salvar Edição"):
                    
                    mes_num_edit = next((k for k, v in MESES.items() if v == mes_edit), None)

                    # 1. Validação de Seleção Vazias
                    if mes_num_edit is None or descricao_edit == "--- Selecione ---" or projeto_edit == "--- Selecione ---":
                        st.error("Por favor, selecione um Mês, Descrição e Projeto válidos.")
                        st.stop()
                    
                    # 2. Validação de Observação
                    if not observacao_edit.strip():
                        st.error("A observação é obrigatória.")
                        st.stop()
                        
                    # --- VALIDAÇÃO DE 100% MENSAL NA EDIÇÃO ---
                    # Exclui a porcentagem da própria atividade_edit
                    total_existente = calcular_porcentagem_existente(
                        st.session_state["usuario"], 
                        mes_num_edit,
                        ano_edit,
                        excluido_id=st.session_state['edit_id']
                    )
                    novo_total = total_existente + porcentagem_edit

                    if novo_total > 100:
                        st.error(
                            f"⚠️ **Alocação Excedida!** A edição faria o total ser **{novo_total}%**, "
                            f"que excede o limite de 100% para o mês {mes_edit}/{ano_edit}."
                        )
                    else:
                        # 3. Salvar Edição
                        if salvar_atividade(
                            st.session_state["usuario"], 
                            mes_num_edit, 
                            ano_edit, 
                            descricao_edit, 
                            projeto_edit, 
                            porcentagem_edit, 
                            observacao_edit, 
                            atividade_id=st.session_state['edit_id']
                        ):
                            carregar_dados.clear()
                            st.session_state['edit_id'] = None # Sai do modo de edição
                            st.success("Atividade editada com sucesso!")
                            st.rerun()
                
                if col_cancel.form_submit_button("Cancelar"):
                    st.session_state['edit_id'] = None
                    st.rerun()

            st.markdown("---") # Fim do Modal de Edição

        if minhas.empty:
            st.info("Nenhuma atividade encontrada.")
            st.stop() # Para a execução aqui se não houver dados
        
        # --- FILTROS E GRÁFICOS ---
        # Filtro por Mês
        minhas['data_mes'] = minhas['data'].dt.strftime('%Y-%m')
        meses_disponiveis = minhas['data_mes'].unique()
        mes_selecionado = st.selectbox("Filtrar por mês/ano", sorted(meses_disponiveis, reverse=True))
        df_filtro = minhas[minhas['data_mes'] == mes_selecionado].sort_values(by='data', ascending=False)
        
        st.markdown("---")
        
        # -----------------------------
        # NOVO GRÁFICO 1: Alocação Total Mensal por Projeto (Stacked Bar)
        # -----------------------------
        total_alocado_no_mes = df_filtro['porcentagem'].sum()
        
        st.subheader(f"Total de Alocação Mensal por Projeto - {mes_selecionado}")
        
        # Agrupa por projeto para a barra empilhada
        df_agrupado_projeto = df_filtro.groupby('projeto')['porcentagem'].sum().reset_index()
        df_agrupado_projeto.columns = ['Projeto', 'Porcentagem']
        df_agrupado_projeto['Categoria'] = 'Alocado'
        
        # Cria o restante (tempo não alocado) para completar 100%
        if total_alocado_no_mes < 100:
            df_restante = pd.DataFrame({
                'Projeto': ['Não Alocado'],
                'Porcentagem': [100 - total_alocado_no_mes],
                'Categoria': ['Restante']
            })
            df_final = pd.concat([df_agrupado_projeto, df_restante])
        elif total_alocado_no_mes == 100:
            df_final = df_agrupado_projeto
        else:
             # Se excedeu 100%, mostra apenas a barra do total alocado (que será maior que 100)
            df_final = df_agrupado_projeto


        fig_stacked = px.bar(
            df_final,
            x='Porcentagem',
            y=['Total Alocado'] * len(df_final), # Usa uma categoria única no eixo Y para empilhar
            color='Projeto',
            orientation='h',
            text='Porcentagem',
            title=f"Total: {total_alocado_no_mes}% de 100%",
            color_discrete_map={'Não Alocado': 'lightgray'}
        )

        fig_stacked.update_traces(texttemplate='%{text}%', textposition='inside')

        fig_stacked.update_layout(
            barmode='stack',
            showlegend=True,
            yaxis={'visible': False, 'showticklabels': False},
            xaxis={'range': [0, max(100, total_alocado_no_mes) + 5], 'title': ''}
        )

        # Adiciona a linha de 100%
        fig_stacked.add_vline(x=100, line_dash="dash", line_color="red", annotation_text="100% (Limite)", annotation_position="top right")

        st.plotly_chart(fig_stacked, use_container_width=True)
        # -----------------------------

        # Gráfico de pizza por Descrição (Requisito do Usuário)
        st.subheader(f"Distribuição Detalhada de Atividades - {mes_selecionado}")
        df_agrupado_descricao = df_filtro.groupby('descricao')['porcentagem'].sum().reset_index()
        fig_descricao = px.pie(
            df_agrupado_descricao, 
            names='descricao', 
            values='porcentagem', 
            title='Distribuição da Porcentagem Lançada no Mês',
            hole=.3,
        )
        st.plotly_chart(fig_descricao, use_container_width=True)
        
        st.subheader("Lançamentos Detalhados")

        # Lista de Atividades com botões de Ações
        for idx, row in df_filtro.iterrows():
            # Usa 3 colunas: Info, Editar e Apagar
            col1, col2, col3 = st.columns([3, 1, 1])
            with col1:
                # Exibe Mês/Ano em vez de Data diária
                mes_str = MESES.get(row['mes'], 'Mês Inválido')
                st.markdown(f"**ID {row['id']}** | 🗓️ **{mes_str}/{row['ano']}** | **{row['descricao']}** ({row['porcentagem']}%)")
                st.markdown(f"**Projeto:** *{row['projeto']}*")
                st.markdown(f"**Obs:** {row['observacao']}")
            
            with col2:
                # Botão Editar
                if col2.button("✍️ Editar", key=f"edit_{row['id']}"):
                    st.session_state['edit_id'] = row['id']
                    st.rerun()
            
            with col3:
                # Botão Apagar
                if col3.button("🗑️ Apagar", key=f"del_{row['id']}"):
                    if apagar_atividade(row['id']):
                        carregar_dados.clear()
                        st.success("Atividade apagada!")
                        st.rerun()
            st.markdown("---")


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
                
                # Gráfico de Barras: % alocada por MÊS para o usuário/mês filtrado
                # Agrupa por MÊS/ANO para visualização
                df_mensal = df_consolidado.groupby(['data_mes'])['porcentagem'].sum().reset_index()
                df_mensal.columns = ['Mês/Ano', 'Total Alocado (%)']
                
                fig_mensal = px.bar(
                    df_mensal, 
                    x='Mês/Ano', 
                    y='Total Alocado (%)', 
                    title=f"Total de Porcentagem Alocada por Mês",
                    color='Total Alocado (%)',
                    color_continuous_scale=px.colors.sequential.Plotly3,
                    height=400
                )
                fig_mensal.add_hline(y=100, line_dash="dash", line_color="red", annotation_text="100% Ideal", annotation_position="top left")
                st.plotly_chart(fig_mensal, use_container_width=True)
                
                # Tabela de dados detalhada
                st.subheader("Tabela de Dados Detalhada")
                st.dataframe(df_consolidado.drop(columns=['data_mes']), use_container_width=True)

                # --- Novo Bloco de Download ---
                st.markdown("---")
                
                # Renomeia as colunas para o arquivo final
                df_download = df_consolidado.drop(columns=['id', 'data_mes']).rename(columns={
                    'usuario': 'Usuário',
                    'data': 'Data (Dia 1 do Mês)',
                    'mes': 'Mês',
                    'ano': 'Ano',
                    'descricao': 'Descrição',
                    'projeto': 'Projeto',
                    'porcentagem': 'Porcentagem',
                    'observacao': 'Observação'
                })

                # Cria um buffer em memória para o arquivo Excel
                buffer = io.BytesIO()
                df_download.to_excel(buffer, index=False, sheet_name='Atividades Consolidadas')
                buffer.seek(0) # Retorna o ponteiro para o início do arquivo

                # Botão de Download
                st.download_button(
                    label="⬇️ Baixar Dados Filtrados (Excel)",
                    data=buffer,
                    file_name=f"atividades_consolidado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                # -----------------------------

            else:
                st.info("Nenhum dado encontrado com os filtros selecionados.")
    
    # ==============================
    # Importar Dados (Admin)
    # ==============================
    elif aba == "Importar Dados" and st.session_state["admin"]:
        st.header("⬆️ Importação de Dados em Massa (Admin)")
        st.warning(
            "⚠️ **Aviso de Formato:** Seu arquivo deve conter uma coluna **'Data'** no formato Mês/Ano (MM/AAAA) ou Dia/Mês/Ano (DD/MM/AAAA). "
            "A porcentagem será multiplicada por 100 (ex: 0.25 -> 25%). **A importação continuará usando a data para extrair Mês e Ano.**"
        )
        
        uploaded_file = st.file_uploader("Carregar arquivo CSV ou XLSX com lançamentos", type=["csv", "xlsx"])
        
        if uploaded_file:
            try:
                # 1. Leitura do Arquivo (Ajuste de ENCODING e DELIMITADOR)
                df_import = None
                
                if uploaded_file.name.endswith('.csv'):
                    uploaded_file.seek(0)
                    file_bytes = uploaded_file.getvalue()
                    
                    # Lista de tentativas de codificação/separador
                    encodings_separators = [
                        ('latin-1', ';'), 
                        ('utf-8', ','), 
                        ('latin-1', ','),
                        ('utf-8', ';')
                    ]
                    
                    for encoding, sep in encodings_separators:
                        try:
                            file_content = file_bytes.decode(encoding, errors='strict')
                            df_attempt = pd.read_csv(io.StringIO(file_content), sep=sep, engine='python')
                            
                            # Verifica se o número de colunas parece razoável (ex: 7 colunas esperadas)
                            # Agora esperamos 5 colunas essenciais: Nome, Data, Descrição, Projeto, Porcentagem
                            if df_attempt.shape[1] >= 5:
                                df_import = df_attempt
                                break
                            else:
                                raise ValueError(f"Número de colunas inesperado ({df_attempt.shape[1]}).")
                        
                        except Exception:
                            continue
                            
                    if df_import is None:
                         raise Exception("Falha ao tokenizar os dados após múltiplas tentativas de delimitador e encoding. Verifique a formatação do CSV.")
                        
                elif uploaded_file.name.endswith('.xlsx'):
                     # Se for XLSX, lê com a biblioteca do pandas, que é mais robusta
                    uploaded_file.seek(0)
                    df_import = pd.read_excel(uploaded_file)
                
                if df_import is None:
                    raise Exception("Não foi possível processar o arquivo. Certifique-se de que é um CSV ou XLSX válido.")


                st.subheader("Pré-visualização dos Dados Carregados")
                st.dataframe(df_import.head())
                
                # 2. Renomear e Mapear Colunas
                # Mapeamento atualizado para a nova coluna 'Data' e removendo 'Mês'/'Ano'
                colunas_mapeamento_origem = {
                    'Nome': 'usuario',
                    'Data': 'data_str', # Coluna temporária para a string/objeto de data
                    'Descrição': 'descricao',
                    'Projeto': 'projeto',
                    'Porcentagem': 'porcentagem',
                    'Observação (Opcional)': 'observacao'
                }
                
                # 2.1 Uniformizar nomes de colunas do arquivo para minúsculas e sem espaços
                # Remove espaços/acentos dos CABEÇALHOS
                df_import.columns = df_import.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.lower()
                
                # 2.2 Cria um dicionário de renomeação case-insensitive
                colunas_renomear = {origem.lower(): destino for origem, destino in colunas_mapeamento_origem.items()}
                
                # Remove as chaves 'mês' e 'ano' do mapeamento, caso existam, para evitar conflitos
                if 'mês' in colunas_renomear: del colunas_renomear['mês']
                if 'ano' in colunas_renomear: del colunas_renomear['ano']

                df_import.rename(columns=colunas_renomear, inplace=True)

                # 2.3 Garantir que a coluna 'usuario' (antes Nome) e 'data_str' (antes Data) existam
                colunas_base_necessarias = ['usuario', 'data_str', 'descricao', 'projeto', 'porcentagem']
                for col in colunas_base_necessarias:
                    if col not in df_import.columns:
                        # Tenta deduzir o nome original ou falha
                        raise KeyError(f"A coluna **'{col.capitalize()}'** não foi encontrada no arquivo após a renomeação. Verifique se o nome do cabeçalho está correto.")

                # 3. PRÉ-CADASTRO DE USUÁRIOS
                
                # CORREÇÃO CRUCIAL: Limpa a coluna de usuários antes de extrair os nomes únicos
                df_import['usuario'] = df_import['usuario'].astype(str).str.strip()
                usuarios_csv = df_import['usuario'].dropna().unique().tolist()
                
                if not usuarios_csv:
                    st.error("Nenhum usuário válido encontrado na coluna 'Nome'. Verifique o arquivo.")
                else:
                    with st.spinner(f"Verificando e pré-cadastrando {len(usuarios_csv)} usuários..."):
                        
                        # Precisa recarregar os usuários do DB para garantir que a lista de existentes está atualizada
                        usuarios_df_reloaded, _ = carregar_dados() 
                        usuarios_existentes_db = usuarios_df_reloaded['usuario'].tolist()
                        
                        usuarios_para_inserir = [u for u in usuarios_csv if u not in usuarios_existentes_db]

                        if usuarios_para_inserir:
                            inserted_count, user_msg = bulk_insert_usuarios(usuarios_para_inserir)
                            st.info(f"Usuários encontrados no arquivo: **{len(usuarios_csv)}**. Novos usuários cadastrados: **{inserted_count}** (senha padrão: '123').")
                        else:
                            st.info(f"Todos os {len(usuarios_csv)} usuários do arquivo já estão cadastrados no sistema.")
                    
                    # 4. Limpeza e Transformação dos Dados de Atividade
                    
                    # 4.1. Conversão Rígida e Limpeza de Dados Sujos/Finais
                    
                    # Converte a coluna de data combinada para o tipo datetime. 
                    df_import['data'] = pd.to_datetime(df_import['data_str'], errors='coerce', dayfirst=True)
                    
                    # Converte a porcentagem para numérico
                    df_import['porcentagem'] = pd.to_numeric(df_import['porcentagem'], errors='coerce')
                    
                    # Remove linhas que não têm Data válida, Usuário ou Porcentagem válidos
                    df_import.dropna(subset=['data', 'usuario', 'porcentagem'], inplace=True)
                    
                    # **PASSO CRUCIAL:** Redefinir o índice após o dropna para evitar problemas de mapeamento
                    df_import.reset_index(drop=True, inplace=True) 

                    # 4.2. Geração de Colunas de Mês e Ano
                    
                    # a) Extrair mes e ano da nova coluna 'data' (já limpa)
                    df_import['mes'] = df_import['data'].dt.month.astype(int)
                    df_import['ano'] = df_import['data'].dt.year.astype(int)
                    
                    # b) Conversão da Porcentagem (float decimal) para INT (0-100)
                    df_import['porcentagem'] = (df_import['porcentagem'] * 100).round().astype(int)
                    
                    # c) Tratar observações nulas (NaN)
                    if 'observacao' in df_import.columns:
                        df_import['observacao'].fillna('', inplace=True)
                    else:
                        df_import['observacao'] = '' # Adiciona coluna vazia se não existir

                    # d) Garantir que apenas colunas necessárias e transformadas existam
                    colunas_finais = ['usuario', 'data', 'mes', 'ano', 'descricao', 'projeto', 'porcentagem', 'observacao']
                    df_para_inserir = df_import[colunas_finais]

                    st.success(f"Pronto para importar **{len(df_para_inserir)}** registros de atividades. ({df_import.shape[0]} linhas válidas mantidas.)")
                    
                    # 5. Botão de Confirmação para Inserção das Atividades
                    if st.button("Confirmar Importação de ATIVIDADES para o Banco de Dados", key="btn_import_final"):
                        with st.spinner('Importando dados de atividades em massa...'):
                            linhas_inseridas, mensagem = bulk_insert_atividades(df_para_inserir)
                        
                        # Invalida o cache para forçar a recarga dos dados na próxima execução
                        carregar_dados.clear()
                        
                        if linhas_inseridas > 0:
                            st.success(f"🎉 {linhas_inseridas} registros de atividades importados com sucesso!")
                        else:
                            st.error(mensagem)
                        
                        # Recarrega o Streamlit
                        st.rerun() # SUBSTITUÍDO: st.experimental_rerun() -> st.rerun()
                    
            except KeyError as e:
                st.error(f"❌ Erro: Uma coluna esperada não foi encontrada no arquivo. Verifique se as colunas estão corretas. Coluna ausente: **{e}**")
            except Exception as e:
                # Captura erros de decodificação genéricos
                st.error(f"❌ Erro ao processar ou ler o arquivo: {e}")

