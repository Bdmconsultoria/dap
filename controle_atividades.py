import streamlit as st
import pandas as pd
from datetime import datetime
import psycopg2
import psycopg2.extras # Importação necessária para inserção em massa
import plotly.express as px
import io # Importação necessária para ler arquivos carregados

# ==============================
# 0. CONFIGURAÇÃO DE ESTILO E TEMA (SINAPSIS)
# ==============================
# --- CORES SINAPSIS DEFINITIVAS ---
COR_PRIMARIA = "#313191" # Azul Principal (Fundo da Sidebar)
COR_SECUNDARIA = "#19c0d1" # Azul Ciano (Usado na paleta de gráficos e realces)
COR_CINZA = "#444444" # Cinza Escuro (Usado na paleta de gráficos)
COR_FUNDO_APP = "#FFFFFF"     # Fundo Branco Limpo do corpo principal do App
COR_FUNDO_SIDEBAR = COR_PRIMARIA # Fundo da lateral na cor principal
# ----------------------------------

# Paleta 
# de cores customizada para Plotly (usada nos gráficos)
SINAPSIS_PALETTE = [COR_SECUNDARIA, COR_PRIMARIA, COR_CINZA, "#888888", "#C0C0C0"]

# O logo foi removido para garantir a estabilidade do carregamento
LOGO_URL = "" 

# ==============================
# 1. Credenciais PostgreSQL
# ==============================
# Nota: st.secrets deve estar configurado no seu seu ambiente Streamlit
# ATENÇÃO: Verifique se st.secrets está acessível no seu ambiente de execução.
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
        # Em produção, você pode 
        # descomentar esta linha para ver erros de conexão:
        # st.error(f"❌ Erro ao conectar ao banco de dados: {e}")
        return None

# ==============================
# 3. Setup do Banco (criação de tabelas)
# ==============================
def setup_db():
    """Cria as tabelas 'usuarios', 'atividades' e 'hierarquia' se elas não existirem
        e garante que a coluna 'status' exista na tabela 'atividades'."""
    conn = get_db_connection()
    if conn is None: return
    try:
        
        with conn.cursor() as cursor:
            # Tabela USUARIOS
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS usuarios (
                    usuario VARCHAR(50) PRIMARY KEY,
                    senha VARCHAR(50) NOT NULL,
   
                    admin BOOLEAN DEFAULT FALSE
                );
            """)
            
            # Tabela ATIVIDADES
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
                    observacao TEXT,
                    status VARCHAR(50) DEFAULT 'Pendente' 
           
                );
            """)
            
            # CORREÇÃO CRÍTICA: Adiciona a coluna STATUS se ela não existir
            try:
                # 1. Verifica se a coluna 'status' existe na tabela 'atividades'
           
                cursor.execute("""
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='atividades' AND column_name='status';
                """)
                exists = cursor.fetchone()
                
                # 2. Se não existir, executa o ALTER TABLE
                if not exists:
                    cursor.execute("""
    
                        ALTER TABLE atividades
                        ADD COLUMN status VARCHAR(50) DEFAULT 'Pendente';
                    """)
                    conn.commit()
         
            except Exception as e:
                # Loga o erro, mas não para o app
                # st.error(f"Aviso de migração de tabela: {e}")
                conn.rollback() 
            
            # NOVA TABELA: HIERARQUIA
    
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS hierarquia (
                    gerente VARCHAR(50) REFERENCES usuarios(usuario),
                    subordinado VARCHAR(50) REFERENCES usuarios(usuario),
                    PRIMARY KEY (gerente, subordinado),
 
                    CHECK (gerente != subordinado)
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

def alterar_senha(usuario, nova_senha):
    """Atualiza a senha do usuário no banco de dados."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
   
                UPDATE usuarios
                SET senha = %s
                WHERE usuario = %s;
            """, (nova_senha, usuario))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao alterar senha: {e}")
        return False
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
            data_db = datetime(year=ano, month=mes, day=1).date()
      
            
            if atividade_id is None:
                # Inserir Nova Atividade (Status 'Pendente' por default)
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

def atualizar_status_atividade(atividade_id, novo_status):
    """Atualiza o status de uma atividade (usado pelo gestor)."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
     
                UPDATE atividades
                SET status = %s
                WHERE id = %s;
            """, (novo_status, atividade_id))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao atualizar status: {e}")
        return False
    finally:
        conn.close()


# --- Funções de Hierarquia ---
def salvar_hierarquia(gerente, subordinado):
    """Associa um subordinado a um gerente."""
    conn = get_db_connection()
    if conn is None: return False
  
    if gerente == subordinado: 
        st.error("Gerente e Subordinado não podem ser a mesma pessoa.")
        return False

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO hierarquia (gerente, subordinado)
                VALUES (%s, %s)
    
                ON CONFLICT (gerente, subordinado) DO UPDATE 
                SET gerente = EXCLUDED.gerente, subordinado = EXCLUDED.subordinado;
            """, (gerente, subordinado))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar hierarquia: {e}")
  
        return False
    finally:
        conn.close()

def apagar_hierarquia(gerente, subordinado):
    """Remove a associação entre gerente e subordinado."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM hierarquia
         
                WHERE gerente = %s AND subordinado = %s;
            """, (gerente, subordinado))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao apagar hierarquia: {e}")
        return False
    finally:
        conn.close()

@st.cache_data(ttl=600)
def carregar_hierarquia():
    """Carrega todas as associações de hierarquia para um DataFrame."""
    conn = get_db_connection()
    if conn is None: return pd.DataFrame()
    try:
 
        hierarquia_df = pd.read_sql("SELECT gerente, subordinado FROM hierarquia ORDER BY gerente, subordinado;", conn)
        return hierarquia_df
    except Exception as e:
        # st.error(f"Erro ao carregar hierarquia: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

@st.cache_data(ttl=600)
def carregar_dados():
    """
    Carrega todos os usuários e atividades do banco de dados para DataFrames.
    """
    conn = get_db_connection()
    if conn is None: 
        return pd.DataFrame(), pd.DataFrame()
    
    # Tentativa de SELECT com a coluna 'status'
    query_full = """
        SELECT id, usuario, data, mes, ano, descricao, projeto, porcentagem, observacao, status
        FROM atividades ORDER BY ano DESC, mes DESC, data DESC;
    """
    # Tentativa de SELECT SEM a coluna 'status' (para migração)
    query_base = """
        SELECT id, usuario, data, mes, ano, descricao, projeto, porcentagem, observacao
        FROM atividades ORDER BY ano DESC, mes DESC, data DESC;
    """

    try:
        usuarios_df = pd.read_sql("SELECT usuario, admin FROM usuarios;", conn)
        atividades_df = pd.read_sql(query_full, conn)
        
        if not atividades_df.empty:
            atividades_df['data'] = pd.to_datetime(atividades_df['data'])
            
        return usuarios_df, atividades_df
        
    except Exception as e:
 
        # Lógica de migração de status
        if 'column "status" does not exist' in str(e):
            # st.warning("⚠️ Tentativa de carregamento sem a coluna 'status' (migração em andamento).")
            
            try:
                atividades_df = pd.read_sql(query_base, conn)
       
                if not atividades_df.empty:
                    atividades_df['data'] = pd.to_datetime(atividades_df['data'])
                    atividades_df['status'] = 'Pendente' 
                    st.session_state['db_migrating'] = True
                    st.rerun() 
 
                
                return usuarios_df, atividades_df 
            except Exception as e2:
                st.error(f"Erro fatal ao carregar dados base: {e2}")
                return pd.DataFrame(), pd.DataFrame()
        else:
   
            st.error(f"Erro ao carregar dados: {e}")
            return pd.DataFrame(), pd.DataFrame()
            
    finally:
        conn.close()

def bulk_insert_usuarios(user_list):
    """Insere usuários inexistentes no banco de dados.
    Senha padrão: '123'."""
    conn = get_db_connection()
    if conn is None:
        return 0, "❌ Falha na conexão com o banco de dados."
    
    data_list = [(user, '123', False) for user in user_list]
    query = """
        INSERT INTO usuarios (usuario, senha, admin)
        VALUES (%s, %s, %s)
        ON CONFLICT (usuario) DO NOTHING
    """
    try:
 
        with conn.cursor() as cursor:
            psycopg2.extras.execute_batch(cursor, query, data_list)
            inserted_count = cursor.rowcount
            conn.commit()
            return inserted_count, "✅ Sucesso! Usuários pré-cadastrados com êxito."
    except Exception as e:
        conn.rollback()
        return 0, f"Erro durante o pré-cadastro de usuários: {e}"
    finally:
        conn.close()


def bulk_insert_atividades(df_to_insert):
    """Insere atividades em massa no banco de dados."""
    conn = get_db_connection()
    if conn is None:
        return 0, "❌ Falha na conexão com o banco de dados."
    
    # O DataFrame 
    # deve ter as colunas na ordem correta, incluindo 'status'
    data_list = [tuple(row) for row in df_to_insert[[
        'usuario', 'data', 'mes', 'ano', 'descricao', 'projeto', 'porcentagem', 'observacao', 'status'
    ]].values]

    # Ajusta a query para incluir o novo campo 'status'
    query = """
        INSERT INTO atividades (usuario, data, mes, ano, descricao, projeto, porcentagem, observacao, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
   
    try:
        with conn.cursor() as cursor:
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
    Limpa espaços em branco iniciais/finais de nomes de usuários no DB.
    """
    conn = get_db_connection()
    if conn is None: return False, "Falha na conexão com o banco de dados."
    
    try:
        with conn.cursor() as cursor:
            # 1. Atualiza a tabela ATIVIDADES e HIERARQUIA para remover espaços nas chaves
            cursor.execute("""UPDATE atividades SET usuario = TRIM(usuario);""")
            atividades_afetadas = cursor.rowcount
   
            
            cursor.execute("""UPDATE hierarquia SET gerente = TRIM(gerente), subordinado = TRIM(subordinado);""")
            hierarquia_afetadas = cursor.rowcount

            # 2. Coletar todos os nomes de usuários únicos e limpos
            cursor.execute("""
                SELECT DISTINCT TRIM(usuario) FROM atividades
   
                UNION
                SELECT DISTINCT TRIM(gerente) FROM hierarquia
                UNION
                SELECT DISTINCT TRIM(subordinado) FROM hierarquia
                UNION
               
                SELECT DISTINCT usuario FROM usuarios;
            """)
            usuarios_limpos = list(set([row[0] for row in cursor.fetchall()])) # Usar set para garantir unicidade
            
            # 3. Preservar status admin
            cursor.execute("SELECT usuario, admin FROM usuarios;")
            status_admin_original = dict(cursor.fetchall())
 
            
            # 4. Limpar e Reinserir a tabela usuarios
            cursor.execute("TRUNCATE TABLE usuarios CASCADE;")
            
            # Reinserir todos os usuários limpos
            usuarios_para_reinserir = []
            
            for user in usuarios_limpos:
                # Tenta manter o status de admin, se não, assume False e senha '123'
                is_admin = status_admin_original.get(user, False)
                usuarios_para_reinserir.append((user, '123', is_admin))

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
                f"{atividades_afetadas} atividades e {hierarquia_afetadas} hierarquias corrigidas. "
                f"{usuarios_reinseridos} usuários reinseridos (senha padrão: '123')."
            )
            
    except Exception as e:
        conn.rollback()
        return False, f"❌ Erro ao limpar nomes no DB: {e}"
    finally:
        conn.close()

# ==============================
# 4.1. FUNÇÕES FALTANTES PARA ABA 'MINHAS ATIVIDADES' (ADICIONADAS)
# ==============================

def carregar_atividades_usuario(usuario, mes, ano):
    """Carrega atividades de um usuário específico para um mês/ano."""
    conn = get_db_connection()
    if conn is None: return []
    try:
        query = """
            SELECT id, descricao, projeto, porcentagem, observacao, status
            FROM atividades
            WHERE usuario = %s AND mes = %s AND ano = %s
            ORDER BY id DESC;
        """
        atividades_df = pd.read_sql(query, conn, params=(usuario, mes, ano))
        # Converte para lista de dicionários para facilitar o uso no front-end
        return atividades_df.to_dict('records')
    except Exception as e:
        # st.error(f"Erro ao carregar atividades do usuário: {e}")
        return []
    finally:
        conn.close()

def atualizar_atividade(atividade_id, nova_porcentagem, nova_observacao):
    """Atualiza a porcentagem e observação de uma atividade específica."""
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE atividades
                SET porcentagem = %s, observacao = %s
                WHERE id = %s;
            """, (nova_porcentagem, nova_observacao, atividade_id))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao atualizar atividade: {e}")
        return False
    finally:
        conn.close()

def excluir_atividade(atividade_id):
    """Exclui uma atividade específica. É um alias para apagar_atividade."""
    return apagar_atividade(atividade_id)

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
ANOS = list(range(datetime.today().year - 2, datetime.today().year + 3))

# Mapeamento de Status para Cores (para uso na Visão do Gestor)
STATUS_CORES = {
    "Pendente": "orange",
    "Aprovado": "green",
    "Rejeitado": "red"
}

# ==============================
# 8. Funções de Callback (on_click)
# ==============================

def set_edit_id(id_atividade):
    """Define o ID da atividade a ser editada e aciona o rerun."""
    st.session_state['edit_id'] = id_atividade
    st.rerun()

def cancelar_edicao():
    """Cancela a edição."""
    st.session_state['edit_id'] = None
    st.rerun() # Precisa de rerun para sair do estado de edição

def handle_delete(atividade_id):
    """Apaga uma atividade e limpa o cache, forçando o rerun."""
    if apagar_atividade(atividade_id):
        carregar_dados.clear()
        st.success("Atividade apagada!")
       
        st.rerun()

def handle_status_update(atividade_id, novo_status):
    """Atualiza o status de uma atividade e limpa o cache, forçando o rerun."""
    if atualizar_status_atividade(atividade_id, novo_status):
        carregar_dados.clear()
        # st.success(f"Lançamento {atividade_id} atualizado para {novo_status}.") # Mensagem será limpa pelo rerun
        st.rerun()

def is_user_a_manager(usuario, hierarquia_df):
    """Verifica se o usuário está listado como gerente na tabela de hierarquia."""
    if hierarquia_df.empty:
        return False
    # Checa se o nome do usuário está na coluna 'gerente'
    return usuario in hierarquia_df['gerente'].unique()
        
# ==============================
# 6. Sessão
# ==============================
if "usuario" not in st.session_state:
    st.session_state["usuario"] = None
    st.session_state["admin"] = False
if 'edit_id' not in st.session_state:
    st.session_state['edit_id'] = None
if 'show_change_password' not in st.session_state:
    st.session_state['show_change_password'] = False

# Carrega os dados
usuarios_df, atividades_df = carregar_dados()
hierarquia_df = carregar_hierarquia() # Agora o DataFrame de hierarquia está disponível

# ==============================
# 7. Login e Navegação
# ==============================

# --- Injeção de CSS para Estilo Sinapsis ---
st.markdown(
    f"""
    <style>
  
        /* Define a cor primária (do config.toml) */
        :root {{
            --primary-color: #19c0d1;
            --secondary-background-color: {COR_FUNDO_SIDEBAR}; 
        }}
        
        /* CORREÇÃO VISUAL DE FUNDO E TEXTO DA SIDEBAR (Azul Principal e Texto Branco) */
        [data-testid="stSidebar"] {{
            background-color: {COR_FUNDO_SIDEBAR};
        }}
        [data-testid="stSidebar"] * {{
            color: #FFFFFF !important;
        }}
        [data-testid="stSidebar"] .stButton > button {{
             background-color: {COR_FUNDO_SIDEBAR} !important;
             border: 1px solid #FFFFFF30;
             color: #FFFFFF !important;
        }}
        [data-testid="stSidebar"] .stButton > button:hover {{
             background-color: {COR_SECUNDARIA} !important;
        }}
        /* Seletor para a opção de rádio selecionada */
        [data-testid="stSidebar"] .st-b5.st-bd.st-be.st-bf.st-bg.st-bh.st-bi.st-bj.st-bk.st-bl.st-bm.st-bn.st-bo.st-bp.st-bq.st-br.st-bs.st-bt.st-bu.st-bv.st-bw.st-bx {{
             background-color: {COR_SECUNDARIA} !important;
        }}
        
        /* Estilo para o corpo principal do APP */
        .stApp {{
            background-color: {COR_FUNDO_APP};
        }}
        
        /* Ajusta o estilo dos gráficos Plotly para serem mais planos */
        .modebar {{
            display: none !important;
        }}
        
        /* Estilos específicos para a tabela de status (Visão do Gestor) */
        .status-badge {{
            padding: 4px 8px;
            border-radius: 12px;
            font-size: 0.9em;
            font-weight: bold;
            display: inline-block;
        }}
        .status-Pendente {{
            background-color: #ffcc99;
            /* Laranja Claro */
            color: #cc6600;
        }}
        .status-Aprovado {{
            background-color: #ccffcc;
            /* Verde Claro */
            color: #008000;
        }}
        .status-Rejeitado {{
            background-color: #ff9999;
            /* Vermelho Claro */
            color: #cc0000;
        }}
    </style>
    """,
    unsafe_allow_html=True
)
st.sidebar.markdown("<br>", unsafe_allow_html=True) # Espaço para o logo

# --------------------------------------------------------------------

if st.session_state["usuario"] is None:
    st.title("🔐 Login")
    usuario = st.text_input("Usuário")
    senha = st.text_input("Senha", type="password")
    if st.button("Entrar"):
        usuario_limpo = usuario.strip()
        ok, admin = validar_login(usuario_limpo, senha)
        if ok:
            st.session_state["usuario"] = usuario_limpo
          
            st.session_state["admin"] = admin
            st.rerun()
        else:
            st.error("Usuário ou senha incorretos")
else:
    st.sidebar.markdown(f"**Usuário:** {st.session_state['usuario']}")

    # --- NOVO: BOTÃO E LÓGICA DE ALTERAR SENHA ---
    if st.sidebar.button("🔑 Alterar Senha"):
        # Alterna o estado de exibição do formulário
        st.session_state['show_change_password'] = not st.session_state['show_change_password']
        st.rerun()

  
    if st.session_state['show_change_password']:
        with st.sidebar.form("form_change_password"):
            nova_senha_1 = st.text_input("Nova Senha", type="password")
            nova_senha_2 = st.text_input("Confirme a Nova Senha", type="password")
            if st.form_submit_button("Atualizar Senha"):
                if nova_senha_1 and nova_senha_1 == nova_senha_2:
                   
                    if alterar_senha(st.session_state["usuario"], nova_senha_1):
                        st.sidebar.success("✅ Senha atualizada com sucesso! Por favor, faça login novamente.")
                        st.session_state["usuario"] = None
                        st.session_state["admin"] = False
                        st.session_state['show_change_password'] = False
                  
                        st.rerun()
                    else:
                        st.sidebar.error("❌ Erro ao salvar a nova senha no banco de dados.")
                else:
                    st.sidebar.error("⚠️ As senhas não coincidem ou estão vazias.")
    # --- FIM LÓGICA ALTERAR SENHA ---
    
    st.sidebar.markdown("---")
    
    if st.sidebar.button("Sair"):
        st.session_state["usuario"] = None
        st.session_state["admin"] = False
        st.session_state['show_change_password'] = False
        st.rerun()

    # --- NOVO: VERIFICA SE O USUÁRIO É GERENTE ---
    is_manager = is_user_a_manager(st.session_state["usuario"], hierarquia_df)
    
    abas = ["Lançar Atividade", "Minhas Atividades"]
    
    # Adiciona a aba "Gerenciar Time" se for Admin OU for Gerente
    if st.session_state["admin"] or is_manager:
        abas.append("Gerenciar Time")
        
    # Adiciona as abas exclusivas do Admin
    if st.session_state["admin"]:
        abas += ["Gerenciar Usuários", "Consolidado", "Importar Dados"]

    aba = st.sidebar.radio("Menu", abas)

    # ==============================
    # 7.1. Gerenciar Usuários
    # ==============================
    if aba == "Gerenciar Usuários" and st.session_state["admin"]:
        st.header("👥 Gerenciar Usuários")
        
        # --- Ferramenta de Limpeza de Nomes (Admin) ---
        st.subheader("Ferramenta de Manutenção (Limpar Espaços)")
        st.warning(
            "Esta ação **REMOVE ESPAÇOS em branco iniciais/finais** de TODOS os nomes de usuários no DB, "
   
            "corrigindo problemas de login, chaves estrangeiras e hierarquia. **Todos os usuários terão a senha redefinida para '123'.**"
        )
        if st.button("Executar Limpeza de Nomes de Usuário (TRIM)", key="btn_limpeza_db"):
            with st.spinner("Executando limpeza no banco de dados..."):
                sucesso, mensagem = limpar_nomes_usuarios_db()
            
  
            carregar_dados.clear()
            
            if sucesso:
                st.success(mensagem)
            else:
                st.error(mensagem)
            
         
            st.rerun()


        # --- Formulário de Adição de Usuário ---
        st.subheader("Adicionar Novo Usuário")
        with st.form("form_add_user"):
            novo_usuario = st.text_input("Usuário")
            nova_senha = st.text_input("Senha", type="password")
            admin_check = st.checkbox("Admin")
            if st.form_submit_button("Adicionar"):
       
                if salvar_usuario(novo_usuario.strip(), nova_senha, admin_check):
                    st.success("Usuário adicionado!")
                    st.rerun()
        
        # Tabela de Usuários
        usuarios_df_reloaded, _ = carregar_dados()
        st.subheader("Tabela de Usuários")
       
        st.dataframe(usuarios_df_reloaded, use_container_width=True)


    # ==============================
    # 7.2. Gerenciar Time (Visão de Gestor e Aprovação)
    # ==============================
    # Habilitado para Admin OU Gerente (pela lógica do menu)
    elif aba == "Gerenciar Time":
        st.header("🤝 Gerenciar Time e Aprovação de Atividades")
        
        # Recarrega a hierarquia para o caso de ter sido alterada na mesma sessão
        hierarquia_df_reloaded = carregar_hierarquia()
        usuarios_list = usuarios_df['usuario'].tolist()
     
        
        # O Gerente Padrão (usuário logado) ou Admin é o foco inicial
        usuario_logado = st.session_state["usuario"]
        
        # --- DEFINIÇÃO DE QUEM PODE GERENCIAR QUEM ---
        
        # 1. ADMIN pode gerenciar TODOS (configurar hierarquia de terceiros)
        if st.session_state["admin"]:
          
            st.info("Você é Administrador e pode configurar e visualizar **qualquer** time.")
            
            # --- 1. CONFIGURAR HIERARQUIA (Apenas para ADMIN) ---
            st.subheader("1. Configurar Hierarquia (Admin)")
            
            gerentes_disponiveis = sorted(usuarios_list)
            
    
            with st.form("form_config_hierarquia"):
                col_g1, col_g2 = st.columns(2)
                
                # Permite que o Admin escolha o Gerente
                gerente_selecionado = col_g1.selectbox("Gerente", gerentes_disponiveis, key="sb_gerente")
             
                
                # Subordinados disponíveis (todos, exceto o gerente selecionado)
                subordinados_disponiveis = [u for u in usuarios_list if u != gerente_selecionado]
                subordinado_selecionado = col_g2.selectbox(
                    "Novo Liderado", 
        
                    ["--- Selecione ---"] + sorted(subordinados_disponiveis),
                    key="sb_subordinado"
                )
                
                if st.form_submit_button("Adicionar/Atualizar Liderado"):
              
                    if subordinado_selecionado != "--- Selecione ---":
                        if salvar_hierarquia(gerente_selecionado, subordinado_selecionado):
                            st.success(f"✅ {subordinado_selecionado} adicionado como liderado de **{gerente_selecionado}**.")
                            carregar_hierarquia.clear()
 
                            st.rerun()
                        else:
                            st.error("Erro ao adicionar hierarquia. Verifique se o usuário existe.")
                    else:
                        st.warning("Selecione um liderado válido.")

            st.markdown("---")
            
            # --- 1.1. Visualização e Remoção da Hierarquia (Apenas para ADMIN) ---
  
            st.subheader("2. Visualizar e Remover Associações (Admin)")
            
            if hierarquia_df_reloaded.empty:
                st.info("Nenhuma hierarquia configurada.")
            else:
                st.dataframe(hierarquia_df_reloaded, use_container_width=True)
             
                
                # Remoção de Hierarquia
                with st.form("form_remover_hierarquia"):
                    st.markdown("##### Remover Associação")
                    
                   
                    gerentes_remover_list = sorted(hierarquia_df_reloaded['gerente'].unique())
                    gerente_remover = st.selectbox("Gerente (Remoção)", gerentes_remover_list, key="gerente_remover")
                    
                    # Filtra subordinados com base no gerente selecionado
                    subordinados_do_gerente = hierarquia_df_reloaded[hierarquia_df_reloaded['gerente'] == gerente_remover]['subordinado'].tolist()
 
                    subordinado_remover = st.selectbox("Liderado a Remover", sorted(subordinados_do_gerente), key="subordinado_remover")

                    if st.form_submit_button("Remover Associação"):
                        if apagar_hierarquia(gerente_remover, subordinado_remover):
                           
                            st.success(f"❌ Associação entre {gerente_remover} e {subordinado_remover} removida.")
                            carregar_hierarquia.clear() # Limpa o cache específico da hierarquia
                            st.rerun()
                        else:
      
                            st.error("Erro ao remover hierarquia.")
        
        # 2. NÃO-ADMIN (Gerente): Só gerencia seu próprio time
        
        # --- 3. APROVAÇÃO E ACOMPANHAMENTO DE EQUIPES ---
        st.markdown("---")
        st.subheader("Análise e Aprovação de Atividades")
       
 
        gerentes_com_time = hierarquia_df_reloaded['gerente'].unique().tolist()
        
        if not gerentes_com_time or (is_manager and usuario_logado not in gerentes_com_time):
            st.warning("Você não está configurado como gerente de nenhum time.")
            st.stop()
        
        if st.session_state["admin"]:
             # Admin 
             # seleciona qualquer time
             gerente_a_analisar = st.selectbox(
                "Selecione o Time para Análise", 
                sorted(gerentes_com_time)
            )
        else:
             # Gerente só vê o próprio time
       
            gerente_a_analisar = usuario_logado
            st.markdown(f"**Time em Análise:** {gerente_a_analisar}")

        if gerente_a_analisar not in gerentes_com_time:
             st.error("Time inválido selecionado.")
             st.stop()


        # --- CONTINUAÇÃO DA ANÁLISE DO TIME SELECIONADO/LOGADO ---
        
        meu_time_df = hierarquia_df_reloaded[hierarquia_df_reloaded['gerente'] == gerente_a_analisar]
        subordinados_list = meu_time_df['subordinado'].tolist()
            
        # Filtros de Mês/Ano para a análise do time
        col_m1, col_m2 = st.columns(2)
        
        hoje = datetime.now()
        mes_vigente_num = hoje.month
        ano_vigente = hoje.year
        
     
        meses_para_filtro = list(MESES.values())
        mes_vigente_str = MESES.get(mes_vigente_num, 'Mês Inválido')
        
        try:
            default_mes_idx = meses_para_filtro.index(mes_vigente_str)
        except ValueError:
            default_mes_idx = 0 
            
        mes_nome_analise = col_m1.selectbox("Mês de Referência", meses_para_filtro, index=default_mes_idx, key="sb_mes_analise")
  
        ano_analise = col_m2.selectbox("Ano de Referência", ANOS, index=ANOS.index(ano_vigente), key="sb_ano_analise")
        
        mes_num_analise = next((k for k, v in MESES.items() if v == mes_nome_analise), None)
        
        if mes_num_analise is None:
            st.error("Mês de análise inválido.")
            st.stop()
        
     
        # DataFrame com atividades do time no mês/ano selecionado
        df_time_mes = atividades_df[
            (atividades_df['usuario'].isin(subordinados_list)) & 
            (atividades_df['mes'] == mes_num_analise) & 
            (atividades_df['ano'] == ano_analise)
        ]
        
        # Calcula o total alocado por usuário
     
        df_resumo_alocacao = df_time_mes.groupby('usuario')['porcentagem'].sum().reset_index()
        df_resumo_alocacao.columns = ['Subordinado', 'Total Alocado (%)']
        
        # Adiciona usuários sem lançamentos (0%)
        usuarios_com_lancamento = df_resumo_alocacao['Subordinado'].tolist()
        usuarios_sem_lancamento = [u for u in subordinados_list if u not in usuarios_com_lancamento]
        
        for u in usuarios_sem_lancamento:
            df_resumo_alocacao.loc[len(df_resumo_alocacao)] = [u, 0]
        
        # Estilização da tabela de resumo
        def color_alocacao(val):
            if isinstance(val, str): return ''
            color = ''
            if val < 50:
                color = 'background-color: #ffcccc'
    
            elif 50 <= val < 100:
                color = 'background-color: #ffffcc'
            elif val == 100:
                color = 'background-color: #ccffcc'
            else:
                color = 'background-color: #ff9999; font-weight: bold'
            return color
        
        df_final_style = df_resumo_alocacao.style.applymap(color_alocacao, subset=['Total Alocado (%)'])
        
        st.markdown(f"##### Status de Alocação do Time **{gerente_a_analisar}** em **{mes_nome_analise}/{ano_analise}**")
        st.dataframe(df_final_style, use_container_width=True)
        
        st.markdown("---")
        
        
        # --- 3. APROVAÇÃO DE LANÇAMENTOS DETALHADOS ---
        st.subheader(f"Lançamentos do Time **{gerente_a_analisar}** para Aprovação")
        
        # Filtros de Status e Usuário para a tabela detalhada
        col_fa1, col_fa2 = st.columns(2)
        
        status_filtro = col_fa1.selectbox("Filtrar por Status", ["Todos", "Pendente", "Aprovado", "Rejeitado"], key="status_filtro_time")
        subordinado_filtro = col_fa2.selectbox("Filtrar por Liderado", ["Todos"] + sorted(subordinados_list), key="liderado_filtro_time")
   
        
        df_aprovacao = df_time_mes.copy()
        
        if status_filtro != "Todos":
            df_aprovacao = df_aprovacao[df_aprovacao['status'] == status_filtro]
        
        if subordinado_filtro != "Todos":
            df_aprovacao = df_aprovacao[df_aprovacao['usuario'] == subordinado_filtro]
            
   
        if df_aprovacao.empty:
            st.info("Nenhuma atividade encontrada com os filtros selecionados.")
        else:
            
            # Exibe as atividades para aprovação
            for idx, row in df_aprovacao.iterrows():
                
       
                # Usa HTML para o badge de status
                badge_status = f'<span class="status-badge status-{row["status"]}">{row["status"]}</span>'

                col1_d, col2_d, col3_d, col4_d = st.columns([2, 1, 1, 1])
                
                with col1_d:
       
                    st.markdown(f"**{row['usuario']}** | ID {row['id']} | {badge_status}", unsafe_allow_html=True)
                    st.markdown(f"**{MESES.get(row['mes'])}/{row['ano']}** | {row['descricao']} ({row['porcentagem']}%)")
                    st.markdown(f"*Projeto:* {row['projeto']}")
                    st.markdown(f"*Obs:* {row['observacao'] if row['observacao'] else '(Não informada)'}")
                    
         
                with col2_d:
                    # --- USANDO on_click CALLBACK ---
                    st.button(
                        "✅ Aprovar", 
                     
                        key=f"apv_{row['id']}", 
                        on_click=handle_status_update, 
                        args=(row['id'], 'Aprovado')
                    )
                          
   
                with col3_d:
                    # --- USANDO on_click CALLBACK ---
                    st.button(
                        "❌ Rejeitar", 
          
                        key=f"rej_{row['id']}", 
                        on_click=handle_status_update, 
                        args=(row['id'], 'Rejeitado')
                    )

               
                with col4_d:
                    # --- USANDO on_click CALLBACK ---
                    st.button(
                        "🗑️ Excluir", 
                        key=f"del_a_{row['id']}",
   
                        on_click=handle_delete,
                        args=(row['id'],)
                    )
                            
       
                st.markdown("---")

    # ==============================
    # 7.3. Lançar Atividade (Versão Final Completa com sugestões)
    # ==============================
    elif aba == "Lançar Atividade":
        st.header("📝 Lançar Atividade (Mensal)")

        with st.form("form_multiplas_atividades"):
            col_mes, col_ano = st.columns(2)
            mes_select = col_mes.selectbox(
                "Mês",
                MESES_SELECT,
                index=list(MESES.values()).index(MESES[datetime.today().month]) + 1
            )
            ano_select = col_ano.selectbox("Ano", ANOS, index=ANOS.index(datetime.today().year))

            mes_num = next((k for k, v in MESES.items() if v == mes_select), None)
            total_existente = 0
            if mes_num:
                total_existente = calcular_porcentagem_existente(
                    st.session_state["usuario"], mes_num, ano_select
                )
            saldo_restante = max(0, 100 - total_existente)

  
            st.info(
                f"📅 **Mês selecionado:** {mes_select}/{ano_select}  \n"
                f"📊 **Total já alocado:** {total_existente:.1f}%  \n"
                f"💡 **Saldo restante disponível:** {saldo_restante:.1f}%"
            )

            # Tipo de lançamento
            tipo_lancamento = st.radio(
                "Tipo de lançamento:",
                ["Porcentagem", "Horas"],
                horizontal=True
            )

            # Quantos lançamentos
            qtd_lancamentos = st.number_input(
                "Quantos lançamentos deseja adicionar?",
                min_value=1,
                max_value=20,
            
                value=1,
                step=1
            )

            st.markdown("---")
            st.subheader("Detalhes dos lançamentos")

            lancamentos = []
            for i in range(qtd_lancamentos):
                st.markdown(f"**Lançamento {i+1}**")

                col1, col2 = st.columns(2)
             
                descricao = col1.selectbox(
                    f"Descrição {i+1}",
                    DESCRICOES_SELECT,
                    key=f"desc_{i}"
                )
                projeto = col2.selectbox(
                    f"Projeto {i+1}",
   
                    PROJETOS_SELECT,
                    key=f"proj_{i}"
                )

                if tipo_lancamento == "Porcentagem":
                    valor = st.number_input(
                        f"Porcentagem {i+1} (%)",
    
                        min_value=1.0,
                        max_value=100.0,
                        value=100.0 if qtd_lancamentos == 1 else 0.0,
                        step=1.0,
                      
                        key=f"porc_{i}"
                    )
                else:
                    valor = st.number_input(
                        f"Horas {i+1}",
                        min_value=0.1,
               
                        max_value=200.0,
                        value=1.0,
                        step=0.5,
                        key=f"hora_{i}"
                    )

                observacao = st.text_area(f"Observação {i+1} (Opcional)", key=f"obs_{i}")
                st.markdown("---")

                lancamentos.append({
                    "descricao": descricao,
                    "projeto": projeto,
                    "valor": valor,
                    "observacao": observacao
      
                })

            # Pré-visualização e cálculo proporcional
            st.subheader("📊 Pré-visualização dos lançamentos")
            preview_data = []

            if lancamentos:
                if tipo_lancamento == "Horas":
                    total_horas = sum(l["valor"] for l in lancamentos)
                   
                    if total_horas > 0:
                        for l in lancamentos:
                            porcent = (l["valor"] / total_horas) * 100
                            preview_data.append({
                       
                                "Descrição": l["descricao"],
                                "Projeto": l["projeto"],
                                "Porcentagem": porcent
                            })
            
                    else:
                        for l in lancamentos:
                            preview_data.append({
                                "Descrição": l["descricao"],
                                "Projeto": l["projeto"],
           
                                "Porcentagem": l["valor"]
                            })

            if preview_data:
                df_preview = pd.DataFrame(preview_data)
                soma_nova = df_preview["Porcentagem"].sum()
                total_final = total_existente + soma_nova
             
                saldo_final = max(0, 100 - total_final)

                col_graf, col_info = st.columns([2, 1])
                with col_graf:
                    fig_preview = px.pie(
                        df_preview,
                        names="Descrição",
         
                        values="Porcentagem",
                        title="Distribuição proporcional dos lançamentos novos",
                        hole=.4,
                        color_discrete_sequence=SINAPSIS_PALETTE
                    )
         
                    fig_preview.update_traces(texttemplate='%{value:.1f}%', textposition='inside')
                    st.plotly_chart(fig_preview, use_container_width=True)

                with col_info:
                    st.markdown(
                        f"**Total novo a ser lançado:** {soma_nova:.1f}%  \n"
                   
                        f"**Total atual + novo:** {total_final:.1f}%  \n"
                        f"**Saldo restante após salvar:** {saldo_final:.1f}%"
                    )
                    if total_final > 100:
                        st.error("⚠️ O total projetado ultrapassa 100%. Ajuste os valores antes de salvar.")

            else:
                st.info("Preencha os lançamentos para visualizar o gráfico e os totais.")

            # Botão final de salvar
            if st.form_submit_button("💾 Salvar Lançamentos"):
                if mes_num is None:
                    st.error("Selecione um mês válido.")
             
                    st.stop()

                for l in lancamentos:
                    if l["descricao"] == "--- Selecione ---" or l["projeto"] == "--- Selecione ---":
                        st.error("Todos os lançamentos devem ter uma Descrição e um Projeto selecionados.")
                        st.stop()

 
                # --- LÓGICA DE CONVERSÃO E VALIDAÇÃO CORRIGIDA ---
                porcentagens_a_salvar = []
                if tipo_lancamento == "Horas":
                    total_horas = sum(l["valor"] for l in lancamentos)
                    if total_horas == 0:
                        st.error("O total de horas deve ser maior que zero.")
                        st.stop()
                        
                    # 1. Calcule a porcentagem proporcional e armazene-a
                    for l in lancamentos:
                        porcent = round((l["valor"] / total_horas) * 100, 2)
                        porcentagens_a_salvar.append(porcent)
                        l["porcentagem_final"] = porcent # Armazena para o cálculo final
                        
                    total_porcentagem_nova = sum(porcentagens_a_salvar)
                else:
                    # 1. Simplesmente pegue o valor percentual e armazene-o
                    for l in lancamentos:
                        porcentagens_a_salvar.append(l["valor"])
                        l["porcentagem_final"] = l["valor"] # Armazena para o cálculo final

                    total_porcentagem_nova = sum(porcentagens_a_salvar)


                # 2. CALCULO FINAL DE VALIDAÇÃO (SEMPRE USANDO PORCENTAGEM)
                total_existente = calcular_porcentagem_existente(
                    st.session_state["usuario"], mes_num, ano_select
                )
                total_final = total_existente + total_porcentagem_nova
                
                # 3. VALIDAÇÃO
                if total_final > 100.0 + 0.001:
                    st.error(
                        f"⚠️ O total de alocação ({total_existente:.1f}% existente + "
                        f"{total_porcentagem_nova:.1f}% novo) "
                        f"excede o limite de 100% para {mes_select}/{ano_select}."
                    )
                    st.stop()

                # 4. SALVAMENTO (Utiliza 'porcentagem_final' criada acima)
                sucesso = True
                for l in lancamentos:
                    obs_final = l["observacao"] if l["observacao"] else ''
                    ok = salvar_atividade(
                        st.session_state["usuario"],
                        mes_num,
                        ano_select,
                        l["descricao"],
                        l["projeto"],
                        # Usa o valor final calculado (que já é proporcional no modo Horas)
                        int(round(l["porcentagem_final"])), 
                        obs_final
                    )
                    if not ok:
                        sucesso = False

                if sucesso:
                    carregar_dados.clear()
                    total_pos = calcular_porcentagem_existente(
                        st.session_state["usuario"], mes_num, ano_select
                    )
                    if total_pos == 100:
                        st.balloons()
                    st.success(
                        f"✅ {len(lancamentos)} lançamentos salvos com sucesso! \n"
                        f"📊 Total alocado em {mes_select}/{ano_select}: {total_pos:.1f}%."
                    )
                    st.rerun()
                else:
                    st.error("❌ Ocorreu um erro ao salvar os lançamentos. Verifique os dados.")


    # ==============================
    # 7.4. Minhas Atividades (Versão Final Completa com extras)
    # ==============================
    elif aba == "Minhas Atividades":
        st.header("📋 Minhas Atividades")

        col_mes, col_ano = st.columns(2)
        mes_select = col_mes.selectbox(
            "Mês",
            MESES_SELECT,
            index=list(MESES.values()).index(MESES[datetime.today().month]) + 1
        )
        ano_select = col_ano.selectbox("Ano", ANOS, index=ANOS.index(datetime.today().year))
        mes_num = next((k for k, v in MESES.items() if v == mes_select), None)

        if mes_num:
            atividades = carregar_atividades_usuario(
   
                st.session_state["usuario"], mes_num, ano_select
            )
        else:
            atividades = []

        if not atividades:
            st.info(f"📅 Nenhuma atividade encontrada para {mes_select}/{ano_select}.")
            st.stop()

        total_alocado = sum(a["porcentagem"] for a in atividades)
        saldo_restante = max(0, 100 - total_alocado)

        st.success(f"📊 **Total alocado:** {total_alocado:.1f}%  | 💡 Saldo restante: {saldo_restante:.1f}%**")

        # Gráfico comparativo (alocado vs saldo)
        fig_saldo = px.pie(
            names=["Alocado", "Disponível"],
            values=[total_alocado, saldo_restante],
            title="Visão geral do mês",
            color_discrete_sequence=["#5B8CFF", "#E0E0E0"]
        )
        fig_saldo.update_traces(texttemplate='%{value:.1f}%', textposition='inside')
        st.plotly_chart(fig_saldo, use_container_width=True)

        # Botão para copiar mês anterior
        if st.button("📋 Copiar lançamentos do mês anterior"):
            mes_anterior = mes_num - 1 if mes_num > 1 else 12
            ano_ref = ano_select if mes_num > 1 else ano_select - 1
            antigos = carregar_atividades_usuario(st.session_state["usuario"], mes_anterior, ano_ref)
            if antigos:
                total_novo = total_alocado + sum(a["porcentagem"] for a in antigos)
                if total_novo > 100.0 + 0.001:
                    st.error(f"⚠️ A cópia excede 100% de alocação para {mes_select}/{ano_select} ({total_novo:.1f}%). Exclua ou ajuste lançamentos atuais antes de copiar.")
                    st.stop()

                for a in antigos:
                    salvar_atividade(
                        st.session_state["usuario"],
   
                        mes_num,
                        ano_select,
                        a["descricao"],
                        a["projeto"],
                        a["porcentagem"],
   
                        a["observacao"]
                    )
                carregar_dados.clear()
                st.success("✅ Lançamentos do mês anterior copiados com sucesso!")
                st.rerun()
            else:
                st.warning("⚠️ Nenhum lançamento encontrado no mês anterior.")

        # Botão de exportar para Excel
        df_export = pd.DataFrame(atividades)
        buffer = io.BytesIO()
        df_export.to_excel(buffer, index=False)
        st.download_button(
            label="📤 Exportar atividades para Excel",
            data=buffer.getvalue(),
            file_name=f"atividades_{mes_select}_{ano_select}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # Exibir gráfico detalhado
        df_graf = pd.DataFrame(atividades)
        df_graf = df_graf.groupby("descricao", as_index=False)["porcentagem"].sum()
        fig_graf = px.pie(
  
            df_graf,
            names="descricao",
            values="porcentagem",
            title="Distribuição atual das atividades",
            hole=.4,
            color_discrete_sequence=SINAPSIS_PALETTE
        )
        fig_graf.update_traces(texttemplate='%{value:.1f}%', textposition='inside')
        st.plotly_chart(fig_graf, use_container_width=True)

        # Exibir lista com edição
        st.subheader("✏️ Editar minhas atividades")
        for idx, a in enumerate(atividades):
            with st.expander(f"📌 {a['descricao']} | {a['projeto']} ({a['porcentagem']}%)", expanded=False):
                col1, col2, col3 = st.columns([2, 2, 1])
                with col1:
                    st.text_input("Descrição", a["descricao"], disabled=True, key=f"desc_{idx}")
                with col2:
                    st.text_input("Projeto", a["projeto"], disabled=True, key=f"proj_{idx}")
                with col3:
   
                    nova_porcentagem = st.number_input(
                        "Porcentagem (%)",
                        min_value=0,
                        max_value=100,
                        value=int(a["porcentagem"]),
    
                        step=1,
                        key=f"porc_{idx}"
                    )

                observacao = st.text_area(
                    "Observação (opcional)",
                    a.get("observacao", ""),
                    key=f"obs_{idx}"
                )

                col_salvar, col_excluir = st.columns(2)
                with col_salvar:
                    if st.button(f"💾 Salvar alterações ({idx})"):
                        
                        # --- VERIFICAÇÃO DE 100% NA EDIÇÃO ---
                        # 1. Calcula o total existente, excluindo a atividade atual
                        total_excluido = calcular_porcentagem_existente(st.session_state["usuario"], mes_num, ano_select, excluido_id=a['id'])
                        
                        # 2. Verifica se a nova porcentagem + o total existente ultrapassa 100%
                        if (total_excluido + nova_porcentagem) > 100.0 + 0.001:
                            st.error(f"⚠️ A edição ultrapassa 100% de alocação para {mes_select}/{ano_select} ({total_excluido + nova_porcentagem:.1f}%). Ajuste o valor.")
                            st.stop()


                        ok = atualizar_atividade(a["id"], nova_porcentagem, observacao)
                        if ok:
                            carregar_dados.clear()
                            st.success("✅ Atividade atualizada com sucesso!")
                            st.rerun()
   
                        else:
                            st.error("❌ Erro ao atualizar atividade.")
                with col_excluir:
                    if st.button(f"🗑️ Excluir ({idx})"):
                        ok = excluir_atividade(a["id"])
 
                        if ok:
                            carregar_dados.clear()
                            st.success("🗑️ Atividade excluída!")
                            st.rerun()
      
                        else:
                            st.error("❌ Erro ao excluir atividade.")

        st.markdown("---")
        st.caption(f"🕓 Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

        # ==============================
        # 7.5. Consolidado para Admin
        # ==============================
    elif aba == "Consolidado" and st.session_state["admin"]:
        st.header("📑 Consolidado Geral de Atividades")
        
        if atividades_df.empty:
            st.info("Nenhuma atividade lançada no sistema.")
        else:
            col_admin1, col_admin2, col_admin3 = st.columns(3)
            
   
            usuarios_unicos = sorted(atividades_df['usuario'].unique())
            usuario_selecionado = col_admin1.selectbox("Filtrar por Usuário", ["Todos"] + usuarios_unicos)
            
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
                
                st.subheader("Tabela de Dados Detalhada")
              
                st.dataframe(df_consolidado.drop(columns=['data_mes']), use_container_width=True)

                st.markdown("---")
                
                df_download = df_consolidado.drop(columns=['id', 'data_mes']).rename(columns={
                    'usuario': 'Usuário',
                    'data': 'Data (Dia 1 do Mês)',
                    'mes': 'Mês',
                    'ano': 'Ano',
                    'descricao': 'Descrição',
                    'projeto': 'Projeto',
                
                    'porcentagem': 'Porcentagem',
                    'observacao': 'Observação',
                    'status': 'Status de Aprovação'
                })

                buffer = io.BytesIO()
                df_download.to_excel(buffer, index=False, sheet_name='Atividades Consolidadas')
                buffer.seek(0)

                st.download_button(
                    label="⬇️ Baixar Dados Filtrados (Excel)",
                    data=buffer,
                    file_name=f"atividades_consolidado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
  
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            else:
                st.info("Nenhum dado encontrado com os filtros selecionados.")
    
        # ==============================
        # 7.6. Importar Dados
        # ==============================
    elif aba == "Importar Dados" and st.session_state["admin"]:
        st.header("⬆️ Importação de Dados em Massa (Admin)")
        st.warning(
            "⚠️ **Aviso de Formato:** Seu arquivo deve conter uma coluna **'Data'** no formato Mês/Ano (MM/AAAA) ou Dia/Mês/Ano (DD/MM/AAAA). "
            "A porcentagem será multiplicada por 100 (ex: 0.25 -> 25%). **O status será definido como 'Pendente'.**"
     
        )
        
        uploaded_file = st.file_uploader("Carregar arquivo CSV ou XLSX com lançamentos", type=["csv", "xlsx"])
        
        if uploaded_file:
            try:
                df_import = None
                
        
                if uploaded_file.name.endswith('.csv'):
                    uploaded_file.seek(0)
                    file_bytes = uploaded_file.getvalue()
                    
                    encodings_separators = [
       
                        ('latin-1', ';'), ('utf-8', ','), ('latin-1', ','), ('utf-8', ';')
                    ]
                    
                    for encoding, sep in encodings_separators:
            
                        try:
                            file_content = file_bytes.decode(encoding, errors='strict')
                            df_attempt = pd.read_csv(io.StringIO(file_content), sep=sep, engine='python')
                         
                            
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
                    uploaded_file.seek(0)
                    df_import = pd.read_excel(uploaded_file)
             
                
                if df_import is None:
                    raise Exception("Não foi possível processar o arquivo. Certifique-se de que é um CSV ou XLSX válido.")


                st.subheader("Pré-visualização dos Dados Carregados")
                st.dataframe(df_import.head())
        
                
                colunas_mapeamento_origem = {
                    'Nome': 'usuario',
                    'Data': 'data_str',
                    'Descrição': 'descricao',
           
                    'Projeto': 'projeto',
                    'Porcentagem': 'porcentagem',
                    'Observação (Opcional)': 'observacao',
                }
                
               
                df_import.columns = df_import.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.lower()
                
                colunas_renomear = {origem.lower(): destino for origem, destino in colunas_mapeamento_origem.items()}
                
                if 'mês' in colunas_renomear: del colunas_renomear['mês']
                if 'ano' in colunas_renomear: del colunas_renomear['ano']

                df_import.rename(columns=colunas_renomear, inplace=True)

                colunas_base_necessarias = ['usuario', 'data_str', 'descricao', 'projeto', 'porcentagem']
                for col in colunas_base_necessarias:
                    if col not in df_import.columns:
                
                        raise KeyError(f"A coluna **'{col.capitalize()}'** não foi encontrada no arquivo após a renomeação. Verifique se o nome do cabeçalho está correto.")

                # --- PRÉ-CADASTRO DE USUÁRIOS ---
                df_import['usuario'] = df_import['usuario'].astype(str).str.strip()
                usuarios_csv = df_import['usuario'].dropna().unique().tolist()
                
                if not usuarios_csv:
  
                    st.error("Nenhum usuário válido encontrado na coluna 'Nome'. Verifique o arquivo.")
                else:
                    with st.spinner(f"Verificando e pré-cadastrando {len(usuarios_csv)} usuários..."):
                        
        
                        usuarios_df_reloaded, _ = carregar_dados() 
                        usuarios_existentes_db = usuarios_df_reloaded['usuario'].tolist()
                        
                        usuarios_para_inserir = [u for u in usuarios_csv if u not in usuarios_existentes_db]

                        if usuarios_para_inserir:
                            inserted_count, user_msg = bulk_insert_usuarios(usuarios_para_inserir)
                            st.info(f"Usuários encontrados no arquivo: **{len(usuarios_csv)}**. Novos usuários cadastrados: **{inserted_count}** (senha padrão: '123').")
                        else:
                            st.info(f"Todos os {len(usuarios_csv)} usuários do arquivo já estão cadastrados no sistema.")
                    
            
                        # --- Limpeza e Transformação dos Dados de Atividade ---
                    df_import['data'] = pd.to_datetime(df_import['data_str'], errors='coerce', dayfirst=True)
                    df_import['porcentagem'] = pd.to_numeric(df_import['porcentagem'], errors='coerce')
                    
                
                    df_import.dropna(subset=['data', 'usuario', 'porcentagem'], inplace=True)
                    df_import.reset_index(drop=True, inplace=True) 

                    df_import['mes'] = df_import['data'].dt.month.astype(int)
                    df_import['ano'] = df_import['data'].dt.year.astype(int)
                    
       
                    df_import['porcentagem'] = (df_import['porcentagem'] * 100).round().astype(int)
                    
                    if 'observacao' in df_import.columns:
                        df_import['observacao'].fillna('', inplace=True)
               
                    else:
                        df_import['observacao'] = ''

                    # Adiciona a coluna status como 'Pendente' para a importação
                    df_import['status'] = 'Pendente'

                  
                    colunas_finais = ['usuario', 'data', 'mes', 'ano', 'descricao', 'projeto', 'porcentagem', 'observacao', 'status']
                    df_para_inserir = df_import[colunas_finais]

                    st.success(f"Pronto para importar **{len(df_para_inserir)}** registros de atividades. ({df_import.shape[0]} linhas válidas mantidas.)")
                    
                    if st.button("Confirmar Importação de ATIVIDADES para o Banco de Dados", key="btn_import_final"):
                        with st.spinner('Importando dados de atividades em massa...'):
                 
                            linhas_inseridas, mensagem = bulk_insert_atividades(df_para_inserir)
                        
                        carregar_dados.clear()
                        
              
                        if linhas_inseridas > 0:
                            st.success(f"🎉 {linhas_inseridas} registros de atividades importados com sucesso!")
                        else:
                            
                            st.error(mensagem)
                        
                        st.rerun()
                    
            except KeyError as e:
                st.error(f"❌ Erro: Uma coluna esperada não foi encontrada no arquivo. Verifique se as colunas estão corretas. Coluna ausente: **{e}**")
            except Exception as e:
                st.error(f"❌ Erro ao processar ou ler o arquivo: {e}")
