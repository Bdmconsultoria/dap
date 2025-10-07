# controle_atividades.py
# Versão 4.0: st.rerun(), Logo Sinapsis, Layout PJ, st.toast
import streamlit as st
import pandas as pd
from datetime import datetime
import psycopg2
import psycopg2.extras
import plotly.express as px
import io
import re
import bcrypt
import traceback
import logging

# ==============================
# CONFIGURAÇÃO BÁSICA DE LOG
# ==============================
# Log em console e (se preferir) em arquivo
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("controle_atividades")

def log_error(msg, exc: Exception = None):
    """Registra erro de forma amigável e opcionalmente registra traceback."""
    if exc is not None:
        logger.error(f"{msg} - Exception: {exc}")
        tb = traceback.format_exc()
        logger.debug(tb)
    else:
        logger.error(msg)

# ==============================
# 0. CONFIGURAÇÃO DE ESTILO E TEMA (SINAPSIS) - Corrigido para Azul #313191
# ==============================
COR_PRIMARIA = "#313191"  # Azul Sinapsis (Sidebar)
COR_SECUNDARIA = "#19c0d1" # Ciano Sinapsis
COR_CINZA = "#444444"
COR_FUNDO_APP = "#FFFFFF"
COR_FUNDO_SIDEBAR = COR_PRIMARIA

SINAPSIS_PALETTE = [COR_SECUNDARIA, COR_PRIMARIA, COR_CINZA, "#888888", "#C0C0C0"]
# Nome do arquivo do logo: crie uma pasta 'images/' e coloque o logo lá
LOGO_PATH = "images/logo_sinapsis.png" 

# ==============================
# 1. Credenciais PostgreSQL
# ==============================
try:
    DB_PARAMS = {
        "host": st.secrets["postgresql"]["host"],
        "port": st.secrets["postgresql"]["port"],
        "database": st.secrets["postgresql"]["database"],
        "user": st.secrets["postgresql"]["user"],
        "password": st.secrets["postgresql"]["password"],
        "sslmode": st.secrets["postgresql"].get("sslmode", "prefer"),
    }
except Exception as e:
    DB_PARAMS = {}
    st.error("Configuração 'st.secrets' não encontrada. Configure secrets.toml com 'postgresql'.")
    logger.warning("st.secrets postgres not configured. Running with DB disabled.")

# ==============================
# 2. Conexão com PostgreSQL
# ==============================
def get_db_connection():
    if not DB_PARAMS:
        return None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        log_error("Erro ao conectar ao banco de dados", e)
        return None

# ==============================
# 3. Setup do Banco (criação de tabelas)
# ==============================
def setup_db():
    conn = get_db_connection()
    if conn is None:
        return
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS usuarios (
                    usuario VARCHAR(100) PRIMARY KEY,
                    senha VARCHAR(255) NOT NULL,
                    admin BOOLEAN DEFAULT FALSE
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS atividades (
                    id SERIAL PRIMARY KEY,
                    usuario VARCHAR(100) REFERENCES usuarios(usuario),
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
            # Mantém os nomes das colunas como 'gerente' e 'subordinado' no DB,
            # mas usa 'Gestor da Área' e 'Pessoa da Equipe' na interface (PJ)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS hierarquia (
                    gerente VARCHAR(100) REFERENCES usuarios(usuario),
                    subordinado VARCHAR(100) REFERENCES usuarios(usuario),
                    PRIMARY KEY (gerente, subordinado),
                    CHECK (gerente != subordinado)
                );
            """)
            conn.commit()
    except Exception as e:
        log_error("Erro ao criar/verificar tabelas", e)
    finally:
        conn.close()

if DB_PARAMS:
    setup_db()

# ==============================
# UTIL: Hash/senha com bcrypt (com fallback/migração)
# ==============================
def hash_password(plain_password: str) -> str:
    hashed = bcrypt.hashpw(plain_password.encode("utf-8"), bcrypt.gensalt())
    return hashed.decode("utf-8")

def verify_password_and_migrate(usuario: str, plain_password: str) -> bool:
    """
    Verifica a senha:
    - Se o hash no DB parecer ser bcrypt, usa bcrypt.checkpw
    - Se parecer texto plano (fallback), compara e, em caso de ok, atualiza para bcrypt.
    """
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT senha FROM usuarios WHERE usuario = %s;", (usuario,))
            row = cur.fetchone()
            if not row:
                return False
            senha_db = row[0] or ""
            # Detecta hash bcrypt (começa com $2b$ ou $2a$ etc)
            if senha_db.startswith("$2"):
                try:
                    ok = bcrypt.checkpw(plain_password.encode("utf-8"), senha_db.encode("utf-8"))
                    return ok
                except Exception as e:
                    log_error("Erro ao verificar bcrypt", e)
                    return False
            else:
                # Senha em texto plano: compara, e migra para bcrypt se bater
                if plain_password == senha_db:
                    try:
                        novo_hash = hash_password(plain_password)
                        cur.execute("UPDATE usuarios SET senha = %s WHERE usuario = %s;", (novo_hash, usuario))
                        conn.commit()
                        logger.info(f"Senha do usuário '{usuario}' migrada para bcrypt.")
                    except Exception as e:
                        # não é crítico se migração falhar; continuar
                        log_error("Falha ao migrar senha para bcrypt", e)
                        conn.rollback()
                    return True
                else:
                    return False
    except Exception as e:
        log_error("Erro ao validar senha", e)
        return False
    finally:
        conn.close()

# ==============================
# 4. CRUD e Consultas
# ==============================
def salvar_usuario(usuario, senha, admin=False):
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        senha_hash = hash_password(senha)
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO usuarios (usuario, senha, admin)
                VALUES (%s, %s, %s)
                ON CONFLICT (usuario) DO NOTHING;
            """, (usuario, senha_hash, admin))
            conn.commit()
            return True
    except Exception as e:
        log_error("Erro ao salvar usuário", e)
        return False
    finally:
        conn.close()

def validar_login(usuario, senha):
    """
    Verifica credenciais. Retorna (ok: bool, is_admin: bool).
    Usa verify_password_and_migrate internamente.
    """
    conn = get_db_connection()
    if conn is None:
        return False, False
    try:
        with conn.cursor() as cursor:
            # Não recupera a senha aqui, pois verify_password_and_migrate fará isso
            cursor.execute("SELECT admin FROM usuarios WHERE usuario = %s;", (usuario,))
            row = cursor.fetchone()
            if not row:
                return False, False
            
            # Primeiro tenta via verify_password_and_migrate (que faz migração se necessário)
            ok = verify_password_and_migrate(usuario, senha)
            
            if not ok:
                return False, False

            # Se ok, retorna o status admin
            return True, bool(row[0]) if row else False

    except Exception as e:
        log_error("Erro ao validar login", e)
        return False, False
    finally:
        conn.close()

def alterar_senha(usuario, nova_senha):
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        senha_hash = hash_password(nova_senha)
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE usuarios
                SET senha = %s
                WHERE usuario = %s;
            """, (senha_hash, usuario))
            conn.commit()
            return True
    except Exception as e:
        log_error("Erro ao alterar senha", e)
        return False
    finally:
        conn.close()

def calcular_porcentagem_existente(usuario, mes, ano, excluido_id=None):
    conn = get_db_connection()
    if conn is None:
        return 101
    try:
        with conn.cursor() as cursor:
            query = """
                SELECT COALESCE(SUM(porcentagem), 0)
                FROM atividades
                WHERE usuario = %s AND mes = %s AND ano = %s AND status != 'Rejeitado'
            """ # Exclui rejeitadas da soma
            params = [usuario, mes, ano]
            if excluido_id is not None:
                query += " AND id != %s"
                params.append(excluido_id)
            cursor.execute(query + ";", params)
            result = cursor.fetchone()
            return int(result[0]) if result and result[0] is not None else 0
    except Exception as e:
        log_error("Erro ao calcular porcentagem existente", e)
        return 101
    finally:
        conn.close()

def salvar_atividade(usuario, mes, ano, descricao, projeto, porcentagem, observacao, atividade_id=None):
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cursor:
            data_db = datetime(year=ano, month=mes, day=1).date()
            if atividade_id is None:
                query = """
                    INSERT INTO atividades (usuario, data, mes, ano, descricao, projeto, porcentagem, observacao)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
                params = (usuario, data_db, mes, ano, descricao, projeto, porcentagem, observacao)
            else:
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
        log_error("Erro ao salvar/editar atividade", e)
        return False
    finally:
        conn.close()

def apagar_atividade(atividade_id):
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM atividades WHERE id = %s;", (atividade_id,))
            conn.commit()
            return True
    except Exception as e:
        log_error("Erro ao apagar atividade", e)
        return False
    finally:
        conn.close()

def atualizar_status_atividade(atividade_id, novo_status):
    conn = get_db_connection()
    if conn is None:
        return False
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
        log_error("Erro ao atualizar status de atividade", e)
        return False
    finally:
        conn.close()

def salvar_hierarquia(gerente, subordinado):
    conn = get_db_connection()
    if conn is None:
        return False
    if gerente == subordinado:
        st.error("Gestor e pessoa da equipe não podem ser a mesma pessoa.")
        return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO hierarquia (gerente, subordinado)
                VALUES (%s, %s)
                ON CONFLICT (gerente, subordinado) DO NOTHING;
            """, (gerente, subordinado))
            conn.commit()
            return True
    except Exception as e:
        log_error("Erro ao salvar hierarquia", e)
        return False
    finally:
        conn.close()

def apagar_hierarquia(gerente, subordinado):
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM hierarquia
                WHERE gerente = %s AND subordinado = %s;
            """, (gerente, subordinado))
            conn.commit()
            return True
    except Exception as e:
        log_error("Erro ao apagar hierarquia", e)
        return False
    finally:
        conn.close()

# ==============================
# 5. Caching e carregamento de dados (melhorias: max_entries)
# ==============================
@st.cache_data(ttl=600, max_entries=10)
def carregar_hierarquia():
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        # Colunas mantidas como 'gerente' e 'subordinado' no DB
        df = pd.read_sql("SELECT gerente, subordinado FROM hierarquia ORDER BY gerente, subordinado;", conn)
        return df
    except Exception as e:
        log_error("Erro ao carregar hierarquia", e)
        return pd.DataFrame()
    finally:
        conn.close()

@st.cache_data(ttl=600, max_entries=10)
def carregar_dados():
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame(), pd.DataFrame()
    query_full = """
        SELECT id, usuario, data, mes, ano, descricao, projeto, porcentagem, observacao, status
        FROM atividades ORDER BY ano DESC, mes DESC, data DESC;
    """
    query_base = """
        SELECT id, usuario, data, mes, ano, descricao, projeto, porcentagem, observacao
        FROM atividades ORDER BY ano DESC, mes DESC, data DESC;
    """
    try:
        usuarios_df = pd.read_sql("SELECT usuario, admin FROM usuarios;", conn)
        try:
            atividades_df = pd.read_sql(query_full, conn)
        except Exception as e:
            # Caso não exista a coluna status (migração), tenta sem ela e adiciona coluna 'status' no DataFrame
            if 'status' in str(e).lower() or 'column "status" does not exist' in str(e).lower():
                atividades_df = pd.read_sql(query_base, conn)
                if not atividades_df.empty:
                    atividades_df['status'] = 'Pendente'
            else:
                raise
        if not atividades_df.empty:
            atividades_df['data'] = pd.to_datetime(atividades_df['data'])
        return usuarios_df, atividades_df
    except Exception as e:
        log_error("Erro ao carregar dados", e)
        return pd.DataFrame(), pd.DataFrame()
    finally:
        conn.close()

def bulk_insert_usuarios(user_list):
    conn = get_db_connection()
    if conn is None:
        return 0, "❌ Falha na conexão com o banco de dados."
    data_list = [(user, hash_password('123'), False) for user in user_list]
    query = """
        INSERT INTO usuarios (usuario, senha, admin)
        VALUES (%s, %s, %s)
        ON CONFLICT (usuario) DO NOTHING
    """
    try:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_batch(cursor, query, data_list)
            conn.commit()
            return cursor.rowcount, "✅ Sucesso! Usuários pré-cadastrados com senha padrão '123'."
    except Exception as e:
        conn.rollback()
        log_error("Erro no pré-cadastro de usuários", e)
        return 0, f"Erro durante o pré-cadastro de usuários: {e}"
    finally:
        conn.close()

def bulk_insert_atividades(df_to_insert):
    conn = get_db_connection()
    if conn is None:
        return 0, "❌ Falha na conexão com o banco de dados."
    required_cols = ['usuario', 'data', 'mes', 'ano', 'descricao', 'projeto', 'porcentagem', 'observacao', 'status']
    if not all(c in df_to_insert.columns for c in required_cols):
        return 0, "❌ DataFrame deve conter as colunas: " + ", ".join(required_cols)
    data_list = [tuple(row) for row in df_to_insert[required_cols].values]
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
        log_error("Erro durante a importação em massa de atividades", e)
        return 0, f"❌ Erro durante a importação em massa: {e}"
    finally:
        conn.close()

def limpar_nomes_usuarios_db():
    conn = get_db_connection()
    if conn is None:
        return False, "Falha na conexão com o banco de dados."
    try:
        with conn.cursor() as cursor:
            # Atualiza atividades/hierarquia para remover espaços (atenção: nomes de tabelas em PT/EN)
            # Mantive nomes originais das tabelas (atividades/hierarquia)
            cursor.execute("""UPDATE atividades SET usuario = TRIM(usuario);""")
            atividades_afetadas = cursor.rowcount
            cursor.execute("""UPDATE hierarquia SET gerente = TRIM(gerente), subordinado = TRIM(subordinado);""")
            hierarquia_afetadas = cursor.rowcount
            cursor.execute("""
                SELECT DISTINCT TRIM(usuario) FROM atividades
                UNION
                SELECT DISTINCT TRIM(gerente) FROM hierarquia
                UNION
                SELECT DISTINCT TRIM(subordinado) FROM hierarquia
                UNION
                SELECT DISTINCT usuario FROM usuarios;
            """)
            usuarios_limpos = list(set([row[0] for row in cursor.fetchall() if row[0]]))
            cursor.execute("SELECT usuario, admin FROM usuarios;")
            status_admin_original = dict(cursor.fetchall())
            cursor.execute("TRUNCATE TABLE usuarios CASCADE;")
            usuarios_para_reinserir = []
            for user in usuarios_limpos:
                is_admin = status_admin_original.get(user, False)
                usuarios_para_reinserir.append((user, hash_password('123'), is_admin))
            usuarios_reinseridos = 0
            if usuarios_para_reinserir:
                query_insert_users = """
                    INSERT INTO usuarios (usuario, senha, admin)
                    VALUES (%s, %s, %s)
                """
                psycopg2.extras.execute_batch(cursor, query_insert_users, usuarios_para_reinserir)
                usuarios_reinseridos = cursor.rowcount
            conn.commit()
            return True, (
                f"✅ Sucesso! Limpeza concluída. "
                f"{atividades_afetadas} atividades e {hierarquia_afetadas} hierarquias corrigidas. "
                f"{usuarios_reinseridos} usuários reinseridos (senha padrão: '123')."
            )
    except Exception as e:
        conn.rollback()
        log_error("Erro ao limpar nomes no DB", e)
        return False, f"❌ Erro ao limpar nomes no DB: {e}"
    finally:
        conn.close()

# ==============================
# Funções Auxiliares (horas e observação)
# ==============================
def extrair_hora_bruta(observacao):
    if observacao is None:
        return 0.0, ''
    match = re.search(r'\[HORA:(\d+\.?\d*)\|(.*)\]', observacao, re.DOTALL)
    if match:
        try:
            hora = float(match.group(1))
        except ValueError:
            hora = 0.0
        obs_limpa = match.group(2).strip()
        return hora, obs_limpa
    return 0.0, observacao.strip()

def atualizar_porcentagem_atividade(atividade_id, nova_porcentagem):
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE atividades
                SET porcentagem = %s
                WHERE id = %s;
            """, (nova_porcentagem, atividade_id))
            conn.commit()
            return True
    except Exception as e:
        log_error(f"Erro ao recalcular porcentagem da atividade {atividade_id}", e)
        return False
    finally:
        conn.close()

def carregar_atividades_usuario(usuario, mes, ano):
    conn = get_db_connection()
    if conn is None:
        return []
    try:
        query = """
            SELECT id, descricao, projeto, porcentagem, observacao, status
            FROM atividades
            WHERE usuario = %s AND mes = %s AND ano = %s
            ORDER BY id DESC;
        """
        df = pd.read_sql(query, conn, params=(usuario, mes, ano))
        return df.to_dict('records')
    except Exception as e:
        log_error("Erro ao carregar atividades do usuário", e)
        return []
    finally:
        conn.close()

def atualizar_atividade(atividade_id, nova_porcentagem, nova_observacao):
    conn = get_db_connection()
    if conn is None:
        return False
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
        log_error(f"Erro ao atualizar atividade {atividade_id}", e)
        return False
    finally:
        conn.close()

def excluir_atividade(atividade_id):
    return apagar_atividade(atividade_id)

# ==============================
# Dados fixos (mantidos)
# ==============================
DESCRICOES = [
    "1.001 - Gestão","1.002 - Geral","1.003 - Conselho","1.004 - Treinamento e Desenvolvimento",
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
    "10.013 - Preparação de treinamento externo","10.014 - Realização de treinamento externo","10.015 - Mapeamento de Integrações"
]

PROJETOS = [
    "101-0 (Interno) Diretoria Executiva","102-0 (Interno) Diretoria Administrativa","103-0 (Interno) Diretoria de Engenharia",
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
    "680-0 (CESI) Angola Banco Mundial","681-0 (CEMACON) Suporte SINAPgrid","682-0 (FECOERGS) Treinamento SINAPgrid"
]

DESCRICOES_SELECT = ["--- Selecione ---"] + DESCRICOES
PROJETOS_SELECT = ["--- Selecione ---"] + PROJETOS

MESES = {
    1: "01 - Janeiro", 2: "02 - Fevereiro", 3: "03 - Março", 4: "04 - Abril",
    5: "05 - Maio", 6: "06 - Junho", 7: "07 - Julho", 8: "08 - Agosto",
    9: "09 - Setembro", 10: "10 - Outubro", 11: "11 - Novembro", 12: "12 - Dezembro"
}
MESES_SELECT = ["--- Selecione ---"] + list(MESES.values())
ANOS = list(range(datetime.today().year - 2, datetime.today().year + 3))

STATUS_CORES = {
    "Pendente": "orange",
    "Aprovado": "green",
    "Rejeitado": "red"
}

# ==============================
# Callbacks e utilitários Streamlit (st.rerun() e st.toast())
# ==============================
def set_edit_id(id_atividade):
    st.session_state['edit_id'] = id_atividade
    st.rerun()

def cancelar_edicao():
    st.session_state['edit_id'] = None
    st.rerun()

def handle_delete(atividade_id):
    if apagar_atividade(atividade_id):
        carregar_dados.clear()
        st.success("✅ Atividade excluída com sucesso.")
        st.rerun()
    else:
        st.error("❌ Erro ao excluir atividade.")

def handle_status_update(atividade_id, novo_status):
    if atualizar_status_atividade(atividade_id, novo_status):
        carregar_dados.clear()
        st.success(f"✅ Status atualizado para: {novo_status}")
        st.rerun()
    else:
        st.error("❌ Erro ao atualizar status.")

def is_user_a_manager(usuario, hierarquia_df):
    if hierarquia_df.empty:
        return False
    # Checa se o usuário é 'gerente' de alguém (Gestor da Área)
    return usuario in hierarquia_df['gerente'].unique()

# ==============================
# Sessão Streamlit (variáveis)
# ==============================
if "usuario" not in st.session_state:
    st.session_state["usuario"] = None
    st.session_state["admin"] = False
if 'edit_id' not in st.session_state:
    st.session_state['edit_id'] = None
if 'show_change_password' not in st.session_state:
    st.session_state['show_change_password'] = False

# Carrega dados
usuarios_df, atividades_df = carregar_dados()
hierarquia_df = carregar_hierarquia()

# ==============================
# Injeção de CSS (corrigido para o Azul Sinapsis na sidebar)
# ==============================
st.markdown(f"""
    <style>
        :root {{
            --primary-color: {COR_SECUNDARIA};
            --sidebar-bg: {COR_FUNDO_SIDEBAR};
        }}
        [data-testid="stSidebar"] {{
            background-color: var(--sidebar-bg);
        }}
        /* Garante texto branco na sidebar */
        [data-testid="stSidebar"] * {{
            color: #FFFFFF !important;
        }}
        [data-testid="stSidebar"] a {{
            color: #FFFFFF !important;
        }}
        .stApp {{
            background-color: {COR_FUNDO_APP};
        }}
        .status-badge {{
            padding: 4px 8px;
            border-radius: 10px;
            font-size: 0.85em;
            font-weight: 600;
            display: inline-block;
        }}
        .status-Pendente {{ background-color: #ffcc99; color: #cc6600; }}
        .status-Aprovado {{ background-color: #ccffcc; color: #008000; }}
        .status-Rejeitado {{ background-color: #ff9999; color: #cc0000; }}
        /* Pequenas melhorias de responsividade */
        .stButton>button {{
            border-radius: 8px;
        }}
        /* Ajusta o logo para ocupar a largura da sidebar e centralizar no topo */
        [data-testid="stSidebar"] .stImage {{
            padding-top: 10px;
            padding-bottom: 20px;
            text-align: center;
        }}
        [data-testid="stSidebar"] .stImage img {{
            max-width: 80%; /* Ajuste conforme o tamanho ideal do logo */
            height: auto;
            margin: auto;
            display: block;
        }}
    </style>
""", unsafe_allow_html=True)

# ==============================
# Layout principal e menus (linguagem PJ)
# ==============================
if st.session_state["usuario"] is None:
    st.title("🔐 Acesso à Plataforma")
    usuario = st.text_input("Usuário")
    senha = st.text_input("Senha", type="password")
    if st.button("Entrar"):
        usuario_limpo = usuario.strip()
        ok, admin = validar_login(usuario_limpo, senha)
        if ok:
            st.session_state["usuario"] = usuario_limpo
            st.session_state["admin"] = admin
            st.toast("✅ Login realizado com sucesso!")
            st.rerun()
        else:
            st.error("❌ Usuário ou senha incorretos. Tente novamente.")
else:
    # --- Sidebar Conteúdo ---
    
    # 1. Logo da Sinapsis
    try:
        st.sidebar.image(LOGO_PATH, use_column_width=True)
    except FileNotFoundError:
        st.sidebar.markdown(f"**{st.session_state['usuario']}**")
        st.sidebar.warning(f"Logo não encontrado em `{LOGO_PATH}`")
    
    st.sidebar.markdown(f"**Usuário Ativo:** **{st.session_state['usuario']}**")
    
    # 2. Alterar Senha
    if st.sidebar.button("🔑 Alterar Senha"):
        st.session_state['show_change_password'] = not st.session_state['show_change_password']
        st.rerun()
        
    if st.session_state['show_change_password']:
        with st.sidebar.form("form_change_password"):
            st.markdown("---")
            st.markdown("##### Alterar Senha")
            nova_senha_1 = st.text_input("Nova Senha", type="password", help="Mínimo 6 caracteres.")
            nova_senha_2 = st.text_input("Confirme a Nova Senha", type="password")
            if st.form_submit_button("Atualizar Senha", type="primary"):
                if nova_senha_1 and nova_senha_1 == nova_senha_2:
                    if alterar_senha(st.session_state["usuario"], nova_senha_1):
                        st.toast("✅ Senha atualizada! Por favor, faça login novamente.")
                        st.session_state["usuario"] = None
                        st.session_state["admin"] = False
                        st.session_state['show_change_password'] = False
                        st.rerun()
                    else:
                        st.sidebar.error("❌ Erro ao atualizar senha no DB.")
                else:
                    st.sidebar.error("❌ As senhas não coincidem ou estão vazias.")
                    
    st.sidebar.markdown("---")
    
    # 3. Sair
    if st.sidebar.button("Sair", type="secondary"):
        st.session_state["usuario"] = None
        st.session_state["admin"] = False
        st.session_state['show_change_password'] = False
        st.toast("👋 Logout realizado.")
        st.rerun()

    # 4. Menu Principal
    is_manager = is_user_a_manager(st.session_state["usuario"], hierarquia_df)

    abas = ["Lançar Atividade", "Minhas Atividades"]
    # Linguagem PJ: Gestão de Equipe
    if st.session_state["admin"] or is_manager:
        abas.append("Gestão de Equipe")
    if st.session_state["admin"]:
        abas += ["Gerenciar Usuários", "Consolidado", "Importar Dados"]

    aba = st.sidebar.radio("Menu de Navegação", abas)

    # ------------------------------
    # Gerenciar Usuários (Admin)
    # ------------------------------
    if aba == "Gerenciar Usuários" and st.session_state["admin"]:
        st.header("👥 Gestão de Usuários (Admin)")
        
        # --- Ferramentas de Manutenção ---
        st.subheader("Ferramentas de Manutenção")
        st.info("A limpeza de nomes remove espaços em branco (TRIM) e redefine as senhas dos usuários afetados para **'123'**.")
        if st.button("⚙️ Executar Limpeza de Nomes (TRIM)"):
            with st.spinner("Executando limpeza e reinserção de usuários..."):
                sucesso, mensagem = limpar_nomes_usuarios_db()
            carregar_dados.clear()
            carregar_hierarquia.clear()
            if sucesso:
                st.success(mensagem)
            else:
                st.error(mensagem)
            st.rerun()

        # --- Adicionar Novo Usuário ---
        st.subheader("Adicionar Novo Usuário")
        with st.form("form_add_user"):
            novo_usuario = st.text_input("Usuário (Login)")
            nova_senha = st.text_input("Senha Inicial", type="password")
            admin_check = st.checkbox("Tornar Administrador", value=False)
            if st.form_submit_button("➕ Adicionar Usuário", type="primary"):
                if novo_usuario and nova_senha:
                    if salvar_usuario(novo_usuario.strip(), nova_senha, admin_check):
                        st.success("✅ Usuário adicionado com sucesso.")
                        carregar_dados.clear()
                        st.rerun()
                    else:
                        st.error("❌ Erro ao adicionar usuário. Pode ser duplicado.")
                else:
                    st.error("❌ Preencha usuário e senha.")

        # --- Lista de Usuários ---
        usuarios_df_reloaded, _ = carregar_dados()
        st.subheader("Lista Completa de Usuários")
        st.dataframe(usuarios_df_reloaded, use_container_width=True)

    # ------------------------------
    # Gestão de Equipe (Gestor da Área) - Linguagem PJ
    # ------------------------------
    elif aba == "Gestão de Equipe":
        # Título ajustado para linguagem PJ
        st.header("🤝 Gestão de Equipe e Validação de Entregas")
        hierarquia_df_reloaded = carregar_hierarquia()
        usuarios_list = usuarios_df['usuario'].tolist() if not usuarios_df.empty else []

        usuario_logado = st.session_state["usuario"]

        if st.session_state["admin"]:
            st.info("Você é **Administrador** e pode configurar e visualizar **qualquer** equipe.")
            
            # --- Configuração de Hierarquia (Admin) ---
            st.subheader("Configurar Relações de Equipe (Admin)")
            # Termos ajustados na interface: Gerente -> Gestor da Área, Subordinado -> Pessoa da Equipe
            gerentes_disponiveis = sorted(usuarios_list)
            with st.form("form_config_hierarquia"):
                col_g1, col_g2 = st.columns(2)
                gestor_selecionado = col_g1.selectbox("Gestor da Área", gerentes_disponiveis, key="sb_gerente")
                subordinados_disponiveis = [u for u in usuarios_list if u != gestor_selecionado]
                pessoa_equipe = col_g2.selectbox("Pessoa da Equipe", ["--- Selecione ---"] + sorted(subordinados_disponiveis), key="sb_subordinado")
                if st.form_submit_button("Adicionar Pessoa à Equipe", type="primary"):
                    if pessoa_equipe != "--- Selecione ---":
                        # Salva no DB com os nomes originais (gerente, subordinado)
                        if salvar_hierarquia(gestor_selecionado, pessoa_equipe):
                            st.success(f"✅ **{pessoa_equipe}** adicionado ao time de **{gestor_selecionado}**.")
                            carregar_hierarquia.clear()
                            st.rerun()
                        else:
                            st.error("❌ Erro ao salvar associação. Verifique se a relação já existe.")
                    else:
                        st.warning("Selecione uma pessoa válida para a equipe.")

            st.markdown("---")
            
            # --- Visualizar/Remover Associações ---
            st.subheader("Visualizar e Remover Associações")
            if hierarquia_df_reloaded.empty:
                st.info("Nenhuma associação Gestor/Equipe configurada.")
            else:
                # Renomeia colunas apenas para exibição
                df_exibicao_hierarquia = hierarquia_df_reloaded.rename(columns={
                    'gerente': 'Gestor da Área',
                    'subordinado': 'Pessoa da Equipe'
                })
                st.dataframe(df_exibicao_hierarquia, use_container_width=True)
                
                with st.form("form_remover_hierarquia"):
                    st.markdown("Remover Relação:")
                    gerentes_remover_list = sorted(hierarquia_df_reloaded['gerente'].unique())
                    gerente_remover = st.selectbox("Gestor da Área (Remoção)", gerentes_remover_list, key="gerente_remover")
                    subordinados_do_gerente = hierarquia_df_reloaded[hierarquia_df_reloaded['gerente'] == gerente_remover]['subordinado'].tolist()
                    subordinado_remover = st.selectbox("Pessoa da Equipe a Remover", sorted(subordinados_do_gerente), key="subordinado_remover")
                    if st.form_submit_button("❌ Remover Associação", type="secondary"):
                        if apagar_hierarquia(gerente_remover, subordinado_remover):
                            st.success(f"❌ Associação removida entre **{gerente_remover}** e **{subordinado_remover}**.")
                            carregar_hierarquia.clear()
                            st.rerun()
                        else:
                            st.error("❌ Erro ao remover associação.")

        # --- Fluxo de Análise e Validação de Entregas ---
        st.markdown("---")
        st.subheader("Análise e Validação de Entregas")

        gerentes_com_time = list(hierarquia_df_reloaded['gerente'].unique()) if not hierarquia_df_reloaded.empty else []
        
        # O usuário logado é um gestor?
        if not gerentes_com_time or (not st.session_state["admin"] and usuario_logado not in gerentes_com_time):
            st.warning("⚠️ Você não está configurado como **Gestor da Área** de nenhuma equipe. Peça a um administrador para configurar sua hierarquia.")
            st.stop()

        # Seleção de equipe a ser analisada
        if st.session_state["admin"]:
            gestor_a_analisar = st.selectbox("Selecione a **Equipe** para Análise", sorted(gerentes_com_time), key="gestor_analise_admin")
        else:
            gestor_a_analisar = usuario_logado
            st.markdown(f"**Equipe em análise:** **{gestor_a_analisar}** (Seu time)")

        if gestor_a_analisar not in gerentes_com_time:
            st.error("❌ Equipe inválida selecionada ou sem membros.")
            st.stop()

        meu_time_df = hierarquia_df_reloaded[hierarquia_df_reloaded['gerente'] == gestor_a_analisar]
        pessoas_equipe = meu_time_df['subordinado'].tolist()

        col_m1, col_m2 = st.columns(2)
        hoje = datetime.now()
        mes_vigente_num = hoje.month
        ano_vigente = hoje.year
        meses_para_filtro = list(MESES.values())
        try:
            default_mes_idx = meses_para_filtro.index(MESES.get(mes_vigente_num))
        except Exception:
            default_mes_idx = 0
        mes_nome_analise = col_m1.selectbox("Mês de Referência", meses_para_filtro, index=default_mes_idx, key="sb_mes_analise")
        ano_analise = col_m2.selectbox("Ano de Referência", ANOS, index=ANOS.index(ano_vigente), key="sb_ano_analise")
        mes_num_analise = next((k for k, v in MESES.items() if v == mes_nome_analise), None)
        if mes_num_analise is None:
            st.error("❌ Mês inválido selecionado.")
            st.stop()

        df_time_mes = atividades_df[
            (atividades_df['usuario'].isin(pessoas_equipe)) &
            (atividades_df['mes'] == mes_num_analise) &
            (atividades_df['ano'] == ano_analise)
        ]

        # --- Resumo de Alocação ---
        df_resumo_alocacao = df_time_mes.groupby('usuario')['porcentagem'].sum().reset_index()
        # Coluna ajustada para linguagem PJ
        df_resumo_alocacao.columns = ['Pessoa da Equipe', 'Total Alocado (%)']

        usuarios_com_lancamento = df_resumo_alocacao['Pessoa da Equipe'].tolist()
        usuarios_sem_lancamento = [u for u in pessoas_equipe if u not in usuarios_com_lancamento]
        for u in usuarios_sem_lancamento:
            df_resumo_alocacao.loc[len(df_resumo_alocacao)] = [u, 0]

        def color_alocacao(val):
            if isinstance(val, str): return ''
            if val < 50:
                return 'background-color: #ffcccc'
            elif 50 <= val < 100:
                return 'background-color: #ffffcc'
            elif val == 100:
                return 'background-color: #ccffcc'
            else:
                return 'background-color: #ff9999; font-weight: bold'

        df_final_style = df_resumo_alocacao.style.applymap(color_alocacao, subset=['Total Alocado (%)'])
        st.markdown(f"##### Status de Alocação da Equipe **{gestor_a_analisar}** em **{mes_nome_analise}/{ano_analise}**")
        st.dataframe(df_final_style, use_container_width=True)
        st.markdown("---")

        # --- Tabela de Entregas para Validação ---
        st.subheader(f"Entregas da Equipe **{gestor_a_analisar}** para Validação")
        col_fa1, col_fa2 = st.columns(2)
        status_filtro = col_fa1.selectbox("Filtrar por Status", ["Todos", "Pendente", "Aprovado", "Rejeitado"], key="status_filtro_time")
        # Termo ajustado na interface: Subordinado -> Pessoa da Equipe
        pessoa_filtro = col_fa2.selectbox("Filtrar por Pessoa da Equipe", ["Todos"] + sorted(pessoas_equipe), key="liderado_filtro_time")

        df_aprovacao = df_time_mes.copy()
        if status_filtro != "Todos":
            df_aprovacao = df_aprovacao[df_aprovacao['status'] == status_filtro]
        if pessoa_filtro != "Todos":
            df_aprovacao = df_aprovacao[df_aprovacao['usuario'] == pessoa_filtro]
            
        # Adiciona exportação para Admin/Gestor
        if not df_aprovacao.empty:
            csv = df_aprovacao.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="⬇️ Exportar Entregas do Time (CSV)",
                data=csv,
                file_name=f"entregas_equipe_{gestor_a_analisar}_{mes_num_analise}_{ano_analise}.csv",
                mime="text/csv",
                key="btn_export_aprov"
            )

        if df_aprovacao.empty:
            st.info("Nenhuma entrega encontrada com os filtros selecionados.")
        else:
            for idx, row in df_aprovacao.iterrows():
                _, observacao_limpa_gestor = extrair_hora_bruta(row.get('observacao', ''))
                badge_status = f'<span class="status-badge status-{row["status"]}">{row["status"]}</span>'
                
                # Layout compacto para aprovação
                col1_d, col2_d, col3_d, col4_d = st.columns([3, 1, 1, 1])
                with col1_d:
                    # Termo ajustado na interface: Colaborador/Membro da Equipe
                    st.markdown(f"**Pessoa da Equipe:** **{row['usuario']}** | ID {row['id']} | {badge_status}", unsafe_allow_html=True)
                    st.markdown(f"**Referência:** {MESES.get(row['mes'])}/{row['ano']} | **{row['porcentagem']}%**")
                    st.markdown(f"*Descrição:* {row['descricao']}")
                    st.markdown(f"*Projeto:* {row['projeto']}")
                    st.markdown(f"*Obs:* {observacao_limpa_gestor if observacao_limpa_gestor else '(Não informada)'}")
                with col2_d:
                    st.button("✅ Validar", key=f"apv_{row['id']}", on_click=handle_status_update, args=(row['id'], 'Aprovado'), type="primary")
                with col3_d:
                    st.button("❌ Rejeitar", key=f"rej_{row['id']}", on_click=handle_status_update, args=(row['id'], 'Rejeitado'), type="secondary")
                with col4_d:
                    st.button("🗑️ Excluir", key=f"del_a_{row['id']}", on_click=handle_delete, args=(row['id'],))
                st.markdown("---")

    # ------------------------------
    # Lançar Atividade
    # ------------------------------
    elif aba == "Lançar Atividade":
        st.header("📝 Lançar Atividade (Mensal)")
        
        col_mes, col_ano = st.columns(2)
        mes_select = col_mes.selectbox(
            "Mês de Referência",
            MESES_SELECT,
            index=list(MESES.values()).index(MESES[datetime.today().month]) + 1,
            key="lanc_mes_select"
        )
        ano_select = col_ano.selectbox("Ano de Referência", ANOS, index=ANOS.index(datetime.today().year), key="lanc_ano_select")
        mes_num = next((k for k, v in MESES.items() if v == mes_select), None)

        if mes_num:
            atividades_do_mes = carregar_atividades_usuario(st.session_state["usuario"], mes_num, ano_select)
        else:
            atividades_do_mes = []

        atividades_ativas = [a for a in atividades_do_mes if a.get('status') != 'Rejeitado']
        total_existente = sum(a.get("porcentagem", 0) for a in atividades_ativas)
        saldo_restante = max(0, 100 - total_existente)

        horas_brutas_ativas = []
        for a in atividades_ativas:
            hora, _ = extrair_hora_bruta(a.get('observacao', ''))
            if hora > 0:
                horas_brutas_ativas.append({'id': a['id'], 'hora': hora, 'obs_original_completa': a.get('observacao', '')})
        total_horas_existentes = sum(h['hora'] for h in horas_brutas_ativas)

        tipo_lancamento = st.radio("Tipo de Lançamento:", ["Porcentagem (%)", "Horas"], horizontal=True, key="lanc_tipo")
        
        # --- Info Boxes ---
        if "Porcentagem" in tipo_lancamento:
            st.info(f"📅 Mês: **{mes_select}/{ano_select}** •  Total alocado: **{total_existente:.1f}%** •  Saldo disponível: **{saldo_restante:.1f}%**")
        else:
            st.info(f"📅 Mês: **{mes_select}/{ano_select}** •  Horas brutas já lançadas: **{total_horas_existentes:.1f} hrs** •  *Modo Horas: atividades serão recalculadas proporcionalmente para 100%*")

        qtd_lancamentos = st.number_input("Quantos Lançamentos deseja adicionar?", min_value=1, max_value=20, value=st.session_state.get("lanc_qtd", 1), step=1, key="lanc_qtd")
        st.markdown("---")
        
        # --- Formulário de Lançamento em Lote ---
        lancamentos = []
        for i in range(qtd_lancamentos):
            st.markdown(f"**Entrada {i+1}**")
            col1, col2 = st.columns(2)
            descricao = col1.selectbox(f"Descrição da Atividade {i+1}", DESCRICOES_SELECT, key=f"desc_{i}")
            projeto = col2.selectbox(f"Projeto Relacionado {i+1}", PROJETOS_SELECT, key=f"proj_{i}")
            
            if "Porcentagem" in tipo_lancamento:
                valor = st.number_input(f"Porcentagem Alocada {i+1} (%)", min_value=0.0, max_value=100.0, value=st.session_state.get(f"valor_{i}", 0.0), step=1.0, key=f"valor_{i}")
            else:
                valor = st.number_input(f"Horas Dedicadas {i+1}", min_value=0.0, max_value=200.0, value=st.session_state.get(f"valor_{i}", 0.0), step=0.5, key=f"valor_{i}")
                
            observacao = st.text_area(f"Observação {i+1} (Opcional)", key=f"obs_{i}", value=st.session_state.get(f"obs_{i}", ""))
            st.markdown("---")
            lancamentos.append({"descricao": descricao, "projeto": projeto, "valor": valor, "observacao": observacao})

        # --- Pré-visualização e Validação ---
        preview_data = []
        lancamentos_validos = [l for l in lancamentos if l["valor"] > 0]
        soma_nova = 0
        total_geral_horas = total_horas_existentes

        if lancamentos_validos:
            if "Horas" in tipo_lancamento:
                total_horas_novas = sum(l["valor"] for l in lancamentos_validos)
                total_geral_horas += total_horas_novas
                if total_geral_horas > 0:
                    for l in lancamentos_validos:
                        # Cálculo proporcional do novo item baseado no novo total de horas
                        porcent = (l["valor"] / total_geral_horas) * 100
                        l["porcentagem_final"] = round(porcent, 2)
                        obs_real = l["observacao"] if l["observacao"] else ""
                        # Formato especial para observação em modo Horas
                        l["observacao"] = f"[HORA:{l['valor']}|{obs_real}]"
                        preview_data.append({"Descrição": l["descricao"], "Projeto": l["projeto"], "Porcentagem": porcent})
                    soma_nova = sum(p["Porcentagem"] for p in preview_data)
            else:
                for l in lancamentos_validos:
                    l["porcentagem_final"] = l["valor"]
                    preview_data.append({"Descrição": l["descricao"], "Projeto": l["projeto"], "Porcentagem": l["valor"]})
                soma_nova = sum(l["valor"] for l in lancamentos_validos)

        if "Porcentagem" in tipo_lancamento:
            total_final = total_existente + soma_nova
            saldo_final = max(0, 100 - total_final)
        else:
            # Em modo Horas, o total final será sempre 100% (após o recálculo proporcional)
            total_final = 100.0 
            saldo_final = 0.0

        st.subheader("📊 Pré-visualização dos Lançamentos")
        if preview_data:
            df_preview = pd.DataFrame(preview_data)
            col_graf, col_info = st.columns([2,1])
            with col_graf:
                fig_preview = px.pie(df_preview, names="Descrição", values="Porcentagem", title="Distribuição dos novos lançamentos", hole=.4, color_discrete_sequence=SINAPSIS_PALETTE)
                fig_preview.update_traces(texttemplate='%{value:.1f}%', textposition='inside')
                st.plotly_chart(fig_preview, use_container_width=True)
            with col_info:
                st.markdown("##### Resumo da Alocação")
                if "Horas" in tipo_lancamento:
                    st.markdown(f"**Total Horas (Mês + Novo):** **{total_geral_horas:.1f} hrs**")
                    st.markdown(f"**Porcentagem Nova (Proporcional):** **{soma_nova:.1f}%**")
                    st.markdown("**Resultado final (após salvar):** **100.0%**")
                    if total_geral_horas == 0:
                        st.warning("Adicione horas para calcular a proporção.")
                else:
                    st.markdown(f"**Total Novo a Lançar:** **{soma_nova:.1f}%**")
                    st.markdown(f"**Total Atual + Novo:** **{total_final:.1f}%**")
                    st.markdown(f"**Saldo após salvar:** **{saldo_final:.1f}%**")
                    if total_final > 100.0 + 0.001:
                        st.error("⚠️ O total projetado **ultrapassa 100%**. Ajuste os valores.")
        else:
            st.info("Preencha os lançamentos (valor > 0) para visualizar o preview e os totais.")

        # --- Botão Salvar ---
        if st.button("💾 Salvar Lançamentos", key="btn_save_multi_lanc", type="primary"):
            if mes_num is None:
                st.error("❌ Selecione um mês válido.")
                st.stop()
            if not lancamentos_validos:
                st.error("❌ Nenhum lançamento válido encontrado (valor > 0).")
                st.stop()
            for l in lancamentos_validos:
                if l["descricao"] == "--- Selecione ---" or l["projeto"] == "--- Selecione ---":
                    st.error("❌ Todas as entradas válidas devem ter **Descrição** e **Projeto** selecionados.")
                    st.stop()
            if "Porcentagem" in tipo_lancamento and (total_final > 100.0 + 0.001):
                st.error("❌ O total de alocação **excede 100%**. Ajuste antes de salvar.")
                st.stop()
            if "Horas" in tipo_lancamento and total_geral_horas <= 0:
                st.error("❌ O total de horas bruto é zero. Adicione valores positivos.")
                st.stop()

            # Lógica de recálculo (só no modo Horas)
            recalcular_e_atualizar = ("Horas" in tipo_lancamento and total_geral_horas > 0)
            if recalcular_e_atualizar:
                with st.spinner("Recalculando lançamentos existentes..."):
                    for h in horas_brutas_ativas:
                        hora_antiga = h['hora']
                        id_antigo = h['id']
                        # Recalcula a porcentagem de cada item antigo baseado no NOVO total geral de horas
                        nova_porcentagem_recalculada = int(round((hora_antiga / total_geral_horas) * 100))
                        if not atualizar_porcentagem_atividade(id_antigo, nova_porcentagem_recalculada):
                            st.error(f"❌ Erro ao recalcular atividade ID {id_antigo}. Abortando.")
                            st.stop()

            sucesso = True
            with st.spinner("Salvando novos lançamentos..."):
                for l in lancamentos_validos:
                    porcent_final = int(round(l["porcentagem_final"]))
                    # Observação com ou sem o formato [HORA:...]
                    obs_final = l["observacao"]
                    
                    ok = salvar_atividade(
                        st.session_state["usuario"],
                        mes_num,
                        ano_select,
                        l["descricao"],
                        l["projeto"],
                        porcent_final,
                        obs_final
                    )
                    if not ok:
                        sucesso = False

            if sucesso:
                carregar_dados.clear()
                # Limpa o formulário após sucesso
                for i in range(qtd_lancamentos):
                    for key_prefix in ["desc_", "proj_", "valor_", "obs_"]:
                        key = f"{key_prefix}{i}"
                        if key in st.session_state:
                            del st.session_state[key]
                if "lanc_qtd" in st.session_state:
                    del st.session_state["lanc_qtd"]
                    
                total_lanc_msg = "100%" if recalcular_e_atualizar else f"{total_final:.1f}%"
                
                if not recalcular_e_atualizar and total_final == 100:
                     st.balloons()
                     st.toast("🎯 100% de alocação atingida!")
                     
                st.success(f"✅ **{len(lancamentos_validos)}** lançamentos salvos. Total alocado: **{total_lanc_msg}**")
                st.rerun()
            else:
                st.error("❌ Erro ao salvar um ou mais lançamentos. Verifique os dados e tente novamente.")


    # ------------------------------
    # Minhas Atividades
    # ------------------------------
    elif aba == "Minhas Atividades":
        st.header("📋 Minhas Atividades")
        
        col_mes, col_ano = st.columns(2)
        mes_select = col_mes.selectbox("Mês de Visualização", MESES_SELECT, index=list(MESES.values()).index(MESES[datetime.today().month]) + 1)
        ano_select = col_ano.selectbox("Ano de Visualização", ANOS, index=ANOS.index(datetime.today().year))
        
        mes_num = next((k for k, v in MESES.items() if v == mes_select), None)
        
        if mes_num:
            atividades = carregar_atividades_usuario(st.session_state["usuario"], mes_num, ano_select)
        else:
            st.warning("Selecione um mês e ano válidos.")
            st.stop()
        
        if not atividades:
            st.info(f"Nenhuma atividade encontrada para **{mes_select}/{ano_select}**.")
            st.stop()
            
        atividades_ativas_mes = [a for a in atividades if a.get('status') != 'Rejeitado']
        total_alocado = sum(a.get("porcentagem", 0) for a in atividades_ativas_mes)
        saldo_restante = max(0, 100 - total_alocado)
        
        # --- Gráfico de Alocação ---
        st.success(f"📊 Total alocado: **{total_alocado:.1f}%** |  Saldo restante: **{saldo_restante:.1f}%**")
        if total_alocado > 100:
            st.warning("⚠️ Atenção: A alocação total excede 100%!")
            
        fig_saldo = px.pie(names=["Alocado", "Disponível"], values=[total_alocado, saldo_restante], 
                           title="Visão Geral da Alocação Mensal", 
                           color_discrete_sequence=["#5B8CFF", "#E0E0E0"], hole=.4)
        fig_saldo.update_traces(texttemplate='%{value:.1f}%', textposition='inside')
        st.plotly_chart(fig_saldo, use_container_width=True)

        # --- Botão Copiar ---
        if st.button("📋 Copiar Lançamentos do Mês Anterior"):
            mes_anterior = mes_num - 1 if mes_num > 1 else 12
            ano_ref = ano_select if mes_num > 1 else ano_select - 1
            antigos = carregar_atividades_usuario(st.session_state["usuario"], mes_anterior, ano_ref)
            
            if antigos:
                # Checagem de limite (só faz sentido em modo Porcentagem)
                horas_antigas_total = sum(extrair_hora_bruta(a.get("observacao", ""))[0] for a in antigos)
                total_novo = total_alocado + sum(a.get("porcentagem", 0) for a in antigos)
                
                if total_novo > 100.0 + 0.001 and horas_antigas_total == 0:
                    st.error(f"❌ A cópia excede 100% para {mes_select}/{ano_select} ({total_novo:.1f}%). Revise e lance manualmente.")
                    st.stop()
                    
                com_sucesso = 0
                com_falha = 0
                for a in antigos:
                    if salvar_atividade(st.session_state["usuario"], mes_num, ano_select, a["descricao"], a["projeto"], a["porcentagem"], a.get("observacao", "")):
                        com_sucesso += 1
                    else:
                        com_falha += 1
                        
                carregar_dados.clear()
                if com_sucesso > 0:
                    st.success(f"✅ **{com_sucesso}** lançamentos do mês anterior copiados. {com_falha} falharam.")
                else:
                    st.warning("Nenhum lançamento novo foi criado.")

                st.rerun()
            else:
                st.warning(f"⚠️ Nenhum lançamento encontrado no mês {MESES[mes_anterior]}/{ano_ref} para copiar.")

        st.markdown("---")
        st.subheader("Detalhe dos Lançamentos")

        # --- Detalhe e Edição de Atividades ---
        for a in atividades:
            is_editing = st.session_state['edit_id'] == a['id']
            # Extrai hora e limpa observação (para exibição)
            hora_bruta, obs_limpa = extrair_hora_bruta(a.get('observacao', ''))
            
            # Badge de Status
            badge_status = f'<span class="status-badge status-{a["status"]}">{a["status"]}</span>'

            # Linha de Exibição
            col1, col2 = st.columns([4, 1])
            with col1:
                st.markdown(f"**{MESES.get(a['mes'])}/{a['ano']}** | **{a['porcentagem']}%** | {badge_status} - *ID {a['id']}*", unsafe_allow_html=True)
                st.markdown(f"**Projeto:** {a['projeto']} | **Descrição:** {a['descricao']}")
                if hora_bruta > 0:
                    st.markdown(f"*Observação:* {obs_limpa} ([**{hora_bruta:.1f} hrs**])")
                else:
                    st.markdown(f"*Observação:* {obs_limpa if obs_limpa else '(Não informada)'}")
            
            with col2:
                if not is_editing:
                    st.button("✏️ Editar", key=f"edit_{a['id']}", on_click=set_edit_id, args=(a['id'],))
                    st.button("🗑️ Excluir", key=f"del_{a['id']}", on_click=handle_delete, args=(a['id'],))
                else:
                    st.button("❌ Cancelar", key=f"cancel_{a['id']}", on_click=cancelar_edicao)

            # Formulário de Edição
            if is_editing:
                st.markdown("---")
                st.markdown("##### ✏️ Editar Lançamento")
                with st.form(f"form_edit_{a['id']}"):
                    # Campo de porcentagem é o único editável diretamente no modo Porcentagem
                    nova_porcentagem = st.number_input(
                        "Nova Porcentagem (%)", 
                        min_value=0, 
                        max_value=100, 
                        value=a['porcentagem'], 
                        step=1, 
                        key=f"e_porc_{a['id']}"
                    )
                    
                    # Campo de Observação é o único editável para Horas (altera o formato [HORA:...]
                    nova_observacao = st.text_area(
                        "Nova Observação", 
                        value=obs_limpa if hora_bruta > 0 else a.get('observacao', ''),
                        key=f"e_obs_{a['id']}"
                    )
                    
                    # Se for modo Horas, permite editar as horas, e o salvamento cuida da formatação
                    if hora_bruta > 0:
                        nova_hora = st.number_input(
                            "Nova Horas Brutas (hrs)",
                            min_value=0.0,
                            max_value=200.0,
                            value=hora_bruta,
                            step=0.5,
                            key=f"e_hora_{a['id']}"
                        )
                        st.warning("⚠️ Edição de horas: Se alterar a hora, a porcentagem de *todas* as atividades do mês que usam horas será recalculada!")
                    else:
                        nova_hora = 0.0
                        
                    submitted = st.form_submit_button("✅ Salvar Edição", type="primary")

                    if submitted:
                        # 1. Checagem de alocação se não for modo Horas
                        if hora_bruta == 0:
                            total_apos_edicao = calcular_porcentagem_existente(st.session_state["usuario"], mes_num, ano_select, excluido_id=a['id'])
                            total_final_porcentagem = total_apos_edicao + nova_porcentagem
                            
                            if total_final_porcentagem > 100:
                                st.error(f"❌ Edição cancelada: a nova porcentagem ({nova_porcentagem}%) faria o total exceder 100% ({total_final_porcentagem}%).")
                                st.stop()
                            
                            obs_final_salvar = nova_observacao
                            porcent_final_salvar = nova_porcentagem
                            recalcular_horas = False
                        
                        # 2. Se for modo Horas, recalcula e usa o formato especial na observação
                        else:
                            recalcular_horas = True
                            # Novo total de horas (horas antigas - hora do item + nova hora)
                            novo_total_horas_mes = total_horas_existentes - hora_bruta + nova_hora
                            
                            # Formata a observação com o novo valor de horas
                            obs_final_salvar = f"[HORA:{nova_hora}|{nova_observacao}]"
                            
                            # A porcentagem *deste* item será atualizada na primeira etapa (se necessário) e depois todas serão recalculadas
                            porcent_final_salvar = int(round((nova_hora / novo_total_horas_mes) * 100)) if novo_total_horas_mes > 0 else 0
                            
                            if novo_total_horas_mes <= 0:
                                st.error("❌ O novo total de horas resultaria em zero. Ajuste o valor da hora.")
                                st.stop()
                                
                        # 3. Salva a edição deste item
                        if atualizar_atividade(a['id'], porcent_final_salvar, obs_final_salvar):
                            st.toast("✅ Atividade editada.")
                            carregar_dados.clear()
                            
                            # 4. Se for modo Horas, recalcula TODOS os itens do mês
                            if recalcular_horas:
                                st.info("Recalculando todas as alocações do mês devido à edição de horas...")
                                
                                # Recarrega todas as atividades do mês (incluindo a editada)
                                atividades_para_recalcular = carregar_atividades_usuario(st.session_state["usuario"], mes_num, ano_select)
                                
                                sucesso_recalc = True
                                for item in atividades_para_recalcular:
                                    item_hora, _ = extrair_hora_bruta(item.get('observacao', ''))
                                    
                                    if item_hora > 0 and item['status'] != 'Rejeitado':
                                        porcent_recalc = int(round((item_hora / novo_total_horas_mes) * 100))
                                        if not atualizar_porcentagem_atividade(item['id'], porcent_recalc):
                                            sucesso_recalc = False
                                            st.error(f"❌ Falha no recálculo da atividade ID {item['id']}.")

                                if sucesso_recalc:
                                    st.success("✅ Recálculo de todas as atividades do mês concluído.")
                                else:
                                    st.warning("⚠️ Algumas atividades não foram recalculadas corretamente. Verifique o console.")
                                
                            cancelar_edicao() # Recarrega a tela principal e sai do modo edição
                        else:
                            st.error("❌ Erro ao salvar a edição da atividade.")
                st.markdown("---")


