import streamlit as st
import pandas as pd
from datetime import datetime
import psycopg2
import psycopg2.extras 
import plotly.express as px
import io 
import re 
import numpy as np
from passlib.context import CryptContext # <<< NOVA IMPORTAÇÃO PARA HASH DE SENHA

# ==============================
# 0. CONFIGURAÇÃO DE ESTILO E TEMA (SINAPSIS)
# ==============================
# --- CORES SINAPSIS DEFINITIVAS ---
COR_PRIMARIA = "#313191" 
COR_SECUNDARIA = "#19c0d1" 
COR_CINZA = "#444444" 
COR_FUNDO_APP = "#FFFFFF"   
COR_FUNDO_SIDEBAR = COR_PRIMARIA 
# ----------------------------------
SINAPSIS_PALETTE = [COR_SECUNDARIA, COR_PRIMARIA, COR_CINZA, "#888888", "#C0C0C0"]
LOGO_URL = "https://raw.githubusercontent.com/Bdmconsultoria/dap/main/logo_sinapsis.png" 

# ==============================
# 0.1. CONFIGURAÇÃO DE SEGURANÇA (HASH DE SENHA)
# ==============================
# <<< NOVA SEÇÃO >>>
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_senha(senha):
    """Gera o hash de uma senha."""
    return pwd_context.hash(senha)

def verificar_senha(senha_plana, hash_senha_db):
    """Verifica se a senha plana corresponde ao hash."""
    if hash_senha_db is None or not hash_senha_db.startswith('$2b$'):
        # Trata senhas antigas em texto plano como inválidas ou implementa migração
        return False
    return pwd_context.verify(senha_plana, hash_senha_db)


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
        "sslmode": st.secrets["postgresql"]["sslmode"],
    }
except KeyError:
    DB_PARAMS = {}
    st.error("Configuração 'st.secrets' não encontrada. Verifique seu arquivo secrets.toml.")
    
# ==============================
# 2. Conexão com PostgreSQL
# ==============================
def get_db_connection():
    """Tenta estabelecer a conexão com o banco de dados e retorna o objeto de conexão."""
    if not DB_PARAMS: return None 
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        st.error(f"Erro de conexão com o banco de dados: {e}")
        return None

# ==============================
# 3. Setup do Banco (criação de tabelas)
# ==============================
def setup_db():
    """Cria as tabelas e garante que a coluna 'senha' seja longa o suficiente para o hash."""
    conn = get_db_connection()
    if conn is None: return
    try:
        with conn.cursor() as cursor:
            # Tabela USUARIOS - Senha agora é VARCHAR(255) para o hash
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS usuarios (
                    usuario VARCHAR(50) PRIMARY KEY,
                    senha VARCHAR(255) NOT NULL,
                    admin BOOLEAN DEFAULT FALSE
                );
            """)
            # Garante que a coluna senha seja VARCHAR(255)
            cursor.execute("ALTER TABLE usuarios ALTER COLUMN senha TYPE VARCHAR(255);")
            
            # Tabela ATIVIDADES
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS atividades (
                    id SERIAL PRIMARY KEY,
                    usuario VARCHAR(50) REFERENCES usuarios(usuario) ON DELETE CASCADE,
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
            
            # Adiciona a coluna STATUS se não existir
            cursor.execute("""
                SELECT 1 FROM information_schema.columns 
                WHERE table_name='atividades' AND column_name='status';
            """)
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE atividades ADD COLUMN status VARCHAR(50) DEFAULT 'Pendente';")

            # Tabela HIERARQUIA
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS hierarquia (
                    gerente VARCHAR(50) REFERENCES usuarios(usuario) ON DELETE CASCADE,
                    subordinado VARCHAR(50) REFERENCES usuarios(usuario) ON DELETE CASCADE,
                    PRIMARY KEY (gerente, subordinado),
                    CHECK (gerente != subordinado)
                );
            """)
            conn.commit()
    except Exception as e:
        st.error(f"Erro ao criar/verificar tabelas: {e}")
    finally:
        if conn:
            conn.close()

if DB_PARAMS:
    setup_db()

# ==============================
# 4. CRUD e Consultas
# ==============================

def salvar_usuario(usuario, senha, admin=False):
    """Salva um novo usuário com senha hasheada."""
    # <<< FUNÇÃO ALTERADA >>>
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            senha_hashed = hash_senha(senha) # Gera o hash
            cursor.execute("""
                INSERT INTO usuarios (usuario, senha, admin)
                VALUES (%s, %s, %s)
                ON CONFLICT (usuario) DO NOTHING;
            """, (usuario, senha_hashed, admin))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar usuário: {e}")
        return False
    finally:
        conn.close()

def validar_login(usuario, senha):
    """Verifica as credenciais de login usando o hash."""
    # <<< FUNÇÃO ALTERADA >>>
    conn = get_db_connection()
    if conn is None: return False, False
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT senha, admin FROM usuarios WHERE usuario = %s;", (usuario,))
            result = cursor.fetchone()
            # Verifica a senha com a função de hash
            if result and verificar_senha(senha, result[0]):
                return True, result[1]
            return False, False
    except Exception as e:
        st.error(f"Erro ao validar login: {e}")
        return False, False
    finally:
        conn.close()

def alterar_senha(usuario, nova_senha):
    """Atualiza a senha do usuário com hash."""
    # <<< FUNÇÃO ALTERADA >>>
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            nova_senha_hashed = hash_senha(nova_senha)
            cursor.execute("""
                UPDATE usuarios SET senha = %s WHERE usuario = %s;
            """, (nova_senha_hashed, usuario))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao alterar senha: {e}")
        return False
    finally:
        conn.close()

def recalcular_horas_apos_exclusao(conn, usuario, mes, ano):
    """Recalcula as porcentagens de um mês se ele era baseado em horas."""
    # <<< NOVA FUNÇÃO PARA CORRIGIR O BUG DE EXCLUSÃO >>>
    try:
        with conn.cursor() as cursor:
            # 1. Verifica se alguma atividade restante no mês tem metadado de hora
            cursor.execute("""
                SELECT observacao FROM atividades
                WHERE usuario = %s AND mes = %s AND ano = %s AND observacao LIKE '%%[HORA:%%';
            """, (usuario, mes, ano))
            
            if not cursor.fetchone():
                return # Nenhuma atividade baseada em horas, não faz nada

            # 2. Se sim, busca todas as atividades restantes para recalcular
            cursor.execute("""
                SELECT id, observacao FROM atividades WHERE usuario = %s AND mes = %s AND ano = %s;
            """, (usuario, mes, ano))
            atividades_restantes = cursor.fetchall()
            
            if not atividades_restantes:
                return # Nenhuma atividade restante

            horas_data = []
            for id_ativ, obs in atividades_restantes:
                hora, _ = extrair_hora_bruta(obs)
                if hora > 0:
                    horas_data.append({'id': id_ativ, 'hora': hora})

            total_horas = sum(h['hora'] for h in horas_data)

            if total_horas > 0:
                # 3. Atualiza cada atividade com a nova porcentagem proporcional
                for h in horas_data:
                    nova_porcentagem = int(round((h['hora'] / total_horas) * 100))
                    cursor.execute("UPDATE atividades SET porcentagem = %s WHERE id = %s;", (nova_porcentagem, h['id']))
                conn.commit()

    except Exception as e:
        st.error(f"Erro ao recalcular horas após exclusão: {e}")
        conn.rollback()

def apagar_atividade(atividade_id):
    """Apaga uma atividade e, se necessário, aciona o recálculo de horas."""
    # <<< FUNÇÃO ALTERADA PARA CORRIGIR O BUG DE EXCLUSÃO >>>
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            # 1. Obter dados da atividade antes de apagar para o recálculo
            cursor.execute("SELECT usuario, mes, ano FROM atividades WHERE id = %s;", (atividade_id,))
            result = cursor.fetchone()
            if not result:
                st.warning(f"Atividade ID {atividade_id} não encontrada para exclusão.")
                return False 
            
            usuario, mes, ano = result

            # 2. Apagar a atividade
            cursor.execute("DELETE FROM atividades WHERE id = %s;", (atividade_id,))
            conn.commit()

            # 3. Chamar a função de recálculo (passando a conexão aberta)
            recalcular_horas_apos_exclusao(conn, usuario, mes, ano)

            return True
    except Exception as e:
        st.error(f"Erro ao apagar atividade: {e}")
        conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

#
# --- O RESTANTE DAS FUNÇÕES CRUD E DE CONSULTA PERMANECE O MESMO ---
# (salvar_atividade, atualizar_status, salvar_hierarquia, etc. não precisam de alteração)
# Cole as funções de `calcular_porcentagem_existente` até `limpar_nomes_usuarios_db` do seu código original aqui.
# A função `limpar_nomes_usuarios_db` é perigosa, considere usar uma versão mais segura como discuti.
# Por ora, manterei a sua original.
#
def calcular_porcentagem_existente(usuario, mes, ano, excluido_id=None):
    conn = get_db_connection()
    if conn is None: return 101
    try:
        with conn.cursor() as cursor:
            query = "SELECT COALESCE(SUM(porcentagem), 0) FROM atividades WHERE usuario = %s AND mes = %s AND ano = %s AND status != 'Rejeitado'"
            params = [usuario, mes, ano]
            if excluido_id is not None:
                query += " AND id != %s"
                params.append(excluido_id)
            cursor.execute(query + ";", params)
            result = cursor.fetchone()
            return result[0] if result else 0
    except Exception as e:
        st.error(f"Erro ao calcular porcentagem: {e}")
        return 101
    finally:
        if conn: conn.close()

def salvar_atividade(usuario, mes, ano, descricao, projeto, porcentagem, observacao, atividade_id=None):
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            data_db = datetime(year=ano, month=mes, day=1).date()
            if atividade_id is None:
                query = "INSERT INTO atividades (usuario, data, mes, ano, descricao, projeto, porcentagem, observacao) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
                params = (usuario, data_db, mes, ano, descricao, projeto, porcentagem, observacao)
            else:
                query = "UPDATE atividades SET data = %s, mes = %s, ano = %s, descricao = %s, projeto = %s, porcentagem = %s, observacao = %s WHERE id = %s;"
                params = (data_db, mes, ano, descricao, projeto, porcentagem, observacao, atividade_id)
            cursor.execute(query, params)
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar atividade: {e}")
        return False
    finally:
        conn.close()

def atualizar_atividade_completa(atividade_id, nova_descricao, novo_projeto, nova_porcentagem, nova_observacao):
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE atividades SET descricao = %s, projeto = %s, porcentagem = %s, observacao = %s WHERE id = %s;", 
                           (nova_descricao, novo_projeto, nova_porcentagem, nova_observacao, atividade_id))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao atualizar atividade: {e}")
        return False
    finally:
        conn.close()

def atualizar_status_atividade(atividade_id, novo_status):
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE atividades SET status = %s WHERE id = %s;", (novo_status, atividade_id))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao atualizar status: {e}")
        return False
    finally:
        conn.close()
        
def salvar_hierarquia(gerente, subordinado):
    conn = get_db_connection()
    if conn is None: return False
    if gerente == subordinado:
        st.error("Gerente e Pessoa da Equipe não podem ser a mesma pessoa.")
        return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO hierarquia (gerente, subordinado) VALUES (%s, %s) ON CONFLICT (gerente, subordinado) DO NOTHING;", (gerente, subordinado))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar hierarquia: {e}")
        return False
    finally:
        conn.close()

def apagar_hierarquia(gerente, subordinado):
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM hierarquia WHERE gerente = %s AND subordinado = %s;", (gerente, subordinado))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao apagar hierarquia: {e}")
        return False
    finally:
        conn.close()

# As funções de carregar dados e outras funções auxiliares permanecem as mesmas
@st.cache_data(ttl=600)
def carregar_hierarquia():
    conn = get_db_connection()
    if conn is None: return pd.DataFrame()
    try:
        return pd.read_sql("SELECT gerente, subordinado FROM hierarquia ORDER BY gerente, subordinado;", conn)
    except Exception:
        return pd.DataFrame()
    finally:
        if conn: conn.close()

@st.cache_data(ttl=600)
def carregar_dados():
    conn = get_db_connection()
    if conn is None: return pd.DataFrame(), pd.DataFrame()
    try:
        usuarios_df = pd.read_sql("SELECT usuario, admin FROM usuarios;", conn)
        atividades_df = pd.read_sql("SELECT id, usuario, data, mes, ano, descricao, projeto, porcentagem, observacao, status FROM atividades ORDER BY ano DESC, mes DESC, data DESC;", conn)
        if not atividades_df.empty:
            atividades_df['data'] = pd.to_datetime(atividades_df['data'])
        return usuarios_df, atividades_df
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame(), pd.DataFrame()
    finally:
        if conn: conn.close()

def carregar_atividades_usuario(usuario, mes, ano):
    conn = get_db_connection()
    if conn is None: return []
    try:
        query = "SELECT id, descricao, projeto, porcentagem, observacao, status FROM atividades WHERE usuario = %s AND mes = %s AND ano = %s ORDER BY id DESC;"
        atividades_df = pd.read_sql(query, conn, params=(usuario, mes, ano))
        return atividades_df.to_dict('records')
    except Exception:
        return []
    finally:
        if conn: conn.close()
        
def atualizar_porcentagem_atividade(atividade_id, nova_porcentagem):
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE atividades SET porcentagem = %s WHERE id = %s;", (nova_porcentagem, atividade_id))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao recalcular porcentagem da atividade {atividade_id}: {e}")
        return False
    finally:
        if conn: conn.close()
# ... (cole aqui as outras funções que não foram alteradas, como `bulk_insert...`, etc.)

# ==============================
# 4.1. FUNÇÕES AUXILIARES DE ATIVIDADE (HORAS E RECALCULO)
# ==============================
def extrair_hora_bruta(observacao):
    if observacao is None: return 0.0, ''
    match = re.search(r'\[HORA:(\d+\.?\d*)\|(.*)\]', observacao, re.DOTALL)
    if match:
        try:
            hora = float(match.group(1))
        except ValueError:
            hora = 0.0
        obs_limpa = match.group(2).strip()
        return hora, obs_limpa
    return 0.0, observacao.strip()


# ==============================
# 5. Dados fixos
# ==============================
# (Seus dados de DESCRICOES, PROJETOS, MESES, etc. permanecem aqui)
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

DESCRICOES_SELECT = ["--- Selecione ---"] + DESCRICOES
PROJETOS_SELECT = ["--- Selecione ---"] + PROJETOS

MESES = {
    1: "01 - Janeiro", 2: "02 - Fevereiro", 3: "03 - Março", 4: "04 - Abril",
    5: "05 - Maio", 6: "06 - Junho", 7: "07 - Julho", 8: "08 - Agosto",
    9: "09 - Setembro", 10: "10 - Outubro", 11: "11 - Novembro", 12: "12 - Dezembro"
}
ANOS = list(range(datetime.today().year - 2, datetime.today().year + 3))

# ==============================
# 8. Funções de Callback e UI
# ==============================
def handle_delete(atividade_id):
    """Apaga uma atividade e força o rerun."""
    # <<< ALTERADO PARA USAR A NOVA FUNÇÃO apagar_atividade >>>
    if apagar_atividade(atividade_id):
        carregar_dados.clear()
        carregar_atividades_usuario.clear()
        st.toast("Atividade apagada e horas recalculadas (se aplicável)!", icon="🗑️")
        st.rerun()

def handle_status_update(atividade_id, novo_status):
    """Atualiza o status de uma atividade e força o rerun."""
    if atualizar_status_atividade(atividade_id, novo_status):
        carregar_dados.clear()
        st.toast(f"Lançamento {atividade_id} atualizado para {novo_status}.", icon="✅")
        st.rerun()

def is_user_a_manager(usuario, hierarquia_df):
    """Verifica se o usuário é gerente."""
    if hierarquia_df.empty: return False
    return usuario in hierarquia_df['gerente'].unique()

def render_seletor_mes_ano(prefixo_key):
    """Renderiza os seletores de Mês e Ano e retorna os valores selecionados."""
    # <<< NOVA FUNÇÃO REUTILIZÁVEL >>>
    col1, col2 = st.columns(2)
    
    today = datetime.today()
    try:
        mes_default_idx = list(MESES.values()).index(MESES[today.month])
        ano_default_idx = ANOS.index(today.year)
    except (KeyError, ValueError):
        mes_default_idx = 0
        ano_default_idx = len(ANOS) - 3

    mes_nome = col1.selectbox("Mês", list(MESES.values()), index=mes_default_idx, key=f"{prefixo_key}_mes")
    ano_select = col2.selectbox("Ano", ANOS, index=ano_default_idx, key=f"{prefixo_key}_ano")
    
    mes_num = next((k for k, v in MESES.items() if v == mes_nome), None)
    
    return mes_num, ano_select

# ==============================
# 6. Sessão e Carregamento Inicial
# ==============================
if "usuario" not in st.session_state:
    st.session_state["usuario"] = None
    st.session_state["admin"] = False
if 'show_change_password' not in st.session_state:
    st.session_state['show_change_password'] = False

usuarios_df, atividades_df = carregar_dados()
hierarquia_df = carregar_hierarquia()

# ==============================
# 7. Lógica Principal da UI
# ==============================
# Injeção de CSS (seu código CSS original aqui)
st.markdown(f"""<style>...</style>""", unsafe_allow_html=True) # Mantenha seu CSS aqui

st.sidebar.image(LOGO_URL, use_container_width=True)
st.sidebar.markdown("<br>", unsafe_allow_html=True)

if st.session_state["usuario"] is None:
    st.title("🔐 Login")
    # ... (seu código de login original aqui)
    # IMPORTANTE: A função `validar_login` já foi alterada para usar hash
else:
    st.sidebar.markdown(f"**Usuário:** {st.session_state['usuario']}")
    # ... (seu código de alterar senha, sair e menu de navegação original aqui)
    # IMPORTANTE: A função `alterar_senha` já foi alterada para usar hash

    # --- Lógica das abas ---
    # ...
    # if aba == "Gerenciar Usuários":
        # ... (seu código de gerenciar usuários)
        # IMPORTANTE: `salvar_usuario` já foi alterado.
    # ...
    # elif aba == "Minhas Atividades":
        # # <<< USO DA NOVA FUNÇÃO DE SELETOR >>>
        # mes_num, ano_select = render_seletor_mes_ano("minhas")
        # if mes_num:
        #     # ... o resto da sua lógica
        #     # IMPORTANTE: A função `handle_delete` já aciona o recálculo
        
    # ... (continue com a lógica das outras abas)
