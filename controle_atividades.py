import streamlit as st
import pandas as pd
from datetime import datetime
import psycopg2
import psycopg2.extras
import plotly.express as px
import io
import re
import numpy as np

# ==============================
# 0. CONFIGURAÇÃO DE ESTILO E TEMA (SINAPSIS)
# ==============================

st.set_page_config(
    layout="wide",
    page_title="Sinapsis - Lançamento de Atividades"
)

# --- CORES SINAPSIS ---
COR_PRIMARIA = "#313191"
COR_SECUNDARIA = "#19c0d1"
COR_CINZA = "#444444"
COR_FUNDO_APP = "#FFFFFF"
COR_FUNDO_SIDEBAR = COR_PRIMARIA

SINAPSIS_PALETTE = [COR_SECUNDARIA, COR_PRIMARIA, COR_CINZA, "#888888", "#C0C0C0"]

# URL DO LOGO (Versão RAW)
LOGO_URL = "https://github.com/Bdmconsultoria/dap/raw/main/logo-branco%202.png" 

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
    
# ==============================
# 2. Conexão com PostgreSQL
# ==============================
def get_db_connection():
    if not DB_PARAMS: return None 
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        return None

# ==============================
# 3. Setup do Banco
# ==============================
def setup_db():
    conn = get_db_connection()
    if conn is None: return
    try:
        with conn.cursor() as cursor:
            # Tabela USUARIOS
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS usuarios (
                    usuario VARCHAR(50) PRIMARY KEY,
                    senha VARCHAR(50) NOT NULL,
                    admin BOOLEAN DEFAULT FALSE,
                    email VARCHAR(255)
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
            
            # Coluna status
            try:
                cursor.execute("SELECT 1 FROM information_schema.columns WHERE table_name='atividades' AND column_name='status';")
                if not cursor.fetchone():
                    cursor.execute("ALTER TABLE atividades ADD COLUMN status VARCHAR(50) DEFAULT 'Pendente';")
                    conn.commit()
            except Exception:
                conn.rollback() 
            
            # Tabela HIERARQUIA
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS hierarquia (
                    gerente VARCHAR(50) REFERENCES usuarios(usuario),
                    subordinado VARCHAR(50) REFERENCES usuarios(usuario),
                    PRIMARY KEY (gerente, subordinado),
                    CHECK (gerente != subordinado)
                );
            """)
            
            # Adiciona a coluna email se ela não existir
            try:
                cursor.execute("SELECT 1 FROM information_schema.columns WHERE table_name='usuarios' AND column_name='email';")
                if not cursor.fetchone():
                    cursor.execute("ALTER TABLE usuarios ADD COLUMN email VARCHAR(255);")
                    conn.commit()
            except Exception:
                conn.rollback()

            conn.commit()
    except Exception as e:
        st.error(f"Erro no setup DB: {e}")
    finally:
        conn.close()

if DB_PARAMS:
    setup_db()

# ==============================
# 4. CRUD, Consultas e Lógica de Cálculo
# ==============================

def salvar_usuario(usuario, senha, admin=False):
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO usuarios (usuario, senha, admin) VALUES (%s, %s, %s)
                ON CONFLICT (usuario) DO NOTHING;
            """, (usuario, senha, admin))
            conn.commit()
            carregar_dados.clear() # Limpa cache de usuários
            return True
    except Exception:
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
    except Exception:
        return False, False
    finally:
        conn.close()

def alterar_senha(usuario, nova_senha):
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE usuarios SET senha = %s WHERE usuario = %s;", (nova_senha, usuario))
            conn.commit()
            return True
    except Exception:
        return False
    finally:
        conn.close()

def extrair_hora_bruta(observacao):
    """Extrai metadado [HORA:X|OBS]"""
    if observacao is None: return 0.0, ''
    match = re.search(r'\[HORA:(\d+\.?\d*)\|(.*)\]', observacao, re.DOTALL)
    if match:
        try:
            return float(match.group(1)), match.group(2).strip()
        except ValueError:
            pass
    return 0.0, observacao.strip()

def atualizar_porcentagem_atividade(conn, atividade_id, nova_porcentagem):
    """Atualiza porcentagem usando uma conexão aberta existente"""
    with conn.cursor() as cursor:
        cursor.execute("UPDATE atividades SET porcentagem = %s WHERE id = %s;", (nova_porcentagem, atividade_id))

# --- ALGORITMO DE CORREÇÃO DE ARREDONDAMENTO (99%/101%) ---
def ajustar_arredondamento_horas(usuario, mes, ano):
    conn = get_db_connection()
    if not conn: return

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, observacao, porcentagem 
                FROM atividades 
                WHERE usuario = %s AND mes = %s AND ano = %s AND status != 'Rejeitado'
            """, (usuario, mes, ano))
            atividades = cursor.fetchall()

        if not atividades: return

        lista_dados = []
        total_horas = 0.0
        tem_hora = False
        
        for aid, obs, perc_atual in atividades:
            h, _ = extrair_hora_bruta(obs)
            if h > 0: tem_hora = True
            lista_dados.append({'id': aid, 'horas': h, 'perc_atual': perc_atual})
            total_horas += h
        
        if not tem_hora or total_horas == 0: return

        for item in lista_dados:
            perc_float = (item['horas'] / total_horas) * 100
            item['novo_perc'] = int(round(perc_float))
        
        soma_perc = sum(item['novo_perc'] for item in lista_dados)
        diferenca = 100 - soma_perc
        
        if diferenca != 0:
            idx_max = max(range(len(lista_dados)), key=lambda i: lista_dados[i]['novo_perc'])
            lista_dados[idx_max]['novo_perc'] += diferenca
        
        update_count = 0
        with conn.cursor() as cursor:
            for item in lista_dados:
                if item['novo_perc'] != item['perc_atual']:
                    atualizar_porcentagem_atividade(conn, item['id'], item['novo_perc'])
                    update_count += 1
        
        if update_count > 0:
            conn.commit()
            carregar_dados.clear() # Limpa cache após ajuste
            return True
        return False


    except Exception as e:
        conn.rollback()
        st.error(f"Erro no ajuste de arredondamento: {e}")
        return False
    finally:
        conn.close()

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
    except Exception:
        return 101 
    finally:
        conn.close()

def salvar_atividade(usuario, mes, ano, descricao, projeto, porcentagem, observacao, atividade_id=None):
    conn = get_db_connection()
    if conn is None: return False
    try:
        with conn.cursor() as cursor:
            data_db = datetime(year=ano, month=mes, day=1).date()
            if atividade_id is None:
                cursor.execute("""
                    INSERT INTO atividades (usuario, data, mes, ano, descricao, projeto, porcentagem, observacao)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """, (usuario, data_db, mes, ano, descricao, projeto, porcentagem, observacao))
            else:
                cursor.execute("""
                    UPDATE atividades SET data=%s, mes=%s, ano=%s, descricao=%s, projeto=%s, porcentagem=%s, observacao=%s
                    WHERE id=%s;
                """, (data_db, mes, ano, descricao, projeto, porcentagem, observacao, atividade_id))
            conn.commit()
        
        ajustar_arredondamento_horas(usuario, mes, ano)
        carregar_dados.clear() # Garante cache limpo
        return True
    except Exception as e:
        st.error(f"Erro salvar: {e}")
        return False
    finally:
        conn.close()

def atualizar_atividade_completa(atividade_id, nova_descricao, novo_projeto, nova_porcentagem, nova_observacao):
    conn = get_db_connection()
    if conn is None: return False
    try:
        dados = None
        with conn.cursor() as cursor:
            cursor.execute("SELECT usuario, mes, ano FROM atividades WHERE id = %s;", (atividade_id,))
            dados = cursor.fetchone()
            if not dados: return False
            usuario, mes, ano = dados

            cursor.execute("""
                UPDATE atividades SET descricao = %s, projeto = %s, porcentagem = %s, observacao = %s WHERE id = %s;
            """, (nova_descricao, novo_projeto, nova_porcentagem, nova_observacao, atividade_id))
            conn.commit()
        
        ajustar_arredondamento_horas(usuario, mes, ano)
        carregar_dados.clear() # Garante cache limpo
        return True
    except Exception as e:
        st.error(f"Erro atualizar completa: {e}")
        return False
    finally:
        conn.close()

def apagar_atividade(atividade_id):
    conn = get_db_connection()
    if conn is None: return False
    dados = None
    try:
        with conn.cursor() as cursor:
            # 1. Obter dados para ajuste (antes de apagar)
            cursor.execute("SELECT usuario, mes, ano FROM atividades WHERE id = %s;", (atividade_id,))
            dados = cursor.fetchone()

            # 2. Deletar a atividade
            cursor.execute("DELETE FROM atividades WHERE id = %s;", (atividade_id,))
            conn.commit()
            
            # 3. Aplicar ajuste se houver dados e a exclusão foi bem-sucedida
            if dados:
                usuario, mes, ano = dados
                ajustar_arredondamento_horas(usuario, mes, ano)
                carregar_dados.clear() # Garante cache limpo
                return True
            return True # Deletou, mas não precisava de ajuste (ou não achou dados, o que é um sucesso na deleção)

    except Exception:
        conn.rollback()
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
            carregar_dados.clear() # Garante cache limpo
            return True
    except Exception:
        return False
    finally:
        conn.close()

def atualizar_status_em_massa(lista_ids, novo_status):
    conn = get_db_connection()
    if conn is None: return False
    if not lista_ids: return False
    try:
        with conn.cursor() as cursor:
            ids_tuple = tuple(lista_ids)
            cursor.execute(f"UPDATE atividades SET status = %s WHERE id IN %s;", (novo_status, ids_tuple))
            conn.commit()
            carregar_dados.clear() # Garante cache limpo
            return True
    except Exception as e:
        conn.rollback()
        st.error(f"Erro massa: {e}")
        return False
    finally:
        conn.close()

def salvar_hierarquia(gerente, subordinado):
    conn = get_db_connection()
    if conn is None: return False
    if gerente == subordinado: return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO hierarquia (gerente, subordinado) VALUES (%s, %s)
                ON CONFLICT (gerente, subordinado) DO NOTHING; 
            """, (gerente, subordinado))
            conn.commit()
            carregar_hierarquia.clear() # Limpa cache de hierarquia
            return True
    except Exception:
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
            carregar_hierarquia.clear() # Limpa cache de hierarquia
            return True
    except Exception:
        return False
    finally:
        conn.close()

@st.cache_data(ttl=600)
def carregar_hierarquia():
    conn = get_db_connection()
    if conn is None: return pd.DataFrame()
    try:
        return pd.read_sql("SELECT gerente, subordinado FROM hierarquia ORDER BY gerente, subordinado;", conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()

@st.cache_data(ttl=600)
def carregar_dados():
    conn = get_db_connection()
    if conn is None: return pd.DataFrame(), pd.DataFrame()
    try:
        usuarios_df = pd.read_sql("SELECT usuario, admin FROM usuarios;", conn)
        try:
            atividades_df = pd.read_sql("SELECT id, usuario, data, mes, ano, descricao, projeto, porcentagem, observacao, status FROM atividades ORDER BY ano DESC, mes DESC, data DESC;", conn)
        except Exception:
             atividades_df = pd.read_sql("SELECT id, usuario, data, mes, ano, descricao, projeto, porcentagem, observacao FROM atividades ORDER BY ano DESC, mes DESC, data DESC;", conn)
             atividades_df['status'] = 'Pendente'

        if not atividades_df.empty:
            atividades_df['data'] = pd.to_datetime(atividades_df['data'])
        return usuarios_df, atividades_df
    finally:
        conn.close()

def bulk_insert_usuarios(user_list):
    conn = get_db_connection()
    if conn is None: return 0, "Erro DB"
    data_list = [(user, '123', False) for user in user_list]
    try:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_batch(cursor, "INSERT INTO usuarios (usuario, senha, admin) VALUES (%s, %s, %s) ON CONFLICT (usuario) DO NOTHING", data_list)
            conn.commit()
            carregar_dados.clear() # Limpa cache de usuários
            return cursor.rowcount, "OK"
    except Exception as e:
        conn.rollback()
        return 0, str(e)
    finally:
        conn.close()

def bulk_insert_atividades(df_to_insert):
    conn = get_db_connection()
    if conn is None: return 0, "Erro DB"
    data_list = [tuple(row) for row in df_to_insert[['usuario', 'data', 'mes', 'ano', 'descricao', 'projeto', 'porcentagem', 'observacao', 'status']].values]
    try:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_batch(cursor, "INSERT INTO atividades (usuario, data, mes, ano, descricao, projeto, porcentagem, observacao, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", data_list)
            conn.commit()
        
        users_meses = df_to_insert[['usuario', 'mes', 'ano']].drop_duplicates()
        for _, row in users_meses.iterrows():
            ajustar_arredondamento_horas(row['usuario'], row['mes'], row['ano'])
            
        carregar_dados.clear() # Garante cache limpo
        return len(data_list), "OK"
    except Exception as e:
        conn.rollback()
        return 0, str(e)
    finally:
        conn.close()

def limpar_nomes_usuarios_db():
    conn = get_db_connection()
    if conn is None: return False, "Erro DB"
    try:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE atividades SET usuario = TRIM(usuario);")
            cursor.execute("UPDATE hierarquia SET gerente = TRIM(gerente), subordinado = TRIM(subordinado);")
            cursor.execute("""
                SELECT DISTINCT TRIM(usuario) FROM atividades UNION
                SELECT DISTINCT TRIM(gerente) FROM hierarquia UNION
                SELECT DISTINCT TRIM(subordinado) FROM hierarquia UNION
                SELECT DISTINCT usuario FROM usuarios;
            """)
            usuarios_limpos = list(set([row[0] for row in cursor.fetchall()]))
            cursor.execute("SELECT usuario, admin FROM usuarios;")
            status_admin = dict(cursor.fetchall())
            cursor.execute("TRUNCATE TABLE usuarios CASCADE;")
            to_insert = [(u, '123', status_admin.get(u, False)) for u in usuarios_limpos]
            if to_insert:
                psycopg2.extras.execute_batch(cursor, "INSERT INTO usuarios (usuario, senha, admin) VALUES (%s, %s, %s)", to_insert)
            conn.commit()
            carregar_dados.clear() # Limpa caches após alteração massiva
            carregar_hierarquia.clear() # Limpa caches após alteração massiva
            return True, "Limpeza concluída."
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

def carregar_atividades_usuario(usuario, mes, ano):
    conn = get_db_connection()
    if conn is None: return []
    try:
        df = pd.read_sql("SELECT id, descricao, projeto, porcentagem, observacao, status FROM atividades WHERE usuario = %s AND mes = %s AND ano = %s ORDER BY id DESC;", conn, params=(usuario, mes, ano))
        return df.to_dict('records')
    except Exception:
        return []
    finally:
        conn.close()

def is_user_a_manager(usuario, hierarquia_df):
    if hierarquia_df.empty: return False
    return usuario in hierarquia_df['gerente'].unique()

# --- CALLBACK DE DELETE ---
def handle_delete(atividade_id):
    if apagar_atividade(atividade_id):
        # A função apagar_atividade agora limpa o cache e faz o ajuste.
        # Basta dar o toast e rerunar.
        st.toast("Atividade apagada e percentuais recalculados!", icon="🗑️")
        st.rerun()
    else:
        st.toast("Erro ao apagar a atividade.", icon="❌")

# --- DADOS FIXOS ---
DESCRICOES = ["1.001 - Gestão","1.002 - Geral","1.003 - Conselho","1.004 - Treinamento e Desenvolvimento", "2.001 - Gestão do administrativo","2.002 - Administrativa","2.003 - Jurídica","2.004 - Financeira", "2.006 - Fiscal","2.007 - Infraestrutura TI","2.008 - Treinamento interno","2.011 - Análise de dados", "2.012 - Logística de viagens","2.013 - Prestação de contas","2.014 - Compras e Suprimentos", "3.001 - Prospecção de oportunidades", "3.002 - Prospecção de temas","3.003 - Administração comercial","3.004 - Marketing Digital", "3.005 - Materiais de apoio","3.006 - Grupos de Estudo","3.007 - Elaboração de POC/Piloto", "3.008 - Elaboração e apresentação de proposta","3.009 - Acompanhamento de proposta", "3.010 - Reunião de acompanhamento de funil","3.011 - Planejamento Estratégico/Comercial", "3.012 - Sucesso do Cliente","3.013 - Participação em eventos","4.001 - Planejamento de projeto", "4.002 - Gestão de projeto","4.003 - Reuniões internas de trabalho","4.004 - Reuniões externas de trabalho", "4.005 - Pesquisa","4.006 - Especificação de software","4.007 - Desenvolvimento de software/rotinas", "4.008 - Coleta e preparação de dados","4.009 - Elaboração de estudos e modelos","4.010 - Confecção de relatórios técnicos", "4.011 - Confecção de apresentações técnicas","4.012 - Confecção de artigos técnicos","4.013 - Difusão de resultados", "4.014 - Elaboração de documentação final","4.015 - Finalização do projeto","5.001 - Gestão de desenvolvimento", "5.002 - Planejamento de projeto","5.003 - Gestão de projeto","5.004 - Reuniões internas de trabalho", "5.005 - Reuniões externa de trabalho","5.006 - Pesquisa","5.007 - Coleta e preparação de dados", "5.008 - Modelagem","5.009 - Análise de tarefa","5.010 - Especificação de tarefa","5.011 - Correção de bug", "5.012 - Desenvolvimento de melhorias","5.013 - Desenvolvimento de novas funcionalidades", "5.014 - Desenvolvimento de integrações","5.015 - Treinamento interno","5.016 - Documentação", "5.017 - Atividades gerenciais","5.018 - Estudos","6.001 - Gestão de equipe","6.002 - Pesquisa", "6.003 - Especificação de testes","6.004 - Desenvolvimento de automações","6.005 - Realização de testes", "6.006 - Reuniões internas de trabalho","6.007 - Treinamento interno","6.008 - Elaboração de material", "7.001 - Gestão de equipe","7.002 - Pesquisa e estudos","7.003 - Análise de ticket","7.004 - Reuniões internas de trabalho", "7.005 - Reuniões externas de trabalho","7.006 - Preparação de treinamento externo","7.007 - Realização de treinamento externo", "7.008 - Documentação de treinamento","7.009 - Treinamento interno","7.010 - Criação de tarefa","7.011 - Acompanhamento dos Chamados em Desenvolvimento","9.001 - Gestão do RH", "9.002 - Recrutamento e seleção","9.003 - Participação em eventos","9.004 - Pesquisa e estratégia","9.005 -Treinamento e desenvolvimento", "9.006 - Registro de feedback","9.007 - Avaliação de RH","9.008 - Elaboração de conteúdo","9.009 - Comunicação interna", "9.010 - Reuniões internas de trabalho","9.011 - Reunião externa","9.012 - Apoio contábil e financeiro","10.001 - Planejamento de operação", "10.002 - Gestão de operação","10.003 - Reuniões internas de trabalho","10.004 - Reuniões externas de trabalho", "10.005 - Especificação de melhoria ou correção de software","10.006 - Desenvolvimento de automações", "10.007 - Coleta e preparação de dados","10.008 - Elaboração de estudos e modelos","10.009 - Confecção de relatórios técnicos", "10.010 - Confecção de apresentações técnicas","10.011 - Confecção de artigos técnicos","10.012 - Difusão de resultados", "10.013 - Preparação de treinamento externo","10.014 - Realização de treinamento externo","10.015 - Mapeamento de Integrações"]
PROJETOS = ["101-0 (Interno) Diretoria Executiva","102-0 (Interno) Diretoria Administrativa","103-0 (Interno) Diretoria de Engenharia", "104-0 (Interno) Diretoria de Negócios","105-0 (Interno) Diretoria de Produtos","106-0 (Interno) Diretoria de Tecnologia", "107-0 (Interno) Departamento Administrativo","108-0 (Interno) Departamento de Gente e Cultura","109-0 (Interno) Departamento de Infraestrutura", "110-0 (Interno) Departamento de Marketing","111-0 (Interno) Departamento de Operação","112-0 (Interno) Departamento de Sucesso do Cliente", "113-0 (Interno) Produto ARIES","114-0 (Interno) Produto ActionWise","115-0 (Interno) Produto Carga Base","116-0 (Interno) Produto Godel Perdas", "117-0 (Interno) Produto Godel Conecta","118-0 (Interno) Produto SIGPerdas","119-0 (Interno) Produto SINAPgrid","120-0 (Interno) Produto SINAP4.0", "121-0 (Interno) SINAPgrid Acadêmico","122-0 (Interno) Produto SINAPgateway (BAGRE)","123-0 (Interno) Produto SINAPautomação e diagnóstico (autobatch)", "302-0 (SENSE - Equatorial) Virtus","402-0 (SOFTEX - Copel) Renovação de Ativos Continuação","573-0 (ENEL SP, RJ e CE) Suporte e Manutenção do Software SINAP T&D","573-1 (ENEL) Suporte SINAPgrid", "573-2 (ENEL) Re-configuração","575-0 (Amazonas) Suporte SINAPgrid","578-1 (Copel) Suporte SINAPgrid","578-2 (Copel) Suporte Godel Conecta", "578-3 (Copel) Suporte GDIS","581-0 (CERILUZ) Suporte SINAPgrid","583-0 (CERTAJA) Suporte SINAPgrid","584-0 (CERTEL) Suporte SINAPgrid", "585-0 (COOPERLUZ) Suporte SINAPgrid","587-0 (COPREL) Suporte SINAPgrid","606-0 (Roraima) Suporte SINAPgrid","615-0 (Energisa) Suporte SIGPerdas","620-0 (CPFL) Plataforma Planejamento Sistema Elétrico","620-1 (CPFL) Suporte SINAPgrid","636-0 (Equatorial Energia GO) Cálculo de Perdas GeoPerdas
","638-1 (Amazonas) Suporte SIGPerdas","638-2 (Roraima) Suporte SIGPerdas","640-0 (SENAI - CTG) Hidrogênio Verde", "647-0 (Energisa) Consultoria de Estudos Elétricos","648-0 (Neoenergia) Suporte SINAPgrid","649-0 (Neoenergia) Godel PCom e Godel Analytics", "653-0 (Roraima) Projeto Gestor GDIS","655-0 (CELESC) Sistema Integrável de Matchmaking","658-0 (Copel) Planauto Continuação", "659-0 (Copel) Cálculo de Benefícios de Investimentos","660-0 (CERFOX) Suporte SINAPgrid","661-0 (ENEL SP, RJ e CE) Consultoria técnica BDGD", "663-0 (Banco Mundial) Eletromobilidade em São Paulo","666-0 (Energisa) Análise MM GD","667-0 (Energisa) Planejamento Decenal MT", "668-0 (Energisa) Critérios de Planejamento de SEs","669-0 (Desenvolve SP) Hub de Dados","670-0 (CPFL) Proteção","671-0 (Equatorial) Godel Perdas", "672-0 (ENEL SP) URD Subterrâneo","673-0 (Equatorial) PDD","674-0 (Energisa PB) Planejamento Decenal 2025","675-0 (EDEMSA) Godel Perdas Suporte Técnico Bromteck", "676-0 (Equatorial) PoC Resiliência","677-0 (Neoenergia) Suporte Godel Perdas","678-0 (CPFL) AMBAR","679-0 (ENEL) Godel Conecta", "680-0 (CESI) Angola Banco Mundial","681-0 (CEMACON) Suporte SINAPgrid","682-0 (FECOERGS) Treinamento SINAPgrid"]

DESCRICOES_SELECT = ["--- Selecione ---"] + DESCRICOES
PROJETOS_SELECT = ["--- Selecione ---"] + PROJETOS
MESES = {1: "01 - Janeiro", 2: "02 - Fevereiro", 3: "03 - Março", 4: "04 - Abril", 5: "05 - Maio", 6: "06 - Junho", 7: "07 - Julho", 8: "08 - Agosto", 9: "09 - Setembro", 10: "10 - Outubro", 11: "11 - Novembro", 12: "12 - Dezembro"}
MESES_SELECT = ["--- Selecione ---"] + list(MESES.values())
ANOS = list(range(datetime.today().year - 2, datetime.today().year + 3))


# --- INFORMAÇÕES FIXAS DA FAMÍLIA (DEPARTAMENTOS) ---
# CORRIGIDO: 9 e 10 corretos
DEPARTAMENTOS_INFO = [
    "1. Diretoria & Conselho (Apenas Diretoria)",
    "2. Administração",
    "3. Comercial",
    "4. Engenharia",
    "5. Desenvolvimento",
    "6. QA-Testes",
    "7. Suporte/Treinamento",
    "9. Gente e Cultura",
    "10. Operação"
]
# ----------------------------------------------------

# --- FUNÇÃO AUXILIAR PARA FORMATAR GUIA ---
def format_department_guide(department_list):
    """Formata a lista de strings em um bloco HTML para evitar o auto-numbering do Markdown."""
    # Envolve cada item em um <li> dentro de um <ul> com estilo none
    items_html = "".join([f"<li>{item}</li>" for item in department_list])
    # list-style: none remove o ponto/número padrão do <ul>. padding-left: 0 alinha.
    return f"<ul style='list-style-type: none; padding-left: 0; margin: 0;'>{items_html}</ul>"


# ==============================
# 6. Sessão e Login
# ==============================
if "usuario" not in st.session_state:
    st.session_state["usuario"] = None
    st.session_state["admin"] = False
if 'show_change_password' not in st.session_state:
    st.session_state['show_change_password'] = False

usuarios_df, atividades_df = carregar_dados()
hierarquia_df = carregar_hierarquia()

st.markdown(
    f"""
    <style>
        :root {{ --primary-color: #19c0d1; --secondary-background-color: {COR_FUNDO_SIDEBAR}; }}
        [data-testid="stSidebar"] {{ background-color: {COR_FUNDO_SIDEBAR}; }}
        [data-testid="stSidebar"] * {{ color: #FFFFFF !important; }}
        [data-testid="stSidebar"] .stButton > button {{ background-color: {COR_FUNDO_SIDEBAR} !important; border: 1px solid #FFFFFF30; color: #FFFFFF !important; }}
        [data-testid="stSidebar"] .stButton > button:hover {{ background-color: {COR_SECUNDARIA} !important; }}
        [data-testid="stSidebar"] .stRadio > label[data-testid*="stRadioInline"]:has(input:checked) {{ background-color: {COR_SECUNDARIA} !important; border-radius: 5px; }}
        .stApp {{ background-color: {COR_FUNDO_APP}; }}
        .modebar {{ display: none !important; }}
        .status-badge {{ padding: 4px 8px; border-radius: 12px; font-size: 0.9em; font-weight: bold; display: inline-block; }}
        .status-Pendente {{ background-color: #ffcc99; color: #cc6600; }}
        .status-Aprovado {{ background-color: #ccffcc; color: #008000; }}
        .status-Rejeitado {{ background-color: #ff9999; color: #cc0000; }}
        /* Garante que o texto dentro dos itens da lista de Guia não mude de cor */
        .stMarkdown ul li {{ color: {COR_CINZA} !important; }} 
        [data-testid="stSidebar"] img {{ filter: brightness(1.5) contrast(1.5); }}
    </style>
    """, unsafe_allow_html=True
)

if LOGO_URL: st.sidebar.image(LOGO_URL, use_container_width=True)
st.sidebar.markdown("<br>", unsafe_allow_html=True)

if st.session_state["usuario"] is None:
    st.title("🔐 Login")
    _, col_login, _ = st.columns([1, 2, 1])
    with col_login:
        usuario = st.text_input("Usuário")
        senha = st.text_input("Senha", type="password")
        if st.button("Entrar", use_container_width=True):
            ok, admin = validar_login(usuario.strip(), senha)
            if ok:
                st.session_state["usuario"] = usuario.strip()
                st.session_state["admin"] = admin
                st.rerun()
            else:
                st.error("Credenciais inválidas")
else:
    st.sidebar.markdown(f"**Usuário:** {st.session_state['usuario']}")
    if st.sidebar.button("🔑 Alterar Senha"):
        st.session_state['show_change_password'] = not st.session_state['show_change_password']
        st.rerun()
    
    if st.session_state['show_change_password']:
        with st.sidebar.form("form_senha"):
            s1 = st.text_input("Nova Senha", type="password")
            s2 = st.text_input("Confirmar", type="password")
            if st.form_submit_button("Salvar"):
                if s1 and s1 == s2:
                    alterar_senha(st.session_state["usuario"], s1)
                    st.sidebar.success("Senha alterada! Faça login.")
                    st.session_state["usuario"] = None
                    st.rerun()
                else:
                    st.sidebar.error("Senhas divergem.")
    
    st.sidebar.markdown("---")
    if st.sidebar.button("Sair"):
        st.session_state["usuario"] = None
        st.rerun()

    is_manager = is_user_a_manager(st.session_state["usuario"], hierarquia_df)
    
    abas = ["Lançar Atividade", "Minhas Atividades", "Importar Dados"]
    if st.session_state["admin"] or is_manager: abas.append("Gerenciar Time")
    if st.session_state["admin"]: abas += ["Gerenciar Usuários", "Consolidado"]
    
    aba = st.sidebar.radio("Menu", abas)

    # ==============================
    # ABA: Gerenciar Usuários (Admin)
    # ==============================
    if aba == "Gerenciar Usuários" and st.session_state["admin"]:
        st.header("👥 Gerenciar Usuários")
        if st.button("Ferramenta: Limpar Nomes (Trim)"):
             ok, msg = limpar_nomes_usuarios_db()
             if ok: st.success(msg)
             else: st.error(msg)
             # As funções de limpeza agora chamam carregar_dados.clear()
             st.rerun()
        
        with st.form("add_user"):
            nu = st.text_input("Novo Usuário")
            ns = st.text_input("Senha", type="password")
            adm = st.checkbox("Admin")
            if st.form_submit_button("Criar"):
                if salvar_usuario(nu.strip(), ns, adm):
                    st.success("Criado!")
                    # salvar_usuario agora limpa o cache.
                    st.rerun()
                else:
                    st.error("Erro ao criar usuário.")
        
        st.dataframe(usuarios_df, use_container_width=True, hide_index=True)

    # ==============================
    # ABA: Gerenciar Time (Aprovação em Massa)
    # ==============================
    elif aba == "Gerenciar Time":
        st.header("🤝 Gerenciar Equipe")
        hierarquia_df = carregar_hierarquia()
        usuarios_list = usuarios_df['usuario'].tolist()
        
        if st.session_state["admin"]:
            st.subheader("Configurar Hierarquia (Admin)")
            with st.form("hierarquia"):
                c1, c2 = st.columns(2)
                # Termos ajustados
                g = c1.selectbox("Gerente da Área", sorted(usuarios_list))
                s = c2.selectbox("Pessoa da Área", ["---"] + sorted([u for u in usuarios_list if u != g]))
                if st.form_submit_button("Associar"):
                    if s != "---":
                        if salvar_hierarquia(g, s):
                            st.success("Associado!")
                            # salvar_hierarquia agora limpa o cache.
                            st.rerun()
                        else:
                            st.error("Erro ao associar.")
            
            if not hierarquia_df.empty:
                # Termos ajustados para exibição
                df_display = hierarquia_df.rename(columns={'gerente': 'Gerente da Área', 'subordinado': 'Pessoa da Área'})
                st.dataframe(df_display, use_container_width=True, hide_index=True)
                
                with st.form("del_hierarquia"):
                     # Termos ajustados
                     g_rem = st.selectbox("Gerente da Área (Remover)", sorted(hierarquia_df['gerente'].unique()))
                     subs = hierarquia_df[hierarquia_df['gerente'] == g_rem]['subordinado'].tolist()
                     s_rem = st.selectbox("Pessoa da Área (Remover)", sorted(subs)) if subs else None
                     if st.form_submit_button("Remover"):
                         if apagar_hierarquia(g_rem, s_rem):
                             # apagar_hierarquia agora limpa o cache.
                             st.rerun()
                         else:
                             st.error("Erro ao remover.")

        # Análise e Aprovação
        st.markdown("---")
        st.subheader("Aprovação")
        gerentes_validos = hierarquia_df['gerente'].unique()
        
        if st.session_state["admin"]:
            # Termos ajustados
            gerente_analise = st.selectbox("Selecione o Gerente da Área", sorted(gerentes_validos))
        elif st.session_state["usuario"] in gerentes_validos:
            gerente_analise = st.session_state["usuario"]
        else:
            # Termos ajustados
            st.warning("Você não é Gerente da Área.")
            st.stop()
            
        time = hierarquia_df[hierarquia_df['gerente'] == gerente_analise]['subordinado'].tolist()
        
        c_mes, c_ano = st.columns(2)
        mes_analise = c_mes.selectbox("Mês", list(MESES.values()), index=datetime.now().month-1)
        ano_analise = c_ano.selectbox("Ano", ANOS, index=ANOS.index(datetime.now().year))
        mes_num = next(k for k,v in MESES.items() if v == mes_analise)
        
        df_time = atividades_df[
            (atividades_df['usuario'].isin(time)) & 
            (atividades_df['mes'] == mes_num) & 
            (atividades_df['ano'] == ano_analise)
        ]
        
        # Resumo Alocação
        resumo = df_time.groupby('usuario')['porcentagem'].sum().reset_index()
        for u in time:
            if u not in resumo['usuario'].values:
                resumo.loc[len(resumo)] = [u, 0]
        
        st.dataframe(
            resumo.sort_values('porcentagem', ascending=False), 
            use_container_width=True, hide_index=True,
            column_config={'porcentagem': st.column_config.ProgressColumn("Alocado", min_value=0, max_value=100, format="%d%%")}
        )
        
        st.markdown("---")
        
        # Tabela de Aprovação com Checkbox
        c_f1, c_f2 = st.columns(2)
        status_f = c_f1.selectbox("Status", ["Todos", "Pendente", "Aprovado", "Rejeitado"])
        # Termos ajustados
        user_f = c_f2.selectbox("Pessoa da Área", ["Todos"] + sorted(time))
        
        df_view = df_time.copy()
        if status_f != "Todos": df_view = df_view[df_view['status'] == status_f]
        if user_f != "Todos": df_view = df_view[df_view['usuario'] == user_f]
        
        if df_view.empty:
            st.info("Sem dados.")
        else:
            df_view['Selecionar'] = False
            df_view['observacao_limpa'] = df_view['observacao'].apply(lambda x: extrair_hora_bruta(x)[1])
            
            edited_df = st.data_editor(
                df_view,
                key="editor_aprovacao",
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Selecionar": st.column_config.CheckboxColumn("Selecionar", default=False),
                    "usuario": st.column_config.TextColumn("Usuário", disabled=True),
                    "descricao": st.column_config.TextColumn("Descrição", disabled=True),
                    "projeto": st.column_config.TextColumn("Projeto", disabled=True),
                    "porcentagem": st.column_config.NumberColumn("%", disabled=True),
                    "status": st.column_config.TextColumn("Status", disabled=True),
                    "observacao_limpa": st.column_config.TextColumn("Obs", disabled=True)
                },
                column_order=["Selecionar", "usuario", "descricao", "projeto", "porcentagem", "status", "observacao_limpa"]
            )
            
            ids_sel = edited_df[edited_df['Selecionar']]['id'].tolist()
            c_btn1, c_btn2 = st.columns(2)
            if c_btn1.button(f"Aprovar ({len(ids_sel)})", type="primary", disabled=not ids_sel, use_container_width=True):
                if atualizar_status_em_massa(ids_sel, "Aprovado"):
                    # atualizar_status_em_massa agora limpa o cache.
                    st.rerun()
            if c_btn2.button(f"Rejeitar ({len(ids_sel)})", type="secondary", disabled=not ids_sel, use_container_width=True):
                if atualizar_status_em_massa(ids_sel, "Rejeitado"):
                    # atualizar_status_em_massa agora limpa o cache.
                    st.rerun()

    # ==============================
    # ABA: Lançar Atividade (Barra de Progresso + Guia CORRIGIDA)
    # ==============================
    elif aba == "Lançar Atividade":
        st.header("📝 Lançar Atividade")
        c1, c2 = st.columns(2)
        mes_sel = c1.selectbox("Mês", MESES_SELECT, index=datetime.now().month)
        ano_sel = c2.selectbox("Ano", ANOS, index=ANOS.index(datetime.now().year))
        mes_num = next((k for k,v in MESES.items() if v == mes_sel), None)
        
        if not mes_num: st.stop()
        
        atividades = carregar_atividades_usuario(st.session_state["usuario"], mes_num, ano_sel)
        ativas = [a for a in atividades if a['status'] != 'Rejeitado']
        
        total_existente = sum(a['porcentagem'] for a in ativas)
        horas_existentes = sum(extrair_hora_bruta(a.get('observacao',''))[0] for a in ativas)
        
        tipo = st.radio("Tipo", ["Porcentagem", "Horas"], horizontal=True)
        qtd = st.number_input("Quantidade", 1, 20, 1)
        
        # --- NOVO BLOCO: GUIA DE DEPARTAMENTO/FAMÍLIA (CORREÇÃO DE RENDERING) ---
        st.subheader("📚 Guia de Classificação (Família da Atividade)")
        
        # Divide a lista em 3 colunas (1-3, 4-6, 7-10)
        grupos = [
            DEPARTAMENTOS_INFO[0:3], 
            DEPARTAMENTOS_INFO[3:6], 
            DEPARTAMENTOS_INFO[6:]
        ]
        
        col_d1, col_d2, col_d3 = st.columns(3)
        
        with col_d1:
            st.markdown(format_department_guide(grupos[0]), unsafe_allow_html=True)
        with col_d2:
            st.markdown(format_department_guide(grupos[1]), unsafe_allow_html=True)
        with col_d3:
            # Usa a nova função de formatação HTML para prevenir que 7, 9, 10 vire 7, 8, 9
            st.markdown(format_department_guide(grupos[2]), unsafe_allow_html=True)
            
        st.markdown("<hr style='margin-top: 5px; margin-bottom: 5px;'>", unsafe_allow_html=True)
        # --------------------------------------------------------------------

        
        with st.form("lancamento"):
            cols = st.columns([0.5, 3, 3, 1.5, 3])
            cols[0].markdown("**Nº**")
            cols[1].markdown("**Descrição**")
            cols[2].markdown("**Projeto**")
            cols[3].markdown("**Valor**")
            cols[4].markdown("**Obs**")
            
            novos = []
            for i in range(qtd):
                r = st.columns([0.5, 3, 3, 1.5, 3])
                r[0].text(f"{i+1}")
                d = r[1].selectbox(f"d{i}", DESCRICOES_SELECT, label_visibility="collapsed")
                p = r[2].selectbox(f"p{i}", PROJETOS_SELECT, label_visibility="collapsed")
                v = r[3].number_input(f"v{i}", min_value=0.0, step=1.0, label_visibility="collapsed")
                o = r[4].text_input(f"o{i}", label_visibility="collapsed")
                novos.append({'desc': d, 'proj': p, 'val': v, 'obs': o})
            
            if st.form_submit_button("Salvar"):
                validos = [n for n in novos if n['val'] > 0 and n['desc'] != "--- Selecione ---"]
                if not validos:
                    st.error("Preencha os dados.")
                    st.stop()
                
                total_novo_val = sum(n['val'] for n in validos)
                
                salvo_ok = False
                if tipo == "Horas":
                    # No modo Horas, o recalculo ocorre em salvar_atividade
                    total_h_final = horas_existentes + total_novo_val
                    if total_h_final == 0: 
                        st.error("Total de horas é zero.")
                        st.stop()
                        
                    for n in validos:
                        # O percentual é temporário (vai ser corrigido por ajustar_arredondamento_horas)
                        perc_est = int(round((n['val']/total_h_final)*100)) 
                        obs = f"[HORA:{n['val']}|{n['obs']}]"
                        if salvar_atividade(st.session_state["usuario"], mes_num, ano_sel, n['desc'], n['proj'], perc_est, obs):
                            salvo_ok = True
                        else:
                            salvo_ok = False # Se falhar, marca para erro
                            break

                else:
                    if total_existente + total_novo_val > 100:
                        st.error("Ultrapassa 100%.")
                        st.stop()
                    
                    for n in validos:
                        if salvar_atividade(st.session_state["usuario"], mes_num, ano_sel, n['desc'], n['proj'], int(n['val']), n['obs']):
                            salvo_ok = True
                        else:
                            salvo_ok = False
                            break
                
                if salvo_ok:
                    # O cache já foi limpo dentro de salvar_atividade
                    st.success("Salvo e recalculado!")
                    st.rerun()
                else:
                    st.error("Erro ao salvar uma ou mais atividades.")


        st.subheader("📊 Status do Mês")
        percentual_decimal = min(total_existente / 100.0, 1.0)
        st.progress(percentual_decimal)
        
        c_k1, c_k2, c_k3 = st.columns(3)
        c_k1.metric("Alocado", f"{total_existente}%")
        c_k2.metric("Disponível", f"{100-total_existente}%")
        c_k3.metric("Horas Brutas", f"{horas_existentes:.1f} h")

    # ==============================
    # ABA: Minhas Atividades
    # ==============================
    elif aba == "Minhas Atividades":
        st.header("📋 Minhas Atividades")
        c1, c2 = st.columns(2)
        mes_sel = c1.selectbox("Mês", MESES_SELECT, index=datetime.now().month, key="m_a")
        ano_sel = c2.selectbox("Ano", ANOS, index=ANOS.index(datetime.now().year), key="a_a")
        mes_num = next(k for k,v in MESES.items() if v == mes_sel)
        
        atividades = carregar_atividades_usuario(st.session_state["usuario"], mes_num, ano_sel)
        ativas = [a for a in atividades if a['status'] != 'Rejeitado']
        total = sum(a['porcentagem'] for a in ativas)
        
        col_met, col_graph = st.columns([1, 2])
        col_met.metric("Total Alocado", f"{total}%", f"{100-total}% restante")
        
        df_g = pd.DataFrame(ativas)
        if not df_g.empty:
            fig = px.pie(df_g, names='descricao', values='porcentagem', hole=0.5, color_discrete_sequence=SINAPSIS_PALETTE)
            fig.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=200)
            col_graph.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        
        c_copy, c_exp = st.columns(2)
        if c_copy.button("Copiar Mês Anterior", use_container_width=True):
            m_ant = mes_num - 1 if mes_num > 1 else 12
            a_ant = ano_sel if mes_num > 1 else ano_sel - 1
            antigos = carregar_atividades_usuario(st.session_state["usuario"], m_ant, a_ant)
            if antigos:
                for a in antigos:
                    # Chamada a salvar_atividade que já limpa o cache.
                    salvar_atividade(st.session_state["usuario"], mes_num, ano_sel, a['descricao'], a['projeto'], a['porcentagem'], a['observacao'])
                st.rerun()
        
        if ativas:
            df_ex = pd.DataFrame(ativas)
            df_ex['observacao'] = df_ex['observacao'].apply(lambda x: extrair_hora_bruta(x)[1])
            buffer = io.BytesIO()
            df_ex.to_excel(buffer, index=False)
            c_exp.download_button("Exportar Excel", buffer, "atividades.xlsx", use_container_width=True)

        st.subheader("Edição")
        
        cols_head = st.columns([0.5, 3, 3, 1.5, 2.5, 1.5])
        cols_head[0].markdown("**ID**")
        cols_head[1].markdown("**Descrição**")
        cols_head[2].markdown("**Projeto**")
        cols_head[3].markdown("**%**")
        cols_head[4].markdown("**Obs**")
        cols_head[5].markdown("**Ações**")
        st.markdown("<hr style='margin: 5px 0;'>", unsafe_allow_html=True)

        for a in atividades:
            h_bruta, obs_clean = extrair_hora_bruta(a.get('observacao', ''))
            disabled = a['status'] != 'Pendente'
            
            with st.form(key=f"f_row_{a['id']}"):
                c_id, c_desc, c_proj, c_perc, c_obs, c_act = st.columns([0.5, 3, 3, 1.5, 2.5, 1.5])
                
                c_id.markdown(f"<div style='padding-top: 10px;'>{a['id']}</div>", unsafe_allow_html=True)
                
                with c_desc:
                    nd = st.selectbox("d", DESCRICOES_SELECT, index=DESCRICOES_SELECT.index(a['descricao']) if a['descricao'] in DESCRICOES_SELECT else 0, key=f"d_{a['id']}", label_visibility="collapsed", disabled=disabled)
                with c_proj:
                    np = st.selectbox("p", PROJETOS_SELECT, index=PROJETOS_SELECT.index(a['projeto']) if a['projeto'] in PROJETOS_SELECT else 0, key=f"p_{a['id']}", label_visibility="collapsed", disabled=disabled)
                with c_perc:
                    nv = st.number_input("%", value=int(a['porcentagem']), min_value=0, max_value=100, key=f"v_{a['id']}", label_visibility="collapsed", disabled=disabled or h_bruta > 0, help="Desabilitado no modo horas.")
                with c_obs:
                    no = st.text_input("o", value=obs_clean, key=f"o_{a['id']}", label_visibility="collapsed", disabled=disabled)
                
                with c_act:
                    st.markdown(f'<span class="status-badge status-{a["status"]}">{a["status"]}</span>', unsafe_allow_html=True)
                    
                    cb1, cb2 = st.columns(2)
                    with cb1:
                        btn_salvar = st.form_submit_button("💾", disabled=disabled, use_container_width=True, help="Salvar")
                    with cb2:
                        # O delete é uma função sem form, pois precisa ser acionado fora do submit
                        btn_excluir = st.form_submit_button("🗑️", use_container_width=True, help="Excluir")

                if btn_salvar:
                    exc = calcular_porcentagem_existente(st.session_state["usuario"], mes_num, ano_sel, excluido_id=a['id'])
                    
                    perc_final = nv
                    if h_bruta > 0:
                        perc_final = a['porcentagem']
                    elif exc + nv > 100:
                        st.toast("Erro: > 100%", icon="❌")
                        st.stop()
                        
                    obs_final = f"[HORA:{h_bruta}|{no}]" if h_bruta > 0 else no
                    actual_update_ok = atualizar_atividade_completa(a['id'], nd, np, perc_final, obs_final)
                    
                    if actual_update_ok:
                        # O cache já foi limpo dentro de atualizar_atividade_completa
                        st.toast("Atualizado!", icon="✅")
                        st.rerun()
                    else:
                        st.toast("Erro ao salvar!", icon="❌")
                
                if btn_excluir:
                    # Chamada fora do if not btn_salvar para garantir que ele é o único acionado
                    handle_delete(a['id'])

                st.markdown("<hr style='margin: 5px 0; border-top: 1px solid #eee;'>", unsafe_allow_html=True)

    # ==============================
    # ABA: Importar Dados
    # ==============================
    elif aba == "Importar Dados":
        st.header("⬆️ Importação de Dados")
        
        if st.session_state["admin"]:
            st.info("Modo Admin: Importa conforme coluna 'Nome'.")
        else:
            st.info(f"Modo Usuário: Dados serão importados para **{st.session_state['usuario']}**.")
            
        up = st.file_uploader("CSV ou Excel", type=["csv", "xlsx"])
        if up:
            try:
                if up.name.endswith('.csv'):
                    df = pd.read_csv(up, sep=None, engine='python')
                else:
                    df = pd.read_excel(up)
                
                # --- NOVO TRATAMENTO DE VÍRGULA DECIMAL ---
                if 'Porcentagem' in df.columns:
                    # Se Pandas leu como string (object), corrige a vírgula
                    if df['Porcentagem'].dtype == object:
                        df['Porcentagem'] = df['Porcentagem'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                        df['Porcentagem'] = pd.to_numeric(df['Porcentagem'], errors='coerce')
                # ------------------------------------------
                
                map_cols = {'Nome': 'usuario', 'Data': 'data', 'Descrição': 'descricao', 'Projeto': 'projeto', 'Porcentagem': 'porcentagem', 'Observação (Opcional)': 'observacao'}
                df.columns = df.columns.str.strip()
                cols_existentes = {c: c for c in df.columns}
                rename_dict = {}
                for k, v in map_cols.items():
                    for c in cols_existentes:
                        if k.lower() == c.lower():
                            rename_dict[c] = v
                df.rename(columns=rename_dict, inplace=True)
                
                colunas_obrigatorias = ['usuario', 'data', 'descricao', 'projeto', 'porcentagem']
                missing = [c for c in colunas_obrigatorias if c not in df.columns]
                if missing:
                    if not st.session_state["admin"] and 'usuario' in missing: pass 
                    else:
                        st.error(f"Colunas faltando: {missing}")
                        st.stop()

                if not st.session_state["admin"]:
                    df['usuario'] = st.session_state["usuario"]
                
                df['data'] = pd.to_datetime(df['data'], errors='coerce', dayfirst=True)
                df.dropna(subset=['data', 'usuario', 'porcentagem'], inplace=True)
                df['mes'] = df['data'].dt.month
                df['ano'] = df['data'].dt.year
                
                if df['porcentagem'].max() <= 1.0: df['porcentagem'] = (df['porcentagem'] * 100)
                df['porcentagem'] = df['porcentagem'].astype(int)

                if 'observacao' not in df.columns: df['observacao'] = ''
                df['observacao'] = df['observacao'].fillna('').astype(str)
                df['status'] = 'Pendente'
                df['descricao'] = df['descricao'].astype(str).str.strip()
                df['projeto'] = df['projeto'].astype(str).str.strip()

                st.markdown("### 🔍 Validação")
                erros_validacao = False
                desc_inv = df[~df['descricao'].isin(DESCRICOES)]
                if not desc_inv.empty:
                    st.error("❌ Descrições inválidas")
                    st.dataframe(desc_inv['descricao'].unique())
                    erros_validacao = True

                proj_inv = df[~df['projeto'].isin(PROJETOS)]
                if not proj_inv.empty:
                    st.error("❌ Projetos inválidos")
                    st.dataframe(proj_inv['projeto'].unique())
                    erros_validacao = True
                
                if erros_validacao: st.stop()
                
                st.success("✅ Validado!")
                st.dataframe(df.head())
                
                if st.button("Confirmar Importação", type="primary"):
                    df_exist = atividades_df[atividades_df['status'] != 'Rejeitado']
                    tot_ex = df_exist.groupby(['usuario','mes','ano'])['porcentagem'].sum().reset_index().rename(columns={'porcentagem':'existente'})
                    tot_new = df.groupby(['usuario','mes','ano'])['porcentagem'].sum().reset_index().rename(columns={'porcentagem':'novo'})
                    merged = pd.merge(tot_ex, tot_new, on=['usuario','mes','ano'], how='outer').fillna(0)
                    
                    violacoes = merged[merged['existente'] + merged['novo'] > 100]
                    if not violacoes.empty:
                        st.error("❌ Soma > 100% detectada.")
                        st.dataframe(violacoes)
                        st.stop()
                        
                    qtd, msg = bulk_insert_atividades(df)
                    if qtd > 0: 
                        st.balloons()
                        st.success(f"🎉 Importado {qtd} registros.")
                        # bulk_insert_atividades limpa o cache e faz o ajuste.
                    else: 
                        st.error(msg)
            
            except Exception as e:
                st.error(f"Erro: {e}")

    # ==============================
    # ABA: Consolidado (Admin)
    # ==============================
    elif aba == "Consolidado" and st.session_state["admin"]:
        st.header("📑 Consolidado")
        if atividades_df.empty:
            st.info("Vazio.")
        else:
            c1, c2, c3 = st.columns(3)
            u_sel = c1.selectbox("Usuário", ["Todos"] + sorted(atividades_df['usuario'].unique()))
            atividades_df['m_a'] = atividades_df['data'].dt.strftime('%Y-%m')
            m_sel = c2.selectbox("Mês", ["Todos"] + sorted(atividades_df['m_a'].unique(), reverse=True))
            s_sel = c3.selectbox("Status", ["Todos", "Pendente", "Aprovado", "Rejeitado"])
            
            df_f = atividades_df.copy()
            if u_sel != "Todos": df_f = df_f[df_f['usuario'] == u_sel]
            if m_sel != "Todos": df_f = df_f[df_f['m_a'] == m_sel]
            if s_sel != "Todos": df_f = df_f[df_f['status'] == s_sel]

            # Renomeia colunas para exportação
            df_export = df_f.drop(columns=['m_a', 'id', 'observacao']).rename(columns={
                'usuario': 'Usuário',
                'data': 'Data',
                'mes': 'Mês',
                'ano': 'Ano',
                'descricao': 'Descrição',
                'projeto': 'Projeto',
                'porcentagem': 'Porcentagem (%)',
                'status': 'Status'
            })
            
            st.plotly_chart(px.bar(df_f.groupby('m_a')['porcentagem'].sum().reset_index(), x='m_a', y='porcentagem', title="Total Alocado"), use_container_width=True)
            
            st.dataframe(df_f.drop(columns=['m_a']), use_container_width=True, hide_index=True)

            # Botão de Exportação para Excel (Consolidado)
            buffer = io.BytesIO()
            df_export.to_excel(buffer, index=False, sheet_name='Consolidado')
            buffer.seek(0)
            
            st.download_button(
                label="⬇️ Exportar Tabela Filtrada para Excel",
                data=buffer,
                file_name=f"consolidado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
