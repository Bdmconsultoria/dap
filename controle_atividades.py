# controle_atividades.py
# Versão ajustada — modo % / modo Horas separados e normalização automática (Opção A)
import streamlit as st
import pandas as pd
from datetime import datetime
import psycopg2
import psycopg2.extras
import re
import math

# ==============================
# Configurações visuais / constantes
# ==============================
COR_PRIMARIA = "#313191"
COR_SECUNDARIA = "#19c0d1"
COR_CINZA = "#444444"
COR_FUNDO_APP = "#FFFFFF"
COR_FUNDO_SIDEBAR = COR_PRIMARIA
SINAPSIS_PALETTE = [COR_SECUNDARIA, COR_PRIMARIA, COR_CINZA, "#888888", "#C0C0C0"]
LOGO_URL = "https://raw.githubusercontent.com/Bdmconsultoria/dap/main/logo_sinapsis.png"

# ==============================
# 1. Credenciais PostgreSQL (via st.secrets)
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
except Exception:
    DB_PARAMS = {}
    # Não estoura aqui - usa em get_db_connection para avisar.

# ==============================
# 2. Conexão com banco
# ==============================
def get_db_connection():
    if not DB_PARAMS:
        return None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        st.error("Erro de conexão ao banco de dados. Verifique st.secrets. (" + str(e) + ")")
        return None

# ==============================
# 3. Setup inicial do DB (cria tabelas caso não existam)
# ==============================
def setup_db():
    conn = get_db_connection()
    if conn is None:
        return
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
                    observacao TEXT,
                    status VARCHAR(50) DEFAULT 'Pendente'
                );
            """)
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
        st.error(f"Erro ao criar tabelas: {e}")
    finally:
        conn.close()

if DB_PARAMS:
    setup_db()

# ==============================
# 4. Helpers - Extrair hora e Observação limpa
# ==============================
def extrair_hora_bruta(observacao):
    """
    Extrai [HORA:X|OBS_REAL] se presente.
    Retorna (hora_float, observacao_limpa)
    """
    if not observacao:
        return 0.0, ''
    match = re.search(r'\[HORA:(\d+\.?\d*)\|(.*)\]', observacao, re.DOTALL)
    if match:
        try:
            hora = float(match.group(1))
        except:
            hora = 0.0
        obs_limpa = match.group(2).strip()
        return hora, obs_limpa
    return 0.0, observacao.strip()

# ==============================
# 5. Funções CRUD básicas
# ==============================
def salvar_usuario(usuario, senha, admin=False):
    conn = get_db_connection()
    if conn is None:
        return False
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
    if conn is None:
        return False, False
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT senha, admin FROM usuarios WHERE usuario = %s;", (usuario,))
            row = cursor.fetchone()
            if row and row[0] == senha:
                return True, row[1]
            return False, False
    except Exception as e:
        st.error(f"Erro validar login: {e}")
        return False, False
    finally:
        conn.close()

def alterar_senha(usuario, nova_senha):
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE usuarios SET senha = %s WHERE usuario = %s;", (nova_senha, usuario))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro alterar senha: {e}")
        return False
    finally:
        conn.close()

# ==============================
# 6. Consultas e leituras
# ==============================
@st.cache_data(ttl=600)
def carregar_dados():
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame(), pd.DataFrame()
    try:
        usuarios_df = pd.read_sql("SELECT usuario, admin FROM usuarios;", conn)
        try:
            atividades_df = pd.read_sql("""
                SELECT id, usuario, data, mes, ano, descricao, projeto, porcentagem, observacao, status
                FROM atividades
                ORDER BY ano DESC, mes DESC, data DESC;
            """, conn)
        except Exception:
            atividades_df = pd.read_sql("""
                SELECT id, usuario, data, mes, ano, descricao, projeto, porcentagem, observacao
                FROM atividades
                ORDER BY ano DESC, mes DESC, data DESC;
            """, conn)
            atividades_df['status'] = 'Pendente'
        if not atividades_df.empty:
            atividades_df['data'] = pd.to_datetime(atividades_df['data'])
        return usuarios_df, atividades_df
    except Exception as e:
        st.error(f"Erro carregar dados: {e}")
        return pd.DataFrame(), pd.DataFrame()
    finally:
        conn.close()

def carregar_atividades_usuario(usuario, mes, ano):
    conn = get_db_connection()
    if conn is None:
        return []
    try:
        df = pd.read_sql("""
            SELECT id, descricao, projeto, porcentagem, observacao, status
            FROM atividades
            WHERE usuario = %s AND mes = %s AND ano = %s
            ORDER BY id DESC;
        """, conn, params=(usuario, mes, ano))
        return df.to_dict('records')
    except Exception:
        return []
    finally:
        conn.close()

# ==============================
# 7. Funções de normalização (núcleo das mudanças)
# ==============================
def _corrigir_residual_e_atualizar(conn, lista_id_porcentagem):
    """
    Recebe lista de tuplas (id, pct_int_provisoria).
    Ajusta residual (100 - sum) adicionando ao maior item para garantir soma==100.
    Atualiza DB.
    """
    if not lista_id_porcentagem:
        return
    total = sum(p for _, p in lista_id_porcentagem)
    if total == 0:
        # se total zero, divide igualmente
        n = len(lista_id_porcentagem)
        base = 100 // n
        resto = 100 - base * n
        nova = []
        for i, (aid, _) in enumerate(lista_id_porcentagem):
            add = 1 if i < resto else 0
            nova.append((aid, base + add))
        lista_id_porcentagem = nova
    else:
        # se total != 100, ajusta proporcional e depois corrige residual
        # Já assume que os pct recebidos estão arredondados; se não, arredonda aqui
        lista_id_porcentagem = [(aid, int(round(p))) for (aid, p) in lista_id_porcentagem]
        total = sum(p for _, p in lista_id_porcentagem)
        if total != 100:
            # encontra índice do maior e ajusta
            dif = 100 - total
            # se houver empate, pega o primeiro
            idx_maior = max(range(len(lista_id_porcentagem)), key=lambda i: lista_id_porcentagem[i][1])
            aid, val = lista_id_porcentagem[idx_maior]
            lista_id_porcentagem[idx_maior] = (aid, val + dif)

    # Atualiza no DB
    try:
        with conn.cursor() as cursor:
            for aid, pct in lista_id_porcentagem:
                cursor.execute("UPDATE atividades SET porcentagem = %s WHERE id = %s;", (int(pct), aid))
        conn.commit()
    except Exception as e:
        conn.rollback()
        st.error(f"Erro ao atualizar porcentagens durante normalização: {e}")

def normalizar_porcentagens_por_valores(usuario, mes, ano):
    """
    Normaliza as atividades do usuário/mes/ano com base no campo 'porcentagem' já armazenado:
    - Opção A: Sempre aplicar normalização (mesmo se soma==100).
    """
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, porcentagem
                FROM atividades
                WHERE usuario=%s AND mes=%s AND ano=%s AND status != 'Rejeitado'
            """, (usuario, mes, ano))
            rows = cursor.fetchall()
            if not rows:
                return True
            total = sum(r[1] for r in rows)
            # Se total==0 -> distribuir igualmente
            if total == 0:
                n = len(rows)
                base = 100.0 / n
                lista = [(r[0], base) for r in rows]
            else:
                lista = [(r[0], (r[1] / float(total)) * 100.0) for r in rows]
            _corrigir_residual_e_atualizar(conn, lista)
            return True
    except Exception as e:
        st.error(f"Erro ao normalizar por valores: {e}")
        return False
    finally:
        conn.close()

def normalizar_porcentagens_por_horas(usuario, mes, ano):
    """
    Normaliza com base no metadado de hora [HORA:X|OBS] nas observações.
    Calcula porcentagem para todas as atividades que tenham HORA>0.
    Se não houver horas, não altera nada.
    """
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, observacao
                FROM atividades
                WHERE usuario=%s AND mes=%s AND ano=%s AND status != 'Rejeitado'
            """, (usuario, mes, ano))
            rows = cursor.fetchall()
            atividades_horas = []
            for r in rows:
                aid = r[0]
                obs = r[1] or ''
                h, _ = extrair_hora_bruta(obs)
                if h > 0:
                    atividades_horas.append((aid, h))
            if not atividades_horas:
                return True
            total_h = sum(h for _, h in atividades_horas)
            if total_h == 0:
                # evita divisão por zero, não altera
                return True
            lista = [(aid, (h / total_h) * 100.0) for (aid, h) in atividades_horas]
            _corrigir_residual_e_atualizar(conn, lista)
            return True
    except Exception as e:
        st.error(f"Erro ao normalizar por horas: {e}")
        return False
    finally:
        conn.close()

# ==============================
# 8. Salvar / atualizar / apagar atividades com integração das normalizações
# ==============================
def inserir_atividade_db(usuario, mes, ano, descricao, projeto, porcentagem, observacao, status='Pendente'):
    conn = get_db_connection()
    if conn is None:
        return False, "Falha na conexão"
    try:
        data_db = datetime(year=ano, month=mes, day=1).date()
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO atividades (usuario, data, mes, ano, descricao, projeto, porcentagem, observacao, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (usuario, data_db, mes, ano, descricao, projeto, int(porcentagem), observacao, status))
        conn.commit()
        return True, "Inserido"
    except Exception as e:
        conn.rollback()
        return False, f"Erro inserir atividade: {e}"
    finally:
        conn.close()

def atualizar_atividade_db(atividade_id, descricao, projeto, porcentagem, observacao):
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        data_db = datetime.now().date()  # atualiza data para hoje (mantém mês/ano separados)
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE atividades
                SET descricao=%s, projeto=%s, porcentagem=%s, observacao=%s
                WHERE id=%s;
            """, (descricao, projeto, int(porcentagem), observacao, atividade_id))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        st.error(f"Erro atualizar atividade: {e}")
        return False
    finally:
        conn.close()

def apagar_atividade_db(atividade_id):
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM atividades WHERE id = %s;", (atividade_id,))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        st.error(f"Erro apagar atividade: {e}")
        return False
    finally:
        conn.close()

# ==============================
# 9. Funções de mais alto nível — salvar múltiplos lançamentos da UI
# ==============================
def salvar_multiplos_lancamentos(usuario, mes, ano, lancamentos, modo):
    """
    lancamentos: lista de dicts {'descricao','projeto','valor','observacao'}
    modo: 'Porcentagem' ou 'Horas'
    Comportamento:
      - Insere cada lançamento
      - Se modo == 'Horas', garante que observacao contenha [HORA:X|texto] (se usuário não enviou, assumimos valor como horas e encapsulamos)
      - Após inserir todos, executa a normalização:
         * Se modo == 'Porcentagem' -> normalizar_porcentagens_por_valores (Opção A: sempre normaliza)
         * Se modo == 'Horas' -> normalizar_porcentagens_por_horas
    """
    inserted = []
    for l in lancamentos:
        desc = l.get('descricao') or '---'
        proj = l.get('projeto') or '---'
        val = l.get('valor') or 0.0
        obs = l.get('observacao') or ''

        if modo == "Horas":
            # garantir que o campo observacao tenha [HORA:X|obs_limpa]
            # se o usuário já passou a tag, respeitamos; senão encapsulamos
            h, obs_limpa = extrair_hora_bruta(obs)
            if h == 0 and val > 0:
                # encapsula
                obs_nova = f"[HORA:{float(val)}|{obs.strip()}]"
            else:
                obs_nova = obs
            # porcentagem inicial inserida como 0 (será recalculada)
            ok, msg = inserir_atividade_db(usuario, mes, ano, desc, proj, 0, obs_nova)
            if ok:
                inserted.append(True)
            else:
                inserted.append(False)
        else:
            # modo Porcentagem
            # valor é porcentagem direta
            ok, msg = inserir_atividade_db(usuario, mes, ano, desc, proj, val, obs)
            if ok:
                inserted.append(True)
            else:
                inserted.append(False)

    # Após inserção, normaliza conforme modo
    if modo == "Horas":
        normalizar_porcentagens_por_horas(usuario, mes, ano)
    else:
        # Opção A: sempre normaliza ao salvar/editar
        normalizar_porcentagens_por_valores(usuario, mes, ano)

    carregar_dados.clear()
    return all(inserted)

# ==============================
# 10. Atualização / exclusão com normalização
# ==============================
def editar_atividade_e_normalizar(atividade_id, usuario, mes, ano, descricao, projeto, porcentagem, observacao, modo):
    """
    Atualiza e normaliza de acordo com modo.
    """
    ok = atualizar_atividade_db(atividade_id, descricao, projeto, porcentagem, observacao)
    if not ok:
        return False
    if modo == "Horas":
        normalizar_porcentagens_por_horas(usuario, mes, ano)
    else:
        normalizar_porcentagens_por_valores(usuario, mes, ano)
    carregar_dados.clear()
    return True

def excluir_atividade_e_normalizar(atividade_id):
    """
    Apaga e normaliza: detecta usuário/mes/ano da atividade antes de apagar para normalizar corretamente.
    """
    conn = get_db_connection()
    if conn is None:
        st.error("Falha na conexão ao apagar.")
        return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT usuario, mes, ano, observacao FROM atividades WHERE id = %s;", (atividade_id,))
            row = cursor.fetchone()
            if not row:
                st.error("Atividade não encontrada.")
                return False
            usuario, mes, ano, obs = row
        # Apaga
        ok = apagar_atividade_db(atividade_id)
        if not ok:
            return False
        # Se havia HORA na observação apagada, normaliza por horas (se ainda houver atividades com HORA),
        # senão normaliza por valores.
        # Simples: tenta normalizar por horas (se não houver horas, função retorna True sem alteração),
        # e em seguida normaliza por valores (Opção A: sempre mantém 100% no modo porcentagem).
        normalizar_porcentagens_por_horas(usuario, mes, ano)
        normalizar_porcentagens_por_valores(usuario, mes, ano)
        carregar_dados.clear()
        return True
    except Exception as e:
        st.error(f"Erro ao apagar atividade: {e}")
        return False
    finally:
        conn.close()

# ==============================
# 11. Funções adicionais: status / hierarquia / bulk insert
# (mantém compatibilidade com o app original)
# ==============================
def atualizar_status_atividade(atividade_id, novo_status):
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE atividades SET status = %s WHERE id = %s;", (novo_status, atividade_id))
        conn.commit()
        carregar_dados.clear()
        return True
    except Exception as e:
        st.error(f"Erro atualizar status: {e}")
        return False
    finally:
        conn.close()

def salvar_hierarquia(gerente, subordinado):
    conn = get_db_connection()
    if conn is None:
        return False
    if gerente == subordinado:
        st.error("Gerente e subordinado não podem ser iguais.")
        return False
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO hierarquia (gerente, subordinado)
                VALUES (%s, %s)
                ON CONFLICT (gerente, subordinado) DO NOTHING;
            """, (gerente, subordinado))
        conn.commit()
        carregar_hierarquia.clear()
        return True
    except Exception as e:
        st.error(f"Erro salvar hierarquia: {e}")
        return False
    finally:
        conn.close()

@st.cache_data(ttl=600)
def carregar_hierarquia():
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT gerente, subordinado FROM hierarquia ORDER BY gerente, subordinado;", conn)
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()

def bulk_insert_usuarios(user_list):
    conn = get_db_connection()
    if conn is None:
        return 0, "Falha conexão"
    data = [(u, '123', False) for u in user_list]
    try:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_batch(cursor, """
                INSERT INTO usuarios (usuario, senha, admin) VALUES (%s, %s, %s) ON CONFLICT (usuario) DO NOTHING;
            """, data)
        conn.commit()
        return len(data), "Inseridos"
    except Exception as e:
        conn.rollback()
        return 0, f"Erro: {e}"
    finally:
        conn.close()

def bulk_insert_atividades(df_to_insert):
    conn = get_db_connection()
    if conn is None:
        return 0, "Falha conexão"
    data_list = [tuple(row) for row in df_to_insert[[
        'usuario', 'data', 'mes', 'ano', 'descricao', 'projeto', 'porcentagem', 'observacao', 'status'
    ]].values]
    query = """
        INSERT INTO atividades (usuario, data, mes, ano, descricao, projeto, porcentagem, observacao, status)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    try:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_batch(cursor, query, data_list)
        conn.commit()
        return len(data_list), "OK"
    except Exception as e:
        conn.rollback()
        return 0, f"Erro: {e}"
    finally:
        conn.close()

# ==============================
# 12. UI - Estrutura principal do app (mantendo layout original, mas simplificada)
# ==============================
st.set_page_config(page_title="Controle de Atividades", layout="wide")
if LOGO_URL:
    st.sidebar.image(LOGO_URL, use_container_width=True)

# CSS básico
st.markdown(f"""
    <style>
        [data-testid="stSidebar"] {{
            background-color: {COR_FUNDO_SIDEBAR};
        }}
        [data-testid="stSidebar"] * {{ color: #FFFFFF !important; }}
        .status-badge {{ padding: 4px 8px; border-radius: 12px; font-weight: bold; }}
    </style>
""", unsafe_allow_html=True)

# Sessão
if "usuario" not in st.session_state:
    st.session_state["usuario"] = None
    st.session_state["admin"] = False
if 'edit_id' not in st.session_state:
    st.session_state['edit_id'] = None
if 'lanc_tipo_aba' not in st.session_state:
    st.session_state['lanc_tipo_aba'] = "Porcentagem"

# Carrega dados
usuarios_df, atividades_df = carregar_dados()
hierarquia_df = carregar_hierarquia()

# LOGIN SIMPLES
if st.session_state["usuario"] is None:
    st.title("🔐 Login")
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        usuario = st.text_input("Usuário", key="login_usuario")
        senha = st.text_input("Senha", type="password", key="login_senha")
        if st.button("Entrar"):
            ok, admin = validar_login(usuario.strip(), senha)
            if ok:
                st.session_state["usuario"] = usuario.strip()
                st.session_state["admin"] = admin
                st.experimental_rerun()
            else:
                st.error("Usuário ou senha incorretos.")
else:
    st.sidebar.markdown(f"**Usuário:** {st.session_state['usuario']}")
    if st.sidebar.button("Sair"):
        st.session_state["usuario"] = None
        st.session_state["admin"] = False
        st.experimental_rerun()

    # Menu
    is_manager = False
    if not hierarquia_df.empty:
        is_manager = st.session_state["usuario"] in hierarquia_df['gerente'].unique().tolist()

    abas = ["Lançar Atividade", "Minhas Atividades"]
    if st.session_state["admin"] or is_manager:
        abas.append("Gerenciar Time")
    if st.session_state["admin"]:
        abas += ["Gerenciar Usuários", "Consolidado", "Importar Dados"]

    aba = st.sidebar.radio("Menu", abas, index=0)

    # Dados fixos resumidos (você pode manter a lista completa do seu arquivo original)
    DESCRICOES = ["1.001 - Gestão","1.002 - Geral","1.003 - Conselho","1.004 - Treinamento e Desenvolvimento"]
    DESCRICOES_SELECT = ["--- Selecione ---"] + DESCRICOES
    PROJETOS = ["101-0 (Interno) Diretoria Executiva","102-0 (Interno) Diretoria Administrativa"]
    PROJETOS_SELECT = ["--- Selecione ---"] + PROJETOS
    MESES = {1: "01 - Janeiro", 2: "02 - Fevereiro", 3: "03 - Março", 4: "04 - Abril",
             5: "05 - Maio", 6: "06 - Junho", 7: "07 - Julho", 8: "08 - Agosto",
             9: "09 - Setembro", 10: "10 - Outubro", 11: "11 - Novembro", 12: "12 - Dezembro"}
    MESES_SELECT = ["--- Selecione ---"] + list(MESES.values())
    ANOS = list(range(datetime.today().year - 2, datetime.today().year + 3))

    # Aba: Lançar Atividade
    if aba == "Lançar Atividade":
        st.header("📝 Lançar Atividade (Mensal)")
        col_mes, col_ano = st.columns(2)
        mes_select = col_mes.selectbox("Mês", MESES_SELECT, index=list(MESES.values()).index(MESES[datetime.today().month]) + 1, key="lanc_mes_select")
        ano_select = col_ano.selectbox("Ano", ANOS, index=ANOS.index(datetime.today().year), key="lanc_ano_select")
        mes_num = next((k for k, v in MESES.items() if v == mes_select), None)

        # Atividades do mês (para mostrar total)
        if mes_num:
            atividades_do_mes = carregar_atividades_usuario(st.session_state["usuario"], mes_num, ano_select)
        else:
            atividades_do_mes = []
        atividades_ativas = [a for a in atividades_do_mes if a['status'] != 'Rejeitado']
        total_existente = sum(a['porcentagem'] for a in atividades_ativas)
        # Contagem de horas existentes
        total_horas_existentes = sum(extrair_hora_bruta(a.get('observacao', '') or '')[0] for a in atividades_ativas)

        tab_porcentagem, tab_horas = st.tabs(["Lançamento por Porcentagem", "Lançamento por Horas"])

        with tab_porcentagem:
            st.session_state['lanc_tipo_aba'] = "Porcentagem"
            st.info(f"Total já alocado: {total_existente:.1f}%")
            qtd_lancamentos_p = st.number_input("Quantos lançamentos deseja adicionar?", min_value=1, max_value=20, value=1, step=1, key="lanc_qtd_p")

        with tab_horas:
            st.session_state['lanc_tipo_aba'] = "Horas"
            st.info(f"Horas brutas já lançadas: {total_horas_existentes:.1f} hrs")
            qtd_lancamentos_h = st.number_input("Quantos lançamentos deseja adicionar?", min_value=1, max_value=20, value=1, step=1, key="lanc_qtd_h")

        tipo_lancamento = st.session_state['lanc_tipo_aba']
        qtd = st.session_state.get("lanc_qtd_p", 1) if tipo_lancamento == "Porcentagem" else st.session_state.get("lanc_qtd_h", 1)

        st.markdown("---")
        st.write("Preencha os lançamentos abaixo:")
        lancamentos = []
        for i in range(qtd):
            cols = st.columns([0.5, 4, 4, 1.5, 3])
            with cols[0]:
                st.text_input("Nº", value=str(i+1), key=f"idx_{i}", disabled=True, label_visibility="collapsed")
            with cols[1]:
                descricao = st.selectbox(f"Descrição {i}", DESCRICOES_SELECT, key=f"desc_{i}", label_visibility="collapsed")
            with cols[2]:
                projeto = st.selectbox(f"Projeto {i}", PROJETOS_SELECT, key=f"proj_{i}", label_visibility="collapsed")
            with cols[3]:
                if tipo_lancamento == "Porcentagem":
                    valor = st.number_input(f"% {i}", min_value=0.0, max_value=100.0, step=1.0, value=0.0, key=f"valor_{i}", label_visibility="collapsed")
                else:
                    valor = st.number_input(f"Horas {i}", min_value=0.0, max_value=200.0, step=0.5, value=0.0, key=f"valor_{i}", label_visibility="collapsed")
            with cols[4]:
                observacao = st.text_area(f"Observação {i}", value="", key=f"obs_{i}", label_visibility="collapsed", height=60)

            lancamentos.append({
                "descricao": descricao,
                "projeto": projeto,
                "valor": valor,
                "observacao": observacao
            })

        if st.form_submit_button("Salvar lançamentos", key="btn_salvar_lancamentos"):
            # filtrar lançamentos válidos (ex.: descrição/projeto não vazios)
            lancs_validos = [l for l in lancamentos if (l['descricao'] and l['projeto'])]
            sucesso = salvar_multiplos_lancamentos(st.session_state["usuario"], mes_num, ano_select, lancs_validos, tipo_lancamento)
            if sucesso:
                st.success("Lançamentos salvos e normalizados com sucesso.")
                carregar_dados.clear()
                st.experimental_rerun()
            else:
                st.error("Erro ao salvar alguns lançamentos. Verifique os logs.")

    # Aba: Minhas Atividades (listagem simples + editar/excluir)
    elif aba == "Minhas Atividades":
        st.header("📋 Minhas Atividades")
        hoje = datetime.now()
        mes_vigente = st.selectbox("Mês", list(MESES.values()), index=list(MESES.values()).index(MESES[hoje.month]), key="minhas_mes")
        ano_vigente = st.selectbox("Ano", ANOS, index=ANOS.index(hoje.year), key="minhas_ano")
        mes_num = next((k for k, v in MESES.items() if v == mes_vigente), None)
        if mes_num is None:
            st.info("Selecione um mês válido.")
        else:
            minhas = carregar_atividades_usuario(st.session_state["usuario"], mes_num, ano_vigente)
            if not minhas:
                st.info("Nenhuma atividade encontrada.")
            else:
                for a in minhas:
                    hora, obs_limpa = extrair_hora_bruta(a.get('observacao', '') or '')
                    st.markdown(f"**ID {a['id']}** — {a['descricao']} | Projeto: {a['projeto']} — {a['porcentagem']}% — Obs: {obs_limpa}")
                    col1, col2, col3 = st.columns([0.2,0.2,0.2])
                    with col1:
                        if st.button("Editar", key=f"edit_{a['id']}"):
                            st.session_state['edit_id'] = a['id']
                            st.experimental_rerun()
                    with col2:
                        if st.button("Excluir", key=f"del_{a['id']}"):
                            ok = excluir_atividade_e_normalizar(a['id'])
                            if ok:
                                st.success("Atividade excluída e alocações atualizadas.")
                                st.experimental_rerun()
                            else:
                                st.error("Erro ao excluir atividade.")
                    with col3:
                        status = a.get('status', 'Pendente')
                        st.write(status)

                # Se está editando
                if st.session_state.get('edit_id'):
                    edit_id = st.session_state['edit_id']
                    # buscar atividade
                    item = None
                    for a in minhas:
                        if a['id'] == edit_id:
                            item = a
                            break
                    if item:
                        st.markdown("---")
                        st.subheader(f"Editando Atividade ID {edit_id}")
                        desc_new = st.selectbox("Descrição", DESCRICOES_SELECT, index=0)
                        proj_new = st.selectbox("Projeto", PROJETOS_SELECT, index=0)
                        pct_new = st.number_input("Porcentagem", min_value=0, max_value=100, value=item['porcentagem'], step=1)
                        obs_new = st.text_area("Observação", value=item.get('observacao','') or '')
                        if st.button("Salvar Edição"):
                            # detectar modo (se observacao tem HORA -> horas mode)
                            hora_val, _ = extrair_hora_bruta(obs_new)
                            modo_local = "Horas" if hora_val > 0 else "Porcentagem"
                            ok = editar_atividade_e_normalizar(edit_id, st.session_state["usuario"], item['mes'], item['ano'], desc_new, proj_new, pct_new, obs_new, modo_local)
                            if ok:
                                st.success("Atividade atualizada e normalizada.")
                                st.session_state['edit_id'] = None
                                st.experimental_rerun()
                            else:
                                st.error("Erro ao atualizar atividade.")
                    else:
                        st.warning("Atividade a editar não encontrada neste mês.")
    # Aba: Gerenciar Time (simplificado)
    elif aba == "Gerenciar Time":
        st.header("Gerenciar Time (Aprovação)")
        if hierarquia_df.empty:
            st.info("Nenhuma hierarquia configurada.")
        else:
            gerentes = hierarquia_df['gerente'].unique().tolist()
            if st.session_state["admin"]:
                gerente_sel = st.selectbox("Gerente", sorted(gerentes))
            else:
                gerente_sel = st.session_state["usuario"]
            time_df = hierarquia_df[hierarquia_df['gerente'] == gerente_sel]
            subordinados = time_df['subordinado'].tolist()
            st.write("Pessoas do time:", subordinados)
            # Lista de atividades do time para aprovar
            col_m, col_a = st.columns(2)
            mes_sel = col_m.selectbox("Mês", list(MESES.values()), index=list(MESES.values()).index(MESES[datetime.now().month]))
            ano_sel = col_a.selectbox("Ano", ANOS, index=ANOS.index(datetime.now().year))
            mes_num = next((k for k, v in MESES.items() if v == mes_sel), None)
            if mes_num:
                df_time = atividades_df[
                    (atividades_df['usuario'].isin(subordinados)) &
                    (atividades_df['mes'] == mes_num) &
                    (atividades_df['ano'] == ano_sel)
                ]
                if df_time.empty:
                    st.info("Nenhuma atividade para o mês/ano selecionado.")
                else:
                    for _, row in df_time.iterrows():
                        _, obs_limpa = extrair_hora_bruta(row['observacao'])
                        st.markdown(f"**{row['usuario']}** — {row['descricao']} | Projeto: {row['projeto']} — {row['porcentagem']}% — Obs: {obs_limpa}")
                        c1, c2, c3 = st.columns([0.2,0.2,0.2])
                        with c1:
                            if st.button("Aprovar", key=f"apv_{row['id']}"):
                                atualizar_status_atividade(row['id'], 'Aprovado')
                                st.experimental_rerun()
                        with c2:
                            if st.button("Rejeitar", key=f"rej_{row['id']}"):
                                atualizar_status_atividade(row['id'], 'Rejeitado')
                                st.experimental_rerun()
                        with c3:
                            if st.button("Excluir", key=f"delg_{row['id']}"):
                                excluir_atividade_e_normalizar(row['id'])
                                st.experimental_rerun()
    # Aba: Gerenciar Usuários (Admin)
    elif aba == "Gerenciar Usuários" and st.session_state["admin"]:
        st.header("Gerenciar Usuários (Admin)")
        with st.form("form_add_user"):
            novo_usuario = st.text_input("Usuário")
            nova_senha = st.text_input("Senha", type="password")
            admin_check = st.checkbox("Admin?")
            if st.form_submit_button("Adicionar"):
                if novo_usuario and nova_senha:
                    if salvar_usuario(novo_usuario.strip(), nova_senha, admin_check):
                        st.success("Usuário adicionado.")
                        carregar_dados.clear()
                        st.experimental_rerun()
                    else:
                        st.error("Erro ao adicionar usuário.")
                else:
                    st.warning("Preencha usuário e senha.")
    # Outras abas (Consolidado / Importar) deixadas de forma minimalista
    elif aba == "Consolidado":
        st.header("Consolidado")
        st.write("Funcionalidade consolidada - mantenha conforme sua versão atual.")
    elif aba == "Importar Dados":
        st.header("Importar Dados")
        st.write("Funcionalidade de importação - mantenha conforme sua versão atual.")

# ==============================
# FIM DO ARQUIVO
# ==============================
