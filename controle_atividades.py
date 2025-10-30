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
LOGO_URL = "https://raw.githubusercontent.com/Bdmconsultoria/dap/main/logo_sinapsis.png"

# ==============================
# Conexão PostgreSQL
# ==============================
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=st.secrets["postgresql"]["host"],
            port=st.secrets["postgresql"]["port"],
            database=st.secrets["postgresql"]["database"],
            user=st.secrets["postgresql"]["user"],
            password=st.secrets["postgresql"]["password"],
            sslmode=st.secrets["postgresql"]["sslmode"],
        )
        return conn
    except Exception as e:
        st.error("Erro ao conectar ao banco: " + str(e))
        return None

# ==============================
# Criação das tabelas
# ==============================
def setup_db():
    conn = get_db_connection()
    if not conn:
        return
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                usuario VARCHAR(50) PRIMARY KEY,
                senha VARCHAR(50),
                admin BOOLEAN DEFAULT FALSE
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS atividades (
                id SERIAL PRIMARY KEY,
                usuario VARCHAR(50),
                data DATE,
                mes INTEGER,
                ano INTEGER,
                descricao TEXT,
                projeto TEXT,
                porcentagem INTEGER,
                observacao TEXT,
                status TEXT DEFAULT 'Pendente'
            );
        """)
    conn.commit()
    conn.close()

setup_db()

# ==============================
# Funções auxiliares
# ==============================
def extrair_hora_bruta(observacao):
    if not observacao:
        return 0.0, ''
    match = re.search(r'\[HORA:(\d+\.?\d*)\|(.*)\]', observacao)
    if match:
        return float(match.group(1)), match.group(2)
    return 0.0, observacao

def _corrigir_residual_e_atualizar(conn, lista):
    total = sum(p for _, p in lista)
    if total == 0:
        return
    dif = 100 - total
    if dif != 0:
        idx_maior = max(range(len(lista)), key=lambda i: lista[i][1])
        lista[idx_maior] = (lista[idx_maior][0], lista[idx_maior][1] + dif)
    with conn.cursor() as cur:
        for aid, pct in lista:
            cur.execute("UPDATE atividades SET porcentagem=%s WHERE id=%s;", (int(round(pct)), aid))
    conn.commit()

def normalizar_porcentagens_por_valores(usuario, mes, ano):
    conn = get_db_connection()
    if not conn:
        return
    cur = conn.cursor()
    cur.execute("SELECT id, porcentagem FROM atividades WHERE usuario=%s AND mes=%s AND ano=%s;", (usuario, mes, ano))
    rows = cur.fetchall()
    if not rows:
        conn.close()
        return
    total = sum(r[1] for r in rows)
    if total == 0:
        base = 100 / len(rows)
        lista = [(r[0], base) for r in rows]
    else:
        lista = [(r[0], (r[1]/total)*100) for r in rows]
    _corrigir_residual_e_atualizar(conn, lista)
    conn.close()

def normalizar_porcentagens_por_horas(usuario, mes, ano):
    conn = get_db_connection()
    if not conn:
        return
    cur = conn.cursor()
    cur.execute("SELECT id, observacao FROM atividades WHERE usuario=%s AND mes=%s AND ano=%s;", (usuario, mes, ano))
    rows = cur.fetchall()
    horas = [(r[0], extrair_hora_bruta(r[1])[0]) for r in rows if extrair_hora_bruta(r[1])[0] > 0]
    if not horas:
        conn.close()
        return
    total = sum(h for _, h in horas)
    lista = [(r[0], (h/total)*100) for r, h in horas]
    _corrigir_residual_e_atualizar(conn, lista)
    conn.close()

# ==============================
# CRUD atividades
# ==============================
def inserir_atividade(usuario, mes, ano, descricao, projeto, porcentagem, observacao):
    conn = get_db_connection()
    if not conn:
        return
    data = datetime(ano, mes, 1).date()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO atividades (usuario, data, mes, ano, descricao, projeto, porcentagem, observacao)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
        """, (usuario, data, mes, ano, descricao, projeto, int(porcentagem), observacao))
    conn.commit()
    conn.close()

def editar_atividade(atividade_id, descricao, projeto, porcentagem, observacao):
    conn = get_db_connection()
    if not conn:
        return
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE atividades SET descricao=%s, projeto=%s, porcentagem=%s, observacao=%s WHERE id=%s;
        """, (descricao, projeto, int(porcentagem), observacao, atividade_id))
    conn.commit()
    conn.close()

def apagar_atividade(atividade_id):
    conn = get_db_connection()
    if not conn:
        return
    with conn.cursor() as cur:
        cur.execute("DELETE FROM atividades WHERE id=%s;", (atividade_id,))
    conn.commit()
    conn.close()

def carregar_atividades_usuario(usuario, mes, ano):
    conn = get_db_connection()
    if not conn:
        return []
    df = pd.read_sql("SELECT * FROM atividades WHERE usuario=%s AND mes=%s AND ano=%s ORDER BY id DESC;",
                     conn, params=(usuario, mes, ano))
    conn.close()
    return df.to_dict('records')

# ==============================
# Streamlit UI
# ==============================
st.set_page_config(page_title="Controle de Atividades", layout="wide")

st.title("📋 Controle de Atividades")
usuario = st.text_input("Usuário")
if not usuario:
    st.stop()

mes = st.number_input("Mês", 1, 12, datetime.now().month)
ano = st.number_input("Ano", 2020, 2030, datetime.now().year)

st.markdown("## Lançar Atividade")

tipo_lancamento = st.radio("Modo de Lançamento", ["Porcentagem", "Horas"])
qtd = st.number_input("Quantos lançamentos deseja adicionar?", 1, 10, 1)

with st.form("form_lancamentos"):
    lancamentos = []
    for i in range(qtd):
        cols = st.columns([3, 3, 2, 4])
        with cols[0]:
            descricao = st.text_input(f"Descrição {i+1}", "")
        with cols[1]:
            projeto = st.text_input(f"Projeto {i+1}", "")
        with cols[2]:
            valor = st.number_input(f"{'%' if tipo_lancamento=='Porcentagem' else 'Horas'} {i+1}", 0.0, 200.0, 0.0)
        with cols[3]:
            observacao = st.text_area(f"Observação {i+1}", "")
        lancamentos.append({
            "descricao": descricao,
            "projeto": projeto,
            "valor": valor,
            "observacao": observacao
        })

    submit = st.form_submit_button("Salvar lançamentos")

if submit:
    for l in lancamentos:
        desc = l["descricao"]
        proj = l["projeto"]
        val = l["valor"]
        obs = l["observacao"]

        if tipo_lancamento == "Horas":
            obs = f"[HORA:{val}|{obs}]"
            inserir_atividade(usuario, mes, ano, desc, proj, 0, obs)
        else:
            inserir_atividade(usuario, mes, ano, desc, proj, val, obs)

    if tipo_lancamento == "Horas":
        normalizar_porcentagens_por_horas(usuario, mes, ano)
    else:
        normalizar_porcentagens_por_valores(usuario, mes, ano)

    st.success("Lançamentos salvos e normalizados com sucesso!")

st.markdown("## Atividades Lançadas")
ativs = carregar_atividades_usuario(usuario, mes, ano)
if not ativs:
    st.info("Nenhuma atividade encontrada.")
else:
    for a in ativs:
        st.write(f"**{a['descricao']}** — {a['projeto']} — {a['porcentagem']}% — Obs: {a['observacao']}")
