import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
from datetime import datetime

# ======================
# CONEXÃO COM DB
# ======================
def get_connection():
    conn = psycopg2.connect(
        host=st.secrets["postgresql"]["host"],
        port=st.secrets["postgresql"]["port"],
        dbname=st.secrets["postgresql"]["database"],
        user=st.secrets["postgresql"]["user"],
        password=st.secrets["postgresql"]["password"],
        sslmode=st.secrets["postgresql"]["sslmode"]
    )
    return conn

# ======================
# LOGIN
# ======================
def login(username, password):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, username, role FROM usuarios WHERE username=%s AND password=%s", (username, password))
    user = cur.fetchone()
    conn.close()
    return user

# ======================
# FUNÇÕES CRUD
# ======================
def get_validacoes(user_id, role):
    conn = get_connection()
    if role == "admin":
        query = "SELECT v.id, u.username, v.descricao, v.status, v.criado_em FROM validacoes v JOIN usuarios u ON v.user_id = u.id ORDER BY v.criado_em DESC"
        df = pd.read_sql(query, conn)
    else:
        query = "SELECT v.id, u.username, v.descricao, v.status, v.criado_em FROM validacoes v JOIN usuarios u ON v.user_id = u.id WHERE v.user_id=%s ORDER BY v.criado_em DESC"
        df = pd.read_sql(query, conn, params=(user_id,))
    conn.close()
    return df

def inserir_validacao(user_id, descricao, status):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO validacoes (user_id, descricao, status) VALUES (%s,%s,%s)", (user_id, descricao, status))
    conn.commit()
    conn.close()

def deletar_validacao(lancamento_id, user_id, role):
    conn = get_connection()
    cur = conn.cursor()
    if role == "admin":
        cur.execute("DELETE FROM validacoes WHERE id=%s", (lancamento_id,))
    else:
        cur.execute("DELETE FROM validacoes WHERE id=%s AND user_id=%s", (lancamento_id, user_id))
    conn.commit()
    conn.close()

def editar_validacao(lancamento_id, descricao, status, user_id, role):
    conn = get_connection()
    cur = conn.cursor()
    if role == "admin":
        cur.execute("UPDATE validacoes SET descricao=%s, status=%s WHERE id=%s", (descricao, status, lancamento_id))
    else:
        cur.execute("UPDATE validacoes SET descricao=%s, status=%s WHERE id=%s AND user_id=%s", (descricao, status, lancamento_id, user_id))
    conn.commit()
    conn.close()

# ======================
# DASHBOARD
# ======================
def gerar_dashboard(df, role):
    if df.empty:
        st.info("Nenhum lançamento para gerar gráfico.")
        return
    df["mes_ano"] = pd.to_datetime(df["criado_em"]).dt.to_period("M")
    df_grouped = df.groupby(["mes_ano","username","status"]).size().reset_index(name="count")
    df_pivot = df_grouped.pivot_table(index=["mes_ano","username"], columns="status", values="count", fill_value=0).reset_index()
    df_pivot["total"] = df_pivot.get("Concluído",0)+df_pivot.get("Pendente",0)
    df_pivot["% Concluído"] = (df_pivot.get("Concluído",0)/df_pivot["total"]*100).round(2)

    if role == "admin":
        usuario = st.selectbox("Selecionar usuário para gráfico", ["Todos"] + list(df["username"].unique()))
        if usuario != "Todos":
            df_pivot = df_pivot[df_pivot["username"]==usuario]

    fig = px.bar(df_pivot, x="mes_ano", y="% Concluído", color="username", text="% Concluído",
                 labels={"mes_ano":"Mês/Ano","% Concluído":"% Concluído"},
                 title="Desempenho Mensal de Conclusão")
    st.plotly_chart(fig, use_container_width=True)

# ======================
# STREAMLIT APP
# ======================
st.title("✅ Sistema de Controle de Atividades")

# Sessão de login
if "user" not in st.session_state:
    st.subheader("Login")
    username = st.text_input("Usuário")
    password = st.text_input("Senha", type="password")
    if st.button("Entrar"):
        user = login(username, password)
        if user:
            st.session_state["user"] = {"id": user[0],"username": user[1],"role": user[2]}
            st.success(f"Bem-vindo, {user[1]} 👋")
            st.rerun()
        else:
            st.error("Usuário ou senha inválidos.")
else:
    user = st.session_state["user"]
    st.sidebar.success(f"Logado como: {user['username']} ({user['role']})")
    if st.sidebar.button("Sair"):
        del st.session_state["user"]
        st.rerun()

    abas = ["Novo Lançamento", "Lançamentos", "Dashboard"]
    if user["role"]=="admin":
        abas.append("Todos Lançamentos")
    aba = st.sidebar.radio("Menu", abas)

    df = get_validacoes(user["id"], user["role"])

    # -------------------
    # NOVO LANÇAMENTO
    # -------------------
    if aba=="Novo Lançamento":
        st.subheader("➕ Novo Lançamento")
        with st.form("novo_lancamento"):
            descricao = st.text_area("Descrição")
            status = st.selectbox("Status", ["Pendente","Concluído"])
            submitted = st.form_submit_button("Salvar")
            if submitted and descricao.strip()!="":
                inserir_validacao(user["id"], descricao, status)
                st.success("Lançamento adicionado!")
                st.rerun()

    # -------------------
    # LANÇAMENTOS DO USUÁRIO
    # -------------------
    elif aba=="Lançamentos":
        st.subheader("📊 Meus Lançamentos")
        if df.empty:
            st.info("Nenhum lançamento encontrado.")
        else:
            st.dataframe(df, use_container_width=True)
            st.subheader("🗑️ Excluir Lançamento")
            lancamento_id = st.selectbox("Selecione o ID para excluir", df["id"])
            if st.button("Excluir"):
                deletar_validacao(lancamento_id, user["id"], user["role"])
                st.success(f"Lançamento {lancamento_id} excluído!")
                st.rerun()
            st.subheader("✏️ Editar Lançamento")
            edit_id = st.selectbox("Selecione o ID para editar", df["id"], key="edit_id")
            edit_row = df[df["id"]==edit_id].iloc[0]
            edit_descricao = st.text_area("Descrição", edit_row["descricao"], key="edit_desc")
            edit_status = st.selectbox("Status", ["Pendente","Concluído"], index=0 if edit_row["status"]=="Pendente" else 1, key="edit_status")
            if st.button("Salvar Alterações"):
                editar_validacao(edit_id, edit_descricao, edit_status, user["id"], user["role"])
                st.success(f"Lançamento {edit_id} atualizado!")
                st.rerun()

    # -------------------
    # TODOS LANÇAMENTOS (ADMIN)
    # -------------------
    elif aba=="Todos Lançamentos" and user["role"]=="admin":
        st.subheader("📊 Todos Lançamentos")
        if df.empty:
            st.info("Nenhum lançamento encontrado.")
        else:
            st.dataframe(df, use_container_width=True)

    # -------------------
    # DASHBOARD
    # -------------------
    elif aba=="Dashboard":
        st.subheader("📈 Dashboard")
        gerar_dashboard(df, user["role"])





