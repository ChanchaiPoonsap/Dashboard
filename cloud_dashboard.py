"""
Cloud Service Business Performance Dashboard
Streamlit + Google Sheets API (OAuth2 — ไม่ต้องใช้ Service Account JSON Key)
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from datetime import datetime
import hashlib

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Cloud Biz Dashboard",
    page_icon="☁️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# THEME / CSS
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');
:root {
    --bg-primary:#0a0f1e; --bg-card:#111827; --bg-card2:#1a2236;
    --accent:#00d4ff; --accent2:#7c3aed; --success:#10b981;
    --warning:#f59e0b; --danger:#ef4444;
    --text-primary:#f0f4ff; --text-muted:#8892a4;
    --border:rgba(0,212,255,0.12); --glow:0 0 20px rgba(0,212,255,0.15);
}
html,body,[data-testid="stAppViewContainer"]{background-color:var(--bg-primary)!important;font-family:'IBM Plex Sans',sans-serif;color:var(--text-primary);}
[data-testid="stSidebar"]{background:linear-gradient(180deg,#0d1526 0%,#111827 100%)!important;border-right:1px solid var(--border);}
.kpi-card{background:var(--bg-card);border:1px solid var(--border);border-radius:14px;padding:18px 20px;box-shadow:var(--glow);position:relative;overflow:hidden;transition:transform .2s;}
.kpi-card:hover{transform:translateY(-2px);}
.kpi-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,var(--accent),var(--accent2));}
.kpi-label{font-size:11px;color:var(--text-muted);letter-spacing:.08em;text-transform:uppercase;margin-bottom:6px;}
.kpi-value{font-size:24px;font-weight:600;color:var(--text-primary);font-family:'IBM Plex Mono',monospace;}
.kpi-delta{font-size:12px;margin-top:4px;}
.kpi-delta.pos{color:var(--success);} .kpi-delta.neg{color:var(--danger);}
.section-header{font-size:13px;font-weight:600;letter-spacing:.1em;text-transform:uppercase;color:var(--accent);margin:24px 0 12px;padding-bottom:6px;border-bottom:1px solid var(--border);}
.alert-warning{background:rgba(245,158,11,.1);border:1px solid rgba(245,158,11,.4);border-radius:10px;padding:10px 16px;font-size:13px;color:var(--warning);margin-bottom:8px;}
.alert-success{background:rgba(16,185,129,.1);border:1px solid rgba(16,185,129,.4);border-radius:10px;padding:10px 16px;font-size:13px;color:var(--success);margin-bottom:8px;}
.login-wrap{max-width:440px;margin:60px auto;background:var(--bg-card);border:1px solid var(--border);border-radius:20px;padding:40px;box-shadow:0 20px 60px rgba(0,0,0,.5),var(--glow);}
.login-logo{text-align:center;font-size:48px;margin-bottom:8px;}
.login-title{text-align:center;font-size:22px;font-weight:600;margin-bottom:4px;}
.login-sub{text-align:center;font-size:13px;color:var(--text-muted);margin-bottom:28px;}
.stButton>button{background:linear-gradient(135deg,var(--accent),var(--accent2));color:#fff;border:none;border-radius:8px;font-weight:600;font-family:'IBM Plex Sans',sans-serif;transition:opacity .2s,transform .15s;}
.stButton>button:hover{opacity:.88;transform:translateY(-1px);}
[data-baseweb="tab-highlight"]{background:var(--accent)!important;}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
SHEET_NAMES = {
    "actual_this": "Actual ปีนี้",
    "actual_last": "Actual ปีที่แล้ว",
    "budget":      "Budget ปีนี้",
    "mapping":     "Mapping & Adjustments",
}
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]
PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="IBM Plex Sans", color="#8892a4", size=12),
    margin=dict(l=10,r=10,t=36,b=10),
    legend=dict(bgcolor="rgba(0,0,0,0)",bordercolor="rgba(255,255,255,.08)",borderwidth=1),
    xaxis=dict(gridcolor="rgba(255,255,255,.05)",linecolor="rgba(255,255,255,.08)"),
    yaxis=dict(gridcolor="rgba(255,255,255,.05)",linecolor="rgba(255,255,255,.08)"),
)

# ─────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────
for k, v in {
    "app_authenticated": False, "app_username": "", "app_role": "",
    "google_creds": None, "user_info": None, "gc": None,
    "df_actual": None, "df_actual_last": None,
    "df_budget": None, "df_mapping": None,
    "spreadsheet_id": "", "last_refresh": None,
    "_oauth_config": None,
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ─────────────────────────────────────────────
# USERS
# ─────────────────────────────────────────────
USERS = {
    "admin":  {"password": hashlib.sha256(b"1234").hexdigest(), "role": "admin"},
    "viewer": {"password": hashlib.sha256(b"viewer123").hexdigest(), "role": "viewer"},
}
def verify_login(u, p):
    user = USERS.get(u)
    return user and user["password"] == hashlib.sha256(p.encode()).hexdigest()

# ─────────────────────────────────────────────
# OAUTH2 HELPERS
# ─────────────────────────────────────────────
def build_flow(client_config: dict, redirect_uri: str) -> Flow:
    return Flow.from_client_config(
        client_config, scopes=SCOPES, redirect_uri=redirect_uri
    )

def get_user_info(creds: Credentials) -> dict:
    svc = build("oauth2", "v2", credentials=creds)
    return svc.userinfo().get().execute()

# ─────────────────────────────────────────────
# GOOGLE SHEETS HELPERS
# ─────────────────────────────────────────────
def load_sheet(gc, sid: str, name: str) -> pd.DataFrame:
    return pd.DataFrame(gc.open_by_key(sid).worksheet(name).get_all_records())

def save_sheet(gc, sid: str, name: str, df: pd.DataFrame):
    sh = gc.open_by_key(sid)
    ws = sh.worksheet(name)
    # Auto-backup
    try:
        bk = sh.add_worksheet(title=f"BACKUP_{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                               rows=ws.row_count, cols=ws.col_count)
        bk.update([ws.row_values(i+1) for i in range(ws.row_count) if ws.row_values(i+1)])
    except Exception:
        pass
    ws.clear()
    ws.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist())

def refresh_data(gc, sid: str):
    with st.spinner("🔄 กำลังดึงข้อมูล…"):
        try:
            st.session_state.df_actual      = load_sheet(gc, sid, SHEET_NAMES["actual_this"])
            st.session_state.df_actual_last = load_sheet(gc, sid, SHEET_NAMES["actual_last"])
            st.session_state.df_budget      = load_sheet(gc, sid, SHEET_NAMES["budget"])
            st.session_state.df_mapping     = load_sheet(gc, sid, SHEET_NAMES["mapping"])
            st.session_state.last_refresh   = datetime.now()
            st.success("✅ โหลดข้อมูลสำเร็จ")
        except Exception as e:
            st.error(f"❌ โหลดไม่สำเร็จ: {e}")

# ─────────────────────────────────────────────
# DEMO DATA
# ─────────────────────────────────────────────
@st.cache_data
def generate_demo_data():
    rng = np.random.default_rng(42)
    base = [12.5,13.1,12.8,14.2,15.0,14.7,16.1,15.8,17.2,16.5,18.0,19.3]
    accs = [
        ("R001","Revenue – IaaS","Revenue"),("R002","Revenue – SaaS","Revenue"),
        ("R003","Revenue – Managed","Revenue"),("C001","Server Cost","Direct Cost"),
        ("C002","License Cost","Direct Cost"),("O001","Staff Cost","OpEx"),
        ("O002","Marketing","OpEx"),("O003","G&A","OpEx"),
        ("X001","Other Revenue","Other Revenue"),("X002","Other Expense","Other Expense"),
    ]
    ra, ral, rb = [], [], []
    for code, name, grp in accs:
        for mi, m in enumerate(MONTHS):
            s = rng.uniform(0.8, 1.2)
            amt = base[mi]*s if grp=="Revenue" else base[mi]*s*0.46 if grp=="Direct Cost" else base[mi]*s*0.12
            ra.append({"Account Code":code,"Account Name":name,"Group":grp,"Month":m,"Amount":round(amt,3)})
            ral.append({"Account Code":code,"Account Name":name,"Group":grp,"Month":m,"Amount":round(amt*rng.uniform(.88,1.05),3)})
            rb.append({"Account Code":code,"Account Name":name,"Group":grp,"Month":m,"Amount":round(amt*1.05,3)})
    mp = [{"Account Code":c,"Account Name":n,"KPI Group":g,"Active":"Yes"} for c,n,g in accs]
    return pd.DataFrame(ra), pd.DataFrame(ral), pd.DataFrame(rb), pd.DataFrame(mp)

# ─────────────────────────────────────────────
# KPI & CHART HELPERS
# ─────────────────────────────────────────────
def gv(df, grp, months=None):
    s = df[df["Group"]==grp]
    if months: s = s[s["Month"].isin(months)]
    return s["Amount"].sum()

def calc_kpis(df_a, df_b, df_al, months):
    ra=gv(df_a,"Revenue",months); rb=gv(df_b,"Revenue",months)
    ca=gv(df_a,"Direct Cost",months); cb=gv(df_b,"Direct Cost",months)
    oa=gv(df_a,"OpEx",months); ob=gv(df_b,"OpEx",months)
    xr=gv(df_a,"Other Revenue",months); xe=gv(df_a,"Other Expense",months)
    ga=ra-ca; gb=rb-cb; na=ga+xr-xe-oa; nb=gb-ob
    def pct(a,b): return round((a-b)/b*100,1) if b else 0
    return {
        "Total Revenue":  (ra,  pct(ra,rb)),
        "Direct Cost":    (ca,  pct(ca,cb)),
        "GOP":            (ga,  pct(ga,gb)),
        "%GOP":           (round(ga/ra*100,1) if ra else 0, 0),
        "Other Revenue":  (xr,  0),
        "Other Expense":  (xe,  0),
        "OpEx":           (oa,  pct(oa,ob)),
        "Net Profit":     (na,  pct(na,nb)),
        "%Net Profit":    (round(na/ra*100,1) if ra else 0, 0),
    }

def chart_revenue(df_a, df_b, df_al):
    ra=[gv(df_a,"Revenue",[m]) for m in MONTHS]
    rb=[gv(df_b,"Revenue",[m]) for m in MONTHS]
    rl=[gv(df_al,"Revenue",[m]) for m in MONTHS]
    fig=go.Figure()
    fig.add_bar(x=MONTHS,y=rl,name="Last Year",marker_color="rgba(124,58,237,.5)")
    fig.add_bar(x=MONTHS,y=rb,name="Budget",marker_color="rgba(245,158,11,.55)")
    fig.add_bar(x=MONTHS,y=ra,name="Actual",marker_color="#00d4ff")
    fig.update_layout(barmode="group",title="Monthly Revenue vs Budget vs Last Year",title_font_color="#f0f4ff",**PLOTLY_LAYOUT)
    return fig

def chart_gop(df_a, df_b):
    ga,gb=[],[]
    for m in MONTHS:
        ra=gv(df_a,"Revenue",[m]); ca=gv(df_a,"Direct Cost",[m])
        rb=gv(df_b,"Revenue",[m]); cb=gv(df_b,"Direct Cost",[m])
        ga.append(round((ra-ca)/ra*100,1) if ra else 0)
        gb.append(round((rb-cb)/rb*100,1) if rb else 0)
    fig=go.Figure()
    fig.add_scatter(x=MONTHS,y=gb,name="Budget %GOP",line=dict(color="#f59e0b",dash="dash",width=2))
    fig.add_scatter(x=MONTHS,y=ga,name="Actual %GOP",line=dict(color="#00d4ff",width=2.5),fill="tozeroy",fillcolor="rgba(0,212,255,.07)")
    fig.update_layout(title="%GOP Trend",title_font_color="#f0f4ff",yaxis_ticksuffix="%",**PLOTLY_LAYOUT)
    return fig

def chart_donut(df_a, months):
    gs=["Direct Cost","OpEx","Other Expense"]
    fig=go.Figure(go.Pie(labels=gs,values=[gv(df_a,g,months) for g in gs],hole=.6,
                         marker_colors=["#00d4ff","#7c3aed","#ef4444"],textfont_size=12))
    fig.update_layout(title="Expense Structure",title_font_color="#f0f4ff",**PLOTLY_LAYOUT)
    return fig

def chart_np(df_a, df_b):
    na,nb=[],[]
    for m in MONTHS:
        ra=gv(df_a,"Revenue",[m]); ca=gv(df_a,"Direct Cost",[m]); oa=gv(df_a,"OpEx",[m])
        xr=gv(df_a,"Other Revenue",[m]); xe=gv(df_a,"Other Expense",[m])
        rb=gv(df_b,"Revenue",[m]); cb=gv(df_b,"Direct Cost",[m]); ob=gv(df_b,"OpEx",[m])
        na.append(ra-ca+xr-xe-oa); nb.append(rb-cb-ob)
    fig=go.Figure()
    fig.add_bar(x=MONTHS,y=nb,name="Budget NP",marker_color="rgba(245,158,11,.45)")
    fig.add_bar(x=MONTHS,y=na,name="Actual NP",marker_color=["#10b981" if v>=0 else "#ef4444" for v in na])
    fig.update_layout(barmode="group",title="Net Profit Analysis",title_font_color="#f0f4ff",**PLOTLY_LAYOUT)
    return fig

# ─────────────────────────────────────────────
# PAGE: APP LOGIN (ชั้น 1)
# ─────────────────────────────────────────────
def page_app_login():
    st.markdown("""
    <div class='login-wrap'>
      <div class='login-logo'>☁️</div>
      <div class='login-title'>Cloud Biz Dashboard</div>
      <div class='login-sub'>Business Performance Intelligence</div>
    </div>""", unsafe_allow_html=True)
    _, col, _ = st.columns([1,1.5,1])
    with col:
        u = st.text_input("Username")
        p = st.text_input("Password", type="password")
        if st.button("🔐 เข้าสู่ระบบ", use_container_width=True):
            if verify_login(u, p):
                st.session_state.app_authenticated = True
                st.session_state.app_username = u
                st.session_state.app_role = USERS[u]["role"]
                st.rerun()
            else:
                st.error("Username หรือ Password ไม่ถูกต้อง")
        st.caption("Demo: admin / admin1234  หรือ  viewer / viewer123")

# ─────────────────────────────────────────────
# PAGE: GOOGLE OAUTH (ชั้น 2)
# ─────────────────────────────────────────────
def page_google_connect():
    st.markdown("""
    <div class='login-wrap'>
      <div class='login-logo'>🔗</div>
      <div class='login-title'>เชื่อมต่อ Google Sheets</div>
      <div class='login-sub'>ใช้ OAuth2 — ไม่ต้องมี Service Account Key</div>
    </div>""", unsafe_allow_html=True)

    _, col, _ = st.columns([1,1.8,1])
    with col:
        # ── Step 1: รับ Client ID / Secret ──
        st.markdown("#### ① ใส่ OAuth2 Credentials")
        st.caption("ดูวิธีสร้างได้ด้านล่าง 👇")

        cid  = st.text_input("Client ID", placeholder="xxx.apps.googleusercontent.com",
                              value=st.session_state.get("_cid",""))
        csec = st.text_input("Client Secret", placeholder="GOCSPX-xxx", type="password",
                              value=st.session_state.get("_csec",""))
        ruri = st.text_input("Redirect URI", value="http://localhost:8501",
                              help="ต้องตรงกับที่ตั้งใน Google Cloud Console")

        if st.button("🔑 สร้าง Login URL", use_container_width=True):
            if cid and csec:
                cfg = {"web":{"client_id":cid,"client_secret":csec,"redirect_uris":[ruri],
                              "auth_uri":"https://accounts.google.com/o/oauth2/auth",
                              "token_uri":"https://oauth2.googleapis.com/token"}}
                flow = build_flow(cfg, ruri)
                auth_url, _ = flow.authorization_url(prompt="consent", access_type="offline")
                st.session_state._oauth_config = cfg
                st.session_state._redirect_uri = ruri
                st.session_state._cid = cid
                st.session_state._csec = csec
                st.session_state._auth_url = auth_url
                st.rerun()
            else:
                st.warning("กรุณาใส่ Client ID และ Client Secret")

        # ── Step 2: แสดง Link ──
        if st.session_state.get("_auth_url"):
            st.divider()
            st.markdown("#### ② คลิกลิงก์เพื่อ Login Google")
            auth_url = st.session_state._auth_url
            st.markdown(f"""
            <a href="{auth_url}" target="_blank">
              <button style="width:100%;padding:12px;background:#fff;color:#333;border:none;border-radius:8px;font-size:15px;font-weight:600;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:8px;">
                🔐 &nbsp; Login ด้วย Google Account
              </button>
            </a>""", unsafe_allow_html=True)
            st.info("หลังจาก Login แล้ว Google จะ redirect → คัดลอก URL ทั้งหมดจาก address bar แล้ววางด้านล่าง")

            st.divider()
            st.markdown("#### ③ วาง Redirect URL ที่ได้")
            callback = st.text_input("Redirect URL", placeholder="http://localhost:8501/?code=4/0AX...")
            if st.button("✅ ยืนยันและเชื่อมต่อ", use_container_width=True) and callback:
                try:
                    cfg = st.session_state._oauth_config
                    ruri2 = st.session_state._redirect_uri
                    flow2 = build_flow(cfg, ruri2)
                    flow2.fetch_token(authorization_response=callback)
                    creds = flow2.credentials
                    st.session_state.google_creds = creds
                    st.session_state.gc = gspread.authorize(creds)
                    info = get_user_info(creds)
                    st.session_state.user_info = info
                    st.success(f"✅ เชื่อมต่อสำเร็จ! ({info.get('email','')})")
                    st.rerun()
                except Exception as e:
                    st.error(f"เกิดข้อผิดพลาด: {e}")
                    st.caption("ลอง copy URL ใหม่อีกครั้ง หรือกด 'สร้าง Login URL' ใหม่")

        st.divider()
        st.markdown("#### หรือ")
        if st.button("🎭 ใช้ Demo Data (ไม่ต้อง Login Google)", use_container_width=True):
            a, al, b, mp = generate_demo_data()
            st.session_state.df_actual = a; st.session_state.df_actual_last = al
            st.session_state.df_budget = b; st.session_state.df_mapping = mp
            st.session_state.last_refresh = datetime.now()
            st.rerun()

        # ── คำแนะนำสร้าง OAuth Client ──
        with st.expander("📖 วิธีสร้าง OAuth2 Client ID (คลิกดู)"):
            st.markdown("""
**ขั้นตอน:**

1. เข้า [console.cloud.google.com](https://console.cloud.google.com) → **New Project**
2. **APIs & Services → Library** → Enable:
   - ✅ Google Sheets API
   - ✅ Google Drive API
3. **APIs & Services → OAuth consent screen**
   - User Type: **External** → Create
   - App name, email → Save
   - Scopes: เพิ่ม `.../auth/spreadsheets` และ `.../auth/drive.readonly`
   - Test users: เพิ่ม Gmail ตัวเอง
4. **APIs & Services → Credentials → + Create Credentials → OAuth Client ID**
   - Application type: **Web application**
   - Authorized redirect URIs: `http://localhost:8501`
   - → **Create**
5. คัดลอก **Client ID** และ **Client Secret** มาใส่ด้านบน ✅
            """)

        st.divider()
        if st.button("🚪 ออกจากระบบ"):
            for k in list(st.session_state.keys()): del st.session_state[k]
            st.rerun()

# ─────────────────────────────────────────────
# PAGE: EXECUTIVE SUMMARY
# ─────────────────────────────────────────────
def page_executive(df_a, df_al, df_b, sel_months):
    st.markdown("<div class='section-header'>📊 KPI Overview</div>", unsafe_allow_html=True)
    kpis  = calc_kpis(df_a, df_b, df_al, sel_months)
    icons = ["💰","🖥️","📈","📊","➕","➖","🏢","🏆","📉"]
    cols  = st.columns(9)
    for i, (label, (val, vs_bud)) in enumerate(kpis.items()):
        disp  = f"{val:,.1f}M" if "%" not in label else f"{val:.1f}%"
        d_txt = f"{'▲' if vs_bud>=0 else '▼'} {abs(vs_bud):.1f}% vs Bud" if vs_bud else ""
        cls   = "pos" if vs_bud >= 0 else "neg"
        with cols[i]:
            st.markdown(f"""<div class='kpi-card'>
              <div class='kpi-label'>{icons[i]} {label}</div>
              <div class='kpi-value'>{disp}</div>
              <div class='kpi-delta {cls}'>{d_txt}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<div class='section-header'>📈 Charts</div>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1: st.plotly_chart(chart_revenue(df_a, df_b, df_al), use_container_width=True)
    with c2: st.plotly_chart(chart_gop(df_a, df_b), use_container_width=True)
    c3, c4 = st.columns(2)
    with c3: st.plotly_chart(chart_donut(df_a, sel_months), use_container_width=True)
    with c4: st.plotly_chart(chart_np(df_a, df_b), use_container_width=True)

# ─────────────────────────────────────────────
# PAGE: DEEP DIVE
# ─────────────────────────────────────────────
def page_deep_dive(df_a, df_al, df_b, sel_month):
    st.markdown("<div class='section-header'>🔍 Deep Dive</div>", unsafe_allow_html=True)
    mode = st.radio("โหมด", ["Monthly Detail","Full Year Trend"], horizontal=True)
    if mode == "Monthly Detail":
        sel_grp = st.selectbox("กลุ่ม", ["ทั้งหมด"] + df_a["Group"].unique().tolist())
        f = lambda df: df[df["Month"]==sel_month] if sel_grp=="ทั้งหมด" else df[(df["Month"]==sel_month)&(df["Group"]==sel_grp)]
        m = (f(df_a).rename(columns={"Amount":"Actual"})
             .merge(f(df_al).rename(columns={"Amount":"Last Year"})[["Account Code","Last Year"]], on="Account Code", how="left")
             .merge(f(df_b).rename(columns={"Amount":"Budget"})[["Account Code","Budget"]], on="Account Code", how="left"))
        m["vs Bud%"] = ((m["Actual"]-m["Budget"])/m["Budget"]*100).round(1)
        m["vs LY%"]  = ((m["Actual"]-m["Last Year"])/m["Last Year"]*100).round(1)
        st.dataframe(m[["Account Code","Account Name","Group","Actual","Budget","Last Year","vs Bud%","vs LY%"]], use_container_width=True, height=420)
    else:
        pivot = (df_a.groupby(["Group","Month"])["Amount"].sum().reset_index()
                 .pivot(index="Group", columns="Month", values="Amount")
                 .reindex(columns=MONTHS).fillna(0))
        pivot["YTD"] = pivot.sum(axis=1)
        st.dataframe(pivot.style.format("{:,.1f}").background_gradient(cmap="Blues", axis=1), use_container_width=True, height=420)

# ─────────────────────────────────────────────
# PAGE: MAPPING & ADJUSTMENTS
# ─────────────────────────────────────────────
def page_mapping(df_mapping, df_a, df_b, gc, sid):
    st.markdown("<div class='section-header'>🗂️ Mapping & Adjustments</div>", unsafe_allow_html=True)
    ca = set(df_a["Account Code"].unique()); cb = set(df_b["Account Code"].unique())
    for c in ca-cb: st.markdown(f"<div class='alert-warning'>⚠️ [{c}] มีใน Actual แต่ไม่มีใน Budget</div>", unsafe_allow_html=True)
    if not (ca-cb): st.markdown("<div class='alert-success'>✅ Account Codes ตรงกันทั้งหมด</div>", unsafe_allow_html=True)
    edited = st.data_editor(df_mapping, use_container_width=True, num_rows="dynamic")
    if st.button("💾 Save Mapping") and gc:
        save_sheet(gc, sid, SHEET_NAMES["mapping"], edited)
        st.session_state.df_mapping = edited; st.success("บันทึกสำเร็จ")
    st.divider()
    st.markdown("<div class='section-header'>🔧 Manual Adjustment</div>", unsafe_allow_html=True)
    with st.form("adj"):
        c1,c2,c3,c4 = st.columns(4)
        code=c1.text_input("Account Code"); mon=c2.selectbox("Month",MONTHS)
        amt=c3.number_input("Amount (+/-)", step=0.001); note=c4.text_input("Note")
        if st.form_submit_button("➕ เพิ่ม") and code:
            row = pd.DataFrame([{"Account Code":code,"Month":mon,"Adjustment":amt,"Note":note,
                                  "By":st.session_state.app_username,"Timestamp":datetime.now().strftime("%Y-%m-%d %H:%M:%S")}])
            st.session_state.df_mapping = pd.concat([st.session_state.df_mapping, row], ignore_index=True)
            if gc: save_sheet(gc, sid, SHEET_NAMES["mapping"], st.session_state.df_mapping)
            st.success(f"เพิ่ม [{code}] {mon} = {amt:+.3f}M")

# ─────────────────────────────────────────────
# PAGE: DATA MANAGEMENT
# ─────────────────────────────────────────────
def page_data_mgmt(gc, sid):
    st.markdown("<div class='section-header'>📂 Data Management</div>", unsafe_allow_html=True)
    state_map = {v: k.replace("actual_this","df_actual").replace("actual_last","df_actual_last").replace("budget","df_budget").replace("mapping","df_mapping")
                 for k,v in SHEET_NAMES.items()}
    state_map = {"Actual ปีนี้":"df_actual","Actual ปีที่แล้ว":"df_actual_last","Budget ปีนี้":"df_budget","Mapping & Adjustments":"df_mapping"}
    t1,t2 = st.tabs(["📋 ดู/แก้ไขข้อมูล","📤 อัปโหลดไฟล์"])
    with t1:
        sc = st.selectbox("ชุดข้อมูล", list(state_map.keys()))
        sk = state_map[sc]; df_src = st.session_state.get(sk)
        if df_src is not None:
            edited = st.data_editor(df_src, use_container_width=True, num_rows="dynamic", key=f"ed_{sk}")
            if st.button("💾 Save") and gc:
                save_sheet(gc, sid, sc, edited); st.session_state[sk]=edited; st.success("บันทึกสำเร็จ + Auto-Backup")
        else:
            st.info("ยังไม่มีข้อมูล")
    with t2:
        su = st.selectbox("บันทึกลง Sheet", list(state_map.keys()), key="up_target")
        md = st.radio("โหมด", ["Overwrite","Append"], horizontal=True)
        up = st.file_uploader("อัปโหลด Excel / CSV", type=["xlsx","xls","csv"])
        if up:
            df_new = pd.read_excel(up) if up.name.endswith(("xlsx","xls")) else pd.read_csv(up)
            st.dataframe(df_new.head(8), use_container_width=True)
            sk2 = state_map[su]
            if st.button("📤 Sync ไปยัง Sheets"):
                if md=="Append" and st.session_state.get(sk2) is not None:
                    df_new = pd.concat([st.session_state[sk2], df_new], ignore_index=True)
                st.session_state[sk2] = df_new
                if gc: save_sheet(gc, sid, su, df_new)
                st.success("Sync สำเร็จ!")

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
def sidebar():
    gc  = st.session_state.gc
    sid = st.session_state.spreadsheet_id
    with st.sidebar:
        st.markdown("### ☁️ Cloud Biz Dashboard")
        info = st.session_state.user_info
        label = info.get("email","") if info else st.session_state.app_username
        st.caption(f"👤 {label}  |  {st.session_state.app_role.upper()}")
        st.markdown("🟢 Google เชื่อมต่อแล้ว" if gc else "🟡 Demo Data")

        # Spreadsheet ID
        if gc:
            st.divider()
            new_sid = st.text_input("🆔 Spreadsheet ID", value=sid, placeholder="1BxiM…")
            if new_sid != sid and st.button("โหลดข้อมูล"):
                st.session_state.spreadsheet_id = new_sid
                refresh_data(gc, new_sid); st.rerun()

        # Filters
        st.divider()
        st.markdown("#### 🗓️ Filter")
        period = st.radio("ช่วงเวลา", ["MTD","YTD","Custom"], horizontal=True)
        cur_m  = datetime.now().month
        if period == "Custom":
            sel_months = st.multiselect("เดือน", MONTHS, default=MONTHS[:3])
        elif period == "MTD":
            sel_months = [MONTHS[cur_m-1]]
        else:
            sel_months = MONTHS[:cur_m]
        sel_month = st.selectbox("Deep Dive เดือน", MONTHS, index=cur_m-1)

        # Actions
        st.divider()
        if gc and st.button("🔄 Refresh Data"):
            refresh_data(gc, st.session_state.spreadsheet_id); st.rerun()
        if st.session_state.last_refresh:
            st.caption(f"Last refresh: {st.session_state.last_refresh.strftime('%H:%M:%S')}")
        st.divider()
        if st.button("🚪 Logout"):
            for k in list(st.session_state.keys()): del st.session_state[k]
            st.rerun()

    return sel_months, sel_month

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    # ชั้น 1: Dashboard Login
    if not st.session_state.app_authenticated:
        page_app_login(); return

    # ชั้น 2: Google OAuth หรือ Demo
    has_data = st.session_state.df_actual is not None
    if not has_data:
        page_google_connect(); return

    sel_months, sel_month = sidebar()
    df_a  = st.session_state.df_actual.copy()
    df_al = st.session_state.df_actual_last
    df_b  = st.session_state.df_budget

    # Apply Adjustments
    mp = st.session_state.df_mapping
    if mp is not None and "Adjustment" in mp.columns:
        for _, row in mp[mp["Adjustment"].notna()].iterrows():
            try:
                mask = (df_a["Account Code"]==row["Account Code"]) & (df_a["Month"]==row["Month"])
                df_a.loc[mask, "Amount"] += float(row["Adjustment"])
            except Exception:
                pass

    pages = ["📊 Executive Summary","🔍 Deep Dive","🗂️ Mapping & Adjustments","📂 Data Management"]
    if st.session_state.app_role == "viewer": pages = pages[:2]
    page = st.sidebar.radio("Navigation", pages)

    if page == "📊 Executive Summary":
        page_executive(df_a, df_al, df_b, sel_months)
    elif page == "🔍 Deep Dive":
        page_deep_dive(df_a, df_al, df_b, sel_month)
    elif page == "🗂️ Mapping & Adjustments":
        page_mapping(mp, df_a, df_b, st.session_state.gc, st.session_state.spreadsheet_id)
    elif page == "📂 Data Management":
        page_data_mgmt(st.session_state.gc, st.session_state.spreadsheet_id)

if __name__ == "__main__":
    main()