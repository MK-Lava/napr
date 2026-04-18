import json
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from db import get_conn, DB_PATH


# ---------- Page setup ----------
st.set_page_config(
    page_title="წინამძღვრიანთკარი — რეგისტრაციები",
    page_icon="🏡",
    layout="wide",
)

st.title("🏡 სოფელი წინამძღვრიანთკარი — რეგისტრაციები")
st.caption("მონაცემების წყარო: naprweb.reestri.gov.ge")


# ---------- Data loading ----------
@st.cache_data(ttl=60)  # cache for 60 seconds — refreshes after scraper runs
def load_data() -> pd.DataFrame:
    with get_conn() as conn:
        df = pd.read_sql_query(
            """
            SELECT app_id, reg_number, web_transact, status, status_id,
                   address, app_reg_date, last_act_date, applicants_json,
                   is_relevant, first_seen_at
            FROM registrations
            """,
            conn,
        )

    # Convert Unix timestamps to real datetimes
    df["submitted_at"] = pd.to_datetime(df["app_reg_date"], unit="s", errors="coerce")
    df["completed_at"] = pd.to_datetime(df["last_act_date"], unit="s", errors="coerce")

    # Parse the JSON applicants list into a readable string
    def _fmt_applicants(raw: str) -> str:
        try:
            items = json.loads(raw) if raw else []
            return " · ".join(items)
        except Exception:
            return ""
    df["applicants"] = df["applicants_json"].apply(_fmt_applicants)

    df["is_relevant"] = df["is_relevant"].astype(bool)
    return df


df = load_data()

if df.empty:
    st.warning("ბაზა ცარიელია. ჯერ გაუშვი scraper.py.")
    st.stop()


# ---------- Sidebar filters ----------
st.sidebar.header("ფილტრები")

show_all = st.sidebar.checkbox(
    "ყველა ტიპის რეგისტრაცია",
    value=False,
    help="გამორთული: მხოლოდ საკუთრების უფლების რეგისტრაციები",
)

# Date range — default to last 12 months, but allow going back to 2007
min_date = df["completed_at"].min().date() if df["completed_at"].notna().any() else date(2007, 1, 1)
max_date = df["completed_at"].max().date() if df["completed_at"].notna().any() else date.today()

default_start = max(min_date, max_date - timedelta(days=365))
date_range = st.sidebar.date_input(
    "პერიოდი (დასრულების თარიღი)",
    value=(default_start, max_date),
    min_value=min_date,
    max_value=max_date,
)

address_query = st.sidebar.text_input(
    "მისამართში ძებნა",
    placeholder="მაგ. მე-16 ქუჩა",
).strip()

person_query = st.sidebar.text_input(
    "პირის სახელი",
    placeholder="მაგ. გიორგი",
).strip()


# ---------- Apply filters ----------
filtered = df.copy()

if not show_all:
    filtered = filtered[filtered["is_relevant"]]

if isinstance(date_range, tuple) and len(date_range) == 2:
    start, end = date_range
    mask = (filtered["completed_at"].dt.date >= start) & (filtered["completed_at"].dt.date <= end)
    filtered = filtered[mask]

if address_query:
    filtered = filtered[filtered["address"].str.contains(address_query, case=False, na=False)]

if person_query:
    filtered = filtered[filtered["applicants"].str.contains(person_query, case=False, na=False)]


# ---------- Top metrics ----------
col1, col2, col3, col4 = st.columns(4)
col1.metric("ნაჩვენები ჩანაწერები", f"{len(filtered):,}")
col2.metric("სულ ბაზაში", f"{len(df):,}")
col3.metric(
    "საკუთრების რეგისტრაციები",
    f"{df['is_relevant'].sum():,}",
)
last_scrape = df["first_seen_at"].max() if not df["first_seen_at"].isna().all() else "—"
col4.metric("ბოლო განახლება", str(last_scrape).split(".")[0])


# ---------- Charts ----------
st.subheader("სტატისტიკა")

tab1, tab2, tab3 = st.tabs(["📅 თვეების მიხედვით", "📆 წლების მიხედვით", "📈 დაგროვილი"])

if filtered["completed_at"].notna().any():
    chart_df = filtered.dropna(subset=["completed_at"]).copy()

    with tab1:
        chart_df["month"] = chart_df["completed_at"].dt.to_period("M").dt.to_timestamp()
        monthly = chart_df.groupby("month").size().reset_index(name="count")
        fig = px.bar(
            monthly, x="month", y="count",
            labels={"month": "თვე", "count": "რაოდენობა"},
            title="რეგისტრაციები თვეების მიხედვით",
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        chart_df["year"] = chart_df["completed_at"].dt.year
        yearly = chart_df.groupby("year").size().reset_index(name="count")
        fig = px.bar(
            yearly, x="year", y="count",
            labels={"year": "წელი", "count": "რაოდენობა"},
            title="რეგისტრაციები წლების მიხედვით",
        )
        st.plotly_chart(fig, use_container_width=True)

    with tab3:
        cumulative = chart_df.sort_values("completed_at").copy()
        cumulative["cumulative"] = range(1, len(cumulative) + 1)
        fig = px.line(
            cumulative, x="completed_at", y="cumulative",
            labels={"completed_at": "თარიღი", "cumulative": "სულ"},
            title="დაგროვილი რეგისტრაციები",
        )
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("არჩეულ პერიოდში ჩანაწერები ვერ მოიძებნა.")


# ---------- Data table ----------
st.subheader("ჩანაწერები")

display = filtered[[
    "reg_number", "completed_at", "submitted_at",
    "web_transact", "status", "address", "applicants",
]].sort_values("completed_at", ascending=False).rename(columns={
    "reg_number":   "რეგ. ნომერი",
    "completed_at": "დასრულდა",
    "submitted_at": "შემოვიდა",
    "web_transact": "ტიპი",
    "status":       "სტატუსი",
    "address":      "მისამართი",
    "applicants":   "პირები",
})

st.dataframe(display, use_container_width=True, height=500, hide_index=True)

# CSV export
csv = display.to_csv(index=False).encode("utf-8-sig")  # utf-8-sig for Excel
st.download_button(
    "⬇️ CSV ფაილის ჩამოტვირთვა",
    data=csv,
    file_name=f"registrations_{date.today().isoformat()}.csv",
    mime="text/csv",
)

st.caption(f"ბაზის ფაილი: `{DB_PATH}`")