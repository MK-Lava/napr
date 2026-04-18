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


# Georgian month names, 1-indexed (GEORGIAN_MONTHS[1] = "იანვარი")
GEORGIAN_MONTHS = [
    "", "იანვარი", "თებერვალი", "მარტი", "აპრილი", "მაისი", "ივნისი",
    "ივლისი", "აგვისტო", "სექტემბერი", "ოქტომბერი", "ნოემბერი", "დეკემბერი",
]
DEFAULT_VILLAGE = "წინამძღვრიანთკარი"
NAPR_VIEW_URL = "https://naprweb.reestri.gov.ge/_dea/#/view/{app_id}"
MONTHLY_CHART_KEY = "monthly_chart"

# Single source of truth for village → color. Used in every chart and the table.
# A village not listed here gets a color from FALLBACK_PALETTE, assigned in
# Georgian-alphabetical order so the mapping stays stable across reruns.
KNOWN_VILLAGE_COLORS = {
    "წინამძღვრიანთკარი": "#1f77b4",   # blue
    "საგურამო":          "#ff7f0e",   # orange
}
FALLBACK_PALETTE = [
    "#2ca02c", "#d62728", "#9467bd", "#8c564b",
    "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
]


def build_color_map(villages: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    fb_idx = 0
    for v in sorted(villages):
        if v in KNOWN_VILLAGE_COLORS:
            result[v] = KNOWN_VILLAGE_COLORS[v]
        else:
            result[v] = FALLBACK_PALETTE[fb_idx % len(FALLBACK_PALETTE)]
            fb_idx += 1
    return result


# ---------- Data loading ----------
@st.cache_data(ttl=60)  # cache for 60 seconds — refreshes after scraper runs
def load_data() -> pd.DataFrame:
    with get_conn() as conn:
        df = pd.read_sql_query(
            """
            SELECT app_id, reg_number, web_transact, status, status_id,
                   address, app_reg_date, last_act_date, applicants_json,
                   is_relevant, village, first_seen_at
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


all_villages = sorted(df["village"].dropna().unique().tolist())  # Georgian alphabetical
color_map = build_color_map(all_villages)


# ---------- Sidebar filters ----------
st.sidebar.header("ფილტრები")

default_villages = [DEFAULT_VILLAGE] if DEFAULT_VILLAGE in all_villages else all_villages[:1]
selected_villages = st.sidebar.multiselect(
    "სოფლები",
    options=all_villages,
    default=default_villages,
    help="აირჩიე ერთი ან რამდენიმე სოფელი შესადარებლად",
)

if not selected_villages:
    st.warning("აირჩიე მინიმუმ ერთი სოფელი გვერდით მენიუში.")
    st.stop()

is_compare = len(selected_villages) >= 2

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
filtered = df[df["village"].isin(selected_villages)].copy()

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

monthly_event = None

if filtered["completed_at"].notna().any():
    chart_df = filtered.dropna(subset=["completed_at"]).copy()

    with tab1:
        chart_df["month"] = chart_df["completed_at"].dt.to_period("M").dt.to_timestamp()
        monthly = chart_df.groupby(["month", "village"]).size().reset_index(name="count")
        fig = px.bar(
            monthly, x="month", y="count",
            color="village", color_discrete_map=color_map, barmode="group",
            labels={"month": "თვე", "count": "რაოდენობა", "village": "სოფელი"},
            title="რეგისტრაციები თვეების მიხედვით (დააჭირე ბარზე — ცხრილი გაიფილტრება)",
        )
        if not is_compare:
            fig.update_layout(showlegend=False)
        monthly_event = st.plotly_chart(
            fig,
            width="stretch",
            on_select="rerun",
            selection_mode="points",
            key=MONTHLY_CHART_KEY,
        )

    with tab2:
        chart_df["year"] = chart_df["completed_at"].dt.year
        yearly = chart_df.groupby(["year", "village"]).size().reset_index(name="count")
        fig = px.bar(
            yearly, x="year", y="count",
            color="village", color_discrete_map=color_map, barmode="group",
            labels={"year": "წელი", "count": "რაოდენობა", "village": "სოფელი"},
            title="რეგისტრაციები წლების მიხედვით",
        )
        if not is_compare:
            fig.update_layout(showlegend=False)
        st.plotly_chart(fig, width="stretch")

    with tab3:
        cum_df = chart_df.sort_values("completed_at").copy()
        cum_df["cumulative"] = cum_df.groupby("village").cumcount() + 1
        fig = px.line(
            cum_df, x="completed_at", y="cumulative",
            color="village", color_discrete_map=color_map,
            labels={"completed_at": "თარიღი", "cumulative": "სულ", "village": "სოფელი"},
            title="დაგროვილი რეგისტრაციები",
        )
        if not is_compare:
            fig.update_layout(showlegend=False)
        st.plotly_chart(fig, width="stretch")
else:
    st.info("არჩეულ პერიოდში ჩანაწერები ვერ მოიძებნა.")


# ---------- Resolve clicked month (only narrows the records table) ----------
# Chart-widget state is the source of truth: the clear button below resets it
# by popping its session_state key, which naturally drops the selection.
selected_month = None
try:
    pts = monthly_event["selection"]["points"]
    if pts:
        selected_month = pd.Timestamp(pts[0]["x"])
except (KeyError, TypeError, IndexError):
    pass

table_df = filtered
if selected_month is not None:
    mask = (filtered["completed_at"].dt.year == selected_month.year) & \
           (filtered["completed_at"].dt.month == selected_month.month)
    table_df = filtered[mask]


# ---------- Data table ----------
st.subheader("ჩანაწერები")

if selected_month is not None:
    label = f"{selected_month.year} {GEORGIAN_MONTHS[selected_month.month]}"
    if is_compare:
        n_villages_in_view = table_df["village"].nunique()
        label = f"{label} ({n_villages_in_view} სოფელი)"
    col_msg, col_btn = st.columns([4, 1])
    col_msg.info(f"ნაჩვენებია: {label}")
    if col_btn.button("შერჩევის გასუფთავება", key="clear_month"):
        st.session_state.pop(MONTHLY_CHART_KEY, None)
        st.rerun()

base_cols = ["reg_number", "app_id", "completed_at", "submitted_at",
             "web_transact", "status", "address", "applicants"]
if is_compare:
    base_cols.insert(6, "village")  # after "status", before "address"

display = table_df[base_cols].sort_values("completed_at", ascending=False).copy()

display["napr_link"] = display["app_id"].apply(
    lambda aid: NAPR_VIEW_URL.format(app_id=aid)
)
display = display.drop(columns=["app_id"]).rename(columns={
    "reg_number":   "რეგ. ნომერი",
    "completed_at": "დასრულდა",
    "submitted_at": "შემოვიდა",
    "web_transact": "ტიპი",
    "status":       "სტატუსი",
    "village":      "სოფელი",
    "address":      "მისამართი",
    "applicants":   "პირები",
    "napr_link":    "ნახვა",
})

if is_compare:
    def _village_style(v: str) -> str:
        c = color_map.get(v, "#888888")
        return f"color: {c}; font-weight: 600"
    styled = display.style.map(_village_style, subset=["სოფელი"])
else:
    styled = display

st.dataframe(
    styled,
    width="stretch",
    height=500,
    hide_index=True,
    column_config={
        "ნახვა": st.column_config.LinkColumn(
            "ნახვა",
            help="გახსენი ჩანაწერი NAPR-ზე",
            display_text="🔗",
        ),
    },
)

# CSV export — drop the URL column so dad's spreadsheet isn't cluttered
csv_df = display.drop(columns=["ნახვა"])
csv = csv_df.to_csv(index=False).encode("utf-8-sig")  # utf-8-sig for Excel
st.download_button(
    "⬇️ CSV ფაილის ჩამოტვირთვა",
    data=csv,
    file_name=f"registrations_{date.today().isoformat()}.csv",
    mime="text/csv",
)

st.caption(f"ბაზის ფაილი: `{DB_PATH}`")
