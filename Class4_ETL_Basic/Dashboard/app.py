"""
Real-time Dashboard để hiển thị dữ liệu từ Data Warehouse và Data Mart
Sử dụng Streamlit để tạo dashboard real-time
"""

import streamlit as st
import pandas as pd
import pyodbc
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time

# Page config
st.set_page_config(
    page_title="Media Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection function


@st.cache_resource
def get_db_connection(server, database, username=None, password=None):
    """Tạo connection đến SQL Server"""
    try:
        if username and password:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        else:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"

        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        st.error(f"Lỗi kết nối database: {e}")
        return None


def load_data_from_dm(conn, query):
    """Load data từ Data Mart"""
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Lỗi khi load data: {e}")
        return None


def main():
    st.title("📊 Media Analytics Dashboard")
    st.markdown("---")

    # Sidebar - Database Configuration
    st.sidebar.header("⚙️ Database Configuration")

    server = st.sidebar.text_input("SQL Server", value="localhost")
    database_dw = st.sidebar.selectbox(
        "Data Warehouse", ["DW_MediaAnalytics"], index=0)
    database_dm = st.sidebar.selectbox(
        "Data Mart", ["DM_MediaAnalytics"], index=0)

    use_auth = st.sidebar.checkbox("Use SQL Authentication")
    username = None
    password = None
    if use_auth:
        username = st.sidebar.text_input("Username", type="default")
        password = st.sidebar.text_input("Password", type="password")

    # Connect to database
    conn_dm = get_db_connection(server, database_dm, username, password)

    if conn_dm is None:
        st.warning("Vui lòng cấu hình database connection trong sidebar")
        return

    # Auto-refresh option
    auto_refresh = st.sidebar.checkbox("Auto Refresh", value=False)
    refresh_interval = st.sidebar.slider(
        "Refresh Interval (seconds)", 5, 60, 10)

    # Main content
    tab1, tab2, tab3, tab4 = st.tabs(
        ["📈 Overview", "📅 Daily Summary", "🎯 Contract Analytics", "📊 Content Type Trends"])

    with tab1:
        st.header("Overview Dashboard")

        # KPI Cards
        col1, col2, col3, col4 = st.columns(4)

        # Query for today's summary
        today = datetime.now().date()
        query_today = f"""
            SELECT 
                SUM(TotalContracts) as TotalContracts,
                SUM(TotalDevices) as TotalDevices,
                SUM(TotalDuration_All) as TotalDuration,
                COUNT(*) as DaysCount
            FROM DM_DailySummary
            WHERE DateValue >= DATEADD(day, -7, '{today}')
        """

        df_summary = load_data_from_dm(conn_dm, query_today)

        if df_summary is not None and len(df_summary) > 0:
            with col1:
                st.metric("Total Contracts (7 days)",
                          f"{int(df_summary['TotalContracts'].iloc[0]):,}" if df_summary['TotalContracts'].iloc[0] else "0")
            with col2:
                st.metric("Total Devices (7 days)",
                          f"{int(df_summary['TotalDevices'].iloc[0]):,}" if df_summary['TotalDevices'].iloc[0] else "0")
            with col3:
                duration_hours = (
                    df_summary['TotalDuration'].iloc[0] / 3600) if df_summary['TotalDuration'].iloc[0] else 0
                st.metric("Total Duration (7 days)",
                          f"{duration_hours:,.0f} hours" if duration_hours else "0 hours")
            with col4:
                st.metric("Days",
                          f"{int(df_summary['DaysCount'].iloc[0])}")

        # Chart: Daily Summary
        st.subheader("Daily Summary - Last 7 Days")
        query_chart = f"""
            SELECT 
                DateValue,
                TotalContracts,
                TotalDevices,
                TotalDuration_All / 3600.0 as TotalDuration_Hours
            FROM DM_DailySummary
            WHERE DateValue >= DATEADD(day, -7, '{today}')
            ORDER BY DateValue
        """

        df_chart = load_data_from_dm(conn_dm, query_chart)

        if df_chart is not None and len(df_chart) > 0:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_chart['DateValue'],
                y=df_chart['TotalDuration_Hours'],
                mode='lines+markers',
                name='Total Duration (Hours)',
                line=dict(color='#1f77b4', width=2)
            ))
            fig.update_layout(
                title="Total Duration by Day",
                xaxis_title="Date",
                yaxis_title="Duration (Hours)",
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.header("Daily Summary")

        # Date range selector
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date", value=today - timedelta(days=7))
        with col2:
            end_date = st.date_input("End Date", value=today)

        query_daily = f"""
            SELECT 
                DateValue,
                Year,
                Month,
                DATENAME(MONTH, DateValue) as MonthName,
                Day,
                TotalContracts,
                TotalDevices,
                TotalDuration_TruyenHinh / 3600.0 as Duration_TH_Hours,
                TotalDuration_PhimTruyen / 3600.0 as Duration_PT_Hours,
                TotalDuration_GiaiTri / 3600.0 as Duration_GT_Hours,
                TotalDuration_ThieuNhi / 3600.0 as Duration_TN_Hours,
                TotalDuration_TheThao / 3600.0 as Duration_TT_Hours,
                TotalDuration_All / 3600.0 as Duration_Total_Hours
            FROM DM_DailySummary
            WHERE DateValue BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY DateValue DESC
        """

        df_daily = load_data_from_dm(conn_dm, query_daily)

        if df_daily is not None and len(df_daily) > 0:
            st.dataframe(df_daily, use_container_width=True)

            # Chart: Content Type Distribution
            st.subheader("Content Type Distribution")
            content_types = ['Duration_TH_Hours', 'Duration_PT_Hours', 'Duration_GT_Hours',
                             'Duration_TN_Hours', 'Duration_TT_Hours']
            labels = ['Truyền Hình', 'Phim Truyện',
                      'Giải Trí', 'Thiếu Nhi', 'Thể Thao']

            total_by_type = [df_daily[col].sum() for col in content_types]

            fig_pie = px.pie(
                values=total_by_type,
                names=labels,
                title="Content Type Distribution"
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("Không có dữ liệu trong khoảng thời gian này")

    with tab3:
        st.header("Contract Analytics")

        # Top contracts selector
        top_n = st.slider("Top N Contracts", 5, 50, 10)

        query_contracts = f"""
            SELECT TOP {top_n}
                ContractID,
                SUM(TotalDevices) as TotalDevices,
                SUM(Duration_Total) / 3600.0 as TotalDuration_Hours,
                SUM(Duration_TruyenHinh) / 3600.0 as Duration_TH_Hours,
                SUM(Duration_PhimTruyen) / 3600.0 as Duration_PT_Hours,
                SUM(Duration_GiaiTri) / 3600.0 as Duration_GT_Hours
            FROM DM_ContractAnalytics
            WHERE DateValue >= DATEADD(day, -30, '{today}')
            GROUP BY ContractID
            ORDER BY TotalDuration_Hours DESC
        """

        df_contracts = load_data_from_dm(conn_dm, query_contracts)

        if df_contracts is not None and len(df_contracts) > 0:
            st.dataframe(df_contracts, use_container_width=True)

            # Bar chart
            fig_bar = px.bar(
                df_contracts.head(10),
                x='ContractID',
                y='TotalDuration_Hours',
                title="Top 10 Contracts by Duration",
                labels={
                    'TotalDuration_Hours': 'Duration (Hours)', 'ContractID': 'Contract'}
            )
            fig_bar.update_xaxis(tickangle=-45)
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("Không có dữ liệu")

    with tab4:
        st.header("Content Type Trends")

        query_trends = f"""
            SELECT 
                DateValue,
                ContentType,
                TotalDuration / 3600.0 as Duration_Hours,
                TotalSessions,
                UniqueContracts
            FROM DM_ContentTypeTrend
            WHERE DateValue >= DATEADD(day, -30, '{today}')
            ORDER BY DateValue, ContentType
        """

        df_trends = load_data_from_dm(conn_dm, query_trends)

        if df_trends is not None and len(df_trends) > 0:
            # Line chart
            fig_line = px.line(
                df_trends,
                x='DateValue',
                y='Duration_Hours',
                color='ContentType',
                title="Content Type Trends Over Time",
                labels={
                    'Duration_Hours': 'Duration (Hours)', 'DateValue': 'Date'}
            )
            st.plotly_chart(fig_line, use_container_width=True)

            # Data table
            st.dataframe(df_trends, use_container_width=True)
        else:
            st.info("Không có dữ liệu")

    # Auto refresh
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()

    # Footer
    st.markdown("---")
    st.markdown(
        f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
