import streamlit as st
from dotenv import load_dotenv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import google_metric_func as google_metric_func

tz = ZoneInfo("Asia/Ho_Chi_Minh")
load_dotenv(override=True)

st.set_page_config(page_title="Dashboard", layout="wide")

st.markdown("""
    <style>
        /* 1. Thu hẹp khoảng trắng trên cùng và dưới cùng của NỘI DUNG CHÍNH */
        .block-container {
            padding-top: 1rem !important;
            padding-bottom: 1rem !important;
        }
        
        /* 2. Thu hẹp khoảng trắng của toàn bộ SIDEBAR */
        [data-testid="stSidebar"] {
            padding-top: 0rem !important;
        }
        
        /* 3. Thu hẹp khu vực Header của Sidebar (Nơi chứa nút đóng/mở mặc định) */
        [data-testid="stSidebarHeader"] {
            padding-top: 1rem !important;
            padding-bottom: 0rem !important;
        }
        
        /* 4. Thu hẹp khoảng trắng nơi bắt đầu hiển thị các code st.sidebar của bạn */
        [data-testid="stSidebarUserContent"] {
            padding-top: 0rem !important;
        }
    </style>
""", unsafe_allow_html=True)

st.markdown(
    """
    <style>
    .block-container {
        padding-top: 0.8rem !important;
        padding-bottom: 0rem !important;
        padding-left: 1rem !important;
        padding-right: 1rem !important;
    }
    h1 {
        margin-top: 0.2rem !important;
        margin-bottom: 0rem !important;
    }
    div[data-testid="stExpander"] {
        margin-top: 0rem !important;
        margin-bottom: 0rem !important;
        margin-left: 0.5rem !important;
        margin-right: 0.5rem !important;
    }
    div[data-testid="stExpanderDetails"] {
        padding-top: 0rem !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

pages = [
    st.Page("analysis_page.py", title="Analysis", icon="📊"),
    st.Page("vms_monitoring_page.py", title="Monitoring", icon="📈")
]

pg = st.navigation(pages, position="hidden")

with st.sidebar:
    st.markdown("# Dashboard", anchors=True)
    st.divider()
    
    for page in pages:
        st.page_link(page, label=page.title, icon=page.icon)

    st.divider()       

pg.run()

st.divider()
st.markdown("<p style='text-align: center; color: gray;'>HI</p>", unsafe_allow_html=True)