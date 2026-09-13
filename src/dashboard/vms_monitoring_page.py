import streamlit as st
from dotenv import load_dotenv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
from google_metric_func import get_metric_value
import plotly.express as px

tz = ZoneInfo("Asia/Ho_Chi_Minh")

load_dotenv(override=True)

metric_config = {
    "CPU": {
        "metric_type": 
            {
                "CPU utilization": "compute.googleapis.com/instance/cpu/utilization",
                "Processes by CPU Usage": "agent.googleapis.com/processes/cpu_time",
                "System Load per vCPU": "agent.googleapis.com/cpu/load_1m",
                "vCPU Core Usage": "compute.googleapis.com/instance/cpu/usage_time",
                "Unused vCPU Cores": "compute.googleapis.com/instance/cpu/reserved_cores",
            },
            "label": "CPU"
    },
    "Disk": {
        "metric_type": 
            {
                "Disk percent used": "agent.googleapis.com/disk/percent_used"
            },
            "label": "Disk"
    },
    "Memory": {
        "metric_type":
            {
                "Memory percent used": "agent.googleapis.com/memory/percent_used",
                "Memory Bytes Used": "agent.googleapis.com/memory/bytes_used",
            },
            "label": "Memory"
    },
    "Network": {
        "metric_type":
            {
                "Network sent bytes": "compute.googleapis.com/instance/network/sent_bytes_count"
            },
            "label": "Network"
    },
}

#start page

if "refresh_token" not in st.session_state:
    st.session_state.refresh_token = 0

if "query_end_time" not in st.session_state:
    st.session_state.query_end_time = datetime.now(tz=tz)

with st.sidebar:
    metric_choice = st.radio(
        "Resource",
        options=["CPU", "Disk", "Memory", "Network"],
        index=0,
    )
    if st.button("Reload data"):
        st.session_state.refresh_token += 1
        st.session_state.query_end_time = datetime.now(tz=tz)

selected_metric = metric_config[metric_choice]


@st.cache_data(show_spinner=True, ttl=3600, show_time=True)
def load_metric_data(project_id, metric_type, start_time, end_time, refresh_token):
    return get_metric_value(
        project_id,
        metric_type,
        start_time=start_time,
        end_time=end_time,
    )

st.markdown(
    """
    <style>
    div[class*="st-key-"] {
        border: 1px solid rgba(128, 128, 128, 0.22);
        border-radius: 12px;
        padding: 0.75rem;
        background: var(--secondary-background-color);
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.03);
    }
    </style>
    """,
    unsafe_allow_html=True,
)
    
filter_container = st.container(key="filter_container")
with filter_container:
    st.title("VM Monitoring Page")
    col1, col2 = st.columns([1, 3])
    with col1:
        range_hours = st.selectbox(
            "Select time range (hours)",
            options=[1, 2, 4, 6, 8, 10, 12, 24],
            index=0,
            help="Select the time range for the metric data to be displayed.",
        )

match selected_metric["label"]:
    case "CPU":
        #cpu utitization data
        cpu_utilization_data = load_metric_data(
            "myproject-e-commercial-analysi",
            selected_metric["metric_type"]["CPU utilization"],
            start_time=st.session_state.query_end_time - timedelta(hours=range_hours),
            end_time=st.session_state.query_end_time,
            refresh_token=st.session_state.refresh_token,
        )
        # cpu_utilization_expander = st.expander("CPU Utilization Data", width="stretch", expanded=True)
        # with cpu_utilization_expander:
        col1, col2 = st.columns([1, 1])
        with col1:
            with st.container(key="cpu_utilization_box"):
                cpu_utilization_fig = px.line(
                    cpu_utilization_data,
                    x="timestamp",
                    y="metric_value",
                    color="hostname",
                    title="CPU Utilization",
                    # markers=True
                )
                cpu_utilization_fig.update_traces(
                    line=dict(width=2),
                    hovertemplate="<b>%{fullData.name}</b>: %{y:.2f}<extra></extra>"
                )
                cpu_utilization_fig.update_layout(
                    xaxis_title="",
                    yaxis_title="",
                    yaxis=dict(
                        tickformat=".2%",  # Định dạng phần trăm với 2 chữ số thập phân
                        range=[0, 1],       # Giới hạn trục Y từ 0 đến 1 (tương ứng với 0% đến 100%)
                        side="right"          # Đặt trục Y bên phải
                    ),
                    hovermode="x unified", # Tooltip gom chung dữ liệu của tất cả hostname tại 1 thời điểm
                    legend=dict(
                        orientation="h",      # Đặt nằm ngang (horizontal)
                        yanchor="top",        # Lấy mép trên của legend làm điểm neo
                        y=-0.2,               # Đẩy xuống dưới trục X (Số âm càng lớn càng tụt xuống sâu)
                        xanchor="center",     # Căn giữa theo chiều ngang
                        x=0.5,                # Đặt ở chính giữa biểu đồ
                        title_text=""         # (Tùy chọn) Xóa chữ "hostname" để tiết kiệm diện tích
                    )
                )
                cpu_utilization_fig.add_hline(
                    y=1.0,                      # Thay bằng 100 nếu dữ liệu của bạn là thang 0-100
                    line_dash="solid",          # Kiểu nét liền
                    line_color="black",         
                    line_width=1,               # Độ dày của đường
                )
                st.plotly_chart(cpu_utilization_fig, use_container_width=True)            

        # System Load per vCPU
        system_load_data = load_metric_data(
            "myproject-e-commercial-analysi",
            selected_metric["metric_type"]["System Load per vCPU"],
            start_time=st.session_state.query_end_time - timedelta(hours=range_hours),
            end_time=st.session_state.query_end_time,
            refresh_token=st.session_state.refresh_token,
        )

        # system_load_expander = st.expander("System Load per vCPU", expanded=True)
        # with system_load_expander:
        with col2:
            with st.container(key="system_load_box"):
                system_load_fig = px.line(
                    system_load_data,
                    x="timestamp",
                    y="metric_value",
                    color="hostname",
                    title="System Load per vCPU",
                )
                system_load_fig.update_traces(
                    line=dict(width=2),
                    hovertemplate="<b>%{fullData.name}</b>: %{y:.2f}<extra></extra>"
                )
                system_load_fig.update_layout(
                    xaxis_title="",
                    yaxis_title="",
                    yaxis=dict(
                        side="right",
                        range=[0, 10]
                    ),
                    hovermode="x unified", 
                    legend=dict(
                        orientation="h",      
                        yanchor="top",        
                        y=-0.2,               
                        xanchor="center",     
                        x=0.5,                
                        title_text=""         
                    )
                )
                system_load_fig.add_hline(
                    y=10.0,                      
                    annotation_text="",
                    line_dash="dash",          
                    line_color="red",         
                    line_width=1,
                    annotation_position="top left"
                )
                st.plotly_chart(system_load_fig, use_container_width=True)
                
        col1, col2 = st.columns([1, 1])                
        # Biểu đồ vCPU Core Usage vào col1 của row2
        vcpu_usage_data = load_metric_data(
            "myproject-e-commercial-analysi",
            selected_metric["metric_type"]["vCPU Core Usage"],
            start_time=st.session_state.query_end_time - timedelta(hours=range_hours),
            end_time=st.session_state.query_end_time,
            refresh_token=st.session_state.refresh_token,
        )
        vcpu_usage_data['metric_value'] = vcpu_usage_data['metric_value'] / 60
        with col1:
            with st.container(key="vcpu_usage_box"):
                vcpu_usage_fig = px.line(
                    vcpu_usage_data,
                    x="timestamp",
                    y="metric_value",
                    color="hostname",
                    title="vCPU Core Usage",
                )
                vcpu_usage_fig.update_traces(
                    line=dict(width=2),
                    hovertemplate="<b>%{fullData.name}</b>: %{y:.2f}<extra></extra>"
                )
                vcpu_usage_fig.update_layout(
                    xaxis_title="",
                    yaxis_title="",
                    yaxis=dict(
                        side="right", # Giữ trục Y bên phải theo phong cách GCP
                        range=[0, 2],  # Giới hạn trục Y từ 0 đến 2
                    ),
                    hovermode="x unified", 
                    legend=dict(
                        orientation="h",      
                        yanchor="top",        
                        y=-0.2,               
                        xanchor="center",     
                        x=0.5,                
                        title_text=""         
                    )
                )
                vcpu_usage_fig.add_hline(
                    y=2,                      
                    annotation_text="",
                    line_dash="dash",          
                    line_color="red",         
                    line_width=1,
                    annotation_position="top left"
                )
                st.plotly_chart(vcpu_usage_fig, use_container_width=True)
            
            # dữ liệu Tổng số Core cấp phát (Reserved Cores)
            reserved_cores_data = load_metric_data(
                "myproject-e-commercial-analysi",
                selected_metric["metric_type"]["Unused vCPU Cores"], # Đây là 'reserved_cores'
                start_time=st.session_state.query_end_time - timedelta(hours=range_hours),
                end_time=st.session_state.query_end_time,
                refresh_token=st.session_state.refresh_token,
            )
            
            # dữ liệu Core đang sử dụng (Usage Time)
            used_cores_data = load_metric_data(
                "myproject-e-commercial-analysi",
                selected_metric["metric_type"]["vCPU Core Usage"], # Đây là 'usage_time'
                start_time=st.session_state.query_end_time - timedelta(hours=range_hours),
                end_time=st.session_state.query_end_time,
                refresh_token=st.session_state.refresh_token,
            )
            
            with col2:
                used_cores_data['metric_value'] = used_cores_data['metric_value'] / 60
                merged_data = pd.merge(
                    reserved_cores_data, 
                    used_cores_data, 
                    on=['timestamp', 'hostname'], 
                    suffixes=('_reserved', '_used')
                )
                merged_data['metric_value'] = merged_data['metric_value_reserved'] - merged_data['metric_value_used']
                merged_data['metric_value'] = merged_data['metric_value'].clip(lower=0)                        
                unused_vcpu_data = merged_data

                with st.container(key="unused_vcpu_box"):
                    unused_vcpu_fig = px.line(
                        unused_vcpu_data,
                        x="timestamp",
                        y="metric_value",
                        color="hostname",
                        title="Unused vCPU Cores",
                    )
                    
                    unused_vcpu_fig.update_traces(
                        line=dict(width=1.5),
                        mode="lines",
                        hovertemplate="<b>%{fullData.name}</b>: %{y:.2f}<extra></extra>",
                    )
                    
                    unused_vcpu_fig.update_layout(
                        xaxis_title="",
                        yaxis_title="",
                        yaxis=dict(
                            side="right",
                            range=[0, 5], 
                        ),
                        hovermode="x unified",
                        legend=dict(
                            orientation="h",
                            yanchor="top",
                            y=-0.25,
                            xanchor="center",
                            x=0.5,
                            title_text="",
                        ),
                    )
                    unused_vcpu_fig.add_hline(
                        y=5,                      
                        annotation_text="",
                        line_dash="dash",          
                        line_color="black",         
                        line_width=1,
                        annotation_position="top left"
                    )
                    st.plotly_chart(unused_vcpu_fig, use_container_width=True)

    case "Disk":
        st.subheader("Disk Metrics")
    case "Memory":
        # Memory percent used data
        memory_data = load_metric_data(
            "myproject-e-commercial-analysi",
            selected_metric["metric_type"]["Memory percent used"],
            start_time=st.session_state.query_end_time - timedelta(hours=range_hours),
            end_time=st.session_state.query_end_time,
            refresh_token=st.session_state.refresh_token,
        )
        
        col1, col2 = st.columns([1, 1])
        with col1:
            with st.container(key="memory_utilization_box"):
                memory_utilization_fig = px.line(
                    memory_data,
                    x="timestamp",
                    y="metric_value",
                    color="hostname",
                    title="Memory Utilization",
                )
                memory_utilization_fig.update_traces(
                    line=dict(width=1),
                    hovertemplate="<b>%{fullData.name}</b>: %{y:.2f}%<extra></extra>"
                )
                memory_utilization_fig.update_layout(
                    xaxis_title="",
                    yaxis_title="",
                    yaxis=dict(
                        side="right",
                        range=[0, 100],
                        tickformat=".0f"
                    ),
                    hovermode="x unified",
                    legend=dict(
                        orientation="h",
                        yanchor="top",
                        y=-0.2,
                        xanchor="center",
                        x=0.5,
                        title_text=""
                    )
                )
                memory_utilization_fig.add_hline(
                    y=100,
                    line_dash="solid",
                    line_color="black",
                    line_width=1,
                )
                st.plotly_chart(memory_utilization_fig, use_container_width=True)
             
        memory_bytes_used_data = load_metric_data(
            "myproject-e-commercial-analysi",
            selected_metric["metric_type"]["Memory Bytes Used"],
            start_time=st.session_state.query_end_time - timedelta(hours=range_hours),
            end_time=st.session_state.query_end_time,
            refresh_token=st.session_state.refresh_token,
        )
        
        memory_bytes_used_data['metric_value'] = memory_bytes_used_data['metric_value'] / (1024 * 1024)
        
        with col2:
            with st.container(key="memory_bytes_used_box"):
                # Nhớ truyền clean_bytes_data vào vẽ thay vì biến cũ
                memory_bytes_used_fig = px.line(
                    memory_bytes_used_data,
                    x="timestamp",
                    y="metric_value",
                    color="hostname",
                    title="Memory Bytes Used",
                )
                
                # Cấu hình làm mờ các đường khác và thêm chữ MiB vào Tooltip
                memory_bytes_used_fig.update_traces(
                    line=dict(width=1.5),
                    hovertemplate="<b>%{fullData.name}</b>: %{y:,.0f} MiB<extra></extra>"
                )
                
                memory_bytes_used_fig.update_layout(
                    xaxis_title="",
                    yaxis_title="",
                    yaxis=dict(
                        side="right",
                        ticksuffix="MiB",
                        range=[0, 10000],
                    ),
                    hovermode="x unified",
                    legend=dict(
                        orientation="h",
                        yanchor="top",
                        y=-0.2,
                        xanchor="center",
                        x=0.5,
                        title_text=""
                    )
                )
                
                # Thêm đường kẻ ngang mốc 10,000 MiB (Tương đương khoảng 10GB)
                memory_bytes_used_fig.add_hline(
                    y=10000,
                    line_dash="solid",
                    line_color="gray",
                    line_width=1,
                )
                
                st.plotly_chart(memory_bytes_used_fig, use_container_width=True)
                
    case "Network":
        st.subheader("Network Metrics")
    case _:
        st.text("Unknown metric type")
