import streamlit as st
from data_wh_func import *
import plotly.express as px

st.markdown(
    """
    <style>
        .block-container {
            padding-top: 1rem !important;
            padding-bottom: 1rem !important;
        }

        .metric-card {
            background: linear-gradient(135deg, rgba(15, 118, 110, 0.10), rgba(59, 130, 246, 0.08));
            border: 1px solid rgba(148, 163, 184, 0.28);
            border-radius: 20px;
            padding: 1.1rem 1.2rem 0.9rem 1.2rem;
            box-shadow: 0 12px 30px rgba(15, 23, 42, 0.08);
            min-height: 132px;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        .metric-inner {
            width: 100%;
        }

        .metric-label {
            color: #475569;
            font-size: 0.8rem;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            margin-bottom: 0.5rem;
        }

        .metric-value-row {
            display: flex;
            align-items: baseline;
            gap: 0.6rem;
            flex-wrap: nowrap;
        }

        .metric-value {
            color: #0f172a;
            font-size: clamp(2.2rem, 3.4vw, 3.1rem);
            font-weight: 800;
            letter-spacing: -0.05em;
            line-height: 1.1;
        }

        .metric-delta {
            display: inline-flex;
            align-items: center;
            gap: 0.2rem;
            font-size: 0.9rem;
            font-weight: 700;
            white-space: nowrap;
        }

        div[class*="st-key-"] {
            border: 1px solid rgba(128, 128, 128, 0.22);
            border-radius: 12px;
            padding: 0.75rem;
            background: var(--secondary-background-color);
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.03);
            margin-bottom: 0.75rem;
        }

        [data-testid="stSidebar"] {
            background: #f8fafc;
        }

        [data-testid="stSidebar"] > div {
            border-right: 1px solid rgba(148, 163, 184, 0.25);
        }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Analysis Page")

# with st.sidebar:
#     st.markdown("# Analysis Page Sidebar")
#     st.divider()
#     st.write("This is the sidebar content for the Analysis Page.")

def render_metric_card(label, value, delta, positive=True):
    arrow = "↗" if positive else "↘"
    color = "#16a34a" if positive else "#dc2626"
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-inner">
                <div class="metric-label">{label}</div>
                <div class="metric-value-row">
                    <div class="metric-value">{value}</div>
                    <div class="metric-delta" style="color: {color};">
                        <span>{arrow}</span>
                        <span>{delta}</span>
                    </div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
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

def shorten_name(name, max_len=28):
    if name is None:
        return ""
    text = str(name).strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 3].rstrip() + "..."


col1, col2, col3 = st.columns(3)
with col1:
    render_metric_card("Total Product", get_total_product(), "+8.2%", True)
with col2:
    render_metric_card("Total Brand", get_total_brand(), "+2.1%", True)
with col3:
    render_metric_card("Total Seller", get_total_seller(), "+5.4%", True)

st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)

col4, col5 = st.columns(2)
with col4:
    with st.container(border=True):
        st.subheader("Top 10 Best-Selling Categories")
        category_distribution = get_top_n_total_product_by_cateogry(10)
        category_distribution = category_distribution.sort_values("Count", ascending=False).reset_index(drop=True)
        palette = [
            "#0ea5e9", "#14b8a6", "#8b5cf6", "#f59e0b",
            "#22c55e", "#f43f5e", "#6366f1", "#f97316",
            "#a78bfa", "#10b981",
        ]
        fig = px.pie(
            category_distribution,
            names="Category",
            values="Count",
            hole=0.52,
            color_discrete_sequence=palette,
        )
        largest_idx = category_distribution["Count"].idxmax()
        pull_values = [0.08 if i == largest_idx else 0 for i in range(len(category_distribution))]
        fig.update_traces(
            pull=pull_values,
            textinfo="percent",
            textposition="inside",
            insidetextorientation="radial",
            hovertemplate="<b>%{label}</b><br>Số lượng: %{value}<extra></extra>",
            marker=dict(line=dict(color="#ffffff", width=2)),
        )
        fig.update_layout(
            margin=dict(t=10, r=18, b=10, l=18),
            showlegend=True,
            legend=dict(
                orientation="v",
                yanchor="middle",
                y=0.5,
                xanchor="left",
                x=1.02,
            ),
            paper_bgcolor="white",
            plot_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True)

with col5:
    with st.container(border=True):
        st.subheader("Top 10 Best-Selling Sellers")
        seller_distribution = get_top_n_sold_seller(10)
        fig = px.bar(seller_distribution, x="Seller", y="Sold")
        fig.update_layout(xaxis_tickangle=-25, margin=dict(t=10, r=20, b=10, l=20))
        st.plotly_chart(fig, use_container_width=True)

with st.container(border=True):
    st.subheader("Top 20 Potential Products")
    potential_products = get_top_n_potential_product(20)
    potential_products["Product"] = potential_products["Product"].apply(shorten_name)
    fig = px.bar(
        potential_products,
        x="Product",
        y="Sold",
        color="Seller Count",
        color_continuous_scale=["#dbeafe", "#60a5fa", "#2563eb"],
        range_color=[potential_products["Seller Count"].min(), potential_products["Seller Count"].max()],
    )
    fig.update_traces(
        marker=dict(line=dict(color="#ffffff", width=1.2)),
        opacity=0.96,
    )
    integer_ticks = sorted({int(v) for v in potential_products["Seller Count"].dropna().tolist()})
    fig.update_coloraxes(
        colorbar=dict(
            title="Seller count",
            tickmode="array",
            tickvals=integer_ticks,
            ticktext=[str(v) for v in integer_ticks],
            len=0.8,
            thickness=18,
            outlinewidth=0,
            tickfont=dict(size=10, color="#475569"),
            ticks="outside",
            ticklen=5,
            tickcolor="#64748b",
        )
    )
    fig.update_layout(
        xaxis_tickangle=-25,
        margin=dict(t=10, r=20, b=20, l=20),
        plot_bgcolor="#f8fafc",
        paper_bgcolor="#ffffff",
        font=dict(color="#0f172a", family="Arial"),
        xaxis=dict(
            title=None,
            showgrid=False,
            tickfont=dict(size=11),
            linecolor="#cbd5e1",
            ticklen=0,
        ),
        yaxis=dict(
            title=dict(text="Sold", font=dict(size=12)),
            gridcolor="#e2e8f0",
            zeroline=False,
            tickfont=dict(size=11),
        ),
        legend=dict(
            title=dict(text="Seller count"),
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            bgcolor="rgba(255,255,255,0.7)",
        ),
        bargap=0.22,
        height=520,
    )
    st.plotly_chart(fig, use_container_width=True)