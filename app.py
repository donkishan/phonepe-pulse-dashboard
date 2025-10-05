import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px

# ================= DATABASE CONNECTION =================
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="kishan21",
        database="phonepe_pulse"
    )

# ================= Sidebar Navigation =================
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Select a page:", ["Home", "Analysis"])
import streamlit as st
import pandas as pd
import pydeck as pdk
import requests


if page == "Home":
    st.title("📊 PhonePe Pulse - India Dashboard")

    # ---- Load raw data from database ----
    conn = get_connection()
    df_trans = pd.read_sql("SELECT * FROM aggregated_transaction", conn)
    df_users = pd.read_sql("SELECT * FROM map_user", conn)
    df_ins = pd.read_sql("SELECT * FROM aggregated_insurance", conn)
    conn.close()

    # ---- Normalize state names ----
    for df in [df_trans, df_users, df_ins]:
        df["States"] = df["States"].str.strip().str.upper()

    # ---- Load India GeoJSON ----
    geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(geojson_url)
    if response.status_code == 200:
        india_geojson = response.json()
    else:
        st.error("Failed to fetch India GeoJSON. Check your internet connection or URL.")
        st.stop()

    state_key = "ST_NM"
    for feature in india_geojson["features"]:
        props = feature.get("properties", {})
        if state_key in props:
            props[state_key] = props[state_key].strip().upper()
        feature["properties"] = props

    # ---- Filters ----
    col1, col2, col3 = st.columns(3)
    with col1:
        selected_data_type = st.selectbox(
            "Select Data Type", ["Transaction", "Users", "Insurance"], key="data_type"
        )
    with col2:
        selected_year = st.selectbox(
            "Select Year", sorted(df_trans["Years"].unique()), key="year_select"
        )
    with col3:
        selected_quarter = st.selectbox(
            "Select Quarter", ["All"] + sorted(df_trans["Quarter"].unique()), key="quarter_select"
        )

    # ---- Select correct dataframe and columns ----
    if selected_data_type == "Transaction":
        df_map = df_trans[df_trans["Years"] == selected_year].copy()
        value_column = "Transaction_amount"
        count_column = "Transaction_count"
    elif selected_data_type == "Users":
        df_map = df_users[df_users["Years"] == selected_year].copy()
        value_column = "RegisteredUser"
        count_column = "AppOpens"
    else:  # Insurance
        df_map = df_ins[df_ins["Years"] == selected_year].copy()
        value_column = "Insurance_amount"
        count_column = "Insurance_count"

    # Apply quarter filter if needed
    if selected_quarter != "All" and "Quarter" in df_map.columns:
        df_map = df_map[df_map["Quarter"] == selected_quarter]

    # Aggregate state-wise
    df_map_agg = df_map.groupby("States")[[value_column, count_column]].sum().reset_index()
    state_value_map = df_map_agg.set_index("States")[value_column].to_dict()
    max_value = max(state_value_map.values()) if state_value_map else 1

    # ---- Prepare choropleth + 3D columns ----
    for feature in india_geojson["features"]:
        props = feature["properties"]
        state_name = props.get(state_key)
        if not state_name:
            continue
        amount = state_value_map.get(state_name, 0)
        props["value"] = amount
        norm = amount / max_value
        props["color"] = [255, int(255 * (1 - norm)), int(255 * (1 - norm))]
        props["elevation"] = amount / max_value * 500000

    # ---- PyDeck Map ----
    layer = pdk.Layer(
        "GeoJsonLayer",
        data=india_geojson,
        get_fill_color="properties.color",
        get_elevation="properties.elevation",
        extruded=True,
        pickable=True,
        auto_highlight=True,
        stroked=True,
        filled=True,
        get_line_color=[255, 255, 255],
        line_width_min_pixels=1,
    )

    view_state = pdk.ViewState(latitude=22.9734, longitude=78.6569, zoom=4, pitch=45, bearing=0)

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "html": "<b>State:</b> {ST_NM} <br/> <b>Value:</b> {value}",
            "style": {"backgroundColor": "white", "color": "black"}
        }
    )

    st.pydeck_chart(deck)

    # ---- Overview Metrics ----
    st.markdown("### Overview Metrics")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(f"Total {selected_data_type} Value", f"₹{df_map_agg[value_column].sum():,.0f}")
    with col2:
        st.metric("Total Count", f"{df_map_agg[count_column].sum():,.0f}")
    with col3:
        total_users = df_users[df_users["Years"] == selected_year]["RegisteredUser"].sum()
        st.metric("Registered Users", f"{total_users:,}")

    # ---- State-wise Bar Chart ----
    st.markdown("### State-wise Overview")
    st.bar_chart(df_map_agg.set_index("States")[value_column])



# ================= Analysis Page =================
elif page == "Analysis":
    st.title("📊 Business Case Study Analysis")

    # Dropdown for Case Studies
    case_study = st.selectbox(
        "Choose Case Study",
        [
            "Decoding Transaction Dynamics on PhonePe",
            "Device Dominance and User Engagement Analysis",
            "Insurance Penetration and Growth Potential Analysis",
            "Transaction Analysis for Market Expansion",
            "User Engagement and Growth Strategy"
        ]
    )

    st.markdown("<h2 style='color:red;'>State-wise Analysis</h2>", unsafe_allow_html=True)

    # Fetch states dynamically from DB
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT States FROM aggregated_transaction ORDER BY States ASC;")
    states = [row[0] for row in cursor.fetchall()]
    conn.close()

    selected_state = st.selectbox("Choose a State:", states)

    # ================= CASE STUDY LOGIC =================
    conn = get_connection()

    # ---------- Case 1 & 4: Transaction Trends ----------
    if case_study in ["Decoding Transaction Dynamics on PhonePe", "Transaction Analysis for Market Expansion"]:
        df = pd.read_sql(f"""
            SELECT Years, SUM(Transaction_count) AS total_transactions,
                   SUM(Transaction_amount) AS total_amount
            FROM aggregated_transaction
            WHERE States = '{selected_state}'
            GROUP BY Years
            ORDER BY Years ASC;
        """, conn)

        if not df.empty:
            # ---------------- Line Charts ----------------
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Total Transactions Over Years")
                fig1 = px.line(df, x="Years", y="total_transactions", markers=True, title="Transactions Trend")
                st.plotly_chart(fig1, use_container_width=True)

            with col2:
                st.subheader("Total Transaction Amount Over Years (₹)")
                fig2 = px.line(df, x="Years", y="total_amount", markers=True, title="Transaction Amount Trend")
                st.plotly_chart(fig2, use_container_width=True)

            # ---------------- Year-over-Year Growth ----------------
            df['transaction_growth'] = df['total_transactions'].pct_change() * 100
            df['amount_growth'] = df['total_amount'].pct_change() * 100
            col3, col4 = st.columns(2)
            with col3:
                st.subheader("YoY Transaction Growth (%)")
                fig3 = px.bar(df, x="Years", y="transaction_growth", text=df['transaction_growth'].round(2),
                              title="Transaction Growth YoY")
                st.plotly_chart(fig3, use_container_width=True)

            with col4:
                st.subheader("YoY Transaction Amount Growth (%)")
                fig4 = px.bar(df, x="Years", y="amount_growth", text=df['amount_growth'].round(2),
                              title="Amount Growth YoY")
                st.plotly_chart(fig4, use_container_width=True)

            # ---------------- Payment Category Performance ----------------
            df_cat = pd.read_sql(f"""
                SELECT Transaction_type, SUM(Transaction_count) AS total_count,
                       SUM(Transaction_amount) AS total_amount
                FROM aggregated_transaction
                WHERE States = '{selected_state}'
                GROUP BY Transaction_type;
            """, conn)

            st.markdown("<h2 style='color:red;'>Payment Category Performance</h2>", unsafe_allow_html=True)
            col5, col6 = st.columns(2)
            with col5:
                st.subheader("Transaction Count Distribution")
                fig5 = px.pie(df_cat, names="Transaction_type", values="total_count", hole=0.4)
                st.plotly_chart(fig5, use_container_width=True)

            with col6:
                st.subheader("Transaction Amount Distribution (₹)")
                fig6 = px.pie(df_cat, names="Transaction_type", values="total_amount", hole=0.4)
                st.plotly_chart(fig6, use_container_width=True)

            # ---------------- Bar Chart Comparison ----------------
            st.subheader("Payment Categories Comparison (Bar Chart)")
            fig_bar = px.bar(df_cat, x="Transaction_type", y="total_amount", hover_data=["total_count"],
                             barmode="group", title="Payment Category vs Amount")
            st.plotly_chart(fig_bar, use_container_width=True)

            # ---------------- Heatmap: Transaction Amount by Type & Year ----------------
            df_cat_year = pd.read_sql(f"""
                SELECT Years, Transaction_type, SUM(Transaction_amount) AS total_amount
                FROM aggregated_transaction
                WHERE States = '{selected_state}'
                GROUP BY Years, Transaction_type;
            """, conn)
            st.subheader("Heatmap: Transaction Amount by Type & Year")
            fig_heat = px.density_heatmap(df_cat_year, x="Years", y="Transaction_type", z="total_amount",
                                          color_continuous_scale='Viridis')
            st.plotly_chart(fig_heat, use_container_width=True)

    # ---------- Case 2: Device Dominance ----------
    elif case_study == "Device Dominance and User Engagement Analysis":
        df_user = pd.read_sql(f"""
            SELECT Years, Brands, SUM(Transaction_count) AS total_users, AVG(Percentage) AS percentage
            FROM aggregated_user
            WHERE States = '{selected_state}'
            GROUP BY Years, Brands
            ORDER BY Years;
        """, conn)

        if not df_user.empty:
            st.subheader("📱 Device-wise User Distribution")
            fig_bar = px.bar(df_user, x="Years", y="total_users", color="Brands", barmode="group",
                             title="Device-wise Users")
            st.plotly_chart(fig_bar, use_container_width=True)

            st.subheader("Device Share % Over Time")
            fig_line = px.line(df_user, x="Years", y="percentage", color="Brands", markers=True,
                               title="Device Market Share Trend")
            st.plotly_chart(fig_line, use_container_width=True)

            # ---------------- Stacked Area ----------------
            st.subheader("Device Usage Trend (Stacked Area)")
            fig_area = px.area(df_user, x="Years", y="total_users", color="Brands", title="Stacked Device Trend")
            st.plotly_chart(fig_area, use_container_width=True)

            # ---------------- Pie Chart Latest Year ----------------
            latest_year = df_user['Years'].max()
            df_latest = df_user[df_user['Years'] == latest_year]
            st.subheader(f"Device Share in {latest_year}")
            fig_pie = px.pie(df_latest, names="Brands", values="total_users", hole=0.4)
            st.plotly_chart(fig_pie, use_container_width=True)

    # ---------- Case 3: Insurance Analysis ----------
    elif case_study == "Insurance Penetration and Growth Potential Analysis":
        df_ins = pd.read_sql(f"""
            SELECT Years, SUM(Insurance_count) AS total_policies,
                   SUM(Insurance_amount) AS total_amount
            FROM aggregated_insurance
            WHERE States = '{selected_state}'
            GROUP BY Years
            ORDER BY Years ASC;
        """, conn)

        if not df_ins.empty:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Insurance Policies Over Years")
                fig1 = px.line(df_ins, x="Years", y="total_policies", markers=True, title="Policy Count Trend")
                st.plotly_chart(fig1, use_container_width=True)

            with col2:
                st.subheader("Insurance Amount Over Years")
                fig2 = px.line(df_ins, x="Years", y="total_amount", markers=True, title="Insurance Amount Trend")
                st.plotly_chart(fig2, use_container_width=True)

            # ---------------- YoY Growth ----------------
            df_ins['policy_growth'] = df_ins['total_policies'].pct_change() * 100
            df_ins['amount_growth'] = df_ins['total_amount'].pct_change() * 100
            col3, col4 = st.columns(2)
            with col3:
                st.subheader("YoY Policy Growth (%)")
                fig3 = px.bar(df_ins, x="Years", y="policy_growth", text=df_ins['policy_growth'].round(2))
                st.plotly_chart(fig3, use_container_width=True)

            with col4:
                st.subheader("YoY Insurance Amount Growth (%)")
                fig4 = px.bar(df_ins, x="Years", y="amount_growth", text=df_ins['amount_growth'].round(2))
                st.plotly_chart(fig4, use_container_width=True)

    # ---------- Case 5: User Engagement ----------
    elif case_study == "User Engagement and Growth Strategy":
        df_map = pd.read_sql(f"""
            SELECT Years, SUM(RegisteredUser) AS total_users,
                   SUM(AppOpens) AS total_opens
            FROM map_user
            WHERE States = '{selected_state}'
            GROUP BY Years
            ORDER BY Years ASC;
        """, conn)

        if not df_map.empty:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Registered Users Over Years")
                fig1 = px.line(df_map, x="Years", y="total_users", markers=True, title="Registered Users Trend")
                st.plotly_chart(fig1, use_container_width=True)

            with col2:
                st.subheader("App Opens Over Years")
                fig2 = px.line(df_map, x="Years", y="total_opens", markers=True, title="App Opens Trend")
                st.plotly_chart(fig2, use_container_width=True)

            # ---------------- Engagement Ratio ----------------
            df_map['opens_per_user'] = df_map['total_opens'] / df_map['total_users']
            st.subheader("App Opens per User Over Years")
            fig3 = px.line(df_map, x="Years", y="opens_per_user", markers=True,
                           title="Engagement Ratio (Opens per User)")
            st.plotly_chart(fig3, use_container_width=True)

            # ---------------- Scatter: Users vs Opens ----------------
            st.subheader("User Engagement Scatter Plot")
            fig4 = px.scatter(df_map, x="total_users", y="total_opens", color="Years",
                              size="total_users", hover_name="Years", title="Users vs App Opens")
            st.plotly_chart(fig4, use_container_width=True)

    conn.close()

# ================= Analysis Page =================
# elif page == "Analysis":
#     st.title("Business Case Study")

#     # Dropdown for Case Studies
#     case_study = st.selectbox(
#         "Choose Case Study",
#         [
#             "Decoding Transaction Dynamics on PhonePe",
#             "Device Dominance and User Engagement Analysis",
#             "Insurance Penetration and Growth Potential Analysis",
#             "Transaction Analysis for Market Expansion",
#             "User Engagement and Growth Strategy"
#         ]
#     )

#     st.markdown(f"<h2 style='color:red;'>State-wise Transaction Analysis</h2>", unsafe_allow_html=True)

#     # Fetch states dynamically from DB
#     conn = get_connection()
#     cursor = conn.cursor()
#     cursor.execute("SELECT DISTINCT States FROM aggregated_transaction ORDER BY States ASC;")
#     states = [row[0] for row in cursor.fetchall()]
#     conn.close()

#     selected_state = st.selectbox("Choose a State:", states)

#     # ================= CASE STUDY LOGIC =================
#     conn = get_connection()

#     # ---------- Case 1 & 4: Transaction Trends ----------
#     if case_study in ["Decoding Transaction Dynamics on PhonePe", "Transaction Analysis for Market Expansion"]:
#         query = f"""
#             SELECT Years, SUM(Transaction_count) AS total_transactions,
#                    SUM(Transaction_amount) AS total_amount
#             FROM aggregated_transaction
#             WHERE States = '{selected_state}'
#             GROUP BY Years
#             ORDER BY Years ASC;
#         """
#         df = pd.read_sql(query, conn)

#         if not df.empty:
#             col1, col2 = st.columns(2)

#             with col1:
#                 st.subheader("Total Transactions Over Years")
#                 fig1 = px.line(df, x="Years", y="total_transactions", markers=True)
#                 st.plotly_chart(fig1, use_container_width=True)

#             with col2:
#                 st.subheader("Total Transaction Amount Over Years")
#                 fig2 = px.line(df, x="Years", y="total_amount", markers=True)
#                 st.plotly_chart(fig2, use_container_width=True)

#             # Payment category distribution (Pie charts)
#             query_cat = f"""
#                 SELECT Transaction_type, SUM(Transaction_count) AS total_count,
#                        SUM(Transaction_amount) AS total_amount
#                 FROM aggregated_transaction
#                 WHERE States = '{selected_state}'
#                 GROUP BY Transaction_type;
#             """
#             df_cat = pd.read_sql(query_cat, conn)

#             st.markdown("<h2 style='color:red;'>Payment Category Performance</h2>", unsafe_allow_html=True)
#             col3, col4 = st.columns(2)

#             with col3:
#                 st.subheader("Transaction Count Distribution")
#                 fig3 = px.pie(df_cat, names="Transaction_type", values="total_count", hole=0.4)
#                 st.plotly_chart(fig3, use_container_width=True)

#             # with col4:
#             #     st.subheader("Transaction Amount Distribution (in ₹)")
#             #     fig4 = px.pie(df_cat, names="Transaction_type", values="total_amount", hole=0.4)
#             #     st.plotly_chart(fig4, use_container_width=True)
#             with col4:
#                 st.subheader("Transaction Amount Distribution (in ₹)")
#                 fig4 = px.pie(df_cat, names="Transaction_type", values="total_amount", hole=0.4)
#                 st.plotly_chart(fig4, use_container_width=True)

#         # ----- Bar Chart for Payment Categories -----
#         st.subheader("Payment Categories Comparison (Bar Chart)")
#         fig_bar = px.bar(
#             df_cat,
#             x="Transaction_type",
#             y="total_amount",
#             hover_data=["total_count"],
#             barmode="group"
#         )
#         st.plotly_chart(fig_bar, use_container_width=True)


#     # ---------- Case 2: Device Dominance ----------
#     elif case_study == "Device Dominance and User Engagement Analysis":
#         query_user = f"""
#             SELECT Years, Brands, SUM(Transaction_count) AS total_users, AVG(Percentage) AS percentage
#             FROM aggregated_user
#             WHERE States = '{selected_state}'
#             GROUP BY Years, Brands
#             ORDER BY Years;
#         """
#         df_user = pd.read_sql(query_user, conn)

#         if not df_user.empty:
#             st.subheader("📱 Device-wise User Distribution")
#             fig = px.bar(df_user, x="Years", y="total_users", color="Brands", barmode="group")
#             st.plotly_chart(fig, use_container_width=True)

#             st.subheader("Device Share % Over Time")
#             fig2 = px.line(df_user, x="Years", y="percentage", color="Brands", markers=True)
#             st.plotly_chart(fig2, use_container_width=True)

#     # ---------- Case 3: Insurance Analysis ----------
#     elif case_study == "Insurance Penetration and Growth Potential Analysis":
#         query_ins = f"""
#             SELECT Years, SUM(Insurance_count) AS total_policies,
#                    SUM(Insurance_amount) AS total_amount
#             FROM aggregated_insurance
#             WHERE States = '{selected_state}'
#             GROUP BY Years
#             ORDER BY Years ASC;
#         """
#         df_ins = pd.read_sql(query_ins, conn)

#         if not df_ins.empty:
#             col1, col2 = st.columns(2)

#             with col1:
#                 st.subheader("Insurance Policies Over Years")
#                 fig = px.line(df_ins, x="Years", y="total_policies", markers=True)
#                 st.plotly_chart(fig, use_container_width=True)

#             with col2:
#                 st.subheader("Insurance Amount Over Years")
#                 fig2 = px.line(df_ins, x="Years", y="total_amount", markers=True)
#                 st.plotly_chart(fig2, use_container_width=True)

#     # ---------- Case 5: User Engagement ----------
#     elif case_study == "User Engagement and Growth Strategy":
#         query_map_user = f"""
#             SELECT Years, SUM(RegisteredUser) AS total_users,
#                    SUM(AppOpens) AS total_opens
#             FROM map_user
#             WHERE States = '{selected_state}'
#             GROUP BY Years
#             ORDER BY Years ASC;
#         """
#         df_map = pd.read_sql(query_map_user, conn)

#         if not df_map.empty:
#             col1, col2 = st.columns(2)

#             with col1:
#                 st.subheader("Registered Users Over Years")
#                 fig = px.line(df_map, x="Years", y="total_users", markers=True)
#                 st.plotly_chart(fig, use_container_width=True)

#             with col2:
#                 st.subheader("App Opens Over Years")
#                 fig2 = px.line(df_map, x="Years", y="total_opens", markers=True)
#                 st.plotly_chart(fig2, use_container_width=True)

#     conn.close()
