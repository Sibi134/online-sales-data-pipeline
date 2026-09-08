import streamlit as st
import pandas as pd
import requests
import time
import subprocess
import sys


# ========================================================
# Configuration
# ========================================================

API_URL = "http://127.0.0.1:8000"

st.set_page_config(
    page_title="Online Sales Analytics",
    page_icon="📊",
    layout="wide"
)


# ========================================================
# Custom Styling
# ========================================================

st.markdown("""
<style>

.main-title {
    font-size: 36px;
    font-weight: 700;
    margin-bottom: 5px;
}

.subtitle {
    color: #666;
    font-size: 16px;
    margin-bottom: 25px;
}

.section-title {
    font-size: 24px;
    font-weight: 600;
    margin-top: 20px;
    margin-bottom: 15px;
}

</style>
""", unsafe_allow_html=True)


# ========================================================
# Header
# ========================================================

st.markdown(
    '<div class="main-title">📊 Online Sales Analytics Dashboard</div>',
    unsafe_allow_html=True
)

st.markdown(
    '<div class="subtitle">'
    'ETL + EDA powered real-time sales analytics'
    '</div>',
    unsafe_allow_html=True
)


# ========================================================
# Load Latest Sales Data
# ========================================================

@st.cache_data(ttl=10)
def load_sales():

    response = requests.get(
        f"{API_URL}/sales",
        timeout=30
    )

    response.raise_for_status()

    return pd.DataFrame(response.json())


# ========================================================
# Check Airflow DAG Status
# ========================================================

def get_airflow_dag_status(run_id):

    airflow_bin = sys.executable.replace(
        "/python",
        "/airflow"
    )

    try:

        result = subprocess.run(
            [
                airflow_bin,
                "dags",
                "state",
                "online_sales_pipeline",
                run_id
            ],
            capture_output=True,
            text=True,
            timeout=10
        )

        output = (
            result.stdout + " " + result.stderr
        ).lower()

        if "success" in output:
            return "success"

        if "failed" in output:
            return "failed"

        if "running" in output:
            return "running"

        if "queued" in output:
            return "queued"

        return "unknown"

    except Exception:

        return "unknown"


# ========================================================
# Load Initial Data
# ========================================================

try:

    df = load_sales()

except Exception as e:

    st.error(
        f"Unable to connect to FastAPI: {e}"
    )

    st.stop()


# ========================================================
# Add New Sales Record
# ========================================================

st.markdown(
    '<div class="section-title">➕ Add New Sales Transaction</div>',
    unsafe_allow_html=True
)

with st.form("sales_form"):

    col1, col2, col3 = st.columns(3)

    with col1:

        transaction_id = st.number_input(
            "Transaction ID",
            min_value=1,
            step=1
        )

        sale_date = st.date_input(
            "Date"
        )

        product_category = st.selectbox(
            "Product Category",
            [
                "Electronics",
                "Home Appliances",
                "Clothing",
                "Books",
                "Beauty Products",
                "Sports"
            ]
        )

    with col2:

        product_name = st.text_input(
            "Product Name"
        )

        units_sold = st.number_input(
            "Units Sold",
            min_value=1,
            step=1
        )

        unit_price = st.number_input(
            "Unit Price",
            min_value=0.01,
            step=0.01
        )

    with col3:

        region = st.selectbox(
            "Region",
            [
                "North America",
                "Europe",
                "Asia"
            ]
        )

        payment_method = st.selectbox(
            "Payment Method",
            [
                "Credit Card",
                "Debit Card",
                "PayPal"
            ]
        )

        submit = st.form_submit_button(
            "🚀 Add & Process Data",
            use_container_width=True
        )


# ========================================================
# Submit New Record
# ========================================================

if submit:

    if not product_name.strip():

        st.error(
            "Please enter a Product Name."
        )

    else:

        payload = {

            "transaction_id": int(transaction_id),

            "date": str(sale_date),

            "product_category": product_category,

            "product_name": product_name,

            "units_sold": int(units_sold),

            "unit_price": float(unit_price),

            "region": region,

            "payment_method": payment_method
        }

        try:

            response = requests.post(
                f"{API_URL}/sales",
                json=payload,
                timeout=30
            )

            if response.status_code == 200:

                result = response.json()

                st.success(
                    f"✅ Transaction "
                    f"{result['transaction_id']} added successfully!"
                )

                st.info(
                    f"Total Revenue: "
                    f"${result['total_revenue']:,.2f} | "
                    f"Total Records: "
                    f"{result['total_records']}"
                )

                # ------------------------------------------------
                # Get Airflow Run ID
                # ------------------------------------------------

                airflow_info = result.get(
                    "airflow_pipeline",
                    {}
                )

                run_id = airflow_info.get(
                    "run_id"
                )

                triggered = airflow_info.get(
                    "triggered",
                    False
                )

                if triggered and run_id:

                    # ------------------------------------------------
                    # 10-Second Processing Timer
                    # ------------------------------------------------

                    st.markdown(
                        "### 🔄 Processing Pipeline"
                    )

                    progress_bar = st.progress(0)

                    status_text = st.empty()

                    for seconds in range(10):

                        remaining = 10 - seconds

                        status_text.info(
                            f"⏳ Airflow pipeline is processing... "
                            f"{remaining} seconds remaining"
                        )

                        progress_bar.progress(
                            (seconds + 1) / 10
                        )

                        time.sleep(1)

                    # ------------------------------------------------
                    # Check Airflow Status
                    # ------------------------------------------------

                    status_text.info(
                        "🔍 Checking Airflow pipeline status..."
                    )

                    pipeline_status = get_airflow_dag_status(
                        run_id
                    )

                    # ------------------------------------------------
                    # Continue checking if still running
                    # ------------------------------------------------

                    additional_wait = 0
                    max_additional_wait = 60

                    while (
                        pipeline_status in ["running", "queued"]
                        and additional_wait < max_additional_wait
                    ):

                        status_text.info(
                            "⏳ Pipeline is still running. "
                            "Waiting for completion..."
                        )

                        time.sleep(2)

                        additional_wait += 2

                        pipeline_status = get_airflow_dag_status(
                            run_id
                        )

                    # ------------------------------------------------
                    # Pipeline Successful
                    # ------------------------------------------------

                    if pipeline_status == "success":

                        progress_bar.progress(1.0)

                        status_text.success(
                            "✅ Airflow pipeline completed successfully!"
                        )

                        st.info(
                            "📊 Updating dashboard with "
                            "the processed data..."
                        )

                        # Clear cached data
                        load_sales.clear()

                        # Reload dashboard
                        time.sleep(1)

                        st.rerun()

                    # ------------------------------------------------
                    # Pipeline Failed
                    # ------------------------------------------------

                    elif pipeline_status == "failed":

                        progress_bar.progress(1.0)

                        status_text.error(
                            "❌ Airflow pipeline failed."
                        )

                        st.warning(
                            f"Pipeline run: {run_id}"
                        )

                    # ------------------------------------------------
                    # Unknown Status
                    # ------------------------------------------------

                    else:

                        status_text.warning(
                            "⚠️ Unable to confirm the final "
                            "Airflow status."
                        )

                        st.info(
                            f"Airflow Run ID: {run_id}"
                        )

                        # Refresh the dashboard anyway
                        load_sales.clear()

                        st.rerun()

                else:

                    st.warning(
                        "⚠️ Transaction was added, but the "
                        "Airflow pipeline was not triggered."
                    )

                    load_sales.clear()

                    st.rerun()

            else:

                try:

                    error_message = response.json()["detail"]

                except Exception:

                    error_message = response.text

                st.error(
                    f"❌ {error_message}"
                )

        except Exception as e:

            st.error(
                f"Unable to add record: {e}"
            )


# ========================================================
# KPI Section
# ========================================================

st.markdown(
    '<div class="section-title">📈 Key Performance Indicators</div>',
    unsafe_allow_html=True
)

total_revenue = df["Total Revenue"].sum()

total_units = df["Units Sold"].sum()

total_transactions = len(df)

average_transaction = (
    total_revenue / total_transactions
    if total_transactions > 0
    else 0
)


kpi1, kpi2, kpi3, kpi4 = st.columns(4)


with kpi1:

    st.metric(
        "💰 Total Revenue",
        f"${total_revenue:,.2f}"
    )


with kpi2:

    st.metric(
        "📦 Units Sold",
        f"{total_units:,}"
    )


with kpi3:

    st.metric(
        "🧾 Transactions",
        f"{total_transactions:,}"
    )


with kpi4:

    st.metric(
        "💳 Avg. Transaction",
        f"${average_transaction:,.2f}"
    )


# ========================================================
# Filters
# ========================================================

st.markdown(
    '<div class="section-title">🔎 Filters</div>',
    unsafe_allow_html=True
)

filter1, filter2, filter3 = st.columns(3)


with filter1:

    selected_region = st.multiselect(
        "Region",
        options=sorted(df["Region"].unique()),
        default=sorted(df["Region"].unique())
    )


with filter2:

    selected_category = st.multiselect(
        "Product Category",
        options=sorted(df["Product Category"].unique()),
        default=sorted(df["Product Category"].unique())
    )


with filter3:

    selected_payment = st.multiselect(
        "Payment Method",
        options=sorted(df["Payment Method"].unique()),
        default=sorted(df["Payment Method"].unique())
    )


filtered_df = df[
    df["Region"].isin(selected_region)
    &
    df["Product Category"].isin(selected_category)
    &
    df["Payment Method"].isin(selected_payment)
]


# ========================================================
# Charts
# ========================================================

st.markdown(
    '<div class="section-title">📊 Sales Analysis</div>',
    unsafe_allow_html=True
)

chart1, chart2 = st.columns(2)


# --------------------------------------------------------
# Revenue by Category
# --------------------------------------------------------

with chart1:

    revenue_category = (
        filtered_df
        .groupby("Product Category")["Total Revenue"]
        .sum()
        .sort_values(ascending=False)
    )

    st.subheader("Revenue by Product Category")

    st.bar_chart(
        revenue_category
    )


# --------------------------------------------------------
# Revenue by Region
# --------------------------------------------------------

with chart2:

    revenue_region = (
        filtered_df
        .groupby("Region")["Total Revenue"]
        .sum()
        .sort_values(ascending=False)
    )

    st.subheader("Revenue by Region")

    st.bar_chart(
        revenue_region
    )


chart3, chart4 = st.columns(2)


# --------------------------------------------------------
# Units Sold by Category
# --------------------------------------------------------

with chart3:

    units_category = (
        filtered_df
        .groupby("Product Category")["Units Sold"]
        .sum()
        .sort_values(ascending=False)
    )

    st.subheader("Units Sold by Category")

    st.bar_chart(
        units_category
    )


# --------------------------------------------------------
# Revenue by Payment Method
# --------------------------------------------------------

with chart4:

    revenue_payment = (
        filtered_df
        .groupby("Payment Method")["Total Revenue"]
        .sum()
        .sort_values(ascending=False)
    )

    st.subheader("Revenue by Payment Method")

    st.bar_chart(
        revenue_payment
    )


# ========================================================
# Data Preview
# ========================================================

st.markdown(
    '<div class="section-title">📋 Latest Sales Data</div>',
    unsafe_allow_html=True
)

st.dataframe(
    filtered_df,
    use_container_width=True,
    hide_index=True
)


# ========================================================
# Refresh
# ========================================================

if st.button(
    "🔄 Refresh Dashboard",
    use_container_width=True
):

    load_sales.clear()

    st.rerun()