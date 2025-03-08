from datetime import timedelta

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
from plotly.subplots import make_subplots

# Page configuration for better appearance
st.set_page_config(
    page_title="Yield Curve Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Backend API URL (update if deployed)
# API_BASE = "http://localhost:8000"
API_BASE = " https://yieldcurveanalysis-production.up.railway.app"


# Function to fetch yield curves
@st.cache_data(ttl=3600)  # Cache data for 1 hour
def fetch_yield_curves():
    """Get all yield curve data from FastAPI"""
    try:
        response = requests.get(f"{API_BASE}/yields")
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"API Error: {response.status_code}")
            return []
    except Exception as e:
        st.error(f"Connection Error: {e}")
        return []


# Function to format date
def format_date(date_obj, format_type="full"):
    if format_type == "full":
        return date_obj.strftime("%B %d, %Y")
    elif format_type == "month":
        return date_obj.strftime("%B %Y")
    return date_obj.strftime("%Y-%m-%d")


# Function to display metrics
def display_metrics(data):
    if data.empty:
        return

    # Get latest data
    latest = data.iloc[0]

    # Create metrics row
    col1, col2, col3, col4 = st.columns(4)

    # 10-year yield
    with col1:
        st.metric(label="10 Year Yield", value=f"{latest['10y']:.2f}%")

    # 2-10 spread
    with col2:
        spread = latest["10y"] - latest["2y"]
        st.metric(label="2-10 Spread", value=f"{spread:.2f}%")

    # 3-month yield
    with col3:
        st.metric(label="3 Month Yield", value=f"{latest['3m']:.2f}%")

    # Date
    with col4:
        st.metric(label="Latest Data", value=format_date(latest["date"]))


# Function to create yields chart
def create_yield_chart(data, date=None, mode="single"):
    # Colors for the chart - using default Plotly colors

    if mode == "single" and date is not None:
        selected_data = data[data['date'] == pd.to_datetime(date)].iloc[0]
        maturities = ['3m', '6m', '1y', '2y', '3y', '5y', '7y', '10y', '30y']
        valid_maturities = []
        valid_yields = []

        for m in maturities:
            if pd.notna(selected_data[m]):
                valid_maturities.append(m)
                valid_yields.append(selected_data[m])

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=valid_maturities,
                y=valid_yields,
                mode='lines+markers',
                line=dict(width=3),
                marker=dict(size=8)
            )
        )

        fig.update_layout(
            title=f"Yield Curve for {format_date(pd.to_datetime(date))}",
            xaxis_title="Maturity",
            yaxis_title="Yield (%)",
            height=500,
            hovermode="x unified"
        )
    else:
        # Time series for multiple dates
        fig = go.Figure()
        maturities = ['3m', '6m', '1y', '2y', '3y', '5y', '7y', '10y', '30y']

        for maturity in maturities:
            fig.add_trace(
                go.Scatter(
                    x=data['date'],
                    y=data[maturity],
                    mode='lines',
                    name=maturity,
                    line=dict(width=2)
                )
            )

        fig.update_layout(
            title="Treasury Yields Over Time",
            xaxis_title="Date",
            yaxis_title="Yield (%)",
            height=500,
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )

    return fig


# Function to find previous valid date
def find_previous_valid_date(df, target_date):
    if not isinstance(target_date, pd.Timestamp):
        target_date = pd.Timestamp(target_date)

    available_dates = df['date'].sort_values(ascending=False)

    for date in available_dates:
        if date <= target_date:
            return date

    return None


# Create yield curve subplot grid
def create_yield_grid(df, view_option):
    if df.empty:
        return None

    # Sort in descending order (newest to oldest)
    df = df.sort_values('date', ascending=False)

    # Limit to first 18 entries for better visualization
    df = df.head(18)

    # Number of entries to display
    n_entries = len(df)

    # Create subplot titles
    format_str = '%B %Y' if view_option == "Monthly" else '%Y-%m-%d'
    subplot_titles = [d.strftime(format_str) for d in df['date']]

    # Calculate grid dimensions based on number of entries
    n_cols = 3  # Fixed number of columns
    n_rows = (n_entries + n_cols - 1) // n_cols  # Calculate rows needed

    # Make sure n_rows is at least 1 to prevent empty grid
    if n_rows < 1:
        n_rows = 1

    # Create subplot grid
    fig = make_subplots(
        rows=n_rows,
        cols=min(n_cols, n_entries),
        subplot_titles=subplot_titles,
        shared_yaxes=True,
        vertical_spacing=0.1
    )

    maturities = ['3m', '6m', '1y', '2y', '3y', '5y', '7y', '10y', '30y']

    # Determine global y-axis range for consistency
    all_yields = []
    for _, row in df.iterrows():
        for m in maturities:
            if pd.notna(row[m]):
                all_yields.append(row[m])

    if all_yields:
        global_min = min(all_yields) - 0.3
        global_max = max(all_yields) + 0.3
    else:
        global_min, global_max = 0, 5  # Default range if no data

    # Plot each curve
    for i, row in enumerate(df.to_dict('records')):
        # Calculate row and column position (1-indexed for plotly)
        row_num = (i // n_cols) + 1
        col_num = (i % n_cols) + 1

        # Make sure we don't exceed grid dimensions
        if row_num <= n_rows and col_num <= n_cols:
            # Create valid maturity and yield pairs, filtering out NaNs
            valid_maturities = []
            valid_yields = []

            for m in maturities:
                if m in row and pd.notna(row[m]):
                    valid_maturities.append(m)
                    valid_yields.append(row[m])

            # Only add trace if we have valid data
            if valid_yields:
                date_obj = pd.to_datetime(row['date'])

                # Determine curve color based on shape (inversion check)
                is_inverted = False
                if '2y' in valid_maturities and '10y' in valid_maturities:
                    idx_2y = valid_maturities.index('2y')
                    idx_10y = valid_maturities.index('10y')
                    is_inverted = valid_yields[idx_2y] > valid_yields[idx_10y]

                # Use standard colors - red for inverted, blue for normal
                line_color = 'red' if is_inverted else 'blue'

                fig.add_trace(
                    go.Scatter(
                        x=valid_maturities,
                        y=valid_yields,
                        mode='lines+markers',
                        line=dict(color=line_color, width=2),
                        marker=dict(size=6),
                        name=date_obj.strftime('%Y-%m-%d')
                    ),
                    row=row_num,
                    col=col_num
                )

                # Set y-axis range based on global min/max
                fig.update_yaxes(
                    range=[global_min, global_max],
                    row=row_num,
                    col=col_num
                )

    # Update layout with standard Plotly theme
    fig.update_layout(
        height=250 * n_rows,  # Adjust height based on number of rows
        title_text=f"{view_option} Yield Curves (Descending Order)",
    )

    return fig


# Main app logic
with st.spinner("Loading yield curve data..."):
    # Fetch data
    data = fetch_yield_curves()

    if not data:
        st.error("No data available. Please check your API connection.")
        st.stop()

    df = pd.DataFrame(data)

    # Process data
    if not df.empty:
        # Convert dates to datetime
        df['date'] = pd.to_datetime(df['date'])

        # Sort by date descending for most recent data first
        df = df.sort_values('date', ascending=False)

# Create a horizontal navbar with standard radio buttons
selected_page = st.radio(
    "Dashboard Navigation",
    ["Yield Curve Analysis", "Monthly Descending Yield Curves"],
    horizontal=True
)

# Page 1: Yield Curve Analysis
if selected_page == "Yield Curve Analysis":
    st.title("Yield Curve Analysis 📈")
    st.write("Analyze historical yield curves with filtering and aggregation")

    # Display key metrics
    display_metrics(df)

    # Create tabs for different views
    tab1, tab2 = st.tabs(["Single Date View", "Time Series View"])

    with tab1:
        # Date Filter
        min_date = df['date'].min().date()
        max_date = df['date'].max().date()
        col1, col2 = st.columns([3, 1])

        with col1:
            selected_date = st.date_input(
                "Select Date",
                max_date,
                min_value=min_date,
                max_value=max_date
            )

        with col2:
            st.write("")  # Spacer
            show_data = st.checkbox("Show Data Table", value=False)

        # Create single date chart
        fig = create_yield_chart(df, selected_date, mode="single")
        st.plotly_chart(fig, use_container_width=True)

        # Show data table if requested
        if show_data:
            filtered_data = df[df['date'] == pd.to_datetime(selected_date)]
            if not filtered_data.empty:
                # Transpose the data for better display
                display_cols = ['date', '3m', '6m', '1y', '2y', '3y', '5y', '7y', '10y', '30y']
                transposed = filtered_data[display_cols].T.reset_index()
                transposed.columns = ['Maturity', 'Yield (%)']
                transposed = transposed.iloc[1:]  # Remove date row

                st.dataframe(
                    transposed,
                    hide_index=True,
                    use_container_width=True
                )

    with tab2:
        # Date range selection
        col1, col2, col3 = st.columns([2, 2, 1])

        with col1:
            start_date = st.date_input(
                "Start Date",
                min_date + timedelta(days=180),  # Default to 6 months from earliest
                min_value=min_date,
                max_value=max_date
            )

        with col2:
            end_date = st.date_input(
                "End Date",
                max_date,
                min_value=min_date,
                max_value=max_date
            )

        with col3:
            st.write("")  # Spacer
            show_ts_data = st.checkbox("Show Time Series Data", value=False)

        # Filter data by date range
        filtered_df = df[(df['date'].dt.date >= start_date) & (df['date'].dt.date <= end_date)]

        if not filtered_df.empty:
            # Create time series chart
            fig = create_yield_chart(filtered_df, mode="time_series")
            st.plotly_chart(fig, use_container_width=True)

            # Show data table if requested
            if show_ts_data:
                st.dataframe(
                    filtered_df.sort_values('date', ascending=False),
                    hide_index=True,
                    use_container_width=True
                )
        else:
            st.warning("No data available for the selected date range.")

# Page 2: Monthly Descending Yield Curves
elif selected_page == "Monthly Descending Yield Curves":
    st.title("Monthly Descending Yield Curves 📉")
    st.write("View yield curves for each month in descending order")

    # Display key metrics
    display_metrics(df)

    # Filter Options
    col1, col2, col3, col4 = st.columns([1.5, 1.5, 1.5, 1])

    with col1:
        view_option = st.selectbox(
            "View By",
            ["Monthly", "Weekly", "Daily"],
            help="Select time period grouping"
        )

    with col2:
        start_date = st.date_input(
            "Start Date",
            df['date'].min().date() + timedelta(days=180),  # Start 6 months from earliest
            min_value=df['date'].min().date(),
            max_value=df['date'].max().date()
        )

    with col3:
        end_date = st.date_input(
            "End Date",
            df['date'].max().date(),
            min_value=df['date'].min().date(),
            max_value=df['date'].max().date()
        )

    with col4:
        st.write("")  # Spacer for alignment
        show_grid_data = st.checkbox("Show Grid Data", value=False)

    # Filter data by date range
    masked_df = df[(df['date'].dt.date >= start_date) & (df['date'].dt.date <= end_date)]

    # Message if no data in range
    if masked_df.empty:
        st.warning("No data available for the selected date range.")
    else:
        # Group and aggregate based on view option
        if view_option == "Monthly":
            # Generate all months in the range
            all_months = pd.date_range(start=start_date, end=end_date, freq='M')
            grouped_data = []

            for month_end in all_months:
                valid_date = find_previous_valid_date(masked_df, month_end)
                if valid_date is not None:
                    row = masked_df[masked_df['date'] == valid_date].iloc[0]
                    grouped_data.append(row)

            grouped_df = pd.DataFrame(grouped_data) if grouped_data else pd.DataFrame()

        elif view_option == "Weekly":
            # Generate all weeks in the range
            all_weeks = pd.date_range(start=start_date, end=end_date, freq='W')
            grouped_data = []

            for week_end in all_weeks:
                valid_date = find_previous_valid_date(masked_df, week_end)
                if valid_date is not None:
                    row = masked_df[masked_df['date'] == valid_date].iloc[0]
                    grouped_data.append(row)

            grouped_df = pd.DataFrame(grouped_data) if grouped_data else pd.DataFrame()

        else:  # Daily
            grouped_df = masked_df.copy()

        # Create visualization if we have data
        if not grouped_df.empty:
            # Create the grid visualization
            fig = create_yield_grid(grouped_df, view_option)
            if fig:
                st.plotly_chart(fig, use_container_width=True)

                # Simple explanation about colors
                st.info(
                    "Red curves indicate an inverted yield curve (2-year yield > 10-year yield), which is often considered a recession indicator. Blue curves represent normal yield curves.")

                # Show data table if requested
                if show_grid_data:
                    # Format the date column for better readability
                    display_df = grouped_df.copy()
                    display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')

                    st.dataframe(
                        display_df.sort_values('date', ascending=False),
                        hide_index=True,
                        use_container_width=True
                    )
            else:
                st.error("Failed to create visualization. Check data structure.")
        else:
            st.warning(f"No data available for {view_option.lower()} view in the selected date range.")

# Simple footer
st.caption("Yield Curve Dashboard • Updated with latest market data")
