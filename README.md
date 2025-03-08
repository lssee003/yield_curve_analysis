# 📈 Yield Curve Analysis Dashboard

**Can you predict a recession?** The yield curve has been one of the most reliable indicators of economic downturns.
This project helps you analyze historical yield curve data to understand trends, identify inversions, and make informed
predictions about the economy.

---

## 🚀 **Purpose**

This project provides a **Streamlit-based dashboard** to visualize and analyze historical yield curve data. It fetches
data from the U.S. Treasury, stores it in a **Supabase database**, and presents it in an interactive and user-friendly
interface.

---

## 🛠️ **Tech Stack**

- **Frontend**: Streamlit
- **Backend**: FastAPI
- **Database**: Supabase (PostgreSQL)
- **Deployment**: Railway
- **Workflow Automation**: GitHub Actions
- **Data Source**: U.S. Treasury API

---

## 🌟 **Features**

1. **Yield Curve Visualization**:
    - View yield curves for specific dates.
    - Analyze trends over time with interactive charts.

2. **Key Metrics**:
    - Display key metrics like 10-year yield, 2-10 spread, and 3-month yield.

3. **Monthly Yield Curves**:
    - Compare yield curves for different months in descending order.

4. **Recession Indicator**:
    - Identify inverted yield curves, a potential indicator of an upcoming recession.

---

## 🧰 **How It Works**

1. **Data Fetching**:
    - The backend fetches yield curve data from the U.S. Treasury API.
    - Data is parsed and stored in a **Supabase database**.

2. **Dashboard**:
    - The Streamlit frontend connects to the backend API to fetch and display data.
    - Users can interact with the dashboard to explore historical yield curves.

3. **Automation**:
    - A GitHub Actions workflow runs weekly to update the database with the latest yield curve data.

---

## 🌐 **Website**

Check out the live dashboard here: [Yield Curve Analysis Dashboard](https://yieldcurveapp-production.up.railway.app/)