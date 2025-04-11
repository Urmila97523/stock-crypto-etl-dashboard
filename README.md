# stock-crypto-etl-dashboard
# 📊 Stock & Crypto Market Insights Dashboard

An end-to-end data analytics project that extracts, transforms, and visualizes stock, crypto, and weather data using Python and Power BI.

## Project Overview

This project demonstrates how to build a complete ETL (Extract, Transform, Load) pipeline using Python and visualize insights in Power BI. It integrates with real-time APIs to fetch data and stores it in a MySQL database. The Power BI dashboard presents key performance indicators and trend insights across stock and crypto markets.

## Components

### Python ETL Script (`myapp1.py`)
- Extracts:
  - **Crypto data** from CoinGecko API
  - **Stock data** from Alpha Vantage API
  - **Weather data** from OpenWeatherMap API
- Loads cleaned data into **MySQL**
- Uses retry mechanisms for reliable data fetching

###  Power BI Dashboard (`market_insights_dashboard.pbix`)
- Tracks:
  - Top stock gainers and losers
  - Daily closing price trends
  - Top 10 cryptos by market cap
  - Crypto ATH/ATL comparison
  - Daily weather summaries
- Includes KPIs, conditional formatting, slicers, and dynamic visuals

##  Dashboard Highlights

- 📈 Daily stock and crypto price trend lines
- 💹 Top Gainers/Losers with conditional color formatting
- 🏆 KPI cards (e.g., Total Volume, Average Price, Top Gainer %)
- 📉 Market cap and volatility comparisons
- 🌦️ Weather metrics for selected cities

##  Tech Stack

- **Python** (requests, pandas, mysql-connector)
- **APIs**: CoinGecko, Alpha Vantage, OpenWeatherMap
- **MySQL** for data storage
- **Power BI** for interactive dashboard creation

##  Files

- `myapp1.py` – Python ETL script
- `market_insights_dashboard.pbix` – Power BI dashboard file
- `README.md` – Project overview and setup

##  How to Use

1. Clone this repository:
   ```
   git clone https://github.com/Urmila97523/market-insights-dashboard.git
   ```

2. Set up MySQL with the required tables: `crypto_data`, `stock_data`, and `weather_data`.

3. Run `myapp1.py` to fetch and load data.

4. Open `market_insights_dashboard.pbix` in Power BI Desktop and connect to your MySQL data.

## 👩‍💻 Author

**Urmila Aglecha**  
_Data Analyst | Python & Power BI Enthusiast_  
[LinkedIn](https://www.linkedin.com/) • [GitHub](https://github.com/Urmila97523)
