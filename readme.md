# Data Engineering ETL Pipeline

## Dataset

This project demonstrates an ETL (Extract, Transform, Load) pipeline using Python to process daily market data for 250 global companies.
The raw input file is `financial_data_250.csv`, which contains company-level identifiers and OHLCV price history for the most recent 3 years, along with revenue and other KPIs.

### Data Dictionary

| Column Name | Data Type | Description |
| --- | --- | --- |
| Company Name | string | Name of the company |
| Ticker | string | Ticker symbol of the company |
| Country | string | Country of the company |
| Industry | string | Industry of the company |
| Year | int | Year of the observation |
| Revenue | float | Revenue of the company |
| Revenue Unit | string | Unit of the revenue |
| Date | datetime | Date of the observation |
| Year_Price | int | Year of the price observation |
| Open | float | Opening price of the company |
| High | float | Highest price of the company |
| Low | float | Lowest price of the company |
| Close | float | Closing price of the company |
| Volume | float | Volume of the company |
| Adj Close | float | Adjusted closing price of the company |


## Pipeline Components

- **Extract**: Read `financial_data_250.csv` using `ETL_demo.ipynb` into a pandas DataFrame.

### Transform steps in this project

1. **Data quality assessment** – Inspect schema (`info()`), basic statistics, missing values and ~9.7k duplicate rows to understand data issues before transforming.
2. **Data cleaning** – Drop duplicate rows, fill the reporting `Year` from `Year_Price`, and cast `Date` to a proper `datetime` type so that each observation is consistent and time-indexed.
3. **Outlier removal (IQR)** – Apply the IQR method on numeric price/volume columns (`Open`, `High`, `Low`, `Close`, `Adj Close`, `Volume`) to remove extreme values that could distort models and summary statistics.
4. **Normalization and scaling** – Use `MinMaxScaler` on the same numeric columns to create scaled versions (`*_Scaled`) in the [0, 1] range, which is important for distance-based and gradient-based ML models.
5. **Time-series feature engineering** – Sort by `Ticker` and `Date`, then create:
   - `Lag_1_Close_Scaled`: previous-day scaled close per ticker
   - `SMA_10` and `SMA_30`: 10- and 30-day rolling averages of `Close_Scaled` per ticker
   - `Close_Scaled_Diff`: first difference of `Close_Scaled` per ticker for stationarity.
6. **Decomposition & stationarity diagnostics (single ticker)** – For one sample ticker, decompose `Close_Scaled` into `Trend`, `Seasonality`, and `Residual` using `seasonal_decompose`, and run Augmented Dickey–Fuller (ADF) tests on the original and differenced series.
7. **Final structured output** – Drop rows with missing values in key model features (`Close_Scaled`, `Close_Scaled_Diff`, `Lag_1_Close_Scaled`, `SMA_10`, `SMA_30`)

## Technologies Used

- Python 3.x
- pandas for data manipulation
- yfinance for data extraction
- numpy for numerical operations
- matplotlib for data visualization
- Git for version control

## reference:
1. [unlocking-financial-data-cleaning-preprocessing-guide](https://www.pyquantnews.com/free-python-resources/unlocking-financial-data-cleaning-preprocessing-guide)
2. [Yahoo Finance](https://finance.yahoo.com)
3. [Kaggle Stock Market Dataset](https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset/data)
