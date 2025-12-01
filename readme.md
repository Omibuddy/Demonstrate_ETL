# Data Engineering ETL Pipeline

## Dataset

This project demonstrates an ETL (Extract, Transform, Load) pipeline using Python to process financial data from a public source.
The dataset used is the Global Financial 250 dataset, containing financial information for 250 companies.
The data includes company names, countries, industries, and financial metrics for the most recent 3 years of data, along with additional KPIs such as revenue, profit margins, and market capitalization.

### KPIs

Company Name,Ticker,Country,Industry,Year,Revenue,Revenue Unit,Date,Year_Price,Open,High,Low,Close,Volume,Adj Close

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

- **Extract**: Data extraction from the CSV file using pandas
- **Transform**: Data cleaning, normalization, and feature engineering
- **Load**: Output to a structured format for analysis

### Transform steps in this project

1. **Data quality assessment** – Inspect schema, basic statistics, missing values and duplicates to understand data issues before transforming.
2. **Handling missing values and duplicates** – Fill the reporting `Year` from `Year_Price` and remove duplicate rows so that each observation is consistent and counted once.
3. **Outlier removal** – Apply the IQR method on price and volume columns to remove extreme values that could distort models and summary statistics.
4. **Normalization and scaling** – Use Min–Max scaling on price and volume-related columns to bring them to a common [0, 1] range, which is important for distance-based and gradient-based ML models.
5. **Feature engineering** – Create lag features, rolling averages (10-day and 30-day SMAs) and first differences of the scaled close price to capture trend, momentum and short-term dynamics.
6. **Final structured output** – Save the cleaned, scaled and feature-enriched time series as `engineered_financial_data_250.csv`, which serves as the input for downstream forecasting, prediction and ML model training.

### Output

`engineered_financial_data_250.csv` is the final output of the ETL pipeline, which contains the cleaned, scaled and feature-enriched time series data for 250 companies.

## Technologies Used

- Python 3.x
- pandas for data manipulation
- CSV file handling
- Git for version control
