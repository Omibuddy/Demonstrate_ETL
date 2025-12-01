import yfinance as yf
import pandas as pd
import numpy as np

tickers_250 = [
    'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'NVDA', 'BRK-B', 'TSLA', 'JPM', 'JNJ',
    'V', 'WMT', 'PG', 'XOM', 'HD', 'MA', 'UNH', 'CVX', 'LLY', 'KO',
    'MRK', 'PEP', 'ABBV', 'PFE', 'ADBE', 'CSCO', 'TMO', 'AVGO', 'COST', 'ACN',
    'DIS', 'CMCSA', 'NFLX', 'WFC', 'DHR', 'NKE', 'MCD', 'NEE', 'INTC', 'CRM',
    'AMD', 'TXN', 'ORCL', 'HON', 'SBUX', 'UPS', 'CAT', 'IBM', 'SPGI', 'AMGN',
    'GILD', 'PM', 'LOW', 'MDLZ', 'BKNG', 'TGT', 'LMT', 'GE', 'SCHW', 'SYK',
    'FIS', 'MU', 'ZTS', 'APL', 'KHC', 'PNC', 'CVS', 'C', 'MO', 'CB',
    'AXP', 'BSX', 'SO', 'BA', 'RTX', 'GM', 'F', 'DG', 'CL', 'MMM',
    'EOG', 'OXY', 'HAL', 'SLB', 'VLO', 'MPC', 'PSX', 'COP', 'KMI', 'WMB',
    'EBAY', 'PYPL', 'ADSK', 'SNAP', 'SQ', 'SHOP', 'UBER', 'LYFT', 'RIVN', 'LCID',
    'TWTR', 'ZM', 'PTON', 'ROKU', 'ETSY', 'DD', 'DOW', 'ECL', 'FCX', 'LIN',
    'MOS', 'PPG', 'SHW', 'APD', 'CE', 'IFF', 'LYB', 'ALB', 'FMC', 'CF',
    'DE', 'MMM', 'ITW', 'PH', 'AOS', 'SWK', 'HII', 'NOC', 'GD', 'LMT',
    'BA', 'RTX', 'HWM', 'TXT', 'TDY', 'ETN', 'ROP', 'IR', 'EMR', 'AAL',
    'UAL', 'DAL', 'LUV', 'ALK', 'JBLU', 'SAVE', 'SKYW', 'MESA', 'HA', 'BLU',
    'WYNN', 'LVS', 'MGM', 'MAR', 'HLT', 'IHG', 'CHH', 'SIX', 'EPR', 'FUN',
    'RE', 'AON', 'MMC', 'AIG', 'TRV', 'ALL', 'MET', 'PRU', 'LNC', 'DFS',
    'MAA', 'EQIX', 'AMT', 'PLD', 'O', 'SPG', 'PSA', 'ARE', 'VTR', 'AVB',
    'CPRT', 'ANET', 'DXCM', 'MCHP', 'ADI', 'KLAC', 'LRCX', 'TEL', 'QRVO', 'MRVL',
    'PAYX', 'ADP', 'MSI', 'LDOS', 'TROW', 'NDAQ', 'CME', 'ICE', 'CBOE', 'MCO',
    'TFC', 'KEY', 'HBAN', 'CFG', 'FITB', 'ZION', 'RF', 'SIVB', 'ALLY', 'STT',
    'CARR', 'OTIS', 'FTV', 'TT', 'AFL', 'CINF', 'WRB', 'RE', 'AON', 'MMC',
    'AIG', 'TRV', 'ALL', 'MET', 'PRU', 'LNC', 'DFS', 'HIG', 'CBOE', 'MCO',
    'BLK', 'COF', 'BEN', 'GS', 'MS', 'SCHW', 'FIS', 'MU', 'ZTS', 'APL',
    'KHC', 'PNC', 'CVS', 'C', 'MO', 'CB', 'AXP', 'BSX', 'SO', 'BA',
    'RTX', 'GM', 'F', 'DG', 'CL', 'MMM', 'EOG', 'OXY', 'HAL', 'SLB'
]
# Ensure we strictly cap at 250, though the list above is already 250
tickers = tickers_250[:250]
num_companies = len(tickers)


def collect_SF_data(tickers, start_date, end_date):
    
    all_data = []
    
    print(f"Starting data collection for {len(tickers)} companies...")
    
    for i, ticker in enumerate(tickers):
        stock_data = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=False)

        ticker_info = yf.Ticker(ticker).info
            
        # KPI 1: Company Name, KPI 3: Country, KPI 4: Industry
        company_name = ticker_info.get('longName', f'{ticker} Company')
        country = ticker_info.get('country', 'USA (S&P Proxy)') # Default to USA as S&P is US-centric
        industry = ticker_info.get('sector', ticker_info.get('industry', 'N/A'))
            
        # KPI 6: Revenue, KPI 7: Revenue Unit, KPI 5: Year
        last_annual_revenue = ticker_info.get('totalRevenue', np.nan) 
        revenue_unit = 'USD'
        revenue_year = ticker_info.get('fiscalYearEnd', 'N/A') # Proxy for the year of the annual report
            
        stock_data.reset_index(inplace=True)
        stock_data['Date'] = stock_data['Date'].dt.date # Keep date clean
            
        stock_data['Company Name'] = company_name
        stock_data['Ticker'] = ticker
        stock_data['Country'] = country
        stock_data['Industry'] = industry
        stock_data['Year'] = revenue_year # Financial Report Year
        stock_data['Revenue'] = last_annual_revenue
        stock_data['Revenue Unit'] = revenue_unit
        stock_data['Year_Price'] = stock_data['Date'].apply(lambda x: x.year) # Daily Price Year

        stock_data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 
                              'Company Name', 'Ticker', 'Country', 'Industry', 
                              'Year', 'Revenue', 'Revenue Unit', 'Year_Price']
            
        final_columns = [
                'Company Name', 'Ticker', 'Country', 'Industry', 'Year',
                'Revenue', 'Revenue Unit', 'Date', 'Year_Price', 
                'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close'
            ]
            
        all_data.append(stock_data[final_columns])

    if not all_data:
        return pd.DataFrame()

    final_df = pd.concat(all_data, ignore_index=True)
    return final_df.sort_values(by=['Ticker', 'Date'])


start_date = '2022-01-01'
end_date = '2025-01-01' 

stock_df_250 = collect_SF_data(tickers, start_date, end_date)

if not stock_df_250.empty:
    print(stock_df_250.head())
    print(f"Total Rows (Daily Observations): {stock_df_250.shape[0]}")
    print(f"Total Columns: {stock_df_250.shape[1]}")
    print(f"Unique tickers Collected: {stock_df_250['Ticker'].nunique()}")
    
    output_file = "financial_data_250.csv"
    stock_df_250.to_csv(output_file, index=False)
    print(f"Data saved to '{output_file}'")