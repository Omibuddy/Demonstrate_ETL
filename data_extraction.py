import yfinance as yf
import pandas as pd
import time
from datetime import datetime

# --- CONFIGURATION ---
# We use a mix of S&P 500, Nasdaq, and Global ADRs to reach ~250 companies.
tickers = [
    # TECH & COMMUNICATION
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "AVGO", "CSCO", "ORCL",
    "ADBE", "CRM", "AMD", "INTC", "QCOM", "TXN", "IBM", "NOW", "INTU", "UBER",
    "ABNB", "FI", "MU", "ADI", "LRCX", "PANW", "SNPS", "CDNS", "KLAC", "ROP",
    "APH", "MCHP", "TEL", "TDY", "ANSS", "IT", "CDW", "KEYS", "FTNT", "NET",
    "ZM", "WDAY", "TEAM", "DDOG", "ZS", "CRWD", "PLTR", "SHOP", "SQ", "ROKU",
    "SPOT", "SNAP", "TWLO", "DOCU", "OKTA", "MDB", "ZI", "U", "PATH", "GTLB",
    
    # FINANCE & BANKING
    "JPM", "BAC", "WFC", "C", "GS", "MS", "BLK", "SCHW", "AXP", "SPGI",
    "V", "MA", "PYPL", "COF", "USB", "PNC", "TFC", "BK", "STT", "HIG",
    "ALL", "TRV", "CB", "MMC", "AON", "AJG", "ICE", "CME", "MCO", "NDAQ",
    "BRK-B", "PGR", "MET", "PRU", "AIG", "ACGL", "WRB", "L", "CINF", "PFG",
    "HSBC", "RY", "TD", "BMO", "BNS", "UBS", "DB", "MUFG", "SMFG", "BCS",

    # HEALTHCARE & PHARMA
    "LLY", "UNH", "JNJ", "MRK", "ABBV", "TMO", "PFE", "ABT", "DHR", "BMY",
    "AMGN", "ELV", "CVS", "CI", "GILD", "ISRG", "SYK", "REGN", "VRTX", "ZTS",
    "BDX", "BSX", "HUM", "MCK", "COR", "HCA", "CNC", "IQV", "A", "RMD",
    "EW", "BAX", "BIIB", "MTD", "STE", "COO", "WAT", "HOLX", "IDXX", "DXCM",
    "NVO", "NVS", "AZN", "SNY", "GSK", "TAK", "RHHBY", "BAYRY", "TEVA", "BNTX",

    # CONSUMER & RETAIL
    "WMT", "PG", "COST", "KO", "PEP", "HD", "MCD", "NKE", "SBUX", "LOW",
    "TGT", "TJX", "EL", "MDLZ", "PM", "MO", "CL", "KMB", "GIS", "SYY",
    "STZ", "MNST", "K", "HSY", "KHC", "ADM", "TSN", "CAG", "CPB", "MKC",
    "LULU", "MAR", "HLT", "BKNG", "EXPE", "RCL", "CCL", "YUM", "CMG", "DRI",
    "DEO", "UL", "BUD", "NSRGY", "LVMUY", "HESAY", "ADDYY", "SNE", "TM", "HMC",

    # INDUSTRIAL & ENERGY
    "XOM", "CVX", "COP", "SLB", "EOG", "MPC", "PSX", "VLO", "OXY", "KMI",
    "WMB", "HAL", "BKR", "DVN", "HES", "FANG", "MRO", "CTRA", "APA", "EQT",
    "GE", "CAT", "DE", "HON", "UNP", "UPS", "RTX", "LMT", "BA", "MMM",
    "ETN", "ITW", "WM", "EMR", "PH", "GD", "NOC", "FDX", "NSC", "CSX",
    "BP", "SHEL", "TTE", "EQNR", "E", "RIO", "BHP", "VALE", "SCCO", "FCX",

    # MATERIALS, UTILITIES, REAL ESTATE
    "LIN", "SHW", "DD", "APD", "ECL", "NEM", "DOW", "CTVA", "PPG", "ALB",
    "NEE", "DUK", "SO", "D", "AEP", "SRE", "EXC", "XEL", "PEG", "ED",
    "PLD", "AMT", "EQIX", "CCI", "PSA", "O", "SPG", "VICI", "DLR", "AVB",
    "WELL", "CBRE", "CSGP", "INVH", "MAA", "ESS", "UDR", "KIM", "REG", "FRT"
]

# Ensure we don't exceed your request or have duplicates
unique_tickers = list(set(tickers))[:250] 
print(f"Starting Data Extraction for {len(unique_tickers)} companies...")

dataset = []

for i, ticker in enumerate(unique_tickers):
    try:
        # Rate limiting to be polite to the API
        if i % 10 == 0:
            print(f"Processing... ({i}/{len(unique_tickers)})")
            time.sleep(1) 

        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Basic Info
        name = info.get('longName', ticker)
        country = info.get('country', 'N/A')
        industry = info.get('industry', 'N/A')
        currency = info.get('currency', 'USD')

        # Financial DataFrames
        income_stmt = stock.financials
        balance_sheet = stock.balance_sheet
        cash_flow = stock.cashflow

        # We need the most recent 3 years columns
        # Note: yfinance returns columns as Timestamps. We take the first 3.
        years = income_stmt.columns[:3]

        for date in years:
            year_val = date.year
            
            # --- DATA EXTRACTION WITH ERROR HANDLING ---
            # 1. Revenue
            try:
                revenue = income_stmt.loc['Total Revenue', date]
            except:
                revenue = None

            # 2. KPI 1: Net Income (Profitability)
            try:
                net_income = income_stmt.loc['Net Income', date]
            except:
                net_income = None

            # 3. KPI 2: Total Assets (Size)
            # Balance sheet dates might vary slightly, but yfinance usually aligns columns
            try:
                total_assets = balance_sheet.loc['Total Assets', date] if date in balance_sheet.columns else None
            except:
                total_assets = None

            # 4. KPI 3: Operating Cash Flow (Liquidity)
            try:
                op_cash_flow = cash_flow.loc['Operating Cash Flow', date] if date in cash_flow.columns else None
                # Fallback naming convention check
                if op_cash_flow is None and 'Total Cash From Operating Activities' in cash_flow.index:
                     op_cash_flow = cash_flow.loc['Total Cash From Operating Activities', date]
            except:
                op_cash_flow = None

            # Add to list
            dataset.append({
                "Company Name": name,
                "Ticker": ticker,
                "Country": country,
                "Industry": industry,
                "Year": year_val,
                "Revenue": revenue,
                "Revenue Unit": currency,
                "Net Income": net_income,
                "Total Assets": total_assets,
                "Operating Cash Flow": op_cash_flow
            })

    except Exception as e:
        print(f"Failed to fetch {ticker}: {e}")

# Create DataFrame
df = pd.DataFrame(dataset)

# --- CLEANING FOR DATA ENGINEERING ---
# 1. Sort by Company and Year
df = df.sort_values(by=['Company Name', 'Year'], ascending=[True, False])

# 2. Output
file_name = "global_financials_250.csv"
df.to_csv(file_name, index=False)

print("------------------------------------------------")
print(f"SUCCESS: Dataset generated with {len(df)} rows.")
print(f"Saved to: {file_name}")
print("------------------------------------------------")
print(df.head(10)) # Preview