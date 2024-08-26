# CryptoPrism ETL Process

## Overview
CryptoPrism performs a daily ETL (Extraction, Transformation, and Loading) process for the top 1000 cryptocurrencies. The data is categorized into Durability, Momentum, and Valuation (DMV) metrics, providing insights into the performance, stability, and potential of these assets.

## Data Categories

### 1. Durability Metrics (D)
- **`d_rat_beta_bin`**: Volatility relative to the market.
- **`d_rat_pain_bin`**: Maximum drawdown, indicating risk.
- **`d_tvv_sma9_18`, `d_tvv_sma21_108`**: Simple Moving Averages.
- **`d_tvv_ema9_18`, `d_tvv_ema21_108`**: Exponential Moving Averages.
- **`d_pct_cum_ret_signal`**: Cumulative return over time.
- **`d_market_cap_signal`**: Market capitalization.
- **`d_met_coin_age_y_signal`**: Coin age, correlating with stability.

### 2. Momentum Metrics (M)
- **`m_rat_alpha_bin`**: Excess return compared to a benchmark.
- **`m_rat_ror_bin`**: Rate of Return.
- **`m_rat_win_rate_bin`**: Percentage of positive returns.
- **`m_osc_macd_crossover_bin`, `m_osc_cci_bin`, `m_osc_adx_bin`, `m_osc_uo_bin`, `m_osc_ao_bin`, `m_osc_trix_bin`**: Various momentum oscillators.
- **`m_mom_williams_%_bin`, `m_mom_smi_bin`, `m_mom_cmo_bin`, `m_mom_mom_bin`**: Momentum indicators.
- **`m_tvv_obv_1d_binary`**: On-Balance Volume over 1 day.
- **`m_tvv_cmf`**: Chaikin Money Flow.

### 3. Valuation Metrics (V)
- **`v_rat_sharpe_bin`**: Risk-adjusted return.
- **`v_rat_sortino_bin`**: Risk-adjusted return focusing on downside volatility.
- **`v_rat_teynor_bin`**: Risk-adjusted return relative to market risk.
- **`v_rat_common_sense_bin`**: Custom metric for fundamental analysis.
- **`v_rat_information_bin`**: Information ratio.
- **`v_rat_win_loss_bin`**: Win-to-loss ratio.

## ETL Process

### 1. Data Extraction
- Daily data for the top 1000 cryptocurrencies is pulled from various sources.

### 2. Transformation
- Raw data is cleaned, normalized, and transformed into Durability, Momentum, and Valuation metrics.
- Financial calculations and binary classifications are applied.

### 3. Loading
- Transformed data is loaded into an **AWS MySQL database** for further analysis.
- Data is then projected onto the CryptoPrism web app for user access and visualization.

## Financial Terms Explained

- **Beta (β):** Volatility measure relative to the market.
- **Alpha (α):** Investment return compared to a benchmark.
- **Sharpe Ratio:** Risk-adjusted return per unit of risk.
- **Sortino Ratio:** Focuses on downside risk in risk-adjusted return.
- **Treynor Ratio:** Uses beta for risk measurement.
- **MACD:** Momentum indicator of moving averages.
- **ADX:** Measures trend strength.
- **RSI:** Identifies overbought or oversold conditions.
- **OBV:** Volume-based momentum indicator.
- **SMA, EMA:** Moving averages indicating trends.

---

This ETL process ensures that the CryptoPrism platform provides up-to-date, comprehensive data analysis for the top 1000 cryptocurrencies, aiding in informed decision-making.
