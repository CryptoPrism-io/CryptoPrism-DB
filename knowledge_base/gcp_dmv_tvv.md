# gcp_dmv_tvv.py - Volume Analysis & Technical Indicators Engine

## Overview
This script is the **volume-focused technical analysis module** of the CryptoPrism-DB system, calculating advanced volume-based indicators including On-Balance Volume (OBV), multiple moving averages, Average True Range (ATR), Keltner Channels, and Donchian Channels to analyze price-volume relationships and volatility patterns.

## Detailed Functionality

### **Volume Analysis Indicators**

#### **1. On-Balance Volume (OBV)**
```python
def calculate_obv(df):
    def obv_calc(group):
        obv = np.zeros(len(group))
        volume_changes = np.where(group["close"].diff().fillna(0) > 0, group["volume"], 
                                  np.where(group["close"].diff().fillna(0) < 0, -group["volume"], 0))
        obv[1:] = np.cumsum(volume_changes[:-1])  
        return obv
    df["obv"] = df.groupby("slug").apply(lambda g: pd.Series(obv_calc(g), index=g.index))
    df["m_tvv_obv_1d"] = df.groupby("slug")["obv"].pct_change()
```
- **Logic**: Adds volume on up days, subtracts on down days
- **Cumulative**: Running total shows buying/selling pressure
- **Signal**: Rising OBV confirms uptrends, falling OBV confirms downtrends

#### **2. Multiple Moving Averages System**
```python
def calculate_moving_averages(df):
    df = df.assign(
        SMA9=df.groupby("slug")["close"].transform(lambda x: x.rolling(9, min_periods=1).mean()),
        SMA18=df.groupby("slug")["close"].transform(lambda x: x.rolling(18, min_periods=1).mean()),
        EMA9=df.groupby("slug")["close"].transform(lambda x: x.ewm(span=9, adjust=False).mean()),
        EMA18=df.groupby("slug")["close"].transform(lambda x: x.ewm(span=18, adjust=False).mean()),
        SMA21=df.groupby("slug")["close"].transform(lambda x: x.rolling(21, min_periods=1).mean()),
        SMA108=df.groupby("slug")["close"].transform(lambda x: x.rolling(108, min_periods=1).mean())
    )
```
- **Short-term**: 9, 18, 21-period averages for trend identification
- **Long-term**: 108-period average for major trend direction
- **Both SMA & EMA**: Simple and exponential moving averages for comparison

#### **3. Average True Range (ATR)**
```python
def calculate_atr(df, window=21):
    df["prev_close"] = df.groupby("slug")["close"].shift(1)
    df["tr1"] = df["high"] - df["low"]
    df["tr2"] = abs(df["high"] - df["prev_close"])
    df["tr3"] = abs(df["low"] - df["prev_close"])
    df["TR"] = df[["tr1", "tr2", "tr3"]].max(axis=1)
    df["ATR"] = df.groupby("slug")["TR"].transform(lambda x: x.rolling(window, min_periods=1).mean())
```
- **Purpose**: Measures market volatility
- **Calculation**: Maximum of three true range calculations
- **Application**: Position sizing and stop-loss placement

#### **4. Keltner Channels**
```python
def calculate_keltner_channels(df, period=21, multiplier=2):
    df["KC_Middle"] = df.groupby("slug")["close"].transform(lambda x: x.rolling(period).mean())
    df["KC_Upper"] = df["KC_Middle"] + (multiplier * df["ATR"])
    df["KC_Lower"] = df["KC_Middle"] - (multiplier * df["ATR"])
```
- **Components**: Middle line (EMA), Upper/Lower bands (ATR-based)
- **Breakout Signals**: Price moves outside bands indicate trend changes
- **Volatility Adjustment**: Bands widen/narrow with volatility changes

#### **5. Donchian Channels**
```python
def calculate_donchian_channels(df, period=21):
    df["DC_Upper"] = df.groupby("slug")["high"].transform(lambda x: x.rolling(period).max())
    df["DC_Lower"] = df.groupby("slug")["low"].transform(lambda x: x.rolling(period).min())
    df["DC_Middle"] = (df["DC_Upper"] + df["DC_Lower") / 2
```
- **Construction**: Highest high and lowest low over N periods
- **Trend Following**: Breakouts above/below channels signal trend continuation
- **Support/Resistance**: Channel boundaries act as dynamic support/resistance

### **Binary Signal Generation**
```python
# Volume-based signals
df['m_tvv_obv_signal'] = np.where(df['m_tvv_obv_1d'] > 0, 1, -1)

# Moving average crossover signals
df['ma_cross_signal'] = np.where(df['EMA9'] > df['EMA18'], 1, -1)

# Channel breakout signals
df['kc_breakout_signal'] = np.where(df['close'] > df['KC_Upper'], 1, 
                                   np.where(df['close'] < df['KC_Lower'], -1, 0))
```

### **Database Architecture**
- **FE_TVV**: Complete volume and technical indicators (20+ columns)
- **FE_TVV_SIGNALS**: Binary signals for DMV aggregation
- **Dual Database**: Production (`dbcp`) + Backtest (`cp_backtest`)
- **Data Period**: 110 days for comprehensive indicator calculation

### **Integration Points**
- **Upstream**: `1K_coins_ohlcv` table with 110 days of data
- **Downstream**: `FE_TVV_SIGNALS` feeds into `gcp_dmv_core.py`
- **Pipeline Position**: **Stage 3.4** in DMV workflow

## Usage
```bash
python gcp_postgres_sandbox/gcp_dmv_tvv.py
# Runtime: ~4-6 minutes for volume analysis of 1000 cryptocurrencies
```

## Dependencies
- **pandas>=2.2.2** - Data manipulation and rolling calculations
- **numpy>=1.26.4** - Array operations and mathematical functions
- **sqlalchemy>=2.0.32** - Database connectivity
- **logging** - Progress tracking and error monitoring
- **time** - Performance measurement

## Key Features
1. **Advanced Volume Analysis**: OBV and volume-price relationship indicators
2. **Multiple Timeframe Moving Averages**: 9, 18, 21, 108-period averages
3. **Volatility Measurement**: ATR-based volatility analysis
4. **Channel Analysis**: Keltner and Donchian channel systems
5. **Signal Generation**: Volume and trend-based binary signals
6. **Dual Database Output**: Production and backtesting data separation
