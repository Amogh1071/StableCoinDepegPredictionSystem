
# Stablecoin Depeg Prediction System

**Using On-Chain & Historical Analysis**

🚀 **Self-project** · **Started: July 2025 – Ongoing**

---

## 📌 Overview

This project is a **stablecoin depeg prediction system** designed to monitor real-time on-chain data, market trades, and liquidity pools to detect early signs of potential depegs.

The system ingests high-frequency price and trade data (5k+ points/sec), applies risk scoring, and generates prioritized alerts when depeg triggers are detected.

---

## 🎯 Objectives

* Predict stablecoin depegs using **historical patterns**, **on-chain flows**, and **market data**.
* Build a **scalable early-warning system** with real-time risk scoring and alerts.

---

## ⚙️ Features

* **High-speed data ingestion**

  * Built with **C++17/20 multithreaded engine**.
  * Sub-5ms alerting latency.

* **Real-time multi-source risk analysis**

  * Fetches liquidity pools, trades, and metrics from **CoinGecko Terminal API**.
  * Supports multiple networks: Ethereum, Polygon, BSC, Pulsechain, Arbitrum.

* **On-chain monitoring**

  * Tracks pools and trades across major stablecoins (USDT, USDC, DAI).
  * Extracts transactions, price changes, volumes, and reserves.

* **Data persistence**

  * Pools stored in **CSV** for analytics.
  * Trades streamed into **SQLite database** with batched inserts.

* **Depeg risk analysis (planned/ongoing)**

  * Pattern matching on historical metrics.
  * Risk scoring and alert classification.

---

## 🏗️ System Architecture

1. **Pool Fetcher Threads**

   * Concurrently fetch liquidity pool metadata per network + stablecoin.
   * Outputs `pools_complete.csv`.

2. **Trade Fetchers (Polling-based)**

   * Continuously query trade activity per pool.
   * Deduplicate trades by transaction hash.

3. **I/O Thread (SQLite Writer)**

   * Consumes trade queue.
   * Stores trades in `trades.db`.

4. **Alert Engine (planned)**

   * Detects deviations in price, reserves, and volumes.
   * Emits early-warning alerts.

---

## 📂 Output Files

* **`pools_complete.csv`** → Metadata of liquidity pools.
* **`trades.db`** → SQLite database storing trade history.

---

## 🖥️ Dependencies

* [cpr](https://github.com/libcpr/cpr) (HTTP client for C++).
* [RapidJSON](https://rapidjson.org/) (JSON parsing).
* [SQLite3](https://www.sqlite.org/) (embedded database).
* Standard C++17/20 libraries.

---

## ▶️ How to Build & Run

### 1. Install dependencies

On Ubuntu/Debian:

```bash
sudo apt-get update
sudo apt-get install -y libcurl4-openssl-dev libssl-dev cmake g++ sqlite3 libsqlite3-dev
```

Clone & build **cpr**:

```bash
git clone https://github.com/libcpr/cpr.git
cd cpr && mkdir build && cd build
cmake ..
make -j
sudo make install
```

Clone & build **RapidJSON**:

```bash
git clone https://github.com/Tencent/rapidjson.git
sudo cp -r rapidjson/include/rapidjson /usr/include/
```

### 2. Compile

```bash
g++ -std=c++20 -O2 -pthread main.cpp -lcpr -lsqlite3 -o depeg_monitor
```

### 3. Run

```bash
./depeg_monitor
```

---

## 📊 Example Output

* `pools_complete.csv` (sample row):

```csv
USDC,eth,pool_123,type,1.00,ETH,1.00,ETH,1.00,1.00,0x123...,USDC/ETH Pool,...
```

* `trades.db` (SQLite schema):

```sql
CREATE TABLE trades (
  tx_hash TEXT PRIMARY KEY,
  tx_from_address TEXT,
  from_token_amount TEXT,
  to_token_amount TEXT,
  price_from_in_usd TEXT,
  price_to_in_usd TEXT,
  block_timestamp TEXT,
  kind TEXT,
  volume_in_usd TEXT,
  from_token_address TEXT,
  to_token_address TEXT
);
```

---

## 🚦 Roadmap

* ✅ Pool fetching + CSV output.
* ✅ Trade polling + SQLite persistence.
* 🚧 Real-time depeg risk scoring.
* 🚧 Alert engine (priority-based notifications).
* 🚧 Dashboard / visualization.

---

## 📜 License

MIT License.
