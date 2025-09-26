#include <cpr/cpr.h>
#include <rapidjson/document.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <chrono>
#include <thread>
#include <atomic>
#include <csignal>
#include <iomanip>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <unordered_set>
#include <sqlite3.h>

using namespace rapidjson;
using namespace std;

// Existing structs
struct PriceChange {
    string m5, m15, m30, h1, h6, h24;
};

struct Transactions {
    int buys, sells;
    int buyers, sellers;
};

struct VolumeUSD {
    string m5, m15, m30, h1, h6, h24;
};

struct Token {
    string id;
    string type;
};

struct Dex {
    string id;
    string type;
};

struct Pool {
    string network;
    string pool_id;
    string type;
    string base_token_price_usd;
    string base_token_price_native_currency;
    string quote_token_price_usd;
    string quote_token_price_native_currency;
    string base_token_price_quote_token;
    string quote_token_price_base_token;
    string address;
    string name;
    string pool_created_at;
    string token_price_usd;
    string fdv_usd;
    string market_cap_usd;
    PriceChange price_change_percentage;
    Transactions transactions_m5, transactions_m15, transactions_m30;
    Transactions transactions_h1, transactions_h6, transactions_h24;
    VolumeUSD volume_usd;
    string reserve_in_usd;
    Token base_token;
    vector<Token> quote_tokens;
    Dex dex;
    string stablecoin_symbol;
};

struct Trade {
    string tx_hash;
    string tx_from_address;
    string from_token_amount;
    string to_token_amount;
    string price_from_in_usd;
    string price_to_in_usd;
    string block_timestamp;
    string kind;
    string volume_in_usd;
    string from_token_address;
    string to_token_address;
};

// Global state
atomic<bool> running{true};
mutex csvMutex;
mutex tradeQueueMutex;
condition_variable tradeQueueCV;
queue<Trade> tradeQueue;

// Signal handler
void signalHandler(int signum) {
    cout << "Interrupt signal (" << signum << ") received. Stopping...\n";
    running = false;
}

// Makes HTTP request with retry logic
cpr::Response makeRequestWithBackoff(const string& url, cpr::Session& session) {
    const int maxRetries = 3;
    const chrono::milliseconds baseDelay(500);
    const chrono::milliseconds timeout(1500);

    session.SetUrl(cpr::Url{url});
    session.SetTimeout(timeout);

    for (int retryCount = 0; retryCount < maxRetries; ++retryCount) {
        auto start = chrono::steady_clock::now();
        cpr::Response r = session.Get();
        auto duration = chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start).count();

        if (r.status_code == 429) {
            auto delay = baseDelay * (1 << retryCount);
            cerr << "HTTP 429, retrying after " << delay.count() << " ms\n";
            this_thread::sleep_for(delay);
            continue;
        }

        if (r.status_code == 200 || r.status_code == 404) {
            cout << "Request completed in " << duration << " ms\n";
            return r;
        }

        if (r.status_code >= 400 && r.status_code < 500) {
            cerr << "Fatal error: HTTP " << r.status_code << " for URL: " << url << "\n";
            return r;
        }

        if (duration >= timeout.count()) {
            cerr << "Request timed out after " << duration << " ms for URL: " << url << "\n";
            return cpr::Response();
        }

        cerr << "Request failed: HTTP " << r.status_code << " for URL: " << url << "\n";
    }

    cerr << "Max retries reached for URL: " << url << "\n";
    return cpr::Response();
}

// Helper functions
string getString(const Value& value) {
    return value.IsString() ? value.GetString() : "N/A";
}

int getInt(const Value& value) {
    return value.IsInt() ? value.GetInt() : -1;
}

Transactions parseTransactions(const Value& tx) {
    Transactions result;
    result.buys = tx.HasMember("buys") ? getInt(tx["buys"]) : -1;
    result.sells = tx.HasMember("sells") ? getInt(tx["sells"]) : -1;
    result.buyers = tx.HasMember("buyers") ? getInt(tx["buyers"]) : -1;
    result.sellers = tx.HasMember("sellers") ? getInt(tx["sellers"]) : -1;
    return result;
}

// Existing fetchPools function
void fetchPools(const string& stablecoin_symbol, const string& token_address, map<string, Pool>& pools, ofstream& csv, mutex& csv_mutex, const string& network) {
    const string base_url = "https://api.geckoterminal.com/api/v2/networks/" + network + "/tokens/" + token_address + "/pools?include=base_token%2Cquote_token&page=";
    const chrono::milliseconds sleepDuration(0);
    int page = 1;
    bool has_next_page = true;
    cpr::Session session;

    while (has_next_page && running) {
        cout << "Fetching pools for " << stablecoin_symbol << " on " << network << " (page " << page << ")\n";
        string url = base_url + to_string(page);

        cpr::Response r = makeRequestWithBackoff(url, session);

        if (r.status_code == 404) {
            return;
        }

        if (r.status_code != 200) {
            cerr << "Failed to fetch pools for " << stablecoin_symbol << " on " << network << ": HTTP " << r.status_code << endl;
            break;
        }

        Document doc;
        doc.Parse(r.text.c_str());
        if (doc.HasParseError() || !doc.HasMember("data") || !doc["data"].IsArray()) {
            cerr << "JSON parse error or invalid data for " << stablecoin_symbol << " on " << network << ": " << r.text << endl;
            this_thread::sleep_for(sleepDuration);
            continue;
        }

        for (const auto& pool : doc["data"].GetArray()) {
            if (!pool.HasMember("attributes") || !pool.HasMember("relationships")) {
                cerr << "Missing attributes or relationships in pool data for " << stablecoin_symbol << " on " << network << endl;
                continue;
            }
            Pool p;
            const auto& attrs = pool["attributes"];
            const auto& rels = pool["relationships"];

            p.network = network;
            p.pool_id = getString(pool["id"]);
            p.type = getString(pool["type"]);
            p.base_token_price_usd = getString(attrs["base_token_price_usd"]);
            p.base_token_price_native_currency = getString(attrs["base_token_price_native_currency"]);
            p.quote_token_price_usd = getString(attrs["quote_token_price_usd"]);
            p.quote_token_price_native_currency = getString(attrs["quote_token_price_native_currency"]);
            p.base_token_price_quote_token = getString(attrs["base_token_price_quote_token"]);
            p.quote_token_price_base_token = getString(attrs["quote_token_price_base_token"]);
            p.address = getString(attrs["address"]);
            p.name = getString(attrs["name"]);
            p.pool_created_at = getString(attrs["pool_created_at"]);
            p.token_price_usd = getString(attrs["token_price_usd"]);
            p.fdv_usd = getString(attrs["fdv_usd"]);
            p.market_cap_usd = getString(attrs["market_cap_usd"]);

            if (attrs.HasMember("price_change_percentage") && attrs["price_change_percentage"].IsObject()) {
                const auto& pc = attrs["price_change_percentage"];
                p.price_change_percentage = {getString(pc["m5"]), getString(pc["m15"]), getString(pc["m30"]),
                                             getString(pc["h1"]), getString(pc["h6"]), getString(pc["h24"])};
            }

            if (attrs.HasMember("transactions") && attrs["transactions"].IsObject()) {
                const auto& tx = attrs["transactions"];
                p.transactions_m5 = tx.HasMember("m5") ? parseTransactions(tx["m5"]) : Transactions{-1, -1, -1, -1};
                p.transactions_m15 = tx.HasMember("m15") ? parseTransactions(tx["m15"]) : Transactions{-1, -1, -1, -1};
                p.transactions_m30 = tx.HasMember("m30") ? parseTransactions(tx["m30"]) : Transactions{-1, -1, -1, -1};
                p.transactions_h1 = tx.HasMember("h1") ? parseTransactions(tx["h1"]) : Transactions{-1, -1, -1, -1};
                p.transactions_h6 = tx.HasMember("h6") ? parseTransactions(tx["h6"]) : Transactions{-1, -1, -1, -1};
                p.transactions_h24 = tx.HasMember("h24") ? parseTransactions(tx["h24"]) : Transactions{-1, -1, -1, -1};
            }

            if (attrs.HasMember("volume_usd") && attrs["volume_usd"].IsObject()) {
                const auto& vol = attrs["volume_usd"];
                p.volume_usd = {getString(vol["m5"]), getString(vol["m15"]), getString(vol["m30"]),
                                getString(vol["h1"]), getString(vol["h6"]), getString(vol["h24"])};
            }

            p.reserve_in_usd = getString(attrs["reserve_in_usd"]);

            if (rels.HasMember("base_token") && rels["base_token"].HasMember("data")) {
                const auto& bt = rels["base_token"]["data"];
                p.base_token = {getString(bt["id"]), getString(bt["type"])};
            }
            if (rels.HasMember("quote_token") && rels["quote_token"].HasMember("data")) {
                const auto& qt = rels["quote_token"]["data"];
                p.quote_tokens.push_back({getString(qt["id"]), getString(qt["type"])});
            }
            if (rels.HasMember("quote_tokens") && rels["quote_tokens"].HasMember("data") && rels["quote_tokens"]["data"].IsArray()) {
                for (const auto& qt : rels["quote_tokens"]["data"].GetArray()) {
                    p.quote_tokens.push_back({getString(qt["id"]), getString(qt["type"])});
                }
            }
            if (rels.HasMember("dex") && rels["dex"].HasMember("data")) {
                const auto& dx = rels["dex"]["data"];
                p.dex = {getString(dx["id"]), getString(dx["type"])};
            }

            p.stablecoin_symbol = stablecoin_symbol;
            pools[p.pool_id] = p;

            {
                lock_guard<mutex> lock(csvMutex);
                csv << "\"" << p.stablecoin_symbol << "\","
                    << "\"" << p.network << "\","
                    << "\"" << p.pool_id << "\","
                    << "\"" << p.type << "\","
                    << "\"" << p.base_token_price_usd << "\","
                    << "\"" << p.base_token_price_native_currency << "\","
                    << "\"" << p.quote_token_price_usd << "\","
                    << "\"" << p.quote_token_price_native_currency << "\","
                    << "\"" << p.base_token_price_quote_token << "\","
                    << "\"" << p.quote_token_price_base_token << "\","
                    << "\"" << p.address << "\","
                    << "\"" << p.name << "\","
                    << "\"" << p.pool_created_at << "\","
                    << "\"" << p.token_price_usd << "\","
                    << "\"" << p.fdv_usd << "\","
                    << "\"" << p.market_cap_usd << "\","
                    << "\"" << p.price_change_percentage.m5 << "\","
                    << "\"" << p.price_change_percentage.m15 << "\","
                    << "\"" << p.price_change_percentage.m30 << "\","
                    << "\"" << p.price_change_percentage.h1 << "\","
                    << "\"" << p.price_change_percentage.h6 << "\","
                    << "\"" << p.price_change_percentage.h24 << "\","
                    << p.transactions_m5.buys << "," << p.transactions_m5.sells << "," << p.transactions_m5.buyers << "," << p.transactions_m5.sellers << ","
                    << p.transactions_m15.buys << "," << p.transactions_m15.sells << "," << p.transactions_m15.buyers << "," << p.transactions_m15.sellers << ","
                    << p.transactions_m30.buys << "," << p.transactions_m30.sells << "," << p.transactions_m30.buyers << "," << p.transactions_m30.sellers << ","
                    << p.transactions_h1.buys << "," << p.transactions_h1.sells << "," << p.transactions_h1.buyers << "," << p.transactions_h1.sellers << ","
                    << p.transactions_h6.buys << "," << p.transactions_h6.sells << "," << p.transactions_h6.buyers << "," << p.transactions_h6.sellers << ","
                    << p.transactions_h24.buys << "," << p.transactions_h24.sells << "," << p.transactions_h24.buyers << "," << p.transactions_h24.sellers << ","
                    << "\"" << p.volume_usd.m5 << "\","
                    << "\"" << p.volume_usd.m15 << "\","
                    << "\"" << p.volume_usd.m30 << "\","
                    << "\"" << p.volume_usd.h1 << "\","
                    << "\"" << p.volume_usd.h6 << "\","
                    << "\"" << p.volume_usd.h24 << "\","
                    << "\"" << p.reserve_in_usd << "\","
                    << "\"" << p.base_token.id << "\","
                    << "\"" << p.base_token.type << "\","
                    << "\"" << (p.quote_tokens.empty() ? "N/A" : p.quote_tokens[0].id) << "\","
                    << "\"" << (p.quote_tokens.empty() ? "N/A" : p.quote_tokens[0].type) << "\","
                    << "\"" << (p.quote_tokens.size() > 1 ? p.quote_tokens[1].id : "N/A") << "\","
                    << "\"" << (p.quote_tokens.size() > 1 ? p.quote_tokens[1].type : "N/A") << "\","
                    << "\"" << p.dex.id << "\","
                    << "\"" << p.dex.type << "\"\n";
                csv.flush();
            }
        }

        has_next_page = doc.HasMember("links") && doc["links"].HasMember("next") && doc["links"]["next"].IsString();
        page++;
    }
    cout << "Completed fetching pools for " << stablecoin_symbol << " on " << network << endl;
}

// Polling-based trade fetching
void fetchTradesPolling(const Pool& pool, unordered_set<string>& seenHashes) {
    cpr::Session session;

    while (running) {
        string url = "https://api.geckoterminal.com/api/v2/networks/" + pool.network + "/pools/" + pool.pool_id + "/trades";
        cpr::Response r = makeRequestWithBackoff(url, session);

        if (r.status_code == 404) {
            cerr << "Pool not found: " << pool.pool_id << " on " << pool.network << endl;
            break;
        }

        if (r.status_code == 200) {
            Document doc;
            doc.Parse(r.text.c_str());
            if (!doc.HasParseError() && doc.HasMember("data") && doc["data"].IsArray()) {
                vector<Trade> newTrades;
                for (const auto& trade : doc["data"].GetArray()) {
                    if (!trade.HasMember("attributes")) continue;
                    const auto& attrs = trade["attributes"];
                    string tx_hash = getString(attrs["tx_hash"]);
                    if (seenHashes.insert(tx_hash).second) {
                        Trade t = {
                            tx_hash,
                            getString(attrs["tx_from_address"]),
                            getString(attrs["from_token_amount"]),
                            getString(attrs["to_token_amount"]),
                            getString(attrs["price_from_in_usd"]),
                            getString(attrs["price_to_in_usd"]),
                            getString(attrs["block_timestamp"]),
                            getString(attrs["kind"]),
                            getString(attrs["volume_in_usd"]),
                            getString(attrs["from_token_address"]),
                            getString(attrs["to_token_address"])
                        };
                        newTrades.push_back(t);
                    }
                }
                if (!newTrades.empty()) {
                    lock_guard<mutex> lock(tradeQueueMutex);
                    for (const auto& trade : newTrades) {
                        tradeQueue.push(trade);
                    }
                    tradeQueueCV.notify_one();
                    cout << "Queued " << newTrades.size() << " new trades for " << pool.pool_id << " on " << pool.network << endl;
                }
            }
        } else {
            cerr << "Failed to fetch trades for " << pool.pool_id << " on " << pool.network << ": HTTP " << r.status_code << endl;
        }

        this_thread::sleep_for(chrono::milliseconds(500)); // Polling interval
    }
}

// I/O thread for SQLite storage
void ioThread(sqlite3* db) {
    const int batchSize = 1000;
    vector<Trade> batch;
    char* errMsg = nullptr;

    string sql = "CREATE TABLE IF NOT EXISTS trades ("
                 "tx_hash TEXT PRIMARY KEY, tx_from_address TEXT, from_token_amount TEXT, to_token_amount TEXT, "
                 "price_from_in_usd TEXT, price_to_in_usd TEXT, block_timestamp TEXT, kind TEXT, "
                 "volume_in_usd TEXT, from_token_address TEXT, to_token_address TEXT);";
    sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
    if (errMsg) {
        cerr << "SQLite error: " << errMsg << endl;
        sqlite3_free(errMsg);
    }

    while (running || !tradeQueue.empty()) {
        {
            unique_lock<mutex> lock(tradeQueueMutex);
            tradeQueueCV.wait(lock, [] { return !tradeQueue.empty() || !running; });
            while (!tradeQueue.empty() && batch.size() < batchSize) {
                batch.push_back(tradeQueue.front());
                tradeQueue.pop();
            }
        }
        if (!batch.empty()) {
            stringstream ss;
            ss << "BEGIN TRANSACTION;";
            for (const auto& trade : batch) {
                ss << "INSERT OR IGNORE INTO trades (tx_hash, tx_from_address, from_token_amount, to_token_amount, "
                   << "price_from_in_usd, price_to_in_usd, block_timestamp, kind, volume_in_usd, "
                   << "from_token_address, to_token_address) VALUES ("
                   << "'" << trade.tx_hash << "', '" << trade.tx_from_address << "', '" << trade.from_token_amount << "', '"
                   << trade.to_token_amount << "', '" << trade.price_from_in_usd << "', '" << trade.price_to_in_usd << "', '"
                   << trade.block_timestamp << "', '" << trade.kind << "', '" << trade.volume_in_usd << "', '"
                   << trade.from_token_address << "', '" << trade.to_token_address << "');";
            }
            ss << "COMMIT;";
            sqlite3_exec(db, ss.str().c_str(), nullptr, nullptr, &errMsg);
            if (errMsg) {
                cerr << "SQLite insert error: " << errMsg << endl;
                sqlite3_free(errMsg);
            }
            cout << "Committed " << batch.size() << " trades to SQLite" << endl;
            batch.clear();
        }
        this_thread::sleep_for(chrono::milliseconds(100)); // Prevent busy-waiting
    }
    sqlite3_close(db);
}

int main() {
    signal(SIGINT, signalHandler);

    ofstream csv("pools_complete.csv", ios::out | ios::app);
    if (!csv.is_open()) {
        cerr << "Failed to open pools_complete.csv\n";
        return 1;
    }
    csv.seekp(0, ios::end);
    if (csv.tellp() == 0) {
        csv << "stablecoin_symbol,network,pool_id,type,base_token_price_usd,base_token_price_native_currency,"
            << "quote_token_price_usd,quote_token_price_native_currency,base_token_price_quote_token,"
            << "quote_token_price_base_token,address,name,pool_created_at,token_price_usd,fdv_usd,market_cap_usd,"
            << "price_change_m5,price_change_m15,price_change_m30,price_change_h1,price_change_h6,price_change_h24,"
            << "transactions_m5_buys,transactions_m5_sells,transactions_m5_buyers,transactions_m5_sellers,"
            << "transactions_m15_buys,transactions_m15_sells,transactions_m15_buyers,transactions_m15_sellers,"
            << "transactions_m30_buys,transactions_m30_sells,transactions_m30_buyers,transactions_m30_sellers,"
            << "transactions_h1_buys,transactions_h1_sells,transactions_h1_buyers,transactions_h1_sellers,"
            << "transactions_h6_buys,transactions_h6_sells,transactions_h6_buyers,transactions_h6_sellers,"
            << "transactions_h24_buys,transactions_h24_sells,transactions_h24_buyers,transactions_h24_sellers,"
            << "volume_usd_m5,volume_usd_m15,volume_usd_m30,volume_usd_h1,volume_usd_h6,volume_usd_h24,"
            << "reserve_in_usd,base_token_id,base_token_type,quote_token_id_1,quote_token_type_1,"
            << "quote_token_id_2,quote_token_type_2,dex_id,dex_type\n";
    }

    mutex csvMutex;

    vector<pair<string, string>> stablecoins = {
        {"USDT", "0xdac17f958d2ee523a2206206994597c13d831ec7"},
        {"USDC", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"},
        {"DAI", "0x6b175474e89094c44da98b954eedeac495271d0f"}
    };
    vector<string> networks = {"eth", "polygon_pos", "bsc", "pulsechain", "arbitrum"};

    map<string, Pool> all_pools;

    // Fetch pools
    vector<thread> poolFetchers;
    for (const auto& network : networks) {
        for (const auto& [symbol, address] : stablecoins) {
            poolFetchers.emplace_back(fetchPools, symbol, address, ref(all_pools), ref(csv), ref(csvMutex), network);
        }
    }
    for (auto& t : poolFetchers) {
        t.join();
    }
    csv.close();

    // Initialize SQLite
    sqlite3* db;
    if (sqlite3_open("trades.db", &db)) {
        cerr << "Cannot open database: " << sqlite3_errmsg(db) << endl;
        return 1;
    }

    // Launch I/O thread
    thread ioThreadObj(ioThread, db);

    // Launch polling-based trade fetchers
    vector<thread> tradeFetchers;
    unordered_set<string> seenHashes; // Shared for simplicity

    for (const auto& [pool_id, pool] : all_pools) {
        tradeFetchers.emplace_back(fetchTradesPolling, pool, ref(seenHashes));
    }

    // Wait for trade fetchers to complete
    for (auto& t : tradeFetchers) {
        if (t.joinable()) t.join();
    }

    running = false; // Signal I/O thread to exit
    ioThreadObj.join();

    cout << "Main thread exiting, stored pools and trades in SQLite" << endl;
    return 0;
}