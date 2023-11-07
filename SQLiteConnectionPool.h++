//
// Created by y-watanabe on 10/16/23.
//

#ifndef MY_PROXY_SQLITECONNECTIONPOOL_H
#define MY_PROXY_SQLITECONNECTIONPOOL_H

#include <iostream>
#include <vector>
#include <sqlite3.h>
#include <mutex>
#include <libpq-fe.h>


static constexpr char *text_create_OORDER = "CREATE TABLE OORDER ("
                                            "o_w_id integer,"
                                            "o_d_id integer,"
                                            "o_id integer,"
                                            "o_c_id integer,"
                                            "o_carrier_id integer,"
                                            "o_ol_cnt REAL,"
                                            "o_all_local REAL,"
                                            "o_entry_d datetime DEFAULT CURRENT_TIMESTAMP,"
                                            "PRIMARY KEY(o_w_id, o_d_id)"
                                            ")";

static constexpr char *text_create_DISTRICT = "CREATE TABLE DISTRICT ("
                                              "d_w_id INTEGER NOT NULL,"
                                              "d_id INTEGER NOT NULL,"
                                              "d_ytd REAL NOT NULL,"
                                              "d_tax REAL NOT NULL,"
                                              "d_next_o_id INTEGER NOT NULL,"
                                              "d_name TEXT NOT NULL,"
                                              "d_street_1 TEXT NOT NULL,"
                                              "d_street_2 TEXT NOT NULL,"
                                              "d_city TEXT NOT NULL,"
                                              "d_state TEXT NOT NULL,"
                                              "d_zip TEXT NOT NULL,"
                                              "PRIMARY KEY (d_w_id,d_id)"
                                              ")";

static constexpr char *text_create_ITEM = "CREATE TABLE ITEM ("
                                          "i_id INTEGER NOT NULL,"
                                          "i_name TEXT NOT NULL,"
                                          "i_price REAL NOT NULL,"
                                          "i_data TEXT NOT NULL,"
                                          "i_im_id INTEGER NOT NULL,"
                                          "PRIMARY KEY (i_id)"
                                          ")";

static constexpr char *text_create_WAREHOUSE = "CREATE TABLE WAREHOUSE ("
                                               "w_id INTEGER NOT NULL,"
                                               "w_ytd REAL NOT NULL,"
                                               "w_tax REAL NOT NULL,"
                                               "w_name TEXT NOT NULL,"
                                               "w_street_1 TEXT NOT NULL,"
                                               "w_street_2 TEXT NOT NULL,"
                                               "w_city TEXT NOT NULL,"
                                               "w_state TEXT NOT NULL,"
                                               "w_zip TEXT NOT NULL,"
                                               "PRIMARY KEY (w_id)"
                                               ")";

static constexpr char *text_create_CUSTOMER = "CREATE TABLE CUSTOMER ("
                                              "c_w_id INTEGER NOT NULL,"
                                              "c_d_id INTEGER NOT NULL,"
                                              "c_id INTEGER NOT NULL,"
                                              "c_discount REAL NOT NULL,"
                                              "c_credit TEXT NOT NULL,"
                                              "c_last TEXT NOT NULL,"
                                              "c_first TEXT NOT NULL,"
                                              "c_credit_lim REAL NOT NULL,"
                                              "c_balance REAL NOT NULL,"
                                              "c_ytd_payment REAL NOT NULL,"
                                              "c_payment_cnt INTEGER NOT NULL,"
                                              "c_delivery_cnt INTEGER NOT NULL,"
                                              "c_street_1 TEXT NOT NULL,"
                                              "c_street_2 TEXT NOT NULL,"
                                              "c_city TEXT NOT NULL,"
                                              "c_state TEXT NOT NULL,"
                                              "c_zip TEXT NOT NULL,"
                                              "c_phone TEXT NOT NULL,"
                                              "c_since TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                                              "c_middle TEXT NOT NULL,"
                                              "c_data TEXT NOT NULL,"
                                              "PRIMARY KEY (c_w_id,c_d_id,c_id)"
                                              ")";

static constexpr char *text_create_ORDER_LINE = "CREATE TABLE ORDER_LINE ("
                                                "ol_w_id INTEGER NOT NULL,"
                                                "ol_d_id INTEGER NOT NULL,"
                                                "ol_o_id INTEGER NOT NULL,"
                                                "ol_number INTEGER NOT NULL,"
                                                "ol_i_id INTEGER NOT NULL,"
                                                "ol_delivery_d TEXT DEFAULT NULL,"
                                                "ol_amount REAL NOT NULL,"
                                                "ol_supply_w_id INTEGER NOT NULL,"
                                                "ol_quantity REAL NOT NULL,"
                                                "ol_dist_info TEXT NOT NULL,"
                                                "PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number)"
                                                ")";

static constexpr char *text_create_NEW_ORDER = "CREATE TABLE NEW_ORDER ("
                                               "no_w_id INTEGER NOT NULL,"
                                               "no_d_id INTEGER NOT NULL,"
                                               "no_o_id INTEGER NOT NULL,"
                                               "PRIMARY KEY (no_w_id,no_d_id,no_o_id)"
                                               ")";

static constexpr char *text_create_STOCK = "CREATE TABLE STOCK ("
                                           "s_w_id INTEGER NOT NULL,"
                                           "s_i_id INTEGER NOT NULL,"
                                           "s_quantity REAL NOT NULL,"
                                           "s_ytd REAL NOT NULL,"
                                           "s_order_cnt INTEGER NOT NULL,"
                                           "s_remote_cnt INTEGER NOT NULL,"
                                           "s_data TEXT NOT NULL,"
                                           "s_dist_01 TEXT NOT NULL,"
                                           "s_dist_02 TEXT NOT NULL,"
                                           "s_dist_03 TEXT NOT NULL,"
                                           "s_dist_04 TEXT NOT NULL,"
                                           "s_dist_05 TEXT NOT NULL,"
                                           "s_dist_06 TEXT NOT NULL,"
                                           "s_dist_07 TEXT NOT NULL,"
                                           "s_dist_08 TEXT NOT NULL,"
                                           "s_dist_09 TEXT NOT NULL,"
                                           "s_dist_10 TEXT NOT NULL,"
                                           "PRIMARY KEY (s_w_id, s_i_id)"
                                           ")";

static constexpr char *text_create_HISTORY = "CREATE TABLE HISTORY ("
                                             "h_c_id INTEGER NOT NULL,"
                                             "h_c_d_id INTEGER NOT NULL,"
                                             "h_c_w_id INTEGER NOT NULL,"
                                             "h_d_id INTEGER NOT NULL,"
                                             "h_w_id INTEGER NOT NULL,"
                                             "h_date TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                                             "h_amount REAL NOT NULL,"
                                             "h_data TEXT NOT NULL"
                                             ")";

static constexpr char *text_create_tbl_sbtest1 = "create table if not exists sbtest1 (id integer primary key, k integer, c text, pad text)";


class SQLiteConnectionPool {
public:
    static SQLiteConnectionPool &getInstance() {
        static SQLiteConnectionPool instance;
        return instance;
    }

    ~SQLiteConnectionPool() {
        // プール内のコネクションを解放
        for (sqlite3 *db: connections_) {
            sqlite3_close(db);
        }
    }

    // クエリを実行するメソッド
    int executeQuery(const std::string &query, int(*callback)(void *, int, char **, char **), void *data) {
        sqlite3 *db = getConnection();
        int rc = sqlite3_exec(db, query.c_str(), callback, data, 0);
        releaseConnection(db);
        return rc;
    }

    void fetch_data_from_postgres(PGconn *conn) {
        executeQuery(text_create_tbl_sbtest1, nullptr, nullptr);

        const char *tables[] = {
                "sbtest1"
        };

        int numTables = sizeof(tables) / sizeof(tables[0]);

        for (int i = 0; i < numTables; i++) {
            if (!migrateTableData(conn, tables[i])) {
                std::cerr << "Migration failed for table " << tables[i] << std::endl;
                PQfinish(conn);
            }
        }

        std::cout << "Migration succeeded." << std::endl;
    }


    sqlite3 *getConnection() {
        std::lock_guard<std::mutex> lock(mutex_);
        sqlite3 *db = nullptr;

        if (connections_.size() <= 1) { // 1つだけ何もしてない残しておくk
            if (sqlite3_open(db_name_.c_str(), &db) == SQLITE_OK) {
                sqlite3_busy_timeout(db, 500);
                connections_.push_back(db);
            }
        }

//        while(connections_)

        db = connections_.back();
        connections_.pop_back();
//        std::cout << "connection**" << connections_.size() << std::endl;
        return db;
    }

    void releaseConnection(sqlite3 *db) {
        std::lock_guard<std::mutex> lock(mutex_);
        sqlite3_exec(db, "COMMIT", 0, 0, 0);
        connections_.push_back(db);
//        std::cout << "connection******" << connections_.size() << std::endl;
    }

private:
    SQLiteConnectionPool() {
        int pool_size = 1;
        // プール内のコネクションを初期化
        for (int i = 0; i < pool_size; ++i) {
            sqlite3 *db;
            if (sqlite3_open(db_name_.c_str(), &db) == SQLITE_OK) {
                sqlite3_busy_timeout(db, 1000);
                connections_.push_back(db);
            }
        }

        static constexpr char *backend_postgres_conninfo = "host=192.168.11.16 port=5433 dbname=yugabyte user=yugabyte password=yugabyte";
        PGconn *_conn;

        _conn = PQconnectdb(backend_postgres_conninfo);
        /* バックエンドとの接続確立に成功したかを確認する */
        if (PQstatus(_conn) != CONNECTION_OK) {
            fprintf(stderr, "Connection to database failed: %s",
                    PQerrorMessage(_conn));
            exit(1);
        }

        fetch_data_from_postgres(_conn);
    }

    // Fetch data from PostgreSQL and insert into SQLite
    bool migrateTableData(PGconn *_conn, const char *tableName) {
        std::string query = "SELECT * FROM " + std::string(tableName);
        PGresult *res = PQexec(_conn, query.c_str());

        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            std::cerr << "Failed to fetch data from PostgreSQL for table: " << PQerrorMessage(_conn) << std::endl;
            PQclear(res);
            return false;
        }

        bool success = insertDataFromResult(res, tableName);

        PQclear(res);
        return success;
    }

    // Insert data from a PostgreSQL query result into an SQLite table
    bool insertDataFromResult(PGresult *res, const char *tableName) {
        int nFields = PQnfields(res);
        std::cout << "size : " << PQntuples(res) << std::endl;
        for (int i = 0; i < PQntuples(res); i++) {
            std::string insertQuery = "INSERT OR REPLACE INTO " + std::string(tableName) + " VALUES(";
            for (int j = 0; j < nFields; j++) {
                if (j > 0) {
                    insertQuery += ",";
                }
                insertQuery += "'" + std::string(PQgetvalue(res, i, j)) + "'";
            }
            insertQuery += ");";

            executeQuery(insertQuery.c_str(), nullptr, nullptr);

        }
        return true;
    }

    std::string db_name_ = "file::memory:?cache=shared";
    std::vector<sqlite3 *> connections_;
    std::mutex mutex_;
};

#endif //MY_PROXY_SQLITECONNECTIONPOOL_H
