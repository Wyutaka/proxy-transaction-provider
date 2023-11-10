#include <cstdlib>
#include <cstddef>
#include <iostream>
#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread.hpp>

#include "SQLiteConnectionPool.h++"
#include "PostgresConnectionPool.h++"
#include "server.h++"



// Execute SQL against SQLite
bool executeSQLite(sqlite3 *db, const char *sql) {
    char *errMsg = nullptr;
    if (sqlite3_exec(db, sql, 0, 0, &errMsg) != SQLITE_OK) {
        std::cerr << "SQLite error: " << errMsg << std::endl;
        sqlite3_free(errMsg);
        return false;
    }
    return true;
}

// Insert data from a PostgreSQL query result into an SQLite table
bool insertDataFromResult(PGconn *_conn, sqlite3 *in_mem_db, PGresult *res, const char *tableName) {
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

        if (!executeSQLite(in_mem_db, insertQuery.c_str())) {
            return false;
        }

//        // todo パーセンテージで確認

//        if (strcmp(tableName, "CUSTOMER") == 0) {
//            if (i == 270000) { break; }
//        } else if (strcmp(tableName, "WAREHOUSE") == 0) {
//            if (i == 9) { break;}
//        } else if (strcmp(tableName, "DISTRICT") == 0) {
//            if (i == 90) {break;}
//        } else if (strcmp(tableName, "STOCK") == 0) {
//            if (i == 900000) {break;}
//        } else if (strcmp(tableName, "ITEM") == 0) {
//            if (i == 90000) {break;}
//        }
    }
    return true;
}

// Fetch data from PostgreSQL and insert into SQLite
bool migrateTableData(PGconn *_conn, sqlite3 *in_mem_db, const char *tableName) {
    std::string query = "SELECT * FROM " + std::string(tableName);
    PGresult *res = PQexec(_conn, query.c_str());

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cerr << "Failed to fetch data from PostgreSQL for table: " << PQerrorMessage(_conn) << std::endl;
        PQclear(res);
        return false;
    }

    bool success = insertDataFromResult(_conn, in_mem_db, res, tableName);

    PQclear(res);
    return success;
}

// PostgreSQLデータベースへの接続の設定
void connectToPostgres(PGconn *_conn, const char *backend_postgres_connInfo) {
    _conn = PQconnectdb(backend_postgres_connInfo);
    if (PQstatus(_conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(_conn));
    }
}


// SQLiteのインメモリデータベースの初期化
void initializeSQLite(PGconn *conn, sqlite3 *&in_mem_db) {
    int ret = sqlite3_open(":memory:", &in_mem_db);
    if (ret != SQLITE_OK) {
        std::cout << "FILE OPEN Error";
    }

    ret = sqlite3_exec(in_mem_db, text_create_OORDER,
                       NULL, NULL, NULL);

    ret = sqlite3_exec(in_mem_db, text_create_DISTRICT,
                       NULL, NULL, NULL);
    ret = sqlite3_exec(in_mem_db, text_create_ITEM,
                       NULL, NULL, NULL);
    ret = sqlite3_exec(in_mem_db, text_create_WAREHOUSE,
                       NULL, NULL, NULL);
    ret = sqlite3_exec(in_mem_db, text_create_CUSTOMER,
                       NULL, NULL, NULL);
    ret = sqlite3_exec(in_mem_db, text_create_ORDER_LINE,
                       NULL, NULL, NULL);
    ret = sqlite3_exec(in_mem_db, text_create_NEW_ORDER,
                       NULL, NULL, NULL);
    ret = sqlite3_exec(in_mem_db, text_create_STOCK,
                       NULL, NULL, NULL);
    ret = sqlite3_exec(in_mem_db, text_create_HISTORY,
                       NULL, NULL, NULL);
    ret = sqlite3_exec(in_mem_db, text_create_tbl_sbtest1,
                       NULL, NULL, NULL);


//        const char *tables[] = {
//                "OORDER", "DISTRICT", "ITEM", "WAREHOUSE",
//                "ORDER_LINE", "STOCK", "HISTORY"
//        };

//    const char *tables[] = {
//            "CUSTOMER", "WAREHOUSE", "DISTRICT", "STOCK", "ITEM"
//    };

    const char *tables[] = {
            "sbtest1"
    };

//    const char *tables[] = {};

    int numTables = sizeof(tables) / sizeof(tables[0]);

    for (int i = 0; i < numTables; i++) {
        if (!migrateTableData(conn, in_mem_db, tables[i])) {
            std::cerr << "Migration failed for table " << tables[i] << std::endl;
//                sqlite3_close(in_mem_db);
            PQfinish(conn);
        }
    }

    // 再オープン // TODO DO NOT DO THIS !! 一度クローズするとメモリの中身が消去される

    if (ret != SQLITE_OK) {
        printf("ERROR(%d) %s\n", ret, sqlite3_errmsg(in_mem_db));
    }
}

void run_proxy(unsigned short local_port,
               unsigned short forward_port, const std::string local_host,
               const std::string forward_host[]) {

    boost::asio::io_service ios;
    // 乱数生成器を初期化
    std::srand(std::time(nullptr));
    // 配列の要素数を計算
    int arraySize = 3;

    // ランダムに要素を選択
    int randomIndex = std::rand() % arraySize;
    std::string selectedElement = forward_host[randomIndex];


    try {
        tcp_proxy::bridge::acceptor acceptor(ios,
                                             local_host, local_port,
                                             selectedElement, forward_port);
        acceptor.accept_connections();

        // マルチスレッドでio_serviceを実行
        const int thread_count = 36;
        std::vector<std::thread> threads;
        for (int i = 0; i < thread_count; ++i) {
            threads.emplace_back([&ios]() {
                ios.run();
            });
        }

        for (auto &t: threads) {
            t.join();
        }

    }
    catch (std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

int main(int argc, char *argv[]) {
    if (argc != 5) {
        std::cerr << "usage: tcpproxy_server <local host ip> <local port> <forward host ip> <forward port>"
                  << std::endl;
        return 1;
    }

    // initialize sqlite and postgres
    // postgresのコネクション
    static constexpr char *backend_postgres_conninfo = "host=192.168.11.16 port=5433 dbname=yugabyte user=yugabyte password=yugabyte";
    PGconn *_conn;

    _conn = PQconnectdb(backend_postgres_conninfo);
    /* バックエンドとの接続確立に成功したかを確認する */
    if (PQstatus(_conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(_conn));
        exit(1);
    }

//    initializeSQLite(_conn, in_mem_db);
    PGConnectionPool& pool = PGConnectionPool::getInstance();

    const auto forward_port = static_cast<unsigned short>(::atoi(argv[4]));
    const std::string local_host = argv[1];
    const std::string forward_host = argv[3];

//    std::string forward_hosts[10] = {"192.168.12.23", "192.168.12.22", "192.168.12.21", "192.168.12.20", "192.168.12.19", "192.168.12.18", "192.168.12.17", "192.168.12.16", "192.168.12.15","192.168.12.14"};
//    unsigned short local_ports[10] = {5432, 5433, 5434, 5435, 5436, 5437, 5438, 5439, 5440, 5441};

    std::string forward_hosts[10] = {"192.168.11.16", "192.168.11.14", "192.168.11.15"};
    unsigned short local_ports[10] = {5432};

//    run_proxy(local_ports[0], forward_port, local_host, forward_hosts, in_mem_db);
    run_proxy(local_ports[0], forward_port, local_host, forward_hosts);

    return 0;
}
