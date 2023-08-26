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

#include "server.h++"

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
bool insertDataFromResult(PGconn *_conn, sqlite3 *in_mem_db, PGresult *res, const char* tableName) {
    int nFields = PQnfields(res);
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
    }
    return true;
}

// Fetch data from PostgreSQL and insert into SQLite
bool migrateTableData(PGconn *_conn, sqlite3 *in_mem_db, const char* tableName) {
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

//int callback(void* notUsed, int argc, char** argv, char** azColName) {
//    for (int i = 0; i < argc; i++) {
//        std::cout << azColName[i] << " = " << (argv[i] ? argv[i] : "NULL") << std::endl;
//    }
//    std::cout << std::endl;
//    return 0;
//}

// SQLiteのインメモリデータベースの初期化
void initializeSQLite(PGconn* conn, sqlite3*& in_mem_db) {
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

//
//        const char *tables[] = {
//                "OORDER", "DISTRICT", "ITEM", "WAREHOUSE", "CUSTOMER",
//                "ORDER_LINE", "NEW_ORDER", "STOCK", "HISTORY"
//        };

        const char *tables[] = {
                "OORDER", "DISTRICT"
        };

        int numTables = sizeof(tables) / sizeof(tables[0]);

        for (int i = 0; i < numTables; i++) {
            if (!migrateTableData(conn, in_mem_db, tables[i])) {
                std::cerr << "Migration failed for table " << tables[i] << std::endl;
//                sqlite3_close(in_mem_db);
                PQfinish(conn);
            }
        }

    // 再オープン // TODO DO NOT DO THIS !! 一度クローズするとメモリの中身が消去される
//    sqlite3_close(in_mem_db);


    // start region test
//    const char* sql = "SELECT D_NEXT_O_ID   FROM DISTRICT WHERE D_W_ID = 4    AND D_ID = 1";
//    char* zErrMsg = 0;
//    int rc = sqlite3_exec(in_mem_db, sql, callback, 0, &zErrMsg);
//    if (rc != SQLITE_OK) {
//        std::cerr << "SQL error: " << zErrMsg << std::endl;
//        sqlite3_free(zErrMsg);
//    } else {
//        std::cout << "Operation done successfully" << std::endl;
//    }

    // end region test
    std::cout << "Migration succeeded." << std::endl;

    if (ret != SQLITE_OK) {
        printf("ERROR(%d) %s\n", ret, sqlite3_errmsg(in_mem_db));
    }
}

void run_proxy(unsigned short local_port,
               unsigned short forward_port
               , const std::string local_host,
               const std::string forward_host,
               sqlite3*& in_mem_db)
{

    boost::asio::io_service ios;
    try
    {
        tcp_proxy::bridge::acceptor acceptor(ios,
                                             local_host, local_port,
                                             forward_host, forward_port, in_mem_db);

        acceptor.accept_connections();

        ios.run();
    }
    catch(std::exception& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

int main(int argc, char* argv[])
{
    if (argc != 5)
    {
        std::cerr << "usage: tcpproxy_server <local host ip> <local port> <forward host ip> <forward port>" << std::endl;
      return 1;
   }


    // initialize sqlite and postgres
    // postgresのコネクション
    static constexpr char *backend_postgres_conninfo = "host=192.168.12.17 port=5433 dbname=yugabyte user=yugabyte password=yugabyte";
    PGconn *_conn;
    sqlite3 *in_mem_db;

    _conn = PQconnectdb(backend_postgres_conninfo);
    /* バックエンドとの接続確立に成功したかを確認する */
    if (PQstatus(_conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(_conn));
        exit(1);
    }

    initializeSQLite(_conn, in_mem_db);

    std::cout << "test" << std::endl;

//    const char* sql = "SELECT D_NEXT_O_ID   FROM DISTRICT WHERE D_W_ID = 4    AND D_ID = 1";
//    char* zErrMsg = 0;
//    int rc = sqlite3_exec(in_mem_db, sql, callback, 0, &zErrMsg);
//    if (rc != SQLITE_OK) {
//        std::cerr << "error code : " << rc << std::endl;
//        std::cerr << "SQL error: " << zErrMsg << std::endl;
//        sqlite3_free(zErrMsg);
//    } else {
//        std::cout << "Operation done successfully" << std::endl;
//    }

   const int num_threads = 4;
   auto local_port   = static_cast<unsigned short>(::atoi(argv[2]));
   const auto forward_port = static_cast<unsigned short>(::atoi(argv[4]));
   const std::string local_host      = argv[1];
   const std::string forward_host    = argv[3];

//    std::string forward_hosts[10] = {"192.168.12.23", "192.168.12.22", "192.168.12.21", "192.168.12.20", "192.168.12.19", "192.168.12.18", "192.168.12.17", "192.168.12.16", "192.168.12.15","192.168.12.14"};
//    unsigned short local_ports[10] = {5432, 5433, 5434, 5435, 5436, 5437, 5438, 5439, 5440, 5441};

    std::string forward_hosts[10] = {"192.168.12.17"};
    unsigned short local_ports[10] = {5432};

    boost::thread_group threads;
//    for (short i = 0; i < 16; i++) {
//        threads.create_thread([local_ports, forward_port, local_host, forward_hosts, i] {
//            return run_proxy(local_ports[0] + i, forward_port, local_host, forward_hosts[i % 10]); });
//        local_port++;
//    }
    threads.create_thread([local_ports, forward_port, local_host, forward_hosts, &in_mem_db] { return run_proxy(local_ports[0], forward_port, local_host, forward_hosts[0], in_mem_db); });

    threads.join_all();

   return 0;
}
