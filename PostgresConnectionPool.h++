//
// Created by y-watanabe on 10/31/23.
//

#ifndef MY_PROXY_POSTGRESCONNECTIONPOOL_H
#define MY_PROXY_POSTGRESCONNECTIONPOOL_H

#include <iostream>
#include <libpq-fe.h>
#include <vector>
#include <mutex>
#include "SQLiteConnectionPool.h++"

// Connection pool class
class PGConnectionPool {
public:

    static PGConnectionPool &getInstance() {
        static PGConnectionPool instance;
        return instance;
    }

    ~PGConnectionPool() {
        for (auto conn : connections) {
            PQfinish(conn);
        }
    }

    PGconn* getConnection() {
        std::unique_lock<std::mutex> lock(mutex_);

        if (!connections.empty()) {
            PGconn* conn = connections.back();
            connections.pop_back();
            return conn;
        } else {
            std::cerr << "Connection pool is empty." << std::endl;
            return nullptr;
        }
    }

    void releaseConnection(PGconn* conn) {
        if (conn != nullptr) {
            std::lock_guard<std::mutex> lock(mutex_);
            connections.push_back(conn);
        }
    }

private:
    std::vector<PGconn*> connections;
    const char *local_pg_conninfo = "host=192.168.11.5 port=5430 dbname=postgres user=postgres";
    const char *backend_pg_conninfo ="host=192.168.11.16 port=5433 dbname=yugabyte user=yugabyte password=yugabyte";
    std::mutex mutex_;

    PGConnectionPool() {
        int pool_size = 16;

        for (int i = 0; i < pool_size; ++i) {
            PGconn* conn = PQconnectdb(local_pg_conninfo);

            if (PQstatus(conn) != CONNECTION_OK) {
                std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
                PQfinish(conn);
            } else {
                connections.push_back(conn);
            }
        }

        std::cout << "conn size" << connections.size() << std::endl;

        auto _conn = PQconnectdb(backend_pg_conninfo);

        /* バックエンドとの接続確立に成功したかを確認する */
        if (PQstatus(_conn) != CONNECTION_OK) {
            fprintf(stderr, "Connection to database failed: %s",
                    PQerrorMessage(_conn));
            exit(1);
        }

        fetch_data_from_postgres(_conn);
    };

    void fetch_data_from_postgres(PGconn *conn) {

        // Execute the SQL query
        PGresult* res = PQexec(conn, text_create_tbl_sbtest1);

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

        // Clear result and close the connection
        PQclear(res);
        PQfinish(conn);

        std::cout << "Migration succeeded." << std::endl;
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

        auto local_db = getConnection();

        for (int i = 0; i < PQntuples(res); i++) {
            std::string insertQuery = "INSERT OR REPLACE INTO " + std::string(tableName) + " VALUES(";
            for (int j = 0; j < nFields; j++) {
                if (j > 0) {
                    insertQuery += ",";
                }
                insertQuery += "'" + std::string(PQgetvalue(res, i, j)) + "'";
            }
            insertQuery += ");";

            PQexec(local_db, insertQuery.c_str());
        }

        releaseConnection(local_db);
        return true;
    }
};
#endif //MY_PROXY_POSTGRESCONNECTIONPOOL_H
