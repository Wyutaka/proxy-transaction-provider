//
// Created by user1 on 22/10/20.
//

#pragma once

#include "../../../kvs.h++"
#include <cassandra.h>
#include <iostream>
#include <memory>
#include "../../../server.h++"
#include <boost/shared_ptr.hpp>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread/mutex.hpp>
#include <utility>
#include "server.h++"
#include <stdlib.h>
#include <iostream>
#include <variant>
#include <libpq-fe.h>
#include "./src/lock/Lock.h++"
#include "./src/transaction/transaction_impl.hpp"
#include "./src/test/DumpHex.h++"
#include <boost/thread.hpp>

static void
exit_nicely(PGconn *conn) {
    PQfinish(conn);
    exit(1);
}

const bool CHACHED = true;

namespace transaction {

    class PostgresConnector {
    private:
        boost::shared_ptr<tcp_proxy::bridge> _bridge;
        std::shared_ptr<CassFuture> _connectFuture;
        PGconn *_conn;

    public:
        explicit PostgresConnector(boost::shared_ptr<tcp_proxy::bridge> bridge, PGconn *conn)
                : _connectFuture(), _conn(conn), _bridge(bridge) {
        }

    private:
        void download_result(PGconn &conn, const Request &req,
                             std::queue<response::sysbench_result_type> results) {
            int nFields;
            int i, j;
            PGresult *res;
            /* トランザクションブロックを開始する。 */
            res = PQexec(&conn, "BEGIN");
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(&conn));
                PQclear(res);
                std::cout << "test" << std::endl;
//                exit_nicely(conn);
            }

//            PQclear(res);

            /*
             * データベースのシステムカタログpg_databaseから行を取り出す。
             */
            auto get_cursor_query = "DECLARE myportal CURSOR FOR ";
//            std::cout << std::string(req.query().query().data()).c_str() << std::endl;
            res = PQexec(&conn, (get_cursor_query + std::string(req.query().query().data())).c_str());
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                fprintf(stderr, "DECLARE CURSOR failed: %s", PQerrorMessage(&conn));
                PQclear(res);
//                exit_nicely(conn);
            }
//            PQclear(res);

            res = PQexec(&conn, "FETCH ALL in myportal");
            if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                fprintf(stderr, "FETCH ALL failed: %s", PQerrorMessage(&conn));
                PQclear(res);
//                exit_nicely(conn);
            }

            nFields = PQnfields(res);

//            std::cout << "nFIelds:" << nFields << std::endl;
//            std::cout << "PQntuples:" << PQntuples(res) << std::endl;
//            std::cout << "test1-1" << std::endl;
            /* 行を結果に追加。 */
            for (i = 0; i < PQntuples(res); i++) {
                if (nFields == 4) { // resultが4つの時
                    response::Sysbench result_record({"id", "k", "c", "pad"},
                                                     nFields); // TODO change table by select query
                    for (j = 0; j < nFields; j++) {
                        result_record.addColumn(PQgetvalue(res, i, j));
                    }
                    results.emplace(result_record);
                } else if (req.query().query()[7] == 'c' || req.query().query()[7] == 'D') { // DISTINCT or c
                    response::Sysbench_one result_record({"c"}, nFields); // TODO change table by select query
                    for (j = 0; j < nFields; j++) {
                        result_record.addColumn(PQgetvalue(res, i, j));
                    }
                    results.emplace(result_record);
                } else if (req.query().query()[7] == 's' || req.query().query()[7] == 'S') { // select sum
                    response::Sysbench_one result_record({"sum"}, nFields); // TODO change table by select query
                    for (j = 0; j < nFields; j++) {
                        result_record.addColumn(PQgetvalue(res, i, j));
                    }
                    results.emplace(result_record);
                }
            }

            PQclear(res);

            /* ポータルを閉ざす。ここではエラーチェックは省略した… */
            res = PQexec(&conn, "CLOSE myportal");
            PQclear(res);

            /* トランザクションを終了する */
            res = PQexec(&conn, "END");
            PQclear(res);
        }

        // Parseメッセージに対するレスポンスParseComplete
        // 識別子: 1
        void create_parse_complete_message(std::vector<unsigned char> &response_buffer) {
            unsigned char message[] = {0x31, 0x00, 0x00, 0x00, 0x04};
            response_buffer.insert(response_buffer.end(), std::begin(message), std::end(message));
        }

        // Parseメッセージに対するレスポンスParseComplete
        // 識別子: 2
        void create_bind_complete_message(std::vector<unsigned char> &response_buffer) {
            unsigned char message[] = {0x32, 0x00, 0x00, 0x00, 0x04};
            response_buffer.insert(response_buffer.end(), std::begin(message), std::end(message));
        }

        // Describeメッセージに対するレスポンスno_data
        // 識別子: 2
        void create_no_data_message(std::vector<unsigned char> &response_buffer) {
            unsigned char message[] = {0x6e, 0x00, 0x00, 0x00, 0x04};
            response_buffer.insert(response_buffer.end(), std::begin(message), std::end(message));
        }

        template<typename T>
        std::vector<unsigned char> toBigEndian(T value) {
            std::vector<unsigned char> bytes(sizeof(T));
            for (size_t i = 0; i < sizeof(T); ++i) {
                bytes[sizeof(T) - 1 - i] = (value >> (i * 8)) & 0xFF;
            }
            return bytes;
        }

        // Describeメッセージに対するレスポンスRowDescription
        // 識別子: T
        void
        create_row_desription_message(sqlite3 *in_mem_db, std::vector<unsigned char> &response_buffer, PGconn &conn,
                                      Request &req) {
            // Tの追加
            response_buffer.push_back('T');

            // メッセージサイズのインデックス(インデックス+1をとりあえず用意しておく)
            unsigned char message_size_offset[] = {0x00, 0x00, 0x00, 0x00};
            response_buffer.insert(response_buffer.end(), std::begin(message_size_offset),
                                   std::end(message_size_offset));

//                unsigned char message[] = {0x32, 0x00, 0x00, 0x00, 0x04};
//                response_buffer.insert(response_buffer.end(), std::begin(message), std::end(message));

            int message_size = 4;
            auto query = req.query().query().data();

            // バックエンドへの問い合わせ
            sqlite3_stmt *stmt;
            int rc = sqlite3_prepare_v2(in_mem_db, query, -1, &stmt, 0);
            rc = sqlite3_step(stmt);

            if (rc == SQLITE_ROW) { // Data exists in SQLite
                int num_field = sqlite3_column_count(stmt);
                auto num_field_bytes = toBigEndian(static_cast<int16_t>(num_field));
                response_buffer.insert(response_buffer.end(), num_field_bytes.begin(), num_field_bytes.end());
                message_size += 2;

                for (int i = 0; i < num_field; ++i) {
                    const char* field_name = sqlite3_column_name(stmt, i);

                    // Placeholder values, replace as needed
                    int32_t table_oid = 0;
                    int16_t column_attribute_number = 0;
                    int32_t field_data_type_oid = 0;
                    int16_t data_type_size = 0;
                    int32_t type_modifier = 0;
                    int16_t format_code = 0;

                    add_row_description_to_buffer(response_buffer, message_size, field_name, table_oid, column_attribute_number,
                                          field_data_type_oid, data_type_size, type_modifier, format_code);
                }
            } else { // No result from SQLite, query from Postgres
                // ... [Same as before, up to the Postgres processing]

                PGresult *pg_res = PQexec(&conn, query);
                if (PQresultStatus(pg_res) == PGRES_TUPLES_OK) {
                    int num_field = PQnfields(pg_res);
                    auto num_field_bytes = toBigEndian(static_cast<int16_t>(num_field));
                    response_buffer.insert(response_buffer.end(), num_field_bytes.begin(), num_field_bytes.end());
                    message_size += 2;

                    for (int i = 0; i < num_field; ++i) {
                        std::string field_name = PQfname(pg_res, i);

                        int32_t table_oid = PQftable(pg_res, i);
                        int16_t column_attribute_number = PQftablecol(pg_res, i);
                        int32_t field_data_type_oid = PQftype(pg_res, i);
                        int16_t data_type_size = 0; // Placeholder, might need actual lookup
                        int32_t type_modifier = 0; // Placeholder, might need actual lookup
                        int16_t format_code = 0; // Placeholder, use appropriate value

                        add_row_description_to_buffer(response_buffer, message_size, field_name, table_oid, column_attribute_number,
                                              field_data_type_oid, data_type_size, type_modifier, format_code);
                    }
//                    PQclear(pg_res);
                }
//                PQfinish(&conn);
            }

            // Insert message size at the 2nd position
            auto message_size_bytes = toBigEndian(message_size);
            response_buffer.insert(response_buffer.begin() + 1, message_size_bytes.begin(), message_size_bytes.end());

//            sqlite3_finalize(stmt);
//            sqlite3_close(sqlite_db);
        }

        template<typename T>
        void insertToResponseBuffer(std::vector<unsigned char> &response_buffer, int &message_size, const T &data) {
            auto bytes = toBigEndian(data);
            response_buffer.insert(response_buffer.end(), bytes.begin(), bytes.end());
            message_size += bytes.size();
        }

// 同様の処理を繰り返す関数
        void add_row_description_to_buffer(std::vector<unsigned char> &response_buffer, int &message_size,
                                           const std::string &field_name,
                                           int32_t table_oid,
                                           int16_t column_attribute_number,
                                           int32_t field_data_type_oid,
                                           int16_t data_type_size,
                                           int32_t type_modifier,
                                           int16_t format_code) {

            response_buffer.insert(response_buffer.end(), field_name.begin(), field_name.end());
            message_size += field_name.size();

            insertToResponseBuffer(response_buffer, message_size, table_oid);
            insertToResponseBuffer(response_buffer, message_size, column_attribute_number);
            insertToResponseBuffer(response_buffer, message_size, field_data_type_oid);
            insertToResponseBuffer(response_buffer, message_size, data_type_size);
            insertToResponseBuffer(response_buffer, message_size, type_modifier);
            insertToResponseBuffer(response_buffer, message_size, format_code);
        }

        void create_data_row_message() {}


//        const

        int COLUMN_COUNT_ALL = 4;
        const char SELECT_ALL_CHAR = '*';
        const char DISTINCT_CHAR = 'D';
        const char SUM_CHAR = 's';
        const char COLUMN_CHAR = 'c';

        using RecordConstructor = std::function<response::sysbench_result_type(const std::vector<std::string> &,
                                                                               int)>;

        template<std::size_t N>
        auto createSysbenchLambda() {
            return [](std::array<std::string_view, N> columns, int count) {
                return response::Sysbench(columns, count);
            };
        }

        /*
         * インメモリdbから値を取得する.インメモリデータがある場合はtrueを返す.
         * sysbenchを回すためにハードコードしてる部分がある
         */
        bool
        get_from_local(sqlite3 *in_mem_db, const Request &req,
                       std::queue<response::sysbench_result_type> &results) {
            if (!req.query().isSelect()) { return false; }

            std::unordered_map<char, std::pair<std::vector<std::string>, RecordConstructor>> result_types = {
                    {'*', {{"id", "k", "c", "pad"}, [](const std::vector<std::string> &columns,
                                                       int count) -> response::sysbench_result_type {
                        return response::Sysbench({"id", "k", "c", "pad"}, count);
                    }}},
                    {'c', {{"c"},                   [](const std::vector<std::string> &columns,
                                                       int count) -> response::sysbench_result_type {
                        return response::Sysbench({"c"}, count);
                    }}},
                    {'D', {{"c"},                   [](const std::vector<std::string> &columns,
                                                       int count) -> response::sysbench_result_type {
                        return response::Sysbench({"c"}, count);
                    }}},
                    {'s', {{"sum"},                 [](const std::vector<std::string> &columns,
                                                       int count) -> response::sysbench_result_type {
                        return response::Sysbench({"sum"}, count);
                    }}},
                    {'S', {{"sum"},                 [](const std::vector<std::string> &columns,
                                                       int count) -> response::sysbench_result_type {
                        return response::Sysbench({"sum"}, count);
                    }}},
            };

            int row_count;
            sqlite3_stmt *statement = nullptr;
            int prepare_rc = sqlite3_prepare_v2(in_mem_db, req.query().query().data(), -1, &statement, nullptr);
            if (prepare_rc == SQLITE_OK) {
                while (sqlite3_step(statement) == SQLITE_ROW) {
                    row_count++;
                    auto res_type_char = req.query().query()[7];
                    int columnCount = sqlite3_column_count(statement);

                    if (result_types.find(res_type_char) != result_types.end() || columnCount == 4) {
                        auto [column_names, constructor] = result_types[res_type_char];
                        auto result_record = constructor(column_names, columnCount);
                        populateColumns(statement, result_record, columnCount);
                        results.emplace(result_record);
                    }

                }
            } else {
                printf("ERROR(%d) %s\n", prepare_rc, sqlite3_errmsg(in_mem_db));
            }
            sqlite3_reset(statement);
            sqlite3_finalize(statement);

            return row_count > 0;
        }

        void populateColumns(sqlite3_stmt *statement, response::sysbench_result_type &result_record_variant,
                             int columnCount) {
            std::visit([&](auto &result_record) {
                for (int j = 0; j < columnCount; j++) {
                    int columnType = sqlite3_column_type(statement, j);
                    switch (columnType) {
                        case SQLITE_TEXT:
                            result_record.addColumn(sqlite3_column_text(statement, j));
                            break;
                        case SQLITE_INTEGER:
                            result_record.addColumn(sqlite3_column_int(statement, j));
                            break;
                    }
                }
            }, result_record_variant);
        }

    public:

        Response operator()(const Request &req, sqlite3 *in_mem_db) {
//            debug::hexdump(req.query().query().data(), req.query().query().size()); // for test
//                std::cout << req.query().query() << std::endl;
            if (req.query().isSelect()) {
                std::queue<response::sysbench_result_type> results;

                bool isCached = get_from_local(in_mem_db, req, results);
                if (!isCached) {
                    return Response({CoResponse(Status::Select_Pending)});
                }

                auto res = Response({CoResponse(Status::Result)});
                res.begin()->set_results(results);
                return res;
            }

            return Response({CoResponse(Status::Ok)});;
        }
    };
} // namespace transaction

#undef CASS_SHARED_PTR

