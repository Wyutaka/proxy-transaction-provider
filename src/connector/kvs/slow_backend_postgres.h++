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
        // 識別子: n
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

        std::vector<unsigned char> intToBigEndian(int32_t value) {
            std::vector<unsigned char> bytes(4);

            bytes[0] = (value >> 24) & 0xFF;
            bytes[1] = (value >> 16) & 0xFF;
            bytes[2] = (value >> 8) & 0xFF;
            bytes[3] = value & 0xFF;

            return bytes;
        }

        std::vector<unsigned char> intToBigEndian(int16_t value) {
            std::vector<unsigned char> bytes(2);

            bytes[0] = (value >> 8) & 0xFF;
            bytes[1] = value & 0xFF;

            return bytes;
        }

        // RowDescriptionメッセージに対するレスポンスRowDescription
        // 識別子: T
        int
        create_row_description_message(sqlite3 *in_mem_db, std::vector<unsigned char> &response_buffer, PGconn &conn,
                                       Request &req) {
            std::cout << "create_row_desc_message" << std::endl;
            // Tの追加
            response_buffer.push_back('T');

            // メッセージサイズを代入するためのインデックスを保存
            size_t lastIndex = response_buffer.size();

            int num_rows = 0;

            int message_size = 4;
            auto query = req.query().query().data();

            // バックエンドへの問い合わせ
            sqlite3_stmt *stmt;
            int rc = sqlite3_prepare_v2(in_mem_db, query, -1, &stmt, 0);
            rc = sqlite3_step(stmt);

            if (rc == SQLITE_ROW) { // Data exists in SQLite
                int num_field = sqlite3_column_count(stmt);
                std::cout << "from sqlite" << num_field << std::endl;
//                auto num_field_bytes = toBigEndian(static_cast<int16_t>(num_field));
//                response_buffer.insert(response_buffer.end(), num_field_bytes.begin(), num_field_bytes.end());

                uint16_t twoBytes = static_cast<uint16_t>(num_field);
                response_buffer.insert(response_buffer.end(), reinterpret_cast<unsigned char *>(&twoBytes),
                                       reinterpret_cast<unsigned char *>(&twoBytes + 1));

                // 上位バイト (high byte) と下位バイト (low byte) を取得
                unsigned char highByte = (twoBytes >> 8) & 0xFF;
                unsigned char lowByte = twoBytes & 0xFF;

                message_size += 2;

                for (int i = 0; i < num_field; ++i) {
                    const char *field_name = sqlite3_column_name(stmt, i);

                    // Placeholder values, replace as needed
                    int32_t table_oid = 0;
                    int16_t column_attribute_number = 0;
                    int32_t field_data_type_oid = 0;
                    int16_t data_type_size = 0;
                    int32_t type_modifier = 0;
                    int16_t format_code = 0;

                    add_row_description_to_buffer(response_buffer, message_size, field_name, table_oid,
                                                  column_attribute_number,
                                                  field_data_type_oid, data_type_size, type_modifier, format_code);
                }
                // Tの次にメッセージサイズついか
                auto message_size_bytes = toBigEndian(message_size);
                response_buffer.insert(response_buffer.begin() + lastIndex, message_size_bytes.begin(),
                                       message_size_bytes.end());

                // Dメッセージを追加
                while (rc == SQLITE_ROW) {
                    num_rows++;
                    addRowDataToBuffer(stmt, num_field, response_buffer);
                    rc = sqlite3_step(stmt);
                }

            } else {
                // No result from SQLite, query from Postgres
                // ... [Same as before, up to the Postgres processing]
                PGresult *pg_res = PQexec(&conn, query);
                if (PQresultStatus(pg_res) == PGRES_TUPLES_OK) {
                    int num_field = PQnfields(pg_res);
                    std::cout << "from postgres" << num_field << std::endl;

//                    auto num_field_bytes = toBigEndian(static_cast<int16_t>(num_field));
//                    response_buffer.insert(response_buffer.end(), num_field_bytes.begin(), num_field_bytes.end());

                    uint16_t twoBytes = static_cast<uint16_t>(num_field);

                    // 上位バイト (high byte) と下位バイト (low byte) を取得
                    unsigned char highByte = (twoBytes >> 8) & 0xFF;
                    unsigned char lowByte = twoBytes & 0xFF;

                    response_buffer.push_back(highByte);
                    response_buffer.push_back(lowByte);

                    message_size += 2;

                    // 各フィールドにたいして
                    for (int i = 0; i < num_field; ++i) {
                        std::string field_name = PQfname(pg_res, i);

                        int32_t table_oid = PQftable(pg_res, i);
                        int16_t column_attribute_number = PQftablecol(pg_res, i);
                        int32_t field_data_type_oid = PQftype(pg_res, i);
                        int16_t data_type_size = -1; // Placeholder, might need actual lookup
                        int32_t type_modifier = 0; // Placeholder, might need actual lookup
                        int16_t format_code = 0; // Placeholder, use appropriate value

                        add_row_description_to_buffer(response_buffer, message_size, field_name, table_oid,
                                                      column_attribute_number,
                                                      field_data_type_oid, data_type_size, type_modifier, format_code);
                    }
                    // Tの次にメッセージサイズついか
                    auto message_size_bytes = intToBigEndian(message_size);

                    response_buffer.insert(response_buffer.begin() + lastIndex, message_size_bytes.begin(),
                                           message_size_bytes.end());

                    // Dのメッセージを追加
                    num_rows = PQntuples(pg_res);
                    for (int row = 0; row < num_rows; ++row) {
                        addRowDataToBuffer(pg_res, row, num_field, response_buffer);
                    }
//                    PQclear(pg_res);
                }
//                PQfinish(&conn);
            }

            std::cout << "num_row in row description :" << num_rows << std::endl;
            return num_rows;
//            sqlite3_finalize(stmt);
//            sqlite3_close(sqlite_db);
        }

        void addRowDataToBuffer(sqlite3_stmt *stmt, int num_field, std::vector<unsigned char> &response_buffer) {
            int message_size;
            size_t last_index_D;
            initializeBuffer(num_field, response_buffer, message_size, last_index_D);

            for (int i = 0; i < num_field; ++i) {
                auto column_data = sqlite3_column_text(stmt, i);
                int column_length = sqlite3_column_bytes(stmt, i);
                int column_type = sqlite3_column_type(stmt, i);
                processColumnData(column_data, column_length, column_type, response_buffer, message_size);
            }

            auto message_size_bytes = intToBigEndian(message_size);

            response_buffer.insert(response_buffer.begin() + last_index_D, message_size_bytes.begin(),
                                   message_size_bytes.end());
        }


        void addRowDataToBuffer(PGresult *pg_res, int row, int num_field, std::vector<unsigned char> &response_buffer) {
            int message_size;
            size_t last_index_D;
            initializeBuffer(num_field, response_buffer, message_size, last_index_D);

            for (int i = 0; i < num_field; ++i) {
                const auto *column_data = reinterpret_cast<const unsigned char *>(PQgetvalue(pg_res, row, i));
                int column_length = PQgetlength(pg_res, row, i);
                Oid column_type = PQftype(pg_res, i);
                processColumnData(column_data, column_length, column_type, response_buffer, message_size);
            }

            std::cout << "num_field:" << num_field << std::endl;
            std::cout << "message_size:" << message_size << std::endl;

            // メッセージサイズの挿入
            auto message_size_bytes = intToBigEndian(message_size);
            response_buffer.insert(response_buffer.begin() + last_index_D, message_size_bytes.begin(),
                                   message_size_bytes.end());
        }

        void initializeBuffer(int num_field, std::vector<unsigned char> &response_buffer, int &message_size, size_t &last_index_D) {
            response_buffer.push_back('D');
            message_size = 4 + 2;  // 4 for initial size and 2 for field count
            last_index_D = response_buffer.size();

            std::cout << "num_field in initializeBuffer:" << num_field << std::endl;
            uint16_t twoBytes = static_cast<uint16_t>(num_field);
//            std::cout << "twoBytes in initializeBuffer:" << num_field << std::endl;
//
//            auto field_count = intToBigEndian(twoBytes);
//            response_buffer.insert(response_buffer.end(), field_count.begin(), field_count.end());

            // 上位バイト (high byte) と下位バイト (low byte) を取得
            unsigned char highByte = (twoBytes >> 8) & 0xFF;
            unsigned char lowByte = twoBytes & 0xFF;

            response_buffer.push_back(highByte);
            response_buffer.push_back(lowByte);

        }


        void processColumnData(const unsigned char* column_data, int column_length, Oid column_type, std::vector<unsigned char> &response_buffer, int &message_size) {
            message_size += 4;

            std::cout << "column_data" << column_data << std::endl;
            std::cout << "column_length" << column_length << std::endl;

            if (column_type == SQLITE_TEXT || column_type != 23) {
                auto column_length_bytes = intToBigEndian(column_length);
                response_buffer.insert(response_buffer.end(), column_length_bytes.begin(), column_length_bytes.end());

                std::cout << "value in text" << std::endl;
                response_buffer.insert(response_buffer.end(), column_data, column_data + column_length);
                message_size += column_length;

            } else {
                auto column_length_bytes = intToBigEndian(4);
                response_buffer.insert(response_buffer.end(), column_length_bytes.begin(), column_length_bytes.end());

                int value = std::stoi(std::string(column_data, column_data + column_length));
                auto binary_representation = intToBigEndian(value);

                std::cout << "value in processColumnData:" << value << std::endl;
                std::cout << "binay_reprensentation" << std::endl;
                for (unsigned char byte: binary_representation) {
                    std::cout << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(byte) << " ";
                }
                std::cout << std::endl;

                response_buffer.insert(response_buffer.end(), binary_representation.begin(), binary_representation.end());
                message_size += 4;
            }
        }

        // Bindメッセージに対するレスポンスRowDescription
        // 識別子: D
        int
        create_row_data_message(sqlite3 *in_mem_db, std::vector<unsigned char> &response_buffer, PGconn &conn,
                                       Request &req) {
            std::cout << "row_data_message" << std::endl;

            int num_rows = 0;
            auto query = req.query().query().data();

            // バックエンドへの問い合わせ
            sqlite3_stmt *stmt;
            int rc = sqlite3_prepare_v2(in_mem_db, query, -1, &stmt, 0);
            rc = sqlite3_step(stmt);

            if (rc == SQLITE_ROW) { // Data exists in SQLite
                int num_field = sqlite3_column_count(stmt);
                std::cout << "from sqlite" << num_field << std::endl;
                // Dメッセージを追加
                while (rc == SQLITE_ROW) {
                    num_rows++;
                    addRowDataToBuffer(stmt, num_field, response_buffer);
                    rc = sqlite3_step(stmt);
                }

            } else {
                // No result from SQLite, query from Postgres
                // ... [Same as before, up to the Postgres processing]
                PGresult *pg_res = PQexec(&conn, query);
                if (PQresultStatus(pg_res) == PGRES_TUPLES_OK) {
                    int num_field = PQnfields(pg_res);
                    std::cout << "from postgres" << num_field << std::endl;
                    // Dのメッセージを追加
                    num_rows = PQntuples(pg_res);
                    for (int row = 0; row < num_rows; ++row) {
                        addRowDataToBuffer(pg_res, row, num_field, response_buffer);
                    }
//                    PQclear(pg_res);
                }
//                PQfinish(&conn);
            }
            std::cout << "num_row in row data :" << num_rows << std::endl;
            return num_rows;
//            sqlite3_finalize(stmt);
//            sqlite3_close(sqlite_db);
        }

        // 小文字に変換する関数
        std::string to_lowercase(const std::string& s) {
            std::string result = s;
            std::transform(result.begin(), result.end(), result.begin(),
                           [](unsigned char c) { return std::tolower(c); });
            return result;
        }


        void create_command_complete_message(std::vector<unsigned char> &response_buffer, Request &req, int num_rows) {
            // 'C'をresponse_bufferの末尾に追加
            response_buffer.push_back('C');

            // message_size_C = 4
            int message_size_C = 4;

            // last_index_Cにresponse_bufferの末尾のインデックスを取得
            size_t last_index_C = response_buffer.size();

            // command_tagを設定
            std::string command_tag;
            std::string query_str_lower = to_lowercase(req.query().query().data());  // Simplified for example

            if (query_str_lower.find("insert") != std::string::npos) {
                command_tag = "INSERT 0 " + std::to_string(num_rows);
            } else if (query_str_lower.find("delete") != std::string::npos) {
                command_tag = "DELETE " + std::to_string(num_rows);
            } else if (query_str_lower.find("update") != std::string::npos) {
                command_tag = "UPDATE " + std::to_string(num_rows);
            } else if (query_str_lower.find("select") != std::string::npos) {
                command_tag = "SELECT " + std::to_string(num_rows);
            } else if (query_str_lower.find("move") != std::string::npos) {
                command_tag = "MOVE " + std::to_string(num_rows);
            } else if (query_str_lower.find("fetch") != std::string::npos) {
                command_tag = "FETCH " + std::to_string(num_rows);
            } else if (query_str_lower.find("copy") != std::string::npos) {
                command_tag = "COPY " + std::to_string(num_rows);
            }

            // message_size_C += 挿入した文字列数
            message_size_C += command_tag.size() + 1;
//            std::cout << "C message size:" << std::to_string(message_size_C) << std::endl;
            std::cout << "num_rows" << num_rows << std::endl;
            std::cout << "command_tag" << command_tag << std::endl;

            auto message_size_bytes = intToBigEndian(message_size_C);

            response_buffer.insert(response_buffer.begin() + last_index_C, message_size_bytes.begin(),
                                   message_size_bytes.end());

            // command_tagをresponse_bufferの末尾に追加
            response_buffer.insert(response_buffer.end(), command_tag.begin(), command_tag.end());

            response_buffer.push_back(0); // null terminator
//            std::cout << "complettion: " << command_tag << std::endl;
        }

        void insertToResponseBuffer(std::vector<unsigned char> &response_buffer, int &message_size, int32_t data) {
            auto bytes = intToBigEndian(data);
            response_buffer.insert(response_buffer.end(), bytes.begin(), bytes.end());
            message_size += bytes.size();
        }

        void insertToResponseBuffer(std::vector<unsigned char> &response_buffer, int &message_size, int16_t data) {
            auto bytes = intToBigEndian(data);
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
            response_buffer.push_back(0); // null terminator
            message_size += field_name.size() + 1;

            insertToResponseBuffer(response_buffer, message_size, table_oid);
            insertToResponseBuffer(response_buffer, message_size, column_attribute_number);
            insertToResponseBuffer(response_buffer, message_size, field_data_type_oid);
            insertToResponseBuffer(response_buffer, message_size, data_type_size);
            insertToResponseBuffer(response_buffer, message_size, type_modifier);
            insertToResponseBuffer(response_buffer, message_size, format_code);
        }

        // Excecuteメッセージに対するレスポンスEmptyQueryResponse
        // 識別子: I
        void create_empty_query_response_message(std::vector<unsigned char> &response_buffer) {
            unsigned char message[] = {0x49, 0x00, 0x00, 0x00, 0x04};
            response_buffer.insert(response_buffer.end(), std::begin(message), std::end(message));
        }

        // Syncメッセージに対するレスポンスReadyForQuery
        // 識別子: Z
        // TODO transaction stateを動的に決定
        void create_ready_for_query_message(std::vector<unsigned char> &response_buffer) {
            unsigned char message[] = {0x5a, 0x00, 0x00, 0x00, 0x05, 0x49};
            response_buffer.insert(response_buffer.end(), std::begin(message), std::end(message));
        }

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

        // query = req.query().query().data()
        // client_queueの先頭をひとつづつ読み込む
        // client_messageがPの時,create_parse_complete_message(std::vector<unsigned char> &response_buffer)を実行
        // client_messageがBの時,create_bind_complete_message(std::vector<unsigned char> &response_buffer)を実行
        // client_messageがDかつqueryが空の時, create_no_data_message(std::vector<unsigned char> &response_buffer)を、client_messageがDの時、create_row_desription_message(sqlite3 *in_mem_db, std::vector<unsigned char> &response_buffer, PGconn &conn,Request &req)を実行
        // client_messageがEの時、create_command_complete_message(std::vector<unsigned char> &response_buffer, Request &req)を実行
        // client_messageがSの時、create_ready_for_query_message(std::vector<unsigned char> &response_buffer)を実行
        void process_client_message(std::vector<unsigned char> &response_buffer, std::queue<unsigned char> client_queue,
                                    sqlite3 *in_mem_db, PGconn &conn, Request req) {
            // commmand_completeメッセージのための行数
            int num_rows = 0;
            while (!client_queue.empty()) {  // キューが空になるまでループ
                unsigned char client_message = client_queue.front(); // client_queueの先頭を読み取る
                client_queue.pop(); // 読み取ったメッセージをキューから削除

                switch (client_message) {
                    case 'P':
                        create_parse_complete_message(response_buffer);
                        break;
                    case 'B':
                        create_bind_complete_message(response_buffer);

                        if (req.query().isSelect() && client_queue.front() != 'D') {
                            // 次が'D'じゃない時,DataRowだけ返す
                            std::cout << "get B with select query so create T D message" << std::endl;
                            num_rows = create_row_data_message(in_mem_db, response_buffer, conn, req);
                        }
//                        if (req.query().query().empty()) {
//                            create_no_data_message(response_buffer);
//                        } else {
//                            std::cout << "tes D Q" << std::endl;
//                            num_rows = create_row_description_message(in_mem_db, response_buffer, conn, req);
//                        }
                        break;
                    case 'D':
                        if (req.query().query().empty()) {
                            create_no_data_message(response_buffer);
                        } else {
                            std::cout << "tes D Q" << std::endl;
                            num_rows = create_row_description_message(in_mem_db, response_buffer, conn, req);
                        }
                        break;
                    case 'E':
                        std::cout << "num rows process client :" << num_rows << std::endl;
                        create_command_complete_message(response_buffer, req, num_rows);
                        num_rows = 0;
                        break;
                    case 'S':
                        create_ready_for_query_message(response_buffer);
                        break;
                    case 'Q':
                        create_row_description_message(in_mem_db, response_buffer, conn, req);
                        // TODO これ、Qがれんぞくしたらだめ？(dequeueで実装する)
                        client_queue.push('E');
                        client_queue.push('S');
                    default:
                        // 何もしないか、エラーメッセージを生成するなどの処理
                        break;
                }

                // response_bufferを何らかの方法で送信するか、他の処理を行う
            }
        }

        void printQueue(std::queue<unsigned char> &q) {
            std::queue<unsigned char> tempQueue;

            std::cout << "Queue content: ";

            while (!q.empty()) {
                unsigned char val = q.front();
                std::cout << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(val) << " ";

                q.pop();
                tempQueue.push(val);
            }

            // Restore the original queue
            while (!tempQueue.empty()) {
                q.push(tempQueue.front());
                tempQueue.pop();
            }

            std::cout << std::endl;
        }

    public:
        // responseを生成する
        // response.set_raw_responseでresponse_bufferを代入する
        // Statusはいらないのでは？？ -> 適当な値を入れてみる、参照しないこと

        Response operator()(const Request &req, sqlite3 *in_mem_db) {
            auto client_queue = req.queue();

//            debug::hexdump(req.query().query().data(), req.query().query().size()); // for test
//                std::cout << req.query().query() << std::endl;
            std::vector<unsigned char> response_buffer;

            printQueue(client_queue);

            std::cout << "process_client_message" << std::endl;
            process_client_message(response_buffer, req.queue(), in_mem_db, *_conn, req);

//            if (req.query().isSelect()) {
//
//                std::queue<response::sysbench_result_type> results;
//
//                bool isCached = get_from_local(in_mem_db, req, results);
//                if (!isCached) {
//                    return Response({CoResponse(Status::Select_Pending)});
//                }
//
//                auto res = Response({CoResponse(Status::Result)});
//                res.begin()->set_results(results);
//                return res;
//            }
//
//            return Response({CoResponse(Status::Ok)});

            std::cout << "initialize response" << std::endl;
            auto res = Response({CoResponse(Status::Ok)});

            std::cout << "response_buf_size:" << response_buffer.size() << std::endl;

            for (unsigned char byte: response_buffer) {
                std::cout << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(byte) << " ";
            }

            res.back().set_raw_response(response_buffer);

            std::cout << "return response" << std::endl;

            return res;

        }
    };
} // namespace transaction

#undef CASS_SHARED_PTR

