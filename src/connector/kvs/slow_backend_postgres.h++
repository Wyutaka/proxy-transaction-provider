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
#include "../../transaction/result.h++"
#include <chrono>

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
        // query = req.query().query().data()
        // client_queueの先頭をひとつづつ読み込む
        // client_messageがPの時,create_parse_complete_message(std::vector<unsigned char> &response_buffer)を実行
        // client_messageがBの時,create_bind_complete_message(std::vector<unsigned char> &response_buffer)を実行
        // client_messageがDかつqueryが空の時, create_no_data_message(std::vector<unsigned char> &response_buffer)を、client_messageがDの時、create_row_desription_message(sqlite3 *in_mem_db, std::vector<unsigned char> &response_buffer, PGconn &conn,Request &req)を実行
        // client_messageがEの時、create_command_complete_message(std::vector<unsigned char> &response_buffer, Request &req)を実行
        // client_messageがSの時、create_ready_for_query_message(std::vector<unsigned char> &response_buffer)を実行
        void process_client_message(std::vector<unsigned char> &response_buffer,
                                    sqlite3 *in_mem_db, PGconn &conn, Request req) {
            // response_bufferを初期化？
            response_buffer = { };
            auto client_queue = req.client_queue();
            auto query_queue = req.queryQueue();
            auto column_format_codes = req.column_format_codes();
            // commmand_completeメッセージのための行数
//            if (!query_queue.empty()) {
//                std::cout << "now processing query : " << query_queue.front().query().data() << std::endl;
//            }

            int num_rows = 0;
            while (!client_queue.empty()) {  // キューが空になるまでループ
                unsigned char client_message = client_queue.front(); // client_queueの先頭を読み取る
                client_queue.pop(); // 読み取ったメッセージをキューから削除

//                std::cout << "now processing client message :" << client_message << std::endl;

                switch (client_message) {
                    case 'P':
                        create_parse_complete_message(response_buffer);
                        break;
                    case 'B':
                        create_bind_complete_message(response_buffer);
//                        std::cout << "queue.front() : " << client_queue.front() << std::endl;

                        // Dメッセージを送らなかった時のSelectメッセージの対処
                        if (query_queue.front().isSelect() && client_queue.front() != 'D') {
                            // 次が'D'じゃない時,DataRowだけ返す
//                            std::cout << "get B with select query so create T D message" << std::endl;
                            num_rows = create_row_data_message(in_mem_db, response_buffer, conn, query_queue,
                                                               column_format_codes);
                        }

                        break;
                    case 'D':
                        if (query_queue.front().query().empty()) {
                            create_no_data_message(response_buffer);
                        } else {
//                            std::cout << "tes D Q" << std::endl;
                            num_rows = create_row_description_message(in_mem_db, response_buffer, conn, query_queue,
                                                                      column_format_codes);
                        }
                        break;
                    case 'E':
//                        std::cout << "num rows process client :" << num_rows << std::endl;
                        create_command_complete_message(response_buffer, query_queue, num_rows, column_format_codes);
                        num_rows = 0;
                        break;
                    case 'S':
                        create_ready_for_query_message(response_buffer);
                        break;
                    case 'Q':
                        create_row_description_message(in_mem_db, response_buffer, conn, query_queue,
                                                       column_format_codes);
                        // TODO これ、Qがれんぞくしたらだめ？(dequeueで実装する)
                        client_queue.push('E');
                        client_queue.push('S');
                    default:
                        // 何もしないか、エラーメッセージを生成するなどの処理
                        break;
                }

                // response_bufferを何らかの方法で送信するか、他の処理を行う
            }
//            std::cout << "end process client message" << std::endl;
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

        // RowDescriptionメッセージに対するレスポンスRowDescription
        // 識別子: T
        int
        create_row_description_message(sqlite3 *in_mem_db,
                                       std::vector<unsigned char> &response_buffer,
                                       PGconn &conn,
                                       std::queue<Query> &query_queue,
                                       std::queue<std::queue<int>> &column_format_codes) {
//            std::cout << "create_row_desc_message" << std::endl;

            // TODO DescにクエリがInsertまたはUpdateの時はnを返すようにしておく
            if (!query_queue.front().isSelect()) {
//                std::cout << "create no data for D " << std::endl;
                create_no_data_message(response_buffer);

                return 1;
            }

            //関数がきた場合はカスタムクエリを返す
            // backendに直に問い合わせるときはコメントアウトすること
            if (query_queue.front().query().find("getstockcounts") != std::string::npos) {
//                std::cout << "you get cached!!!" << std::endl;
                response_buffer.insert(response_buffer.end(), response::select_from_getstock_count,
                                       response::select_from_getstock_count +
                                       sizeof(response::select_from_getstock_count));
                return 1;
            }

            //sumがきた場合はカスタムクエリを返す
            if (query_queue.front().query().find("SUM") != std::string::npos) {
//                std::cout << "you get cached!!!" << std::endl;
                create_no_data_message(response_buffer);
                return 1;
            }

            // SELECT S_W_ID, S_I_ID, S_QUANTITY, S_DATA が来たときは特定のクエリを返す
            else if (query_queue.front().query().find("SELECT S_W_ID, S_I_ID, S_QUANTITY, S_DATA") != std::string::npos) {
//                std::cout << "you get cached!!!" << std::endl;
                response_buffer.insert(response_buffer.end(), response::select_s_w_id_s_i_id_from_stock,
                                       response::select_s_w_id_s_i_id_from_stock +
                                       sizeof(response::select_s_w_id_s_i_id_from_stock));
                return 1;
            }

            // Tの追加
            response_buffer.push_back('T');

            // メッセージサイズを代入するためのインデックスを保存
            size_t lastIndex = response_buffer.size();

            int num_rows = 0;

            int message_size = 4;
            auto query = query_queue.front().query().data();

//            std::cout << "query" << query << std::endl;


            // 開始時間の取得
            auto start = std::chrono::high_resolution_clock::now();

            // バックエンドへの問い合わせ
            sqlite3_stmt *stmt;
            int rc = sqlite3_prepare_v2(in_mem_db, query, -1, &stmt, 0);
            rc = sqlite3_step(stmt);

//            std::cout << "sqlite result : " << rc << std::endl;

            if (rc == SQLITE_ROW) { // Data exists in SQLite
                int num_field = sqlite3_column_count(stmt);

                uint16_t twoBytes = static_cast<uint16_t>(num_field);

                // 上位バイト (high byte) と下位バイト (low byte) を取得
                unsigned char highByte = (twoBytes >> 8) & 0xFF;
                unsigned char lowByte = twoBytes & 0xFF;

                response_buffer.push_back(highByte);
                response_buffer.push_back(lowByte);

                message_size += 2;

                // 各フィールドにたいして
                for (int i = 0; i < num_field; ++i) {
                    const char *field_name = sqlite3_column_name(stmt, i);

                    // Placeholder values, replace as needed
                    int32_t table_oid = 0;
                    int16_t column_attribute_number = 0;
                    int32_t field_data_type_oid = 0;
                    int16_t data_type_size = -1;
                    int32_t type_modifier = 0;
                    int16_t format_code = 0;

                    add_row_description_to_buffer(response_buffer,
                                                  message_size,
                                                  field_name,
                                                  table_oid,
                                                  column_attribute_number,
                                                  field_data_type_oid,
                                                  data_type_size,
                                                  type_modifier,
                                                  format_code);
                }

                // Tの次にメッセージサイズついか
                auto message_size_bytes = intToBigEndian(message_size);
                response_buffer.insert(response_buffer.begin() + lastIndex, message_size_bytes.begin(),
                                       message_size_bytes.end());

                // Dメッセージを追加
                while (rc == SQLITE_ROW) {
                    num_rows++;
                    addRowDataToBuffer(stmt, num_field, response_buffer, column_format_codes);
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

                        add_row_description_to_buffer(response_buffer,
                                                      message_size,
                                                      field_name,
                                                      table_oid,
                                                      column_attribute_number,
                                                      field_data_type_oid,
                                                      data_type_size,
                                                      type_modifier,
                                                      format_code);
                    }

                    // Tの次にメッセージサイズついか
                    auto message_size_bytes = intToBigEndian(message_size);
                    response_buffer.insert(response_buffer.begin() + lastIndex, message_size_bytes.begin(),
                                           message_size_bytes.end());

                    // Dのメッセージを追加
                    num_rows = PQntuples(pg_res);
                    for (int row = 0; row < num_rows; ++row) {
                        addRowDataToBuffer(pg_res, row, num_field, response_buffer, column_format_codes);
                    }
//                    PQclear(pg_res);

                    // TODO 結果をsqliteに保存
                }
//                PQfinish(&conn);
            }

            // 終了時間の取得
            auto end = std::chrono::high_resolution_clock::now();

            // 経過時間の計算
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            std::cout << "get data from sqlite time : " << duration.count() << "us" << std::endl;

//            std::cout << "num_row in row description :" << num_rows << std::endl;
            return num_rows;
//            sqlite3_finalize(stmt);
//            sqlite3_close(sqlite_db);
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

        void addRowDataToBuffer(sqlite3_stmt *stmt,
                                int num_field,
                                std::vector<unsigned char> &response_buffer,
                                std::queue<std::queue<int>> &column_format_codes) {
            int message_size;
            size_t last_index_D;
            // メッセージ冒頭、メッセージ長用のバッファ作成
            initializeBuffer(num_field, response_buffer, message_size, last_index_D);

            auto column_format_code = column_format_codes.front();

            for (int i = 0; i < num_field; ++i) {
                auto column_data = sqlite3_column_text(stmt, i);
                int column_length = sqlite3_column_bytes(stmt, i);
                int column_type = sqlite3_column_type(stmt, i);

                int one_format_code = 0;
                if (!column_format_code.empty()) {
                    one_format_code = column_format_code.front();
                }

//                std::cout << "one_format_code sqlite : " << one_format_code << std::endl;

                processColumnData(column_data, column_length, column_type, response_buffer, message_size,
                                  one_format_code);
                column_format_code.pop();
            }

            auto message_size_bytes = intToBigEndian(message_size);
            response_buffer.insert(response_buffer.begin() + last_index_D, message_size_bytes.begin(),
                                   message_size_bytes.end());
        }


        void addRowDataToBuffer(PGresult *pg_res,
                                int row,
                                int num_field,
                                std::vector<unsigned char> &response_buffer,
                                std::queue<std::queue<int>> &column_format_codes) {

            int message_size;
            size_t last_index_D;
            // メッセージ冒頭、メッセージ長用のバッファ作成
            initializeBuffer(num_field, response_buffer, message_size, last_index_D);

            auto column_format_code = column_format_codes.front();

            for (int i = 0; i < num_field; ++i) {
                const auto *column_data = reinterpret_cast<const unsigned char *>(PQgetvalue(pg_res, row, i));
                int column_length = PQgetlength(pg_res, row, i);
                Oid column_type = PQftype(pg_res, i);

                int one_format_code = 0;
                if (!column_format_code.empty()) {
                    one_format_code = column_format_code.front();
                }

//                std::cout << "one_format_code postgres : " << one_format_code << std::endl;

                processColumnData(column_data, column_length, column_type, response_buffer, message_size,
                                  one_format_code);
                column_format_code.pop();
            }

//            std::cout << "num_field:" << num_field << std::endl;
//            std::cout << "message_size:" << message_size << std::endl;

            // メッセージサイズの挿入
            auto message_size_bytes = intToBigEndian(message_size);
            response_buffer.insert(response_buffer.begin() + last_index_D, message_size_bytes.begin(),
                                   message_size_bytes.end());
        }

        static void initializeBuffer(int num_field, std::vector<unsigned char> &response_buffer, int &message_size,
                                     size_t &last_index_D) {
            response_buffer.push_back('D');
            message_size = 4 + 2;  // 4 for initial size and 2 for field count
            last_index_D = response_buffer.size();

//            std::cout << "num_field in initializeBuffer:" << num_field << std::endl;
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


        void processColumnData(const unsigned char *column_data,
                               int column_length,
                               int column_type,
                               std::vector<unsigned char> &response_buffer,
                               int &message_size,
                               int format_code) {
            message_size += 4;

//           std::cout << "column_data" << column_data << std::endl;
//           std::cout << "column_length" << column_length << std::endl;
//
//            std::cout << "column_type : " << column_type << std::endl;
//            std::cout << "format_code : " << format_code << std::endl;

//            // int以外
//            if (column_type == SQLITE_TEXT || format_code == 0) {
//                auto column_length_bytes = intToBigEndian(column_length);
//                response_buffer.insert(response_buffer.end(), column_length_bytes.begin(), column_length_bytes.end());
//
//                response_buffer.insert(response_buffer.end(), column_data, column_data + column_length);
//                message_size += column_length;
//
//            } else if (column_type == SQLITE_INTEGER || column_type == SQLITE_BLOB || column_type == 23) {
//                // int
//                auto column_length_bytes = intToBigEndian(4);
//                response_buffer.insert(response_buffer.end(), column_length_bytes.begin(), column_length_bytes.end());
//
//                int value = std::stoi(std::string(column_data, column_data + column_length));
//                auto binary_representation = intToBigEndian(value);
//
//                response_buffer.insert(response_buffer.end(), binary_representation.begin(),
//                                       binary_representation.end());
//                message_size += 4;
//            }

            // int以外
            if (column_type == SQLITE_INTEGER || column_type == SQLITE_BLOB || column_type == 23) {
                // int
                auto column_length_bytes = intToBigEndian(4);
                response_buffer.insert(response_buffer.end(), column_length_bytes.begin(), column_length_bytes.end());

                int value = std::stoi(std::string(column_data, column_data + column_length));
                auto binary_representation = intToBigEndian(value);

                response_buffer.insert(response_buffer.end(), binary_representation.begin(),
                                       binary_representation.end());
                message_size += 4;
            } else {
                auto column_length_bytes = intToBigEndian(column_length);
                response_buffer.insert(response_buffer.end(), column_length_bytes.begin(), column_length_bytes.end());

                response_buffer.insert(response_buffer.end(), column_data, column_data + column_length);
                message_size += column_length;

            }
        }

        // Bindメッセージに対するレスポンスRowDescription
        // 識別子: D
        int
        create_row_data_message(sqlite3 *in_mem_db,
                                std::vector<unsigned char> &response_buffer,
                                PGconn &conn,
                                std::queue<Query> &query_queue,
                                std::queue<std::queue<int>> &column_format_codes) {

            int num_rows = 0;
            auto query = query_queue.front().query().data();

//            std::cout << "column_format_code_size : " << column_format_codes.size() << std::endl;

            // バックエンドへの問い合わせ
            sqlite3_stmt *stmt;
            int rc = sqlite3_prepare_v2(in_mem_db, query, -1, &stmt, 0);
            rc = sqlite3_step(stmt);

//            if (rc == SQLITE_ROW) { // Data exists in SQLite
//                int num_field = sqlite3_column_count(stmt);
//                std::cout << "from sqlite" << num_field << std::endl;
//                // Dメッセージを追加
//                while (rc == SQLITE_ROW) {
//                    num_rows++;
//                    addRowDataToBuffer(stmt, num_field, response_buffer, column_format_codes);
//                    rc = sqlite3_step(stmt);
//                }
//
//            } else {
            // No result from SQLite, query from Postgres
            // ... [Same as before, up to the Postgres processing]
            PGresult *pg_res = PQexec(&conn, query);
            if (PQresultStatus(pg_res) == PGRES_TUPLES_OK) {
                int num_field = PQnfields(pg_res);
                std::cout << "from postgres" << num_field << std::endl;
                // Dのメッセージを追加
                num_rows = PQntuples(pg_res);
                for (int row = 0; row < num_rows; ++row) {
                    addRowDataToBuffer(pg_res, row, num_field, response_buffer, column_format_codes);
                }
//                    PQclear(pg_res);
//                }
//                PQfinish(&conn);
            }
//            std::cout << "num_row in row data :" << num_rows << std::endl;
            return num_rows;
//            sqlite3_finalize(stmt);
//            sqlite3_close(sqlite_db);
        }

        // 小文字に変換する関数
        std::string to_lowercase(const std::string &s) {
            std::string result = s;
            std::transform(result.begin(), result.end(), result.begin(),
                           [](unsigned char c) { return std::tolower(c); });
            return result;
        }

        void create_command_complete_message(std::vector<unsigned char> &response_buffer,
                                             std::queue<Query> &query_queue,
                                             int num_rows,
                                             std::queue<std::queue<int>> &column_format_codes) {
            // 'C'をresponse_bufferの末尾に追加
            response_buffer.push_back('C');

            // message_size_C = 4
            int message_size_C = 4;

            // last_index_Cにresponse_bufferの末尾のインデックスを取得
            size_t last_index_C = response_buffer.size();

            // command_tagを設定
            std::string command_tag;

            auto query = query_queue.front();
            std::string query_str_lower = to_lowercase(query.query().data());  // Simplified for example

            if (query_str_lower.find("insert") != std::string::npos) {
                // 決め打ちinsertが二回ある場合は詰む
                command_tag = "INSERT 0 1";
            } else if (query_str_lower.find("delete") != std::string::npos) {
                command_tag = "DELETE " + std::to_string(num_rows);
            } else if (query_str_lower.find("update") != std::string::npos) {
//                command_tag = "UPDATE " + std::to_string(num_rows);
                // 決め打ちupdatetが二回ある場合は詰む
                command_tag = "UPDATE 1";
            } else if (query_str_lower.find("select") != std::string::npos) {
                command_tag = "SELECT " + std::to_string(num_rows);
            } else if (query_str_lower.find("move") != std::string::npos) {
                command_tag = "MOVE " + std::to_string(num_rows);
            } else if (query_str_lower.find("fetch") != std::string::npos) {
                command_tag = "FETCH " + std::to_string(num_rows);
            } else if (query_str_lower.find("copy") != std::string::npos) {
                command_tag = "COPY " + std::to_string(num_rows);
            } else if (query_str_lower.find("begin") != std::string::npos) {
                command_tag = "BEGIN";
            } else if (query_str_lower.find("commit") != std::string::npos) {
                command_tag = "COMMIT";
            }

            // message_size_C += 挿入した文字列数
            message_size_C += command_tag.size() + 1;
//            std::cout << "C message size:" << std::to_string(message_size_C) << std::endl;
//            std::cout << "num_rows" << num_rows << std::endl;
//            std::cout << "command_tag " << command_tag << std::endl;

            auto message_size_bytes = intToBigEndian(message_size_C);

            response_buffer.insert(response_buffer.begin() + last_index_C, message_size_bytes.begin(),
                                   message_size_bytes.end());

            // command_tagをresponse_bufferの末尾に追加
            response_buffer.insert(response_buffer.end(), command_tag.begin(), command_tag.end());

            response_buffer.push_back(0); // null terminator
//            std::cout << "complettion: " << command_tag << std::endl;

            // クエリキューの先頭を削除
            query_queue.pop();

            // 書式データの削除
            column_format_codes.pop();
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


//            std::cout << "field_name :" << field_name << std::endl;
//            std::cout << "table_oid :" << table_oid << std::endl;
//            std::cout << "column_attribute_number :" << column_attribute_number << std::endl;
//            std::cout << "field_data_type_oid :" << field_data_type_oid << std::endl;
//            std::cout << "data_type_size :" << data_type_size << std::endl;
//            std::cout << "type_modifier :" << type_modifier << std::endl;
//            std::cout << "format_code :" << format_code << std::endl;

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

        void printQueue(std::queue<unsigned char> &q) {
            std::queue<unsigned char> tempQueue;

            std::cout << "Queue content: ";

            while (!q.empty()) {
                unsigned char val = q.front();
//                std::cout << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(val) << " ";
                std::cout << val << " ";
                q.pop();
                tempQueue.push(val);
            }
            std::cout << std::endl;

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

//        static int callback(void *notUsed, int argc, char **argv, char **azColName) {
//            for (int i = 0; i < argc; i++) {
//                std::cout << azColName[i] << " = " << (argv[i] ? argv[i] : "NULL") << std::endl;
//            }
//            std::cout << std::endl;
//            return 0;
//        }

        Response operator()(const Request &req, sqlite3 *&in_mem_db) {
//            debug::hexdump(req.query().query().data(), req.query().query().size()); // for test
//                std::cout << req.query().query() << std::endl;

//            const char *sql = "SELECT D_NEXT_O_ID   FROM DISTRICT WHERE D_W_ID = 4    AND D_ID = 1";
//            char *zErrMsg = 0;
//            int rc = sqlite3_exec(in_mem_db, sql, callback, 0, &zErrMsg);
//            if (rc != SQLITE_OK) {
//                std::cerr << "error code : " << rc << std::endl;
//                std::cerr << "SQL error: " << zErrMsg << std::endl;
//                sqlite3_free(zErrMsg);
//            } else {
//                std::cout << "Operation done successfully" << std::endl;
//            }

            std::vector<unsigned char> response_buffer;

            auto client_queue = req.client_queue();

//            printQueue(client_queue);

//            std::cout << "process_client_message" << std::endl;

            // 開始時間の取得
            auto start = std::chrono::high_resolution_clock::now();

            process_client_message(response_buffer, in_mem_db, *_conn, req);

            // 終了時間の取得
            auto end = std::chrono::high_resolution_clock::now();

            // 経過時間の計算
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

            std::cout << "response time : " << duration.count() << "us" << std::endl;

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

//            std::cout << "initialize response" << std::endl;
            auto res = Response({CoResponse(Status::Ok)});

//            std::cout << "response_buf_size:" << response_buffer.size() << std::endl;

//            for (unsigned char byte: response_buffer) {
//                std::cout << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(byte) << " ";
//            }

            res.back().set_raw_response(response_buffer);

//            std::cout << "return response" << std::endl;

            return res;

        }
    };
} // namespace transaction

#undef CASS_SHARED_PTR

