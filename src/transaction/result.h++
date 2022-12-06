//
// Created by user1 on 22/11/08.
//

#pragma once


/*
0x84
0x00
0x00,0x00
0x08
0x00,0x00,0x02,0x44
0x00,0x00,0x00,0x02 // Rows
0x00,0x00,0x00,0x01  // flags 1
0x00,0x00,0x00,0x12  // column count
0x00,0x06 // len(ksname)
0x73,0x79,0x73,0x74,0x65,0x6d // ksname
0x00,0x05 // len(tablename)
0x6c,0x6f,0x63,0x61,0x6c // tablename
// column 1
0x00,0x03
0x6b,0x65,0x79
0x00,0x0d  // varchar
// column 2
0x00,0x0c
0x62,0x6f,0x6f,0x74,0x73,0x74,0x72,0x61,0x70,0x70,0x65,0x64
0x00,0x0d // varchar
// column 3
0x00,0x11
0x62,0x72,0x6f,0x61,0x64,0x63,0x61,0x73,0x74,0x5f,0x61,0x64,0x64,0x72,0x65,0x73,0x73
0x00,0x10 // Inet
// column 4
0x00,0x0c
0x63,0x6c,0x75,0x73,0x74,0x65,0x72,0x5f,0x6e,0x61,0x6d,0x65
0x00,0x0d // varchar
// column 5
0x00,0x0b
0x63,0x71,0x6c,0x5f,0x76,0x65,0x72,0x73,0x69,0x6f,0x6e
0x00,0x0d // varchar
// column 6
0x00,0x0b
0x64,0x61,0x74,0x61,0x5f,0x63,0x65,0x6e,0x74,0x65,0x72
0x00,0x0d // varchar
// column 7
0x00,0x11
0x67,0x6f,0x73,0x73,0x69,0x70,0x5f,0x67,0x65,0x6e,0x65,0x72,0x61,0x74,0x69,0x6f,0x6e
0x00,0x09 // int
// column 8
0x00,0x07
0x68,0x6f,0x73,0x74,0x5f,0x69,0x64
0x00,0x0c // uuid
// column 9
0x00,0x0e
0x6c,0x69,0x73,0x74,0x65,0x6e,0x5f,0x61,0x64,0x64,0x72,0x65,0x73,0x73
0x00,0x10 // inet
// column 10 (a)
0x00,0x17
0x6e,0x61,0x74,0x69,0x76,0x65,0x5f,0x70,0x72,0x6f,0x74,0x6f,0x63,0x6f,0x6c,0x5f,0x76,0x65,0x72,0x73,0x69,0x6f,0x6e
0x00,0x0d // varchar
// column 11 (b)
0x00,0x0b
0x70,0x61,0x72,0x74,0x69,0x74,0x69,0x6f,0x6e,0x65,0x72
0x00,0x0d // varchar
// column 12 (c)
0x00,0x04
0x72,0x61,0x63,0x6b
0x00,0x0d // varchar
// column 13 (d)
0x00,0x0f
0x72,0x65,0x6c,0x65,0x61,0x73,0x65,0x5f,0x76,0x65,0x72,0x73,0x69,0x6f,0x6e
0x00,0x0d // varchar
// column 14 (e)
0x00,0x0b
0x72,0x70,0x63,0x5f,0x61,0x64,0x64,0x72,0x65,0x73,0x73
0x00,0x10 // inet
// column 15 (f)
0x00,0x0e
0x73,0x63,0x68,0x65,0x6d,0x61,0x5f,0x76,0x65,0x72,0x73,0x69,0x6f,0x6e
0x00,0x0c // uuid
// column 16 (10)
0x00,0x0e
0x74,0x68,0x72,0x69,0x66,0x74,0x5f,0x76,0x65,0x72,0x73,0x69,0x6f,0x6e
0x00,0x0d // varchar
// column 17 (11)
0x00,0x06
0x74,0x6f,0x6b,0x65,0x6e,0x73,0x00,0x22
0x00,0x0d // varchar
// column 18 (12)
0x00,0x0c
0x74,0x72,0x75,0x6e,0x63,0x61,0x74,0x65,0x64,0x5f,0x61,0x74
0x00,0x21 // map<uuid, blob>
0x00,0x0c //  uuid
0x00,0x03 //  blob
0x00,0x00,0x00,0x01

0x00,0x00,0x00,0x05
0x6c,0x6f,0x63,0x61,0x6c

0x00,0x00,0x00,0x09
0x43,0x4f,0x4d,0x50,0x4c,0x45,0x54,0x45,0x44

0x00,0x00,0x00,0x04
0xc0,0xa8,0x0b,0x3c // 192.168.11.60

0x00,0x00,0x00,0x0d
0x6c,0x6f,0x63,0x61,0x6c,0x20,0x63,0x6c,0x75,0x73,0x74,0x65,0x72

0x00,0x00,0x00,0x05
0x33,0x2e,0x34,0x2e,0x32

0x00,0x00,0x00,0x0b
0x64,0x61,0x74,0x61,0x63,0x65,0x6e,0x74,0x65,0x72,0x31

0x00,0x00,0x00,0x04
0x00,0x00,0x00,0x00

0x00,0x00,0x00,0x10,0x38,0x7e,0x73,0x4c,0x04,0x68,0x08,0x90,0x28,0x4d,0xa1,0xfa,0xd2,0xad,0x7a,0x6f,0x00,0x00,0x00,0x04,0xc0,0xa8,0x0b,0x3c,0x00,0x00,0x00,0x01,0x34,0x00,0x00,0x00,0x2b,0x6f,0x72,0x67,0x2e,0x61,0x70,0x61,0x63,0x68,0x65,0x2e,0x63,0x61,0x73,0x73,0x61,0x6e,0x64,0x72,0x61,0x2e,0x64,0x68,0x74,0x2e,0x4d,0x75,0x72,0x6d,0x75,0x72,0x33,0x50,0x61,0x72,0x74,0x69,0x74,0x69,0x6f,0x6e,0x65,0x72,0x00,0x00,0x00,0x05,0x72,0x61,0x63,0x6b,0x31,0x00,0x00,0x00,0x0c,0x33,0x2e,0x39,0x2d,0x53,0x4e,0x41,0x50,0x53,0x48,0x4f,0x54,0x00,0x00,0x00,0x04,0xc0,0xa8,0x0b,0x3c,0x00,0x00,0x00,0x10,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x06,0x32,0x30,0x2e,0x31,0x2e,0x30,0x00,0x00,0x00,0x1b,0x00,0x00,0x00,0x01,0x00,0x00,0x00,0x13,0x33,0x36,0x38,0x39,0x30,0x31,0x31,0x30,0x34,0x34,0x37,0x36,0x39,0x38,0x35,0x37,0x35,0x33,0x36,0xff,0xff,0xff,0xff*/

#include <vector>
#include <cstddef>
#include <string>
#include <cstdint>
#include <array>
#include <iostream>
#include <stdlib.h>
#include <string.h>

    namespace response {
        namespace detail {

            inline int PackBytes(std::vector<unsigned char>& bytes, const std::string& str) {
                std::uint32_t len_be = htobe32(str.size()); // 文字列の長さを取得
                auto len_be_first = static_cast<const unsigned char*>(static_cast<const void*>(&len_be)); // len_beの1バイト目
                bytes.insert(bytes.end(), len_be_first, len_be_first + sizeof(len_be)); // データ長を入力
                bytes.insert(bytes.end(), str.begin(), str.begin() + str.size()); // データの中身を入力
                return str.size() + 4; // // 4はデータ長分
            }

            inline size_t PackBytes(std::vector<unsigned char>& bytes, const unsigned char* str) {
                std::string_view str_v = std::string_view(reinterpret_cast<const char *>(str));
                std::uint32_t len_be = htobe32(str_v.size()); // 文字列の長さを取得
                auto len_be_first = static_cast<const unsigned char*>(static_cast<const void*>(&len_be)); // len_beの1バイト目
                bytes.insert(bytes.end(), len_be_first, len_be_first + sizeof(len_be)); // データ長を入力
                bytes.insert(bytes.end(), str_v.begin(), str_v.begin() + str_v.size()); // データの中身を入力
                return str_v.size() + 4;
            }

// cassandraのint用
//            inline void PackBytes(std::vector<unsigned char>& bytes, int integer) { // columnのバイト列の長さとcolumのデータ charに直す
//                std::uint32_t len_be = htobe32(4);
//                auto len_be_first = static_cast<const unsigned char*>(static_cast<const void*>(&len_be));
//                bytes.insert(bytes.end(), len_be_first, len_be_first + sizeof(len_be));
//
//                auto int_first = reinterpret_cast<const unsigned char*>(&integer);
//                // int の場合は リトルエンディアン
//                auto itr = int_first[0];
//                for(int i = 3; i >= 0; i--) {
//                    bytes.push_back(int_first[i]);
//                }
//                // bytes.insert(bytes.end(), int_first, int_first + 4);
//            }

// TODO INTの場合は数字をcharに変換する (ex.32 -> 0x33 0x32)
            inline int PackBytes(std::vector<unsigned char>& bytes, int integer) { // columnのバイト列の長さとcolumのデータ charに直す

                char buf[12]; // 桁数を取る必要がある
//                int size = std::string_view(reinterpret_cast<const char *>(buf)).length();
                sprintf(buf, "%d", integer); // bufにintegerの文字列コピー
                size_t size = strlen(buf);
                auto len_be = htobe32(size);
                auto len_be_first = static_cast<const unsigned char*>(static_cast<const void*>(&len_be)); // len_beの1バイト目
                bytes.insert(bytes.end(), len_be_first, len_be_first + sizeof(len_be)); // データ長を入力
                bytes.insert(bytes.end(), buf, buf + size); // データサイズを入力
                return size + 4; // 4はデータ長分
            }

            inline void PackBytes(std::vector<unsigned char>& bytes, float floating) { // columnのバイト列の長さとcolumのデータ
                std::uint32_t len_be = htobe32(4);
                auto len_be_first = static_cast<const unsigned char*>(static_cast<const void*>(&len_be));
                bytes.insert(bytes.end(), len_be_first, len_be_first + sizeof(len_be));

                auto int_first = reinterpret_cast<const unsigned char*>(&floating);
                // float の場合は リトルエンディアン
                auto itr = int_first[0];
                for(int i = 3; i >= 0; i--) {
                    bytes.push_back(int_first[i]);
                }
            }

            inline void SetBytes(std::vector<unsigned char>& bytes) {}

            inline void SetBytes(int &size, std::vector<unsigned char>& bytes) {}

//            template<class Column, class... Columns>
//            void SetBytes(std::vector<unsigned char>& bytes, Column&& column, Columns&&... columns) {
//                // pack
//                int ds = 0;
//                ds += PackBytes(bytes, std::forward<Column>(column));
//
//                SetBytes(bytes, std::forward<Columns>(columns)...);
//            }

            template<class Column, class... Columns>
            void SetBytes(int &size, std::vector<unsigned char>& bytes, Column&& column, Columns&&... columns) {
                // pack
                size += PackBytes(bytes, std::forward<Column>(column));

                SetBytes(size, bytes, std::forward<Columns>(columns)...);
            }


#define INSERT_BYTES4(target, data) do{\
                std::uint32_t _data_tmp_swap_bytes_ = htobe32(data); \
                auto int_first = reinterpret_cast<const unsigned char*>(&_data_tmp_swap_bytes_); \
                target.insert(target.end(), int_first, int_first + 4);\
            }while(0)
#define INSERT_BYTES2(target, data) do{\
                std::uint32_t _data_tmp_swap_bytes_ = htobe16(data); \
                auto int_first = reinterpret_cast<const unsigned char*>(&_data_tmp_swap_bytes_); \
                target.insert(target.end(), int_first, int_first + 2);\
            }while(0)

        }

        // usage
        //          response::GetItem res("tiny_tpc_c", "item", {"i_name", "i_price", "i_data"}, column_count);
        //          res.addRow(std::string(sqlite3_column_name(stmt, 0)), float(sqlite3_column_int(stmt, 1)), std::string(sqlite3_column_name(stmt, 2)));
        //          addQueryOnly(std::move(query));
        template<class... Columns>
        class BasicResult {
        private:
            std::vector<unsigned char> bytes_;
            int message_size = 6; // データ長4バイト+コラム数2バイト

        public:
//            BasicResult(std::string_view ks, std::string_view tbl, const std::array<std::string_view, sizeof...(Columns)>& columns, std::uint32_t column_count) { // ヘッダの書き込み
                BasicResult(const std::array<std::string_view, sizeof...(Columns)>& columns, std::uint32_t column_count) { // ヘッダの書き込み
/*0x00,0x00,0x00,0x02 // Rows
0x00,0x00,0x00,0x01  // flags 1
0x00,0x00,0x00,0x12  // column count
0x00,0x06 // len(ksname)
0x73,0x79,0x73,0x74,0x65,0x6d // ksname
0x00,0x05 // len(tablename)
0x6c,0x6f,0x63,0x61,0x6c // tablename*/
// ヘッダ情報のh
                if (columns.size() != column_count ) {return;}
                bytes_.push_back(0x44); // D
                INSERT_BYTES4(bytes_, 0); // データ長 後から入れる(bytes.insert(1, size(hex)))
                INSERT_BYTES2(bytes_, column_count); // コラム数
                // データ長、データ -> addRowで実装

            }


        public:
            void addRow(Columns... columns) {
                detail::SetBytes(message_size, bytes_, columns...);
            }

            template<class Column>
            void addColumn(Column column) {
                detail::SetBytes(message_size, bytes_, column);
            }

            void end_message() {

            }

            void next_nessage() {

            }

            std::vector<unsigned char>& bytes() noexcept {
                auto len_be = htobe32(message_size);
                auto len_be_first = static_cast<const unsigned char*>(static_cast<const void*>(&len_be)); // len_beの1バイト目
                for (int i = 1; i <= 4; i++) {
                    bytes_.at(i) = len_be_first[i-1]; // データ長更新
                }
//                bytes_.insert(bytes_.begin() + 1, len_be_first, len_be_first + 4);
//                INSERT_BYTES4(bytes_, message_size);
//                INSERT_BYTES2(bytes_, 1);
                return bytes_;}
        };

#undef INSERT_BYTES4
#undef INSERT_BYTES2
//        using Result = BasicResult<int>;
//        using GetCust = BasicResult<float, std::string, std::string>;
//        using GetWhse = BasicResult<float>;
//        using GetDist = BasicResult<int, float>;
//        using GetItem = BasicResult<std::string, float, std::string>;
//        using GetStock = BasicResult<int, std::string, std::string, std::string, std::string, std::string, std::string, std::string, std::string, std::string, std::string, std::string>;
        using Sysbench = BasicResult<std::string, int, int, int>;

                /* TODO クエリに対するリクエストを返す
                     クライアントに返す文字列の形式: <C/Z
                     Byte1('C') |  Int32       |  String    |  Byte1('Z')  |  Int32(5)   |  Byte1
                     C          |   Int32      |  hoge      |      Z       | 00 00 00 05 |  {'I'|'T'|'E'}  ('I'-> not in transaction/'T'-> in transaction/'E'-> 'Error transaction')
                     43         |  xx xx xx xx | xx xx ...  |      5a      | 00 00 00 05 |  {49/54/45}
                */

        unsigned char sysbench_tbl_header[] = {0x54, 0x00, 0x00, 0x00, 0x66, 0x00, 0x04, 0x70, 0x6b, 0x00, 0x00, 0x00, 0x42, 0x00,
                                                      0x00, 0x01,0x00, 0x00, 0x04, 0x13, 0xff, 0xff, 0x00, 0x00, 0x00, 0x25, 0x00, 0x00, 0x66, 0x69,
                                                      0x65, 0x6c, 0x64, 0x31, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x17, 0x00,
                                                      0x04, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x32, 0x00, 0x00, 0x00,
                                                      0x42, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x17, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00,
                                                      0x66, 0x69, 0x65, 0x6c, 0x64, 0x33, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
                                                      0x17, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00};
        static constexpr unsigned char sysbench_slct_cmd[] = {
                0x43, 0x00, 0x00, 0x00, 0x0d, 0x53, 0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x31, 0x00
        };
        static constexpr unsigned char sysbench_insrt_cmd[] = {
                0x43, 0x00, 0x00, 0x00, 0x0f, 0x49, 0x4e, 0x53, 0x45, 0x52, 0x54, 0x20, 0x30, 0x20, 0x31, 0x00
        };
        static constexpr unsigned char sysbench_status[] = {
                0x5a, 0x00, 0x00, 0x00, 0x05, 0x49
        };
    }
