//
// Created by MiyaMoto on 2019-07-04.
//

#ifndef TRANSACTION_OFFICIAL_SQLITE3_HPP
#define TRANSACTION_OFFICIAL_SQLITE3_HPP

#include <map>
#include <regex>
#include <shared_mutex>
#include <sstream>
#include <unordered_map>

#include <boost/algorithm/string/join.hpp>
#include "../journal/mmap_journal.hpp"
#include <sqlite3.h>

#include "../../transaction_provider.h++"

namespace transaction {
    template <class NextF> class Sqlite3WalTransactionProvider : public TransactionProvider<NextF> {
    public:
        static constexpr bool kAsyncCommit = false;

    private:
        std::shared_ptr<journal::MmapPool> _pool;
        std::shared_ptr<sqlite3> _s3;
        std::map<Peer, journal::MmapJournal> _uncommittedQueries;

    public:
        using TransactionProvider<NextF>::TransactionProvider;

    public:
        Sqlite3WalTransactionProvider(const std::string &db, NextF f)
            : TransactionProvider<NextF>(std::move(f))
            , _pool(journal::MmapPool::New("./data", 16_KiB, 10))
            , _s3(nullptr) {
            unlink(db.c_str());

            sqlite3 *s3;
            auto err = sqlite3_open(db.c_str(), &s3);
            if (err != SQLITE_OK) {
                std::cerr << "cannot open: " << sqlite3_errstr(err) << std::endl;
            }
            _s3 = std::shared_ptr<sqlite3>(s3, [](sqlite3 *s3) {
                printf("closed\n");
                sqlite3_close(s3);
            });
        }

        explicit Sqlite3WalTransactionProvider(NextF f)
            : Sqlite3WalTransactionProvider(":memory:", std::move(f)) {}

    private:
        bool _execute(std::string query) {
            char *errMsg = nullptr;

            // SELECT * FROM aaa.bbb; 14
            // INSERT INTO aaa.bbb...; 12
            // REPLACE INTO aaa.bbb...; 13
            // CREATE TABLE aaa.bbb...; 13
            // CREATE TABLE IF NOT EXISTS aaa.bbb...; 27
            auto dot = query.find('.', 12);
            if (dot != std::string::npos) {
                query[dot] = '_';
            }
            auto res = sqlite3_exec(_s3.get(), query.c_str(), nullptr, nullptr, &errMsg);
            if (res != SQLITE_OK) {
                std::cerr << "SQLite3 execute failed: " << errMsg << std::endl;
                std::cerr << "    query: " << query << std::endl;
            }

            return res == SQLITE_OK;
        }

        std::optional<std::vector<transaction::Row>> _select(std::string query) {
            auto dot = query.find('.', 14); // SELECT * FROM aaa.bbb...; 14
            if (dot != std::string::npos) {
                query[dot] = '_';
            }

            sqlite3_stmt *stmt;
            auto err = sqlite3_prepare_v2(_s3.get(), query.c_str(), query.size(), &stmt, nullptr);

            if (err != SQLITE_OK) {
                std::cerr << "cannot prepare: " << sqlite3_errstr(err) << std::endl;
                std::cerr << "    find .: " << dot << std::endl;
                std::cerr << "    query: " << query << std::endl;
                return std::nullopt;
            }

            std::vector<transaction::Row> ret;
            while (SQLITE_ROW == (err = sqlite3_step(stmt))) {
                transaction::Row row;
                for (int i = 0; i < sqlite3_column_count(stmt); ++i) {

                    switch (sqlite3_column_type(stmt, i)) {
                    case SQLITE_INTEGER:
                        row[sqlite3_column_name(stmt, i)] = RowData(sqlite3_column_int(stmt, i));
                        break;
                    case SQLITE_TEXT:
                        row[sqlite3_column_name(stmt, i)] =
                            RowData(std::string(static_cast<const char *>(
                                static_cast<const void *>(sqlite3_column_text(stmt, i)))));
                        break;
                    default:
                        std::cerr << "not supporting type" << std::endl;
                        std::exit(100);
                    }
                }
                ret.emplace_back(row);
            }

            if (err != SQLITE_DONE) {
                std::cerr << "cannot fetch: " << sqlite3_errstr(err) << std::endl;
                return std::nullopt;
            }
            return ret;
        }

        std::optional<std::vector<Query>> _getQueriesAndRemove(const Peer &peer) {
            auto itr = _uncommittedQueries.find(peer);
            if (itr == std::end(_uncommittedQueries)) {
                return std::nullopt;
            }
            auto queries = itr->second;
            _uncommittedQueries.erase(itr);
            auto queriesString = queries.get();
            std::vector<Query> queriesInstance(queriesString.size());
            std::transform(std::begin(queriesString), std::end(queriesString),
                           std::begin(queriesInstance),
                           [](const std::string_view &query) { return Query(query); });
            return queriesInstance;
        }

    private:
        static std::vector<std::string> _createInsertQuery(std::string table, const Response &res) {
            const auto &row = res[0].data();

            std::vector<std::string> queries;
            queries.reserve(row.size());

            for (const auto &r : row) {
                std::vector<std::string> keys, values;
                keys.reserve(r.size());
                values.reserve(r.size());

                auto first = std::begin(r);
                std::string keyQuery = first->first, valueQuery = first->second.queryStr();
                first++;
                std::for_each(first, std::end(r),
                              [&keyQuery, &valueQuery](const std::pair<std::string, RowData> &kv) {
                                  keyQuery += ',' + kv.first;
                                  valueQuery += ',' + kv.second.queryStr();
                              });
                /*for (const auto &kv : r) {
                    keys.emplace_back(kv.first);
                    values.emplace_back(kv.second.queryStr());
                }*/

                /*auto keyQuery = boost::algorithm::join(keys, ",");
                auto valueQuery = boost::algorithm::join(values, ",");*/

                std::stringstream ss;

                boost::algorithm::replace_first(table, ".", "_");
                ss << "insert into " << table << "(" << keyQuery << ")values(" << valueQuery
                   << ");";

                queries.emplace_back(ss.str());
            }

            return queries;
        }

        static std::optional<std::string>
        _getTableNameFromSelectQuery(const std::string_view &query) {
            static const std::regex TABLE_SELECT_REGEX(
                R"(^select\s[\w*,]+\s+from\s+([\w.]+)(\s*|\s+where.+))",
                std::regex_constants::icase);

            std::cmatch sm;
            if (std::regex_match(std::begin(query), std::end(query), sm, TABLE_SELECT_REGEX)) {
                return sm[1].str();
            }
            return std::nullopt;
        }

        Response _processNormalQueryInTransaction(const Request &req) {
            using transaction::pipes::operator|;

            auto query = req.query();
            std::string queryString(std::begin(query.query()), std::end(query.query()));

            if (query.isSelect()) {
                return {_processSelectQuery(req.peer(), req.query())};
            } else {
                return {_processInsertInTransaction(req.peer(), query)};
            }
        }

        CoResponse _processInsertInTransaction(const Peer &peer, const Query &query) {
            static const std::regex INSERT_REGEX(R"(^insert)", std::regex_constants::icase);

            std::string queryString(std::begin(query.query()), std::end(query.query()));

            if (query.isInsert()) {
                queryString = std::regex_replace(queryString, INSERT_REGEX, "replace");
            }
            if (!_execute(query.query().data())) {
                return CoResponse(Status::Error);
            }
            _uncommittedQueries.at(peer).append(query.query());
            return CoResponse(Status::Ok);
        }

        CoResponse _processSelectQuery(const Peer &peer, const Query &query) {
            auto optRows = _select(query.query().data());
            if (!optRows.has_value()) {
                std::cerr << "failed to execute select query: " << query.query() << std::endl;
                return CoResponse(Status::Error);
            }

            const auto &rows = *optRows;
            if (rows.empty()) {
                auto table = _getTableNameFromSelectQuery(query.query());
                if (!table.has_value()) {
                    std::cerr << "cannot get table name from query: " << query << std::endl;
                    return CoResponse(Status::Error);
                }

                auto kvsRes = this->next(Request(peer, query));
                auto insertQueries = _createInsertQuery(*table, kvsRes);
                for (const auto &insertQuery : insertQueries) {
                    if (!_execute(insertQuery)) {
                        return CoResponse(Status::Error);
                    }
                }

                return kvsRes[0];
            } else {
                return CoResponse(Status::Result, rows);
            }
        }

        CoResponse _processNormalQuery(const Peer &peer, const Query &query) {
            static const std::regex INSERT_REGEX(R"(^insert)", std::regex_constants::icase);

            if (query.isSelect()) {
                return _processSelectQuery(peer, query);

            } else {
                auto sqlQuery = std::basic_string(query.query().begin(), query.query().end());
                if (query.isInsert()) {
                    sqlQuery = std::regex_replace(sqlQuery, INSERT_REGEX, "replace");
                }
                if (_execute(sqlQuery)) {
                    auto kvsRes = this->next(Request(peer, query));
                    return kvsRes[0];
                } else {
                    std::cerr << "Execute query in SQLite3 failed. query: " << query << std::endl;
                    return CoResponse(Status::Error);
                }
            }
        }

    public:
        Response begin(const Request &req) override {
            if (_execute("BEGIN")) {
                _uncommittedQueries.emplace(req.peer(), journal::MmapJournal(_pool, kAsyncCommit));
                return Response({CoResponse(Status::Ok)});
            }
            return Response({CoResponse(Status::Error)});
        }
        Response commit(const Request &req) override {
            auto queries = _getQueriesAndRemove(req.peer());
            if (queries.has_value() && _execute("COMMIT")) {

                if (queries->empty()) {
                    return Response({CoResponse(Status::Ok)});
                }
                return this->next(Request(req.peer(), *queries));
            }
            return Response({CoResponse(Status::Error)});
        }
        Response rollback(const Request &req) override {
            if (_execute("ROLLBACK")) {
                _getQueriesAndRemove(req.peer());
                return Response({CoResponse(Status::Ok)});
            }
            return Response({CoResponse(Status::Error)});
        }

        Response insertIfNotExists(const Request &req) override {
            static const std::regex INSERT_IF_NOT_EXISTS(
                R"(INSERT\s+INTO\s+([\w\d.]+)\s*\(([\w\d,\s]+)\)\s*VALUES\s*\(([\w\d,\s']+)\)\s+IF\s+NOT\s+EXISTS)",
                std::regex_constants::icase);

            if (begin(req).front().status() != Status::Ok) {
                return Response({CoResponse(Status::Error)});
            }

            auto query = req.query().query();

            std::cmatch cm;
            if (!std::regex_match(std::begin(query), std::end(query), cm, INSERT_IF_NOT_EXISTS)) {
                return Response({CoResponse(Status::Error)});
            }
            auto table = cm[1].str();

            auto keys = cm[2].str();
            auto keys_sv = std::string_view(keys);
            keys_sv = keys_sv.substr(0, keys_sv.find(','));

            auto values = cm[3].str();
            auto values_sv = std::string_view(values);
            values_sv = values_sv.substr(0, values_sv.find(','));

            auto select =
                _processNormalQuery(req.peer(), Query("SELECT * FROM " + table + " WHERE " +
                                                      keys_sv.data() + '=' + values_sv.data()));
            if (select.data().empty()) {
                query.remove_suffix(13); // IF NOT EXISTS 13
                _processInsertInTransaction(req.peer(), Query(query));

                if (commit(req).front().status() == Status::Ok) {
                    return Response({CoResponse(Status::Ok)});
                }
            } else {
                _execute("ROLLBACK");
                return Response({CoResponse(Status::Ok)});
            }
            _execute("ROLLBACK");
            return Response({CoResponse(Status::Error)});
        }

        Response query(const Request &req) override {
            const auto &peer = req.peer();
            auto itr = _uncommittedQueries.find(peer);
            if (itr == std::end(_uncommittedQueries)) {
                std::vector<CoResponse> responses(req.queries().size());
                std::transform(std::begin(req.queries()), std::end(req.queries()),
                               std::begin(responses), [this, &req](const Query &query) {
                                   return _processNormalQuery(req.peer(), query);
                               });
                return responses;
            } else {
                return _processNormalQueryInTransaction(req);
            }
        }
    };
} // namespace transaction

#endif // TRANSACTION_OFFICIAL_SQLITE3_HPP
