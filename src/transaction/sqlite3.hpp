//
// Created by MiyaMoto on 2019-07-04.
//

#ifndef TRANSACTION_SQLITE3_HPP
#define TRANSACTION_SQLITE3_HPP

#include <map>
#include <shared_mutex>
#include <unordered_map>
#include <optional>

#include <sqlite3.h>

#include "../../transaction_provider.h++"
#include <regex>
#include <boost/algorithm/string/join.hpp>

namespace transaction {
    template <class NextF> class Sqlite3TransactionProvider : public TransactionProvider<NextF> {
    private:
        std::shared_ptr<sqlite3> _s3;
        std::map<Peer, std::vector<Query>> _uncommittedQueries;

    public:
        using TransactionProvider<NextF>::TransactionProvider;

    public:
        Sqlite3TransactionProvider(const std::string &db, NextF f)
            : TransactionProvider<NextF>(std::move(f))
            , _s3(nullptr) {
            unlink(db.c_str());
            std::cout << db << std::endl;

            sqlite3 *s3;
            sqlite3_open(db.c_str(), &s3);
            _s3 = std::shared_ptr<sqlite3>(s3, [](sqlite3 *s3) { sqlite3_close(s3); });
        }

        explicit Sqlite3TransactionProvider(NextF f)
            : Sqlite3TransactionProvider("sqlite3-transaction.db", std::move(f)) {}

    private:
        bool _execute(std::string query) {
            char *errMsg = nullptr;

            auto dot = query.find('.');
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
            auto dot = query.find('.');
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
            return queries;
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

                for (const auto &kv : r) {
                    keys.emplace_back(kv.first);
                    values.emplace_back(kv.second.queryStr());
                }

                auto keyQuery = boost::algorithm::join(keys, ",");
                auto valueQuery = boost::algorithm::join(values, ",");

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
            static const std::regex INSERT_REGEX(R"(^insert)", std::regex_constants::icase);

            using transaction::pipes::operator|;

            auto query = req.query();
            std::string queryString(std::begin(query.query()), std::end(query.query()));

            if (query.isSelect()) {
                auto optRows = _select(req.query().query().data());
                if (!optRows.has_value()) {
                    std::cerr << "failed to execute select query" << std::endl;
                    return Response({CoResponse(Status::Error)});
                }

                const auto &rows = *optRows;
                if (rows.empty()) {
                    auto table = _getTableNameFromSelectQuery(queryString);
                    if (!table.has_value()) {
                        return Response({CoResponse(Status::Error)});
                    }

                    auto kvsRes = this->next(req);
                    auto insertQueries = _createInsertQuery(*table, kvsRes);
                    for (const auto &insertQuery : insertQueries) {
                        if (!_execute(insertQuery)) {
                            return Response({CoResponse(Status::Error)});
                        }
                    }

                    return kvsRes;
                } else {
                    return Response({CoResponse(Status::Result, rows)});
                }
            } else {

                if (query.isInsert()) {
                    queryString = std::regex_replace(queryString, INSERT_REGEX, "replace");
                }
                if (!_execute(query.query().data())) {
                    return Response({CoResponse(Status::Error)});
                }
                _uncommittedQueries[req.peer()].emplace_back(req.query());
                return Response({CoResponse(Status::Ok)});
            }
        }

        CoResponse _processNormalQuery(const Peer &peer, const Query &query) {
            static const std::regex INSERT_REGEX(R"(^insert)", std::regex_constants::icase);

            if (query.isSelect()) {
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
                _uncommittedQueries[req.peer()] = {};
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

#endif // TRANSACTION_SQLITE3_HPP
