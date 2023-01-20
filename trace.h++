//
// Created by y-watanabe on 1/12/23.
//
#include <boost/stacktrace.hpp>
#include <boost/exception/all.hpp>

#ifndef MY_PROXY_TRACE_H
#define MY_PROXY_TRACE_H

typedef boost::error_info<struct tag_stacktrace, boost::stacktrace::stacktrace> traced;

template <class E>
void throw_with_trace(const E& e) {
    throw boost::enable_error_info(e)
            << traced(boost::stacktrace::stacktrace());
}

#endif //MY_PROXY_TRACE_H
