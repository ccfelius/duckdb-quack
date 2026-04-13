#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct OrchestrateFunction {
	static TableFunction GetFunction();
};

} // namespace duckdb
