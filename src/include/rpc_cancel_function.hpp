 #pragma once

namespace duckdb {

class TableFunction;

class RpcCancelFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb