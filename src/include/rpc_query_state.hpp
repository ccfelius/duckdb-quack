#pragma once

namespace duckdb {
class TableFunction;

class RpcConnectionsFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb
