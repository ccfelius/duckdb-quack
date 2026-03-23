#pragma once

namespace duckdb {

class TableFunction;

class RpcScanFunction {
public:
	static TableFunction GetFunction();
};

class RpcOrchestrateFunction {
public:
	static TableFunction GetFunction();
};

class RpcScanByNameFunction {
public:
	static TableFunction GetFunction();
};
} // namespace duckdb
