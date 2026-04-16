#pragma once

namespace duckdb {
class RpcClient;
struct RpcBindData : FunctionData {
	bool Equals(const FunctionData &other_p) const override {
		throw NotImplementedException("Equals not implemented");
	}

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("Copy not implemented");
	}
	string connection_id;
	RpcUri server_uri;
	string table_name;
	optional_idx estimated_cardinality;
	mutex initial_client_mutex;
	unique_ptr<RpcClient> initial_client;
	vector<string> column_names;
	vector<LogicalType> column_types;
};
} // namespace duckdb
