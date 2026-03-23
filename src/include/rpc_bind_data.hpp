#include "duckdb/function/table_function.hpp"

namespace duckdb {
class RpcClient;

struct RpcClientMetadata {

	explicit RpcClientMetadata(string client_id) : client_id(client_id) {

	}

	string client_id;
	//string panel_id;
};

struct RpcBindData : FunctionData {
	bool Equals(const FunctionData &other_p) const override {
		throw NotImplementedException("Equals not implemented");
	}

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("Copy not implemented");
	}
	string connection_id;
	string uri;
	string query_string;
	optional_idx estimated_cardinality;
	unique_ptr<RpcClientMetadata> client_metadata;
	unique_ptr<ScalarFunction> orchestrate_callback_func;
};
} // namespace duckdb
