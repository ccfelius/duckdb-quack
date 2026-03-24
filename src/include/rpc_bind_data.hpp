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
};

inline LogicalType GetOrchestratorInfoStructFormat() {
	child_list_t<LogicalType> struct_children;

	struct_children.emplace_back("request_id",  LogicalType::VARCHAR);
	struct_children.emplace_back("query",        LogicalType::VARCHAR);
	struct_children.emplace_back("server_name",  LogicalType::VARCHAR);
	struct_children.emplace_back("server_uri",   LogicalType::VARCHAR);
	struct_children.emplace_back("result",       LogicalType::VARCHAR);
	auto input_struct_type = LogicalType::STRUCT(std::move(struct_children));
	return input_struct_type;
}

struct RpcBindDataOrchestrate : RpcBindData {

public:
	RpcBindDataOrchestrate() : RpcBindData() {
		input_struct_type = GetOrchestratorInfoStructFormat();
	}

	bool Equals(const FunctionData &other_p) const override {
		throw NotImplementedException("Equals not implemented");
	}

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("Copy not implemented");
	}

public:
	unique_ptr<ScalarFunction> RcpBindOrchestrateCallbackFunc(ClientContext &context);

public:
	string compute_connection_id;
	unique_ptr<RpcClientMetadata> client_metadata;
	unique_ptr<ScalarFunction> orchestrate_callback_func;

	LogicalType input_struct_type;
};
} // namespace duckdb
