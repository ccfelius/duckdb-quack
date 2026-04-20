#include "rpc_query_state.hpp"
#include "rpc_storage_extension.hpp"
#include "rpc_server.hpp"
#include "rpc_uri.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"

using namespace duckdb;

struct RpcConnectionsBindData : FunctionData {
	string server_uri_str;

	bool Equals(const FunctionData &other_p) const override {
		return server_uri_str == other_p.Cast<RpcConnectionsBindData>().server_uri_str;
	}
	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<RpcConnectionsBindData>();
		copy->server_uri_str = server_uri_str;
		return copy;
	}
};

struct RpcConnectionsGlobalState : GlobalTableFunctionState {
	vector<RpcConnectionSnapshot> snapshots;
	idx_t offset = 0;
	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> RpcConnectionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("rpc_connections: server URI cannot be NULL");
	}
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT};
	names = {"connection_id", "current_query", "duration_ms"};
	auto bind_data = make_uniq<RpcConnectionsBindData>();
	bind_data->server_uri_str = input.inputs[0].GetValue<string>();
	return bind_data;
}

static unique_ptr<GlobalTableFunctionState> RpcConnectionsInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<RpcConnectionsBindData>();
	auto &ext_info = RpcStorageExtensionInfo::GetState(*context.db);

	RpcUri uri(bind_data.server_uri_str, true);
	auto server = ext_info.FindServer(uri);
	if (!server) {
		throw InvalidInputException("No running RPC server found for URI: %s", bind_data.server_uri_str);
	}

	auto state = make_uniq<RpcConnectionsGlobalState>();
	state->snapshots = server->GetConnectionSnapshots();
	return state;
}

static void RpcConnectionsScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &gstate = input.global_state->Cast<RpcConnectionsGlobalState>();
	if (gstate.offset >= gstate.snapshots.size()) {
		return;
	}
	idx_t count = 0;
	while (gstate.offset < gstate.snapshots.size() && count < STANDARD_VECTOR_SIZE) {
		auto &snap = gstate.snapshots[gstate.offset++];
		output.data[0].SetValue(count, Value(snap.connection_id));
		output.data[1].SetValue(count, Value(snap.current_query));
		output.data[2].SetValue(count, Value::BIGINT(snap.duration_ms));
		count++;
	}
	output.SetCardinality(count);
}

TableFunction RpcConnectionsFunction::GetFunction() {
	return TableFunction("rpc_connections", {LogicalType::VARCHAR}, RpcConnectionsScan, RpcConnectionsBind,
	                     RpcConnectionsInitGlobal);
}
