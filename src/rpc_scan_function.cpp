#include "rpc_scan_function.hpp"
#include "client.hpp"
#include "catalog.hpp"

#include "duckdb/function/table_function.hpp"
#include "rpc_bind_data.hpp"

using namespace duckdb;

static unique_ptr<FunctionData> RpcBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	// Set logging to be pretty verbose (everything except message payloads)
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("call_rpc URI and query parameters cannot be NULL");
	}

	auto query = input.inputs[1].GetValue<string>();
	auto bind_data = make_uniq<RpcBindData>();
	bind_data->uri = input.inputs[0].GetValue<string>();
	auto client = RpcClient::GetClient(bind_data->uri);

	auto connection_request_response =
	    client->MakeRequest<ConnectionResponseMessage>(make_uniq<ConnectionRequestMessage>());
	bind_data->connection_id = connection_request_response->ConnectionId();

	auto bind_response = client->MakeRequest<PrepareResponseMessage>(
	    make_uniq<PrepareRequestMessage>(bind_data->connection_id, query, true));

	bind_data->estimated_cardinality = bind_response->EstimatedCardinality();
	return_types = bind_response->Types();
	names = bind_response->Names();

	return bind_data;
}

RpcCatalog &GetRpcCatalog(ClientContext &context, Value &catalog_name) {
	if (catalog_name.IsNull()) {
		throw BinderException("Catalog cannot be NULL");
	}
	// look up the database to query
	auto db_name = catalog_name.GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\"", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "rpc") {
		throw BinderException("Attached database \"%s\" does not refer to a RPC database", db_name);
	}
	return catalog.Cast<RpcCatalog>();
}

static unique_ptr<FunctionData> RpcBindCatalogName(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("catalog_name and query parameters cannot be NULL");
	}

	auto &rpc_catalog = GetRpcCatalog(context, input.inputs[0]);

	// TODO some of this stuff below is duplicated af

	auto query = input.inputs[1].GetValue<string>();
	auto bind_data = make_uniq<RpcBindData>();
	bind_data->uri = rpc_catalog.GetServerString();
	bind_data->connection_id = rpc_catalog.GetConnectionId();

	auto bind_response = rpc_catalog.GetRawClient().MakeRequest<PrepareResponseMessage>(
	    make_uniq<PrepareRequestMessage>(bind_data->connection_id, query, true));

	bind_data->estimated_cardinality = bind_response->EstimatedCardinality();
	return_types = bind_response->Types();
	names = bind_response->Names();

	return bind_data;
}

struct RpcLocalState : public LocalTableFunctionState {
	unique_ptr<RpcClient> client;

	explicit RpcLocalState() {
	}
	~RpcLocalState() override {
	}
};

struct RpcGlobalState : GlobalTableFunctionState {
	explicit RpcGlobalState(idx_t max_threads_p) : max_threads(max_threads_p), done(false) {
	}
	idx_t MaxThreads() const override {
		return max_threads;
	}
	idx_t max_threads;
	atomic<bool> done;
};

unique_ptr<GlobalTableFunctionState> RpcInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<RpcBindData>();
	auto num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();

	idx_t max_threads = 1;

	// small heuristic that scales down the number of threads working on retrieving a result set somewhat gracefully.
	if (bind_data.estimated_cardinality.IsValid()) {
		auto min_chunks_per_thread = 10.0; // TODO make this a parameter
		auto target_threads =
		    (bind_data.estimated_cardinality.GetIndex() / STANDARD_VECTOR_SIZE / min_chunks_per_thread) * num_threads;
		if (target_threads > 1) {
			max_threads = target_threads;
		}
		if (target_threads > num_threads) {
			max_threads = GlobalTableFunctionState::MAX_THREADS;
		}
	}

	return make_uniq<RpcGlobalState>(max_threads);
}

unique_ptr<LocalTableFunctionState> RpcInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                 GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->Cast<RpcBindData>();
	auto &global_state = global_state_p->Cast<RpcGlobalState>();
	if (global_state.done) {
		return nullptr;
	}
	auto local_state = make_uniq<RpcLocalState>();
	local_state->client = RpcClient::GetClient(bind_data.uri);
	// TODO re-use client from bind data for first conn

	return local_state;
}

static void RpcScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<RpcBindData>();
	auto &global_state = input.global_state->Cast<RpcGlobalState>();
	auto &local_state = input.local_state->Cast<RpcLocalState>();

	if (global_state.done) {
		return;
	}
	auto fetch_response =
	    local_state.client->MakeRequest<FetchResponseMessage>(make_uniq<FetchRequestMessage>(bind_data.connection_id));
	if (!fetch_response->ResponseData() || fetch_response->ResponseData()->size() == 0) {
		global_state.done = true;
		return;
	}

	output.Reference(*fetch_response->ResponseData());
	output.SetCardinality(fetch_response->ResponseData()->size());
}

TableFunction RpcScanFunction::GetFunction() {
	return TableFunction("rpc_call", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RpcScan, RpcBind, RpcInitGlobal,
	                     RpcInitLocal);
}

TableFunction RpcScanByNameFunction::GetFunction() {
	return TableFunction("rpc_call_by_name", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RpcScan, RpcBindCatalogName,
	                     RpcInitGlobal, RpcInitLocal);
}

