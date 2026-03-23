#include "rpc_scan_function.hpp"
#include "client.hpp"
#include "catalog.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "rpc_bind_data.hpp"

#include <duckdb/parser/parser.hpp>

using namespace duckdb;

static unique_ptr<ScalarFunction> RcpBindOrchestrateCallbackFunc(ClientContext &context) {
	auto callback = Catalog::GetEntry<ScalarFunctionCatalogEntry>(
	   context, INVALID_CATALOG, DEFAULT_SCHEMA, "orchestrate", OnEntryNotFound::RETURN_NULL);
	if (!callback) {
		throw InvalidInputException("No 'orchestrate' function registered in the catalog. You must configure an external orchestrate function");
	}

	return make_uniq<ScalarFunction>(callback->functions.GetFunctionByArguments(context, {LogicalType::VARCHAR, LogicalType::VARCHAR}));
}

static unique_ptr<BoundFunctionExpression> RcpGetBoundOrchestrateCallback(const RpcBindData &bind_data) {
	// push the input arguments
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundConstantExpression>(Value(bind_data.uri)));
	children.push_back(make_uniq<BoundConstantExpression>(Value(bind_data.connection_id)));

	return make_uniq<BoundFunctionExpression>(bind_data.orchestrate_callback_func->return_type, *bind_data.orchestrate_callback_func, std::move(children), nullptr);
}

static unique_ptr<FunctionData> RpcBindOrchestrate(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {

	// Set logging to be pretty verbose (everything except message payloads)
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull() || input.inputs[2].IsNull()) {
		throw BinderException("coordinator server, query and metadata struct cannot be empty");
	}

	auto bind_data = make_uniq<RpcBindData>();

	bind_data->orchestrate_callback_func = RcpBindOrchestrateCallbackFunc(context);

	bind_data->uri = input.inputs[0].GetValue<string>();
	auto client = RpcClient::GetClient(bind_data->uri);

	bind_data->query_string = input.inputs[1].GetValue<string>();

	auto connection_request_response =
	    client->MakeRequest<ConnectionResponseMessage>(make_uniq<ConnectionRequestMessage>());
	bind_data->connection_id = connection_request_response->ConnectionId();

	auto bind_response = client->MakeRequest<PrepareResponseMessage>(
	    make_uniq<PrepareRequestMessage>(bind_data->connection_id, bind_data->query_string, true));

	bind_data->estimated_cardinality = bind_response->EstimatedCardinality();
	bind_data->client_metadata = make_uniq<RpcClientMetadata>(input.inputs[2].GetValue<string>());

	return_types = bind_response->Types();
	names = bind_response->Names();

	return bind_data;
}

struct RpcLocalStateOrchestrate : public LocalTableFunctionState {
	unique_ptr<RpcClient> client;

	explicit RpcLocalStateOrchestrate() {
	}
	~RpcLocalStateOrchestrate() override {
	}
};

struct RpcGlobalStateOrchestrate : GlobalTableFunctionState {
	explicit RpcGlobalStateOrchestrate(idx_t max_threads_p) : max_threads(max_threads_p), done(false) {
	}
	idx_t MaxThreads() const override {
		return max_threads;
	}
	idx_t max_threads;
	atomic<bool> done;
};

unique_ptr<GlobalTableFunctionState> RpcInitGlobalOrchestrate(ClientContext &context, TableFunctionInitInput &input) {
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

	return make_uniq<RpcGlobalStateOrchestrate>(max_threads);
}

unique_ptr<LocalTableFunctionState> RpcInitLocalOrchestrate(ExecutionContext &context, TableFunctionInitInput &input,
                                                 GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->Cast<RpcBindData>();
	auto &global_state = global_state_p->Cast<RpcGlobalStateOrchestrate>();
	if (global_state.done) {
		return nullptr;
	}
	auto local_state = make_uniq<RpcLocalStateOrchestrate>();
	local_state->client = RpcClient::GetClient(bind_data.uri);
	// TODO re-use client from bind data for first conn

	return local_state;
}

static void RpcOrchestrate(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<RpcBindData>();
	auto &global_state = input.global_state->Cast<RpcGlobalStateOrchestrate>();
	auto &local_state = input.local_state->Cast<RpcLocalStateOrchestrate>();

	if (global_state.done) {
		return;
	}

	// TODO hash query?

	Parser parser;
	parser.ParseQuery(bind_data.query_string);
	// auto statements = parser.statements;

	// TODO store parsed statements

	// TODO bind query

	// Resolve the matching overload and invoke it directly
	auto bound_expr = RcpGetBoundOrchestrateCallback(bind_data);
	auto result_value = ExpressionExecutor::EvaluateScalar(context, *bound_expr);

	output.SetCardinality(1);
	output.SetValue(0, 0, result_value);
	global_state.done = true;

}

TableFunction RpcOrchestrateFunction::GetFunction() {
	// input; coordinator_server, query, metadata -> client_id, panel_id, result_set_id
	return TableFunction("rpc_orchestrate", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, RpcOrchestrate, RpcBindOrchestrate, RpcInitGlobalOrchestrate,
	                     RpcInitLocalOrchestrate);
}
