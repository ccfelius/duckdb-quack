#include "rpc_scan_function.hpp"
#include "client.hpp"
#include "catalog.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "rpc_bind_data.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include <duckdb/parser/parser.hpp>

#include "duckdb/main/database.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/common/types/uuid.hpp"

using namespace duckdb;

unique_ptr<ScalarFunction> RpcBindDataOrchestrate::RcpBindOrchestrateCallbackFunc(ClientContext &context) {
	auto callback = Catalog::GetEntry<ScalarFunctionCatalogEntry>(
	   context, INVALID_CATALOG, DEFAULT_SCHEMA, "orchestrate", OnEntryNotFound::RETURN_NULL);
	if (!callback) {
		throw InvalidInputException("No 'orchestrate' function registered in the catalog. You must configure an external orchestrate function");
	}

	return make_uniq<ScalarFunction>(callback->functions.GetFunctionByArguments(context, {input_struct_type}));
}

static unique_ptr<BoundFunctionExpression> RcpGetBoundOrchestrateCallback(const RpcBindDataOrchestrate &bind_data) {
	// push the input arguments
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundConstantExpression>(Value(bind_data.uri)));
	children.push_back(make_uniq<BoundConstantExpression>(Value(bind_data.connection_id)));

	return make_uniq<BoundFunctionExpression>(bind_data.orchestrate_callback_func->return_type, *bind_data.orchestrate_callback_func, std::move(children), nullptr);
}

static unique_ptr<FunctionData> RpcBindOrchestrate(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {

	if (input.inputs[0].IsNull() || input.inputs[1].IsNull() || input.inputs[2].IsNull()) {
		throw BinderException("coordinator server, query and metadata struct cannot be empty");
	}

	auto bind_data = make_uniq<RpcBindDataOrchestrate>();

	// bind the custom orchestrate callback function
	bind_data->orchestrate_callback_func = bind_data->RcpBindOrchestrateCallbackFunc(context);

	bind_data->uri = input.inputs[0].GetValue<string>();
	auto client = RpcClient::GetClient(bind_data->uri);

	bind_data->query_string = input.inputs[1].GetValue<string>();
	bind_data->client_metadata = make_uniq<RpcClientMetadata>(input.inputs[2].GetValue<string>());

	// we make a connection to the orchestrate server
	auto connection_request_response =
	    client->MakeRequest<ConnectionResponseMessage>(make_uniq<ConnectionRequestMessage>());
	bind_data->connection_id = connection_request_response->ConnectionId();

	// we do not need a bind a response yet
	// todo; maybe make this orchestrat bind respond?
	auto orchestrate_bind_response = client->MakeRequest<OrchestrateResponseMessage>(
		make_uniq<OrchestrateRequestMessage>(bind_data->connection_id, bind_data->query_string));

	if (!orchestrate_bind_response->IsAvailable()) {
		throw NotImplementedException("No orchestrator function found on remote server");
	}

	bind_data->estimated_cardinality = orchestrate_bind_response->EstimatedCardinality();

	return_types = orchestrate_bind_response->Types();
	names = orchestrate_bind_response->Names();

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

 static void CreateQueryStateTableLazy(ClientContext &context) {
        auto &catalog = Catalog::GetSystemCatalog(context);

        if (catalog.GetEntry<TableCatalogEntry>(context, DEFAULT_SCHEMA, "query_state",
                                                OnEntryNotFound::RETURN_NULL)) {
        	// table already exists
        	// TODO; put a bool or something in the global state
            return;
        }

        auto info = make_uniq<CreateTableInfo>(INVALID_CATALOG, DEFAULT_SCHEMA, "query_state");
        info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
        info->columns.AddColumn(ColumnDefinition("query_id",    LogicalType::UUID));
        info->columns.AddColumn(ColumnDefinition("client_id",   LogicalType::VARCHAR));
        info->columns.AddColumn(ColumnDefinition("state",       LogicalType::VARCHAR));
        info->columns.AddColumn(ColumnDefinition("server",      LogicalType::VARCHAR));
        info->columns.AddColumn(ColumnDefinition("query",       LogicalType::VARCHAR));
        info->columns.AddColumn(ColumnDefinition("hashed_query",LogicalType::VARCHAR));
 		info->columns.AddColumn(ColumnDefinition("time_received", LogicalType::TIMESTAMP));
 		info->columns.AddColumn(ColumnDefinition("time_finished", LogicalType::TIMESTAMP));

        // Set default for query_id: gen_random_uuid()
        auto uuid_call = make_uniq<FunctionExpression>("gen_random_uuid", vector<unique_ptr<ParsedExpression>>());
        info->columns.GetColumnMutable(LogicalIndex(0)).SetDefaultValue(std::move(uuid_call));

        auto &schema = catalog.GetSchema(context, DEFAULT_SCHEMA);
        auto binder = Binder::CreateBinder(context);
        auto bound_info = binder->BindCreateTableInfo(std::move(info), schema);
        schema.CreateTable(catalog.GetCatalogTransaction(context), *bound_info);
    }

unique_ptr<GlobalTableFunctionState> RpcInitGlobalOrchestrate(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<RpcBindDataOrchestrate>();
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

	auto &bind_data = input.bind_data->Cast<RpcBindDataOrchestrate>();
	auto &global_state = global_state_p->Cast<RpcGlobalStateOrchestrate>();
	if (global_state.done) {
		return nullptr;
	}
	auto local_state = make_uniq<RpcLocalStateOrchestrate>();
	local_state->client = RpcClient::GetClient(bind_data.uri);
	// TODO re-use client from bind data for first conn

	return local_state;
}

// static void InsertQueryState(ClientContext &context, const string &client_id, const string &state,
// 								 const string &server, const string &query, const string &hashed_query) {
//
//
//  	auto &catalog = Catalog::GetSystemCatalog(context);
//  	// Todo, maybe store a pointer to table catalog entry in the LocalOrchestrateState
//  	auto const &table = catalog.GetEntry<TableCatalogEntry>(context, DEFAULT_SCHEMA, "query_state", OnEntryNotFound::THROW_EXCEPTION);
//
//  	auto &db = DatabaseInstance::GetDatabase(context);
//  	InternalAppender appender(context, table);
//  	appender.BeginRow();
//  	appender.Append(Value::UUID(UUID::GenerateRandomUUID()));
//  	appender.Append(Value(client_id));
//  	appender.Append(Value(state));
//  	appender.Append(Value(server));
//  	appender.Append(Value(query));
//  	appender.Append(Value(hashed_query));
//  	appender.Append(Value::TIMESTAMP(Timestamp::GetCurrentTimestamp()));
//  	appender.Append(Value());  // time_finished is NULL initially
//  	appender.EndRow();
//  	appender.Flush();
//  }


static void RpcOrchestrate(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<RpcBindDataOrchestrate>();
	auto &global_state = input.global_state->Cast<RpcGlobalStateOrchestrate>();
	auto &local_state = input.local_state->Cast<RpcLocalStateOrchestrate>();

	if (global_state.done) {
		return;
	}

 	// here same as fetch data etc.

	// TODO; hash query?
 	// TODO; check if query is running with the same hash
	// TODO; gather statistics

	// Create the table if its not there
 	CreateQueryStateTableLazy(context);
 	// TODO add query to internal query state table

	// Q; will this lead to concurrency issues?

	// Resolve the matching overload and invoke it directly
	auto bound_expr = RcpGetBoundOrchestrateCallback(bind_data);
	auto result_value = ExpressionExecutor::EvaluateScalar(context, *bound_expr);

	// after getting the struct, we deserialize it and open a new RPC call to the compute server
 	// then is basically makes this request
 // 	auto fetch_response =
	// local_state.client->MakeRequest<FetchResponseMessage>(make_uniq<FetchRequestMessage>(bind_data.connection_id));
 // 	if (!fetch_response->ResponseData() || fetch_response->ResponseData()->size() == 0) {
 // 		global_state.done = true;
 // 		return;
 // 	}

	output.SetCardinality(1);
	output.SetValue(0, 0, result_value);
	global_state.done = true;

}

TableFunction RpcOrchestrateFunction::GetFunction() {
	// input; coordinator_server, query, metadata -> client_id, panel_id, result_set_id
	return TableFunction("rpc_orchestrate", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, RpcOrchestrate, RpcBindOrchestrate, RpcInitGlobalOrchestrate,
	                     RpcInitLocalOrchestrate);
}
