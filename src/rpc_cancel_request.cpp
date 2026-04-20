#include "duckdb/function/function.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "catalog.hpp"
#include "message.hpp"
#include "rpc_cancel_function.hpp"

namespace duckdb {
struct RpcCancelFunctionData : public duckdb::TableFunctionData {
	bool finished = false;
	RpcUri server_uri;
	string connection_id;
};

// Bind: parse inputs, set return type
static unique_ptr<FunctionData> RpcCancelBind(ClientContext &context,
	TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<RpcCancelFunctionData>();
	bind_data->server_uri = RpcUri(input.inputs[0].GetValue<string>());
	bind_data->connection_id = input.inputs[1].GetValue<string>();
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("status");
	return std::move(bind_data);
}

// Execute: create a fresh client, send CANCEL_REQUEST, return status
static void RpcCancelFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<RpcCancelFunctionData>();
	if (bind_data.finished) { return; }

	auto client = RpcClient::GetClient(bind_data.server_uri);
	client->SetContext(&context);  // enables logging
	client->Request<CancelResponseMessage>(
		make_uniq<CancelRequestMessage>(bind_data.connection_id));

	output.data[0].SetValue(0, "Cancelled " + bind_data.connection_id);
	output.SetCardinality(1);
	bind_data.finished = true;
}

TableFunction RpcCancelFunction::GetFunction() {
	auto fun = TableFunction("rpc_cancel",
		{LogicalType::VARCHAR, LogicalType::VARCHAR}, RpcCancelFun, RpcCancelBind);
	fun.named_parameters["disable_ssl"] = LogicalType::BOOLEAN;
	return fun;
}
}