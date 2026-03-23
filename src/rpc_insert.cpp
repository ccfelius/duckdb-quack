#include "rpc_insert.hpp"

#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "include/catalog.hpp"

namespace duckdb {

RpcInsert::RpcInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
                     physical_index_vector_t<idx_t> column_index_map_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(&table), schema(nullptr),
      column_index_map(std::move(column_index_map_p)) {
}

RpcInsert::RpcInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
                     unique_ptr<BoundCreateTableInfo> info)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(nullptr), schema(&schema),
      info(std::move(info)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class RpcInsertGlobalState : public GlobalSinkState {
public:
	explicit RpcInsertGlobalState(ClientContext &context, RpcTableCatalogEntry &table) : table(table), insert_count(0) {
	}

	RpcTableCatalogEntry &table;
	// PostgresCopyState copy_state;
	// DataChunk varchar_chunk;
	idx_t insert_count;
	// PostgresCopyFormat format;
	// vector<string> insert_column_names;
	// bool copy_is_active = false;
	//
	// void FinishCopyTo(PostgresConnection &connection) {
	// 	if (!copy_is_active) {
	// 		return;
	// 	}
	// 	connection.FinishCopyTo(copy_state);
	// 	copy_is_active = false;
	// }
};

unique_ptr<GlobalSinkState> RpcInsert::GetGlobalSinkState(ClientContext &context) const {
	if (!table.get()) {
		if (!schema.get()) {
			throw NotImplementedException("NO table and schema found");
		}

		return make_uniq<RpcInsertGlobalState>(context, schema.get_mutable()->Cast<RpcTableCatalogEntry>());
	}
	return make_uniq<RpcInsertGlobalState>(context, table.get_mutable()->Cast<RpcTableCatalogEntry>());
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType RpcInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &rpc_catalog = table->catalog.Cast<RpcCatalog>();
	auto &global_state = input.global_state.Cast<RpcInsertGlobalState>();
	auto append_chunk = make_uniq<DataChunk>();
	append_chunk->Initialize(context.client, chunk.GetTypes());
	append_chunk->Reference(chunk);
	// TODO do we have to deal with differing column names??
	auto append_message = make_uniq<AppendRequestMessage>(rpc_catalog.GetConnectionId(), table->schema.name,
	                                                      table->name, std::move(append_chunk));
	rpc_catalog.GetRawClient().MakeRequest<AppendResponseMessage>(std::move(append_message));
	global_state.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType RpcInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                     OperatorSinkFinalizeInput &input) const {
	// TODO nop?
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType RpcInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<RpcInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.insert_count));
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string RpcInsert::GetName() const {
	return table ? "RPC_INSERT" : "RPC_CREATE_TABLE_AS";
}

InsertionOrderPreservingMap<string> RpcInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table ? table->name : info->Base().table;
	return result;
}

PhysicalOperator &RpcCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                         optional_ptr<PhysicalOperator> plan) {
	D_ASSERT(plan);
	auto &insert = planner.Make<RpcInsert>(op, op.table, op.column_index_map);
	insert.children.push_back(*plan);
	return insert;
}

PhysicalOperator &RpcCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalCreateTable &op, PhysicalOperator &plan) {
	auto &insert = planner.Make<RpcInsert>(op, op.schema, std::move(op.info));
	insert.children.push_back(plan);
	return insert;
}

} // namespace duckdb
