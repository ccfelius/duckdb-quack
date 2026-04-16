#include "rpc_server.hpp"
#include "message.hpp"
#include "rpc_log_type.hpp"
#include "rpc_storage_extension.hpp"

#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/render_tree.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/temporary_file_manager.hpp"
#include "duckdb/main/database.hpp"

#include "duckdb/common/types/blob.hpp"
#include <openssl/rand.h>

using namespace duckdb;

RpcServer::RpcServer(ClientContext &context_p) : db(context_p.db) {
}

RpcServer::~RpcServer() {
}

optional_ptr<RpcConnection> RpcServer::GetConnection(const string &connection_id) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);
	auto it = active_connections.find(connection_id);
	if (it != active_connections.end()) {
		return it->second.get();
	}
	return nullptr;
}

string RpcServer::CreateNewConnection(const string &session_id) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);

	D_ASSERT(active_connections.find(session_id) == active_connections.end());

	auto new_connection = make_uniq<RpcConnection>();
	new_connection->duckdb_connection = make_uniq<Connection>(*db);
	new_connection->duckdb_connection->context->config.enable_progress_bar = false;
	// new_connection->duckdb_connection->context->config.streaming_buffer_size = 10 * 1000000; // 10 MB

	auto &client_config = ClientConfig::GetConfig(*new_connection->duckdb_connection->context);
	client_config.enable_profiler = true;
	client_config.profiling_coverage = ProfilingCoverage::SELECT;
	client_config.emit_profiler_output = false;
	client_config.profiler_settings = {
	    MetricType::EXTRA_INFO}; // 'EXTRA_INFO' means return estimated cardinality (among other things)

	active_connections[session_id] = std::move(new_connection);
	return session_id;
}

// try to get an estimated cardinality from the profiler, orrr
static optional_idx GetEstimatedCardinality(ClientContext &context) {
	optional_idx estimated_cardinality = optional_idx::Invalid();
	auto &profiler = QueryProfiler::Get(context);
	if (profiler.GetRoot() && profiler.GetRoot()->children.size() == 1) {
		auto &profiler_info = profiler.GetRoot()->children[0]->GetProfilingInfo();
		if (profiler_info.Enabled(profiler_info.settings, MetricType::EXTRA_INFO)) {
			auto extra_info_map =
			    profiler_info.GetMetricValue<InsertionOrderPreservingMap<string>>(MetricType::EXTRA_INFO);
			if (extra_info_map.contains(RenderTreeNode::ESTIMATED_CARDINALITY)) {
				estimated_cardinality = stoll(extra_info_map[RenderTreeNode::ESTIMATED_CARDINALITY]);
			}
		}
	}
	if (estimated_cardinality == 0) {
		estimated_cardinality = optional_idx::Invalid();
	}
	return estimated_cardinality;
}

static string GetSettingString(DatabaseInstance &db, const string &setting_name) {
	Value setting_val;
	auto &config = DBConfig::GetConfig(db);

	auto lookup_result = config.TryGetCurrentSetting(setting_name, setting_val);
	D_ASSERT(lookup_result);
	D_ASSERT(setting_val.type().id() == LogicalTypeId::VARCHAR);
	auto setting_str = setting_val.GetValue<string>();
	D_ASSERT(!setting_str.empty());
	return setting_str;
}

template <typename... ARGS>
static bool EvaluateAuthQuery(DatabaseInstance &db, const string &sql, ARGS... values) {
	Connection dummy_connection(db);
	auto auth_result = dummy_connection.Query(sql, values...);
	if (!auth_result || auth_result->HasError()) {
		return false;
	}
	auto auth_result_chunk = auth_result->Fetch();
	if (!auth_result_chunk || !auth_result_chunk->GetValue(0, 0).template GetValue<bool>()) {
		return false;
	}
	return true;
}

string RpcServer::GenerateSessionId() {
	string session_id_bytes;
	session_id_bytes.resize(32);
	// we force the use of the (safer) OpenSSL random generator here, the session ids should really not be
	// predictable
	if (!RAND_bytes((unsigned char *)session_id_bytes.data(), session_id_bytes.size())) {
		throw InternalException("RAND_bytes failed");
	}
	auto session_id = Blob::ToBase64(session_id_bytes);
	D_ASSERT(!session_id.empty());
	return session_id;
}

// Extract connection_id from a message if available
static string ExtractConnectionId(ProtocolMessage &msg) {
	switch (msg.Type()) {
	case MessageType::PREPARE_REQUEST:
		return msg.Cast<PrepareRequestMessage>().ConnectionId();
	case MessageType::FETCH_REQUEST:
		return msg.Cast<FetchRequestMessage>().ConnectionId();
	case MessageType::CATALOG_REQUEST:
		return msg.Cast<CatalogRequestMessage>().ConnectionId();
	case MessageType::APPEND_REQUEST:
		return msg.Cast<AppendRequestMessage>().ConnectionId();
	case MessageType::CANCEL_REQUEST:
		return msg.Cast<CancelRequestMessage>().ConnectionId();
	default:
		return "";
	}
}

static optional_idx ExtractClientQueryId(ProtocolMessage &msg) {
	return msg.ClientQueryId();
}

static string ExtractQuery(ProtocolMessage &msg) {
	if (msg.Type() == MessageType::PREPARE_REQUEST) {
		return msg.Cast<PrepareRequestMessage>().Query();
	}
	return "";
}

// main switcheroo happens here
unique_ptr<ProtocolMessage> RpcServer::HandleMessage(ProtocolMessage &received_message) {
	auto &logger = Logger::Get(*db);
	bool should_log = logger.ShouldLog(RPCLogType::NAME, RPCLogType::LEVEL);

	string rpc_connection_id;
	string query;
	optional_idx client_query_id;
	int64_t start_time = 0;
	if (should_log) {
		rpc_connection_id = ExtractConnectionId(received_message);
		client_query_id = ExtractClientQueryId(received_message);
		query = ExtractQuery(received_message);
		start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                 .time_since_epoch()
		                 .count();
	}

	auto response = HandleMessageInternal(received_message);

	if (should_log) {
		int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                       .time_since_epoch()
		                       .count();
		string error;
		if (response->Type() == MessageType::RPC_ERROR) {
			error = response->Cast<ErrorMessage>().Error();
		}
		auto msg = RPCLogType::ConstructLogMessage(received_message.Type(), rpc_connection_id, client_query_id, query,
		                                           "", end_time - start_time, response->Type(), error);
		logger.WriteLog(RPCLogType::NAME, RPCLogType::LEVEL, msg);
	}

	return response;
}

unique_ptr<ProtocolMessage> RpcServer::HandleMessageInternal(ProtocolMessage &received_message) {
	switch (received_message.Type()) {
	case MessageType::CONNECTION_REQUEST: {
		auto &connection_request_message = received_message.Cast<ConnectionRequestMessage>();
		string session_id = RpcServer::GenerateSessionId();
		if (!EvaluateAuthQuery(
		        *db, StringUtil::Format("SELECT %s(?, ?)", GetSettingString(*db, "rpc_authentication_function")),
		        Value(session_id), Value(connection_request_message.AuthString()))) {
			return make_uniq<ErrorMessage>("Authentication failed");
		}
		return make_uniq<ConnectionResponseMessage>(CreateNewConnection(session_id));
	}
	case MessageType::PREPARE_REQUEST: {
		auto &prepare_request_message = received_message.Cast<PrepareRequestMessage>();
		optional_ptr<RpcConnection> rpc_connection = GetConnection(prepare_request_message.ConnectionId());
		if (!rpc_connection) {
			return make_uniq<ErrorMessage>("Invalid connection id");
		}

		if (!EvaluateAuthQuery(
		        *db, StringUtil::Format("SELECT %s(?, ?)", GetSettingString(*db, "rpc_authorization_function")),
		        Value(prepare_request_message.ConnectionId()), Value(prepare_request_message.Query()))) {
			return make_uniq<ErrorMessage>("Authorization failed");
		}

		std::unique_lock<std::mutex> lock(rpc_connection->lock);
		rpc_connection->duckdb_query_result.reset();

		auto statement = rpc_connection->duckdb_connection->Prepare(prepare_request_message.Query());
		if (statement->HasError()) {
			return make_uniq<ErrorMessage>(statement->GetError());
		}
		if (prepare_request_message.ImmediatelyExecute()) {
			vector<Value> params; // TODO allow parameters here?
			auto query_result = statement->PendingQuery(params, true)->Execute();
			if (query_result->HasError()) {
				return make_uniq<ErrorMessage>(query_result->GetError());
			}
			rpc_connection->duckdb_query_result = std::move(query_result);
		}
		return make_uniq<PrepareResponseMessage>(statement->GetTypes(), statement->GetNames(),
		                                         GetEstimatedCardinality(*rpc_connection->duckdb_connection->context));
	}

	case MessageType::FETCH_REQUEST: {
		auto &fetch_request_message = received_message.Cast<FetchRequestMessage>();
		optional_ptr<RpcConnection> rpc_connection = GetConnection(fetch_request_message.ConnectionId());
		if (!rpc_connection) {
			return make_uniq<ErrorMessage>("Invalid connection id");
		}
		std::unique_lock<std::mutex> lock(rpc_connection->lock);

		if (!rpc_connection->duckdb_query_result) {
			return make_uniq<FetchResponseMessage>();
		}
		if (rpc_connection->duckdb_query_result->HasError()) {
			return make_uniq<ErrorMessage>(rpc_connection->duckdb_query_result->GetError());
		}

		Value max_chunks_val;
		Value max_bytes_val;
		DBConfig::GetConfig(*db).TryGetCurrentSetting("quack_fetch_batch_chunks", max_chunks_val);
		DBConfig::GetConfig(*db).TryGetCurrentSetting("quack_fetch_batch_bytes", max_bytes_val);
		auto max_chunks_per_batch = max_chunks_val.GetValue<uint64_t>();
		auto max_bytes_per_batch = max_bytes_val.GetValue<uint64_t>();

		vector<unique_ptr<DataChunk>> batch;
		idx_t bytes_est = 0;
		while (batch.size() < max_chunks_per_batch && bytes_est < max_bytes_per_batch) {
			auto result_chunk = rpc_connection->duckdb_query_result->Fetch();
			if (!result_chunk && rpc_connection->duckdb_query_result->HasError()) {
				auto error = make_uniq<ErrorMessage>(rpc_connection->duckdb_query_result->GetError());
				rpc_connection->duckdb_query_result.reset();
				return error;
			}
			if (!result_chunk || result_chunk->size() == 0) {
				rpc_connection->duckdb_query_result.reset();
				break;
			}
			bytes_est += result_chunk->size() * result_chunk->ColumnCount() * 8;
			batch.push_back(std::move(result_chunk));
		}
		return make_uniq<FetchResponseMessage>(std::move(batch));
	}

	case MessageType::CATALOG_REQUEST: {
		auto &catalog_request_message = received_message.Cast<CatalogRequestMessage>();
		optional_ptr<RpcConnection> rpc_connection = GetConnection(catalog_request_message.ConnectionId());
		if (!rpc_connection) {
			return make_uniq<ErrorMessage>("Invalid connection id");
		}
		std::unique_lock<std::mutex> lock(rpc_connection->lock);
		auto &context = *rpc_connection->duckdb_connection->context;

		// FIXME handle other types!
		auto parse_info = catalog_request_message.GetParseInfo();

		switch (parse_info->info_type) {
		case ParseInfoType::CREATE_INFO: {
			auto &create_info = parse_info->Cast<CreateInfo>();
			auto &catalog = Catalog::GetCatalog(context, create_info.catalog);
			switch (create_info.type) {
			case CatalogType::TABLE_ENTRY: {
				unique_ptr<CreateTableInfo> create_table_info(
				    reinterpret_cast<CreateTableInfo *>(parse_info.release()));
				auto &meta_transaction = MetaTransaction::Get(context);
				meta_transaction.ModifyDatabase(catalog.GetAttached(), DatabaseModificationType::CREATE_CATALOG_ENTRY);
				auto create_result = catalog.CreateTable(context, std::move(create_table_info));
				return make_uniq<CatalogResponseMessage>(create_result->GetInfo());
			}
			case CatalogType::SCHEMA_ENTRY: {
				auto &meta_transaction = MetaTransaction::Get(context);
				meta_transaction.ModifyDatabase(catalog.GetAttached(), DatabaseModificationType::CREATE_CATALOG_ENTRY);
				auto create_result = catalog.CreateSchema(context, parse_info->Cast<CreateSchemaInfo>());
				return make_uniq<CatalogResponseMessage>(create_result->GetInfo());
			}
			default:
				return make_uniq<ErrorMessage>(
				    StringUtil::Format("Unimplemented catalog type %s", CatalogTypeToString(create_info.type)));
			}
		}
		case ParseInfoType::DROP_INFO: {
			auto &drop_info = parse_info->Cast<DropInfo>();
			auto &catalog = Catalog::GetCatalog(context, drop_info.catalog);
			switch (drop_info.type) {
			case CatalogType::TABLE_ENTRY: {
				auto &meta_transaction = MetaTransaction::Get(context);
				meta_transaction.ModifyDatabase(catalog.GetAttached(), DatabaseModificationType::DROP_CATALOG_ENTRY);
				catalog.DropEntry(context, drop_info);
				return make_uniq<CatalogResponseMessage>(drop_info.Copy()); // The copy is not used but oh well
			}
			default:
				return make_uniq<ErrorMessage>(
				    StringUtil::Format("Unimplemented catalog type %s", CatalogTypeToString(drop_info.type)));
			}
		}
		default:
			return make_uniq<ErrorMessage>("Unimplemented parse info type");
		}
	}
	case MessageType::APPEND_REQUEST: {
		auto &append_request_message = received_message.Cast<AppendRequestMessage>();
		optional_ptr<RpcConnection> rpc_connection = GetConnection(append_request_message.ConnectionId());
		std::unique_lock<std::mutex> lock(rpc_connection->lock);
		auto &context = *rpc_connection->duckdb_connection->context;
		auto table_info = context.TableInfo(append_request_message.SchemaName(), append_request_message.TableName());
		ColumnDataCollection collection(Allocator::Get(context), append_request_message.AppendChunk().GetTypes());
		collection.Append(append_request_message.AppendChunk());
		rpc_connection->duckdb_connection->Append(*table_info, collection);
		return make_uniq<AppendResponseMessage>();
	}
	case MessageType::CANCEL_REQUEST: {
		auto &cancel_message = received_message.Cast<CancelRequestMessage>();
		auto rpc_connection = GetConnection(cancel_message.ConnectionId());
		if (!rpc_connection) {
			return make_uniq<ErrorMessage>("Invalid connection id");
		}
		// interrupt
		rpc_connection->duckdb_connection->context->Interrupt();
		return make_uniq<CancelResponseMessage>();
	}
	default: {
		return make_uniq<ErrorMessage>(
		    StringUtil::Format("Unimplemented message type %s", MessageTypeToString(received_message.Type())));
	}
	}
}
