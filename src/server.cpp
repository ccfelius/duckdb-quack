#include "server.hpp"
#include "message.hpp"
#include "ssl_key_generator.hpp"
#include "client.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/render_tree.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/planner/binder.hpp"

#include <duckdb/execution/expression_executor.hpp>

using namespace duckdb;

using websocketpp::lib::placeholders::_1;
using websocketpp::lib::placeholders::_2;

static std::string GetCertificatePassword() {
	throw InternalException("get_password called without a valid password");
}
// 1294 default port? seems to be unused

optional_ptr<RpcConnection> RpcServer::GetConnection(const string &connection_id) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);
	auto it = active_connections.find(connection_id);
	if (it != active_connections.end()) {
		return it->second.get();
	}
	return nullptr;
}

string RpcServer::CreateNewConnection() {
	std::lock_guard<std::mutex> lock(active_connections_mutex);
	// TODO this will need cryptographic randomness I fear
	auto connection_id = StringUtil::GenerateRandomName(40);
	D_ASSERT(active_connections.find(connection_id) == active_connections.end());

	auto new_connection = make_uniq<RpcConnection>();
	new_connection->duckdb_connection = make_uniq<Connection>(*db);
	active_connections[connection_id] = std::move(new_connection);
	return connection_id;
}

// TLS init gunk...
context_ptr WebSocketRpcServer::OnTlsInit(WebSocketRpcServer *rpc_server, const websocketpp::connection_hdl &) {
	D_ASSERT(rpc_server);
	namespace asio = websocketpp::lib::asio;

	context_ptr ctx = websocketpp::lib::make_shared<asio::ssl::context>(asio::ssl::context::sslv23);

	try {
		ctx->set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
		                 asio::ssl::context::no_sslv3 | asio::ssl::context::single_dh_use);

		// TODO, make this a secret?
		ctx->set_password_callback(bind(&GetCertificatePassword));

		auto &fs = FileSystem::GetFileSystem(*rpc_server->db);
		auto certificate_directory = SslKeyGenerator::GetDefaultCertificateDirectory(fs);
		auto server_key_file = fs.JoinPath(certificate_directory, "server.pem");
		auto private_key_file = fs.JoinPath(certificate_directory, "private_key.pem");
		auto dh_param_file = fs.JoinPath(certificate_directory, "dh.pem");

		if (!fs.FileExists(server_key_file) || !fs.FileExists(private_key_file) || !fs.FileExists(dh_param_file)) {
			throw InvalidInputException("Certificate files not found in %s - use rpc_generate_keys() to generate them",
			                            certificate_directory.c_str());
		}

		ctx->use_certificate_chain_file(server_key_file);
		ctx->use_private_key_file(private_key_file, asio::ssl::context::pem);
		ctx->use_tmp_dh_file(dh_param_file);

		std::string ciphers;

		ciphers = "ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-"
		          "ECDSA-AES256-GCM-SHA384:DHE-RSA-AES128-GCM-SHA256:DHE-DSS-AES128-GCM-SHA256:kEDH+AESGCM:ECDHE-"
		          "RSA-AES128-SHA256:ECDHE-ECDSA-AES128-SHA256:ECDHE-RSA-AES128-SHA:ECDHE-ECDSA-AES128-SHA:ECDHE-"
		          "RSA-AES256-SHA384:ECDHE-ECDSA-AES256-SHA384:ECDHE-RSA-AES256-SHA:ECDHE-ECDSA-AES256-SHA:DHE-RSA-"
		          "AES128-SHA256:DHE-RSA-AES128-SHA:DHE-DSS-AES128-SHA256:DHE-RSA-AES256-SHA256:DHE-DSS-AES256-SHA:"
		          "DHE-RSA-AES256-SHA:AES128-GCM-SHA256:AES256-GCM-SHA384:AES128-SHA256:AES256-SHA256:AES128-SHA:"
		          "AES256-SHA:AES:CAMELLIA:DES-CBC3-SHA:!aNULL:!eNULL:!EXPORT:!DES:!RC4:!MD5:!PSK:!aECDH:!EDH-DSS-"
		          "DES-CBC3-SHA:!EDH-RSA-DES-CBC3-SHA:!KRB5-DES-CBC3-SHA";

		if (SSL_CTX_set_cipher_list(ctx->native_handle(), ciphers.c_str()) != 1) {
			throw InternalException("Error setting cipher list");
		}
	} catch (std::exception &e) {
		throw InternalException(e.what());
	}
	return ctx;
}

RpcServer::RpcServer(ClientContext &context_p) : db(context_p.db) {
}

RpcServer::~RpcServer() {
}

// this cannot be in the generic destructor because it looks like the subclass-specific stuff gets destroyed before we
// get there
static void JoinListenThread(std::thread &listen_thread) {
	try {
		listen_thread.join();
	} catch (std::exception &) {
	}
}

WebSocketRpcServer::~WebSocketRpcServer() {
	websocket_server.stop();
	JoinListenThread(listen_thread);
}

UnixSocketRpcServer::~UnixSocketRpcServer() {
	// this should interrupt accept() in the listen thread
	close(unix_socket_server_fd);
	unix_socket_keep_listening = false;
	JoinListenThread(listen_thread);
}

void WebSocketRpcServer::WebsocketListenThread(WebSocketRpcServer *rpc_server) {
	D_ASSERT(rpc_server);

	rpc_server->websocket_server.start_accept();
	rpc_server->websocket_server.run();
}

void UnixSocketRpcServer::UnixSocketListenThread(UnixSocketRpcServer *rpc_server) {
	D_ASSERT(rpc_server);

	while (rpc_server->unix_socket_keep_listening) {
		unsigned int sock_len = 0;
		auto client_socket_fd = accept(rpc_server->unix_socket_server_fd,
		                               reinterpret_cast<sockaddr *>(&rpc_server->unix_socket_address), &sock_len);
		if (client_socket_fd == -1) {
			continue;
		}

		std::thread accept_thread([rpc_server, client_socket_fd] {
			bool open = true;
			MemoryStream read_stream, write_stream;

			do {
				try {
					auto received_message = ProtocolMessage::FromSocket(client_socket_fd, read_stream);
					// printf("S RECV %s\n", MessageTypeToString(received_message->Type()).c_str());
					auto response_message = rpc_server->HandleMessage(*received_message);
					// printf("S SEND %s\n", MessageTypeToString(response_message->Type()).c_str());

					response_message->ToSocket(client_socket_fd, write_stream);
				} catch (IOException &) {
					open = false;
				}

			} while (open);
			close(client_socket_fd);
		});
		accept_thread.detach(); // TODO do we need this?
	}
	// TODO clean up socket?
}

void UnixSocketRpcServer::Listen(const string &listen_string_p) {
	if (listen_string_p.empty()) {
		throw InvalidInputException("Empty listen string specified");
	}
	D_ASSERT(!StringUtil::StartsWith(listen_string_p, "wss:"));
	listen_string = listen_string_p;

	unix_socket_server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (unix_socket_server_fd == -1) {
		throw IOException("Error creating socket");
	}

	unix_socket_address.sun_family = AF_UNIX;
	memset(&unix_socket_address, 0, sizeof(sockaddr_un));
	strncpy(unix_socket_address.sun_path, listen_string.c_str(), sizeof(unix_socket_address.sun_path) - 1);

	auto unlink_result = unlink(unix_socket_address.sun_path);
	if (unlink_result && errno != ENOENT) {
		throw IOException("Error cleaning up socket %s: %s", listen_string, strerror(errno));
	}

	if (bind(unix_socket_server_fd, reinterpret_cast<sockaddr *>(&unix_socket_address),
	         SUN_LEN(&unix_socket_address)) ||
	    chmod(listen_string.c_str(), S_IWUSR | S_IRUSR) ||
	    listen(unix_socket_server_fd, 100 /* TODO: magic constant for connect queue length, should be fine */)) {
		throw IOException("Error listening to socket %s: %s", listen_string, strerror(errno));
	}

	unix_socket_keep_listening = true;
	listen_thread = std::thread(UnixSocketListenThread, this);
}

void WebSocketRpcServer::Listen(const string &listen_string_p) {
	if (listen_string_p.empty()) {
		throw InvalidInputException("Empty listen string specified");
	}
	D_ASSERT(StringUtil::StartsWith(listen_string_p, "wss://"));
	listen_string = listen_string_p;
	{
		websocket_server.set_access_channels(websocketpp::log::alevel::none);
		websocket_server.set_tls_init_handler(bind(&WebSocketRpcServer::OnTlsInit, this, ::_1));
		websocket_server.set_message_handler(bind(&WebSocketRpcServer::OnMessage, this, ::_1, ::_2));
		websocket_server.set_open_handler(bind(&WebSocketRpcServer::OnOpen, this, ::_1));

		websocket_server.init_asio();

		// TODO this is overly simplistic but fine for now
		auto listen_port = atoi(StringUtil::Replace(listen_string, "wss://localhost:", "").c_str());
		if (listen_port < 1 || listen_port > 65535) {
			throw InvalidInputException("Invalid port specified for websocket server (%d)", listen_port);
		}
		websocket_server.listen(listen_port);

		listen_thread = std::thread(WebsocketListenThread, this);
	}
}

LogicalType GetOrchestratorInfoStructFormat() {
	child_list_t<LogicalType> struct_children;

	struct_children.emplace_back("request_id",  LogicalType::VARCHAR);
	struct_children.emplace_back("query",        LogicalType::VARCHAR);
	struct_children.emplace_back("server_name",  LogicalType::VARCHAR);
	struct_children.emplace_back("server_uri",   LogicalType::VARCHAR);
	struct_children.emplace_back("result",       LogicalType::VARCHAR);
	auto input_struct_type = LogicalType::STRUCT(std::move(struct_children));
	return input_struct_type;
}

Value InitializeOrchestratorInfoStruct(const string &query) {
		auto struct_type = GetOrchestratorInfoStructFormat();

		vector<Value> struct_values;
		struct_values.push_back(Value(rand())); // request_id
		struct_values.push_back(Value(query));         // query
		struct_values.push_back(Value(nullptr));                // server_name
		struct_values.push_back(Value(nullptr));          // server_uri
		struct_values.push_back(Value(nullptr));                // result

		return Value::STRUCT(struct_type, std::move(struct_values));
}

unique_ptr<ScalarFunction> RcpGetOrchestrateCallbackFunc(ClientContext &context) {
	auto callback = Catalog::GetEntry<ScalarFunctionCatalogEntry>(
	   context, INVALID_CATALOG, DEFAULT_SCHEMA, "orchestrate", OnEntryNotFound::RETURN_NULL);
	if (!callback) {
		throw InvalidInputException("No 'orchestrate' function registered in the catalog. You must configure an external orchestrate function");
	}

	return make_uniq<ScalarFunction>(callback->functions.GetFunctionByArguments(context, {GetOrchestratorInfoStructFormat()}));
}



static unique_ptr<BoundFunctionExpression> RcpGetBoundOrchestrateCallback(unique_ptr<ScalarFunction> &callback_func, const string &query) {
	// TODO; move this out of this function
	auto orchestrator_info_struct = InitializeOrchestratorInfoStruct(query);

	// push the input arguments
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundConstantExpression>(orchestrator_info_struct));

	return make_uniq<BoundFunctionExpression>(callback_func->return_type, *callback_func, std::move(children), nullptr);
}

unique_ptr<ProtocolMessage> RpcServer::HandleOrchestrateRequest(ProtocolMessage &received_message) {
	auto &orchestrate_request_message = received_message.Cast<OrchestrateRequestMessage>();
	auto &query = orchestrate_request_message.Query();

	optional_ptr<RpcConnection> rpc_connection = GetConnection(orchestrate_request_message.ConnectionId());
	if (!rpc_connection) {
		return make_uniq<ErrorMessage>("Invalid connection id");
	}

	// Locks and resets the connection
	// std::unique_lock<std::mutex> lock(rpc_connection->lock);
	// rpc_connection->duckdb_query_result.reset();
	//
	// // ExecuteQuery()
	// auto statement = rpc_connection->duckdb_connection->Prepare(orchestrate_request_message.Query());
	// if (statement->HasError()) {
	// 	return make_uniq<ErrorMessage>(statement->GetError());
	// }

	// Resolve the matching overload and invoke it directly
	auto callback_bind_data = RcpGetOrchestrateCallbackFunc(*rpc_connection->duckdb_connection->context);
	auto bound_expr = RcpGetBoundOrchestrateCallback(callback_bind_data, query);
	auto result_value = ExpressionExecutor::EvaluateScalar(*rpc_connection->duckdb_connection->context, *bound_expr);

	//TODO; deserialize the result struct of the orchestrator callback

	//! we only deserialize the minimum here right now
	auto &children = StructValue::GetChildren(result_value);
	const string &server_uri = children[2].ToString();

	D_ASSERT(!server_uri.empty());


	// use the server id (or connection id (?)) to connect with the worker node
	// TODO; maybe create a mapping; server_id -> available connections

	// TODO; with server id, issue a new connection request
	auto client = RpcClient::GetClient(server_uri);
	auto connection_request_response =
	client->MakeRequest<ConnectionResponseMessage>(make_uniq<ConnectionRequestMessage>());
	auto connection_id = connection_request_response->ConnectionId();

	// TODO; send prepare (forward)
	auto bind_response = client->MakeRequest<PrepareResponseMessage>(
	make_uniq<PrepareRequestMessage>(connection_id, query, true));

	// TODO; send fetch (forward)




	return nullptr;
}
// main switcheroo happens here
unique_ptr<ProtocolMessage> RpcServer::HandleMessage(ProtocolMessage &received_message) {
	switch (received_message.Type()) {
	case MessageType::CONNECTION_REQUEST: {
		auto &connection_request_message = received_message.Cast<ConnectionRequestMessage>();
		// TODO handle auth here! Only return a connection ID if auth succeeds.
		return make_uniq<ConnectionResponseMessage>(CreateNewConnection());
	}
	case MessageType::PREPARE_REQUEST: {
		auto &prepare_request_message = received_message.Cast<PrepareRequestMessage>();
		optional_ptr<RpcConnection> rpc_connection = GetConnection(prepare_request_message.ConnectionId());
		if (!rpc_connection) {
			return make_uniq<ErrorMessage>("Invalid connection id");
		}

		std::unique_lock<std::mutex> lock(rpc_connection->lock);
		rpc_connection->duckdb_query_result.reset();
		auto statement = rpc_connection->duckdb_connection->Prepare(prepare_request_message.Query());
		if (statement->HasError()) {
			return make_uniq<ErrorMessage>(statement->GetError());
		}

		auto estimated_cardinality = optional_idx::Invalid();
		if (prepare_request_message.ImmediatelyExecute()) {
			auto &client_config = ClientConfig::GetConfig(*rpc_connection->duckdb_connection->context);
			client_config.enable_profiler = true;
			client_config.profiling_coverage = ProfilingCoverage::ALL;
			client_config.profiler_settings = {
			    MetricType::EXTRA_INFO}; // 'EXTRA_INFO' means return estimated cardinality (among other things)
			client_config.emit_profiler_output = false;
			// TODO do we need to restore the previous config after this?

			vector<Value> params; // TODO allow parameters here?
			auto query_result = statement->PendingQuery(params, true)->Execute();
			if (query_result->HasError()) {
				return make_uniq<ErrorMessage>(query_result->GetError());
			}
			rpc_connection->duckdb_query_result = std::move(query_result);

			// for some reason the profiler is only alive here
			auto &profiler = QueryProfiler::Get(*rpc_connection->duckdb_connection->context);
			if (profiler.GetRoot() && profiler.GetRoot()->children.size() == 1) {
				auto &profiler_info = profiler.GetRoot()->children[0]->GetProfilingInfo();
				// this should always be true, see above
				D_ASSERT(profiler_info.Enabled(profiler_info.settings, MetricType::EXTRA_INFO));
				auto extra_info_map =
				    profiler_info.GetMetricValue<InsertionOrderPreservingMap<string>>(MetricType::EXTRA_INFO);
				if (extra_info_map.find(RenderTreeNode::ESTIMATED_CARDINALITY) != extra_info_map.end()) {
					estimated_cardinality = atoll(extra_info_map[RenderTreeNode::ESTIMATED_CARDINALITY].c_str());
				}
			}
		}
		return make_uniq<PrepareResponseMessage>(statement->GetTypes(), statement->GetNames(), estimated_cardinality);
	}

	// checks if
	case MessageType::ORCHESTRATE_REQUEST: {
		return HandleOrchestrateRequest(received_message);
	}

	case MessageType::FETCH_REQUEST: {
		auto &fetch_request_message = received_message.Cast<FetchRequestMessage>();
		optional_ptr<RpcConnection> rpc_connection = GetConnection(fetch_request_message.ConnectionId());
		if (!rpc_connection) {
			return make_uniq<ErrorMessage>("Invalid connection id");
		}
		std::unique_lock<std::mutex> lock(rpc_connection->lock);

		if (!rpc_connection->duckdb_query_result) {
			return make_uniq<FetchResponseMessage>(nullptr);
		}
		if (rpc_connection->duckdb_query_result->HasError()) {
			return make_uniq<ErrorMessage>(rpc_connection->duckdb_query_result->GetError());
		}
		auto result_chunk = rpc_connection->duckdb_query_result->Fetch();
		if (!result_chunk && rpc_connection->duckdb_query_result->HasError()) {
			auto error = make_uniq<ErrorMessage>(rpc_connection->duckdb_query_result->GetError());
			rpc_connection->duckdb_query_result.reset();
			return error;
		}
		if (!result_chunk || result_chunk->size() == 0) {
			rpc_connection->duckdb_query_result.reset();
			return make_uniq<FetchResponseMessage>(nullptr);
		}
		return make_uniq<FetchResponseMessage>(std::move(result_chunk));
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
	default: {
		return make_uniq<ErrorMessage>(
		    StringUtil::Format("Unimplemented message type %s", MessageTypeToString(received_message.Type())));
	}
	}
}

void WebSocketRpcServer::OnMessage(const websocketpp::connection_hdl &hdl, const message_ptr &msg) {
	MemoryStream read_stream((data_ptr_t)msg->get_payload().data(), msg.get()->get_payload().size());
	auto received_message = ProtocolMessage::FromMemoryStream(read_stream);
	auto response_message = HandleMessage(*received_message);

	MemoryStream write_stream;
	response_message->ToMemoryStream(write_stream);
	try {
		websocket_server.send(hdl, write_stream.GetData(), write_stream.GetPosition(),
		                      websocketpp::frame::opcode::binary);
	} catch (websocketpp::exception const &e) {
		// TODO we should not fail here but log something
		std::cout << "sending reply failed because: "
		          << "(" << e.what() << ")" << std::endl;
	}
}

void WebSocketRpcServer::OnOpen(const websocketpp::connection_hdl &hdl) {
	// hdl.
}
