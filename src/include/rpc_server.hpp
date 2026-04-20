#pragma once

#define ASIO_STANDALONE // no boost!

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "rpc_uri.hpp"

#include <thread>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

namespace duckdb {

class ClientContext;
class ProtocolMessage;
class Connection;
class MemoryStream;
class QueryResult;
class DatabaseInstance;
class PreparedStatement;

struct RpcConnection {
	mutex lock;
	unique_ptr<Connection> duckdb_connection;
	//	unordered_map<string, std::pair<unique_ptr<PreparedStatement>, unique_ptr<QueryResult>>> duckdb_statements;
	unique_ptr<QueryResult> duckdb_query_result;
	string current_query;
	int64_t query_started_ms = 0;
};

struct RpcConnectionSnapshot {
	string connection_id;
	string current_query;
	int64_t duration_ms;
};

class RpcServer {
public:
	explicit RpcServer(ClientContext &context_p);
	// TODO should listen be part of the constructor?
	virtual void Listen(const RpcUri &uri) {};

	optional_ptr<RpcConnection> GetConnection(const string &connection_id);
	string CreateNewConnection(const string &session_id);
	vector<RpcConnectionSnapshot> GetConnectionSnapshots();
	// TODO need something to destroy connections

	static string GenerateSessionId();

	virtual ~RpcServer();

protected:
	unique_ptr<ProtocolMessage> HandleMessage(ProtocolMessage &received_message);
	unique_ptr<ProtocolMessage> HandleMessageInternal(ProtocolMessage &received_message);

protected:
	std::vector<std::thread> listen_threads;

	shared_ptr<DatabaseInstance> db;
	mutex active_connections_mutex;
	unordered_map<string, unique_ptr<RpcConnection>> active_connections;
};

class HttpsRpcServer : public RpcServer {
public:
	HttpsRpcServer(ClientContext &context_p) : RpcServer(context_p) {
	}
	void Listen(const RpcUri &uri) override;

	~HttpsRpcServer() override;

private:
	static void ListenThread(HttpsRpcServer *rpc_server, const string &listen_host, int listen_port);

	unique_ptr<duckdb_httplib_openssl::Server> server;
};
;

} // namespace duckdb
