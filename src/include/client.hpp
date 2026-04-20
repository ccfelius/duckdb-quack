#pragma once

#include "message.hpp"
#include "rpc_log_type.hpp"
#include "rpc_uri.hpp"

#include "duckdb/logging/logger.hpp"

namespace duckdb_httplib_openssl {
class Client;
}

namespace duckdb {

class RpcClient {
public:
	explicit RpcClient(const RpcUri &uri_p) : uri(uri_p) {};

	void CancelRequest(const string &connection_id) {
		Request<CancelResponseMessage>(make_uniq<CancelRequestMessage>(connection_id));
	}

	void SetContext(optional_ptr<ClientContext> context_p) {
		context = context_p;
	}

	template <class TARGET>
	unique_ptr<TARGET> Request(unique_ptr<ProtocolMessage> request_message) {
		lock_guard<mutex> guard(request_mutex);

		// Extract metadata before move
		auto request_type = request_message->Type();
		string rpc_connection_id;
		string query;
		optional_idx client_query_id;
		switch (request_type) {
		case MessageType::PREPARE_REQUEST: {
			auto &msg = request_message->Cast<PrepareRequestMessage>();
			rpc_connection_id = msg.ConnectionId();
			query = msg.Query();
			break;
		}
		case MessageType::FETCH_REQUEST:
			rpc_connection_id = request_message->Cast<FetchRequestMessage>().ConnectionId();
			break;
		case MessageType::CATALOG_REQUEST:
			rpc_connection_id = request_message->Cast<CatalogRequestMessage>().ConnectionId();
			break;
		case MessageType::APPEND_REQUEST:
			rpc_connection_id = request_message->Cast<AppendRequestMessage>().ConnectionId();
			break;
		case MessageType::CANCEL_REQUEST:
			rpc_connection_id = request_message->Cast<CancelRequestMessage>().ConnectionId();
			break;
		default:
			break;
		}

		if (context && context->transaction.HasActiveTransaction()) {
			client_query_id = context->transaction.GetActiveQuery();
			request_message->SetClientQueryId(client_query_id);
		}

		// Time the request
		int64_t start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                         .time_since_epoch()
		                         .count();

		auto response_message = RequestInternal(std::move(request_message)).release();

		int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                       .time_since_epoch()
		                       .count();

		// Log RPC message
		if (context) {
			auto &logger = Logger::Get(*context);
			if (logger.ShouldLog(RPCLogType::NAME, RPCLogType::LEVEL)) {
				string error;
				if (response_message->Type() == MessageType::RPC_ERROR) {
					error = response_message->Cast<ErrorMessage>().Error();
				}
				auto msg =
				    RPCLogType::ConstructLogMessage(request_type, rpc_connection_id, client_query_id, query, uri.Http(),
				                                    end_time - start_time, response_message->Type(), error);
				logger.WriteLog(RPCLogType::NAME, RPCLogType::LEVEL, msg);
			}
		}

		if (response_message->Type() != TARGET::TYPE) {
			if (response_message->Type() == MessageType::RPC_ERROR) {
				throw IOException("Expected %s message, got error message instead: %s",
				                  MessageTypeToString(TARGET::TYPE),
				                  response_message->Cast<ErrorMessage>().Error().c_str());
			}
			throw IOException("Expected %s message, got %s instead", MessageTypeToString(TARGET::TYPE),
			                  MessageTypeToString(response_message->Type()));
		}
		return unique_ptr<TARGET>(reinterpret_cast<TARGET *>(response_message));
	}

	// Sends a FETCH_REQUEST and returns FETCH_RESPONSE, FINISH_RESPONSE, or CANCEL_RESPONSE.
	unique_ptr<ProtocolMessage> Fetch(const string &connection_id);
	static unique_ptr<RpcClient> GetClient(const RpcUri &uri);

	virtual ~RpcClient() {};

protected:
	virtual void CancelInFlight() {};

protected:
	mutex request_mutex;
	MemoryStream read_stream, write_stream;
	RpcUri uri;
	optional_ptr<ClientContext> context;

private:
	virtual unique_ptr<ProtocolMessage> RequestInternal(unique_ptr<ProtocolMessage> request_message) = 0;
};

class HttpsRpcClient : public RpcClient {
public:
	HttpsRpcClient(const RpcUri &uri_p);
	~HttpsRpcClient() override;

public:
private:
	unique_ptr<ProtocolMessage> RequestInternal(unique_ptr<ProtocolMessage> request_message) override;

private:
	unique_ptr<duckdb_httplib_openssl::Client> https_client;

protected:
	void CancelInFlight() override;
};

} // namespace duckdb
