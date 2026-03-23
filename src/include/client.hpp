#pragma once

#include "message.hpp"

#define ASIO_STANDALONE // no boost!

#include "websocketpp/client.hpp"
#include "websocketpp/config/asio_client.hpp"

namespace duckdb {

class RpcClient {
public:
	explicit RpcClient(const string &uri_p) : uri(uri_p) {};
	template <class TARGET>
	unique_ptr<TARGET> MakeRequest(unique_ptr<ProtocolMessage> request_message) {
		printf("C SEND %s\n", MessageTypeToString(request_message->Type()).c_str());
		Send(std::move(request_message));
		auto response_message = WaitForMessageInternal(TARGET::TYPE).release();
		//	printf("C RECV %s\n", MessageTypeToString(response_message->Type()).c_str());

		return unique_ptr<TARGET>(reinterpret_cast<TARGET *>(response_message));
	}

	static unique_ptr<RpcClient> GetClient(const string &uri);

	virtual ~RpcClient() {};

protected:
	MemoryStream read_stream, write_stream;
	const string uri;

private:
	unique_ptr<ProtocolMessage> WaitForMessageInternal(MessageType expected_type);
	virtual void Send(unique_ptr<ProtocolMessage> message_p) {};
	virtual unique_ptr<ProtocolMessage> Receive() {
		return nullptr;
	};
};

class UnixSocketRpcClient : public RpcClient {
public:
	UnixSocketRpcClient(const string &uri_p);
	~UnixSocketRpcClient() override;

private:
	void Send(unique_ptr<ProtocolMessage> message_p) override;
	unique_ptr<ProtocolMessage> Receive() override;

private:
	int unix_socket_fd;
};

// TODO move this to a separate header
typedef websocketpp::connection_hdl connection_ptr;
typedef websocketpp::config::asio_client::message_type::ptr message_ptr;
typedef websocketpp::lib::shared_ptr<websocketpp::lib::asio::ssl::context> context_ptr;
typedef websocketpp::client<websocketpp::config::asio_tls_client> client;

class WebSocketRpcClient : public RpcClient {
public:
	WebSocketRpcClient(const string &uri_p);
	~WebSocketRpcClient() override;

private:
	void Send(unique_ptr<ProtocolMessage> message_p) override;
	unique_ptr<ProtocolMessage> Receive() override;
	void OnOpen(connection_ptr hdl);
	void OnMessage(const connection_ptr &hdl, message_ptr msg);
	void OnFail(connection_ptr hdl);

private:
	std::thread websocket_listen_thread;
	unique_ptr<ProtocolMessage> message;
	deque<unique_ptr<ProtocolMessage>> messages;
	std::mutex messages_mutex;
	std::condition_variable messages_wait;
	client websocket_client;
	client::connection_ptr websocket_connection;
	bool connection_open = false;
	std::string websocket_exception;
};

} // namespace duckdb
