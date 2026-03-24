#pragma once

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

enum class MessageType : uint8_t {
	INVALID = 0,
	CONNECTION_REQUEST = 1,
	CONNECTION_RESPONSE = 2,
	PREPARE_REQUEST = 3,
	PREPARE_RESPONSE = 4,
	FETCH_REQUEST = 7,
	FETCH_RESPONSE = 8,
	CATALOG_REQUEST = 9,
	CATALOG_RESPONSE = 10,
	APPEND_REQUEST = 11,
	APPEND_RESPONSE = 12,
	ORCHESTRATE_REQUEST = 13,
	FORWARD_PREPARE_REQUEST = 14,
	FORWARD_PREPARE_RESPONSE = 15,
	FORWARD_FETCH_REQUEST = 16,
	FORWARD_FETCH_RESPONSE = 17,
	ERROR = 100
};

string MessageTypeToString(MessageType type);

class ProtocolMessage {
public:
	void ToMemoryStream(MemoryStream &write_stream) const;
	static unique_ptr<ProtocolMessage> FromMemoryStream(MemoryStream &read_stream);

	void ToSocket(int fd, MemoryStream &write_stream) const;

	static unique_ptr<ProtocolMessage> FromSocket(int fd, MemoryStream &read_stream);

	template <class TARGET>
	TARGET &Cast() {
		if (message_type != TARGET::TYPE) {
			throw InternalException("Failed to cast message to type - message type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (message_type != TARGET::TYPE) {
			throw InternalException("Failed to cast message to type - message type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

	virtual void Serialize(Serializer &serializer) const;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

	const MessageType &Type() const {
		return message_type;
	}

	virtual ~ProtocolMessage() {
	}

protected:
	explicit ProtocolMessage(MessageType type) : message_type(type) {
	}
	MessageType message_type = MessageType::INVALID;
};

class PrepareRequestMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::PREPARE_REQUEST;

	PrepareRequestMessage(const string &connection_id_p, const string &sql_query_p, bool immediately_execute_p = false)
	    : ProtocolMessage(TYPE), connection_id(connection_id_p), sql_query(sql_query_p),
	      immediately_execute(immediately_execute_p) {
	}
	const std::string &Query() const {
		return sql_query;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

	const std::string &ConnectionId() const {
		return connection_id;
	}

	bool ImmediatelyExecute() const {
		return immediately_execute;
	}

private:
	string connection_id; // FIXME abstract this to some superclass
	string sql_query;
	bool immediately_execute;
};

class PrepareResponseMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::PREPARE_RESPONSE;
	PrepareResponseMessage(const vector<LogicalType> &types_p, const vector<string> &names_p,
	                       optional_idx estimated_cardinality_p)
	    : ProtocolMessage(TYPE), result_types(types_p), result_names(names_p),
	      estimated_cardinality(estimated_cardinality_p) {};

	const vector<LogicalType> &Types() const {
		return result_types;
	}

	const vector<string> &Names() const {
		return result_names;
	}
	optional_idx EstimatedCardinality() const {
		return estimated_cardinality;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

private:
	vector<LogicalType> result_types;
	vector<string> result_names;
	optional_idx estimated_cardinality;
};

class ForwardPrepareRequestMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::FORWARD_PREPARE_REQUEST;

	ForwardPrepareRequestMessage(unique_ptr<PrepareRequestMessage> base_request_p,
				     string compute_conn_id_p)
	    : ProtocolMessage(TYPE),
	      prepare_request(std::move(base_request_p)),
	      compute_conn_id(std::move(compute_conn_id_p)) {
	}

	// This is the only function you need to access the original data
	const PrepareRequestMessage& GetRequest() const { return *prepare_request; }

	const string& ComputeConnectionId() const { return compute_conn_id; }

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

private:
	unique_ptr<PrepareRequestMessage> prepare_request;
	string compute_conn_id;
};

class ForwardPrepareResponseMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::FORWARD_PREPARE_RESPONSE;

	ForwardPrepareResponseMessage(unique_ptr<PrepareResponseMessage> base_response_p,
				      string client_connection_id_p)
	    : ProtocolMessage(TYPE),
	      prepare_response(std::move(base_response_p)),
	      client_connection_id(std::move(client_connection_id_p)) {
	}

	// Accessor for the original response data
	const PrepareResponseMessage& GetResponse() const { return *prepare_response; }

	const string& ClientConnectionId() const { return client_connection_id; }

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

private:
	unique_ptr<PrepareResponseMessage> prepare_response;
	string client_connection_id;
};

// TODO this is where auth goes
class ConnectionRequestMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::CONNECTION_REQUEST;

	ConnectionRequestMessage() : ProtocolMessage(TYPE) {
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);
};

class ConnectionResponseMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::CONNECTION_RESPONSE;

	explicit ConnectionResponseMessage(const string &connection_id_p)
	    : ProtocolMessage(TYPE), connection_id(connection_id_p) {
	}

	const std::string &ConnectionId() const {
		return connection_id;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

private:
	string connection_id;
};

class OrchestrateRequestMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::ORCHESTRATE_REQUEST;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

	const std::string &ConnectionId() const {
		return client_connection_id;
	}
	explicit OrchestrateRequestMessage(const string &connection_id_p, const string &sql_query_p)
	    : ProtocolMessage(TYPE), client_connection_id(connection_id_p), sql_query(sql_query_p) {};

	const string &Query() const {
		return sql_query;
	}
private:
	string client_connection_id;
	string sql_query;
	// string metadata;
};

// Todo, this is just PrepareResponseMessage + is_avail
class OrchestrateResponseMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::FETCH_RESPONSE;

	explicit OrchestrateResponseMessage(const vector<LogicalType> &types_p, const vector<string> &names_p,
			       optional_idx estimated_cardinality_p)
	    : ProtocolMessage(TYPE), result_types(types_p), result_names(names_p),
	  estimated_cardinality(estimated_cardinality_p) {};

	const vector<LogicalType> &Types() const {
		return result_types;
	}

	const vector<string> &Names() const {
		return result_names;
	}
	optional_idx EstimatedCardinality() const {
		return estimated_cardinality;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

private:
	vector<LogicalType> result_types;
	vector<string> result_names;
	optional_idx estimated_cardinality;
};

class FetchRequestMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::FETCH_REQUEST;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);
	const std::string &ConnectionId() const {
		return connection_id;
	}
	explicit FetchRequestMessage(const string &connection_id_p)
	    : ProtocolMessage(TYPE), connection_id(connection_id_p) {};

	// TODO what was this for again?
	// TODO contain the query ref
private:
	string connection_id;
};

class FetchResponseMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::FETCH_RESPONSE;

	explicit FetchResponseMessage(unique_ptr<DataChunk> response_data_p)
	    : ProtocolMessage(TYPE), response_data(std::move(response_data_p)) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);
	optional_ptr<DataChunk> ResponseData() const {
		return response_data.get();
	}

private:
	unique_ptr<DataChunk> response_data;
};

class ForwardFetchRequestMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::FORWARD_FETCH_REQUEST;

	ForwardFetchRequestMessage(unique_ptr<FetchRequestMessage> base_request_p,
				   string compute_conn_id_p)
	    : ProtocolMessage(TYPE),
	      fetch_request(std::move(base_request_p)),
	      compute_conn_id(std::move(compute_conn_id_p)) {
	}

	// Accessor for the original request
	const FetchRequestMessage& GetRequest() const { return *fetch_request; }

	const string& ComputeConnectionId() const { return compute_conn_id; }

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

private:
	unique_ptr<FetchRequestMessage> fetch_request;
	string compute_conn_id;
};

class ForwardFetchResponseMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::FORWARD_FETCH_RESPONSE;

	ForwardFetchResponseMessage(unique_ptr<FetchResponseMessage> base_response_p,
				    string client_connection_id_p)
	    : ProtocolMessage(TYPE),
	      fetch_response(std::move(base_response_p)),
	      client_connection_id(std::move(client_connection_id_p)) {
	}

	// Accessor for the original response
	const FetchResponseMessage& GetResponse() const { return *fetch_response; }

	const string& ClientConnectionId() const { return client_connection_id; }

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

private:
	unique_ptr<FetchResponseMessage> fetch_response;
	string client_connection_id;
};

// orrr
static unique_ptr<ParseInfo> ParseInfoCopy(ParseInfo &parse_info) {
	switch (parse_info.info_type) {
	case ParseInfoType::CREATE_INFO: {
		return std::move(parse_info.Cast<CreateInfo>().Copy());
	}
	case ParseInfoType::DROP_INFO: {
		return std::move(parse_info.Cast<DropInfo>().Copy());
	}
	default:
		throw NotImplementedException("Unsupported ParseInfoType");
	}
}

class CatalogRequestMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::CATALOG_REQUEST;

	explicit CatalogRequestMessage(const string &connection_id_p, unique_ptr<ParseInfo> parse_info_p)
	    : ProtocolMessage(TYPE), connection_id(connection_id_p), parse_info(std::move(parse_info_p)) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);
	unique_ptr<ParseInfo> GetParseInfo() const {
		return ParseInfoCopy(*parse_info);
	}
	const std::string &ConnectionId() const {
		return connection_id;
	}

private:
	string connection_id;
	unique_ptr<ParseInfo> parse_info;
};

class CatalogResponseMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::CATALOG_RESPONSE;

	explicit CatalogResponseMessage(unique_ptr<ParseInfo> parse_info_p)
	    : ProtocolMessage(TYPE), parse_info(std::move(parse_info_p)) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);
	unique_ptr<ParseInfo> GetParseInfo() const {
		return ParseInfoCopy(*parse_info);
	}

private:
	unique_ptr<ParseInfo> parse_info;
};

class AppendRequestMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::APPEND_REQUEST;

	explicit AppendRequestMessage(const string &connection_id_p, const string &schema_name_p,
	                              const string &table_name_p, unique_ptr<DataChunk> append_chunk_p)
	    : ProtocolMessage(TYPE), connection_id(connection_id_p), schema_name(schema_name_p), table_name(table_name_p),
	      append_chunk(std::move(append_chunk_p)) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);
	DataChunk &AppendChunk() const {
		return *append_chunk;
	}
	const std::string &ConnectionId() const {
		return connection_id;
	}
	const std::string &SchemaName() const {
		return schema_name;
	}
	const std::string &TableName() const {
		return table_name;
	}

private:
	string connection_id;
	string schema_name;
	string table_name;
	unique_ptr<DataChunk> append_chunk;
};

class AppendResponseMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::APPEND_RESPONSE;

	explicit AppendResponseMessage() : ProtocolMessage(TYPE) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);
};

class ErrorMessage : public ProtocolMessage {
public:
	static constexpr MessageType TYPE = MessageType::ERROR;
	explicit ErrorMessage(const string &error_message_p) : ProtocolMessage(TYPE), error_message(error_message_p) {
	}
	const std::string &Error() const {
		return error_message;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ProtocolMessage> Deserialize(Deserializer &deserializer);

private:
	ErrorMessage() : ProtocolMessage(TYPE) {};
	string error_message;
};



} // namespace duckdb
