#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

#include "quack_message.hpp"

namespace duckdb {

QuackMessage::QuackMessage(MessageType type) : header(type, string()) {
}
QuackMessage::QuackMessage(MessageType type, string connection_id_p) : header(type, std::move(connection_id_p)) {
}

string MessageTypeToString(MessageType type) {
	return EnumUtil::ToString(type);
}

template <>
MessageType EnumUtil::FromString<MessageType>(const char *value) {
	if (StringUtil::Equals(value, "INVALID")) {
		return MessageType::INVALID;
	}
	if (StringUtil::Equals(value, "CONNECTION_REQUEST")) {
		return MessageType::CONNECTION_REQUEST;
	}
	if (StringUtil::Equals(value, "CONNECTION_RESPONSE")) {
		return MessageType::CONNECTION_RESPONSE;
	}
	if (StringUtil::Equals(value, "PREPARE_REQUEST")) {
		return MessageType::PREPARE_REQUEST;
	}
	if (StringUtil::Equals(value, "PREPARE_RESPONSE")) {
		return MessageType::PREPARE_RESPONSE;
	}
	if (StringUtil::Equals(value, "CANCEL_REQUEST")) {
		return MessageType::CANCEL_REQUEST;
	}
	if (StringUtil::Equals(value, "CANCEL_RESPONSE")) {
		return MessageType::CANCEL_RESPONSE;
	}
	if (StringUtil::Equals(value, "FETCH_REQUEST")) {
		return MessageType::FETCH_REQUEST;
	}
	if (StringUtil::Equals(value, "FETCH_RESPONSE")) {
		return MessageType::FETCH_RESPONSE;
	}
	if (StringUtil::Equals(value, "APPEND_REQUEST")) {
		return MessageType::APPEND_REQUEST;
	}
	if (StringUtil::Equals(value, "APPEND_RESPONSE")) {
		return MessageType::APPEND_RESPONSE;
	}
	if (StringUtil::Equals(value, "ERROR_RESPONSE")) {
		return MessageType::ERROR_RESPONSE;
	}

	throw NotImplementedException(StringUtil::Format("Enum value of type MessageType: '%s' not implemented", value));
}

template <>
const char *EnumUtil::ToChars<MessageType>(MessageType value) {
	switch (value) {
	case MessageType::CONNECTION_REQUEST:
		return "CONNECTION_REQUEST";
	case MessageType::CONNECTION_RESPONSE:
		return "CONNECTION_RESPONSE";
	case MessageType::PREPARE_REQUEST:
		return "PREPARE_REQUEST";
	case MessageType::PREPARE_RESPONSE:
		return "PREPARE_RESPONSE";
	case MessageType::CANCEL_REQUEST:
		return "CANCEL_REQUEST";
	case MessageType::CANCEL_RESPONSE:
		return "CANCEL_RESPONSE";
	case MessageType::FETCH_REQUEST:
		return "FETCH_REQUEST";
	case MessageType::FETCH_RESPONSE:
		return "FETCH_RESPONSE";
	case MessageType::APPEND_REQUEST:
		return "APPEND_REQUEST";
	case MessageType::APPEND_RESPONSE:
		return "APPEND_RESPONSE";
	case MessageType::ERROR_RESPONSE:
		return "ERROR_RESPONSE";

	default:
		throw NotImplementedException(
		    StringUtil::Format("Enum value of type MessageType: '%d' not implemented", value));
	}
}

void QuackMessage::ToMemoryStream(MemoryStream &write_stream) const {
	write_stream.Rewind();
	SerializationOptions options;
	options.serialization_compatibility = SerializationCompatibility::FromIndex(10);
	BinarySerializer serializer(write_stream, options);

	// write the header
	serializer.Begin();
	header.Serialize(serializer);
	serializer.End();
	// write the body
	serializer.Begin();
	Serialize(serializer);
	serializer.End();
}

unique_ptr<QuackMessage> QuackMessage::Deserialize(Deserializer &deserializer, MessageType message_type) {
	switch (message_type) {
	case MessageType::CONNECTION_REQUEST:
		return ConnectionRequestMessage::Deserialize(deserializer);
	case MessageType::CONNECTION_RESPONSE:
		return ConnectionResponseMessage::Deserialize(deserializer);
	case MessageType::PREPARE_REQUEST:
		return PrepareRequestMessage::Deserialize(deserializer);
	case MessageType::PREPARE_RESPONSE:
		return PrepareResponseMessage::Deserialize(deserializer);
	case MessageType::CANCEL_REQUEST:
		return CancelRequestMessage::Deserialize(deserializer);
	case MessageType::CANCEL_RESPONSE:
		return CancelResponseMessage::Deserialize(deserializer);
	case MessageType::FETCH_REQUEST:
		return FetchRequestMessage::Deserialize(deserializer);
	case MessageType::FETCH_RESPONSE:
		return FetchResponseMessage::Deserialize(deserializer);
	case MessageType::APPEND_REQUEST:
		return AppendRequestMessage::Deserialize(deserializer);
	case MessageType::APPEND_RESPONSE:
		return AppendResponseMessage::Deserialize(deserializer);
	case MessageType::ERROR_RESPONSE:
		return ErrorMessage::Deserialize(deserializer);
	default:
		throw InternalException("Unsupported type for message deserialization");
	}
}

MessageHeader QuackMessage::DeserializeHeader(BinaryDeserializer &deserializer) {
	deserializer.Begin();
	auto header = MessageHeader::Deserialize(deserializer);
	deserializer.End();
	return header;
}

unique_ptr<QuackMessage> QuackMessage::DeserializeMessage(BinaryDeserializer &deserializer, MessageHeader header) {
	// read the body
	deserializer.Begin();
	auto result = Deserialize(deserializer, header.type);
	result->SetHeader(std::move(header));
	deserializer.End();
	return result;
}

unique_ptr<QuackMessage> QuackMessage::FromMemoryStream(MemoryStream &read_stream) {
	read_stream.Rewind();
	BinaryDeserializer deserializer(read_stream);

	// read the header
	auto header = DeserializeHeader(deserializer);
	// read the message
	return DeserializeMessage(deserializer, std::move(header));
}

void DataChunkWrapper::Serialize(Serializer &serializer) const {
	serializer.WriteObject(300, "chunk", [&](Serializer &inner) { chunk.Serialize(inner); });
}

unique_ptr<DataChunkWrapper> DataChunkWrapper::Deserialize(Deserializer &deserializer) {
	DataChunk chunk;
	deserializer.ReadObject(300, "chunk", [&](Deserializer &inner) { chunk.Deserialize(inner); });
	return make_uniq<DataChunkWrapper>(chunk);
}
} // namespace duckdb
