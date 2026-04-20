#pragma once

#include "duckdb/storage/storage_extension.hpp"
#include "rpc_server.hpp"
namespace duckdb {
class DatabaseInstance;

class RpcStorageExtensionInfo : public StorageExtensionInfo {
public:
	static RpcStorageExtensionInfo &GetState(const DatabaseInstance &instance);

	RpcServer &FindOrCreateServer(ClientContext &context, const RpcUri &listen_uri);
	optional_ptr<RpcServer> FindServer(const RpcUri &listen_uri);
	bool StopServer(ClientContext &context, const RpcUri &listen_uri);

	static constexpr const char *STORAGE_EXTENSION_KEY = "quack";

private:
	std::mutex servers_mutex;
	unordered_map<string, unique_ptr<RpcServer>> servers;
};
} // namespace duckdb
