#include "rpc_storage_extension.hpp"
#include "duckdb/main/database.hpp"
#include "rpc_server.hpp"

using namespace duckdb;

RpcStorageExtensionInfo &RpcStorageExtensionInfo::GetState(const DatabaseInstance &instance) {
	auto &config = instance.config;
	auto ext = StorageExtension::Find(config, STORAGE_EXTENSION_KEY);
	if (!ext) {
		throw std::runtime_error("Fatal error: couldn't find rpc extension state.");
	}
	return *static_cast<RpcStorageExtensionInfo *>(ext->storage_info.get());
}

RpcServer &RpcStorageExtensionInfo::FindOrCreateServer(ClientContext &context, const RpcUri &listen_uri) {
	std::lock_guard<std::mutex> lock(servers_mutex);
	auto it = servers.find(listen_uri.Uri());
	if (it != servers.end()) {
		return *it->second;
	}
	unique_ptr<RpcServer> server;
	server = make_uniq<HttpsRpcServer>(context);
	server->Listen(listen_uri);
	servers.emplace(listen_uri.Uri(), std::move(server));
	return *servers[listen_uri.Uri()];
}

bool RpcStorageExtensionInfo::StopServer(ClientContext &context, const RpcUri &listen_uri) {
	std::lock_guard<std::mutex> lock(servers_mutex);
	const auto it = servers.find(listen_uri.Uri());
	if (it == servers.end()) {
		return false;
	}
	servers.erase(it);
	return true;
}
