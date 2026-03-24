#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"

#include "rpc_extension.hpp"
#include "rpc_scan_function.hpp"
#include "rpc_start_function.hpp"
#include "rpc_storage_extension.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"

#include "catalog.hpp"

namespace duckdb {

static unique_ptr<Catalog> RpcAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                     AttachedDatabase &db, const string &name, AttachInfo &info,
                                     AttachOptions &attach_options) {
	return make_uniq<RpcCatalog>(db, info.path);
}

static unique_ptr<TransactionManager> RpcCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                  AttachedDatabase &db, Catalog &catalog) {
	auto &rpc_catalog = catalog.Cast<RpcCatalog>();
	return make_uniq<RpcTransactionManager>(db, rpc_catalog);
}

class RpcStorageExtension : public StorageExtension {
public:
	RpcStorageExtension() {
		attach = RpcAttach;
		create_transaction_manager = RpcCreateTransactionManager;
	}
};

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("Adds support for DuckDB Remote Procedure Calls (RPC)");

	loader.RegisterFunction(RpcScanFunction::GetFunction());
	loader.RegisterFunction(RpcOrchestrateFunction::GetFunction());
	loader.RegisterFunction(RpcScanByNameFunction::GetFunction());

	loader.RegisterFunction(RpcStartFunction::GetFunction());
	loader.RegisterFunction(RpcStopFunction::GetFunction());
	loader.RegisterFunction(RpcGenerateKeysFunction::GetFunction());

	// (ab)use storage extension info to store our state
	auto ext = duckdb::make_shared_ptr<RpcStorageExtension>();
	ext->storage_info = duckdb::make_uniq<RpcStorageExtensionInfo>();
	StorageExtension::Register(loader.GetDatabaseInstance().config, RpcStorageExtensionInfo::STORAGE_EXTENSION_KEY,
	                           ext);
}

void RpcExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string RpcExtension::Name() {
	return "rpc";
}

std::string RpcExtension::Version() const {
#ifdef EXT_VERSION_RPC
	return EXT_VERSION_RPC;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(rpc, loader) {
	LoadInternal(loader);
}
}
