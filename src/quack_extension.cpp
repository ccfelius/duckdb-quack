#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"

#include "quack_extension.hpp"
#include "rpc_log_type.hpp"
#include "rpc_scan_function.hpp"
#include "rpc_start_function.hpp"
#include "rpc_storage_extension.hpp"
#include "rpc_uri.hpp"

#include "duckdb/logging/log_manager.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "catalog.hpp"

namespace duckdb {

static unique_ptr<Catalog> RpcAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                     AttachedDatabase &db, const string &name, AttachInfo &info,
                                     AttachOptions &attach_options) {
	auto diable_ssl = attach_options.options.find("disable_ssl") != attach_options.options.end() &&
	                  attach_options.options["disable_ssl"].GetValue<bool>();
	return make_uniq<RpcCatalog>(db, RpcUri("quack:" + info.path, !diable_ssl), context);
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

// pass session id
static void RpcAuthToken(const DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.size() == 2);
	D_ASSERT(args.GetTypes()[0].id() == LogicalTypeId::VARCHAR);
	D_ASSERT(args.GetTypes()[1].id() == LogicalTypeId::VARCHAR);
	D_ASSERT(result.GetType().id() == LogicalTypeId::BOOLEAN);

	auto auth_str = args.GetValue(1, 0).GetValue<string>();

	Value default_token_val;
	auto &config = DBConfig::GetConfig(state.GetContext());
	auto lookup_result = config.TryGetCurrentSetting("rpc_default_token", default_token_val);
	D_ASSERT(lookup_result);
	D_ASSERT(!default_token_val.IsNull());
	D_ASSERT(default_token_val.type().id() == LogicalTypeId::VARCHAR);
	auto default_token = default_token_val.GetValue<string>();

	result.SetValue(0, Value(auth_str == default_token));
}

static void RpcDummyAuthorization(const DataChunk &args, ExpressionState &, Vector &result) {
	D_ASSERT(args.size() == 2);
	D_ASSERT(args.GetTypes()[0].id() == LogicalTypeId::VARCHAR); // session id
	D_ASSERT(args.GetTypes()[1].id() == LogicalTypeId::VARCHAR); // query
	D_ASSERT(result.GetType().id() == LogicalTypeId::BOOLEAN);

	result.SetValue(0, Value(true)); // choose life
}

static void RpcUriParser(const DataChunk &args, ExpressionState &, Vector &result) {
	D_ASSERT(args.size() == 2);
	D_ASSERT(args.GetTypes()[0].id() == LogicalTypeId::VARCHAR);
	D_ASSERT(args.GetTypes()[1].id() == LogicalTypeId::BOOLEAN);
	D_ASSERT(result.GetType().id() == LogicalTypeId::STRUCT);

	RpcUri parsed(args.GetValue(0, 0).GetValue<string>(), args.GetValue(1, 0).GetValue<bool>());

	result.SetValue(0, Value::STRUCT({{"host", Value(parsed.Host())},
	                                  {"port", Value::USMALLINT(parsed.Port())},
	                                  {"ipv6", Value::BOOLEAN(parsed.IPv6())},
	                                  {"ssl", Value::BOOLEAN(parsed.Ssl())},
	                                  {"url", Value(parsed.Http())}}));
}

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("Adds support for DuckDB Remote Procedure Calls (RPC)");

	loader.RegisterFunction(RpcScanFunction::GetFunction());
	loader.RegisterFunction(RpcScanByNameFunction::GetFunction());

	loader.RegisterFunction(RpcStartFunction::GetFunction());
	loader.RegisterFunction(RpcStopFunction::GetFunction());
	loader.RegisterFunction(RpcGenerateKeysFunction::GetFunction());

	// the default authentication function
	ScalarFunction rpc_auth_token("rpc_auth_token",
	                              {/* session id */ LogicalType::VARCHAR, /* auth string */ LogicalType::VARCHAR},
	                              LogicalType::BOOLEAN, RpcAuthToken);
	loader.RegisterFunction(rpc_auth_token);

	ScalarFunction rpc_authorization("rpc_dummy_authorization",
	                                 {/* session id */ LogicalType::VARCHAR, /* query string */ LogicalType::VARCHAR},
	                                 LogicalType::BOOLEAN, RpcDummyAuthorization);
	loader.RegisterFunction(rpc_authorization);

	ScalarFunction rpc_uri_parser("rpc_uri_parser", {/* uri */ LogicalType::VARCHAR, /* ssl */ LogicalType::BOOLEAN},
	                              LogicalType::STRUCT({{"host", LogicalType::VARCHAR},
	                                                   {"port", LogicalType::USMALLINT},
	                                                   {"ipv6", LogicalType::BOOLEAN},
	                                                   {"ssl", LogicalType::BOOLEAN},
	                                                   {"url", LogicalType::VARCHAR}}),
	                              RpcUriParser);
	loader.RegisterFunction(rpc_uri_parser);

	loader.GetDatabaseInstance().GetLogManager().RegisterLogType(make_uniq<RPCLogType>());

	// (ab)use storage extension info to store our state
	auto ext = duckdb::make_shared_ptr<RpcStorageExtension>();
	ext->storage_info = duckdb::make_uniq<RpcStorageExtensionInfo>();
	StorageExtension::Register(loader.GetDatabaseInstance().config, RpcStorageExtensionInfo::STORAGE_EXTENSION_KEY,
	                           ext);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("rpc_authentication_function", "Name of a callback function for authentication",
	                          LogicalType::VARCHAR, Value("rpc_auth_token"));
	config.AddExtensionOption("rpc_authorization_function", "Name of a callback function for authorization",
	                          LogicalType::VARCHAR, Value("rpc_dummy_authorization"));

	// TODO make this readonly from SQL?
	config.AddExtensionOption("rpc_default_token", "Authorization token used by default", LogicalType::VARCHAR, Value(),
	                          nullptr, SetScope::GLOBAL);

	config.AddExtensionOption("quack_fetch_batch_chunks", "Maximum number of DataChunks returned per FETCH response",
	                          LogicalType::UBIGINT, Value::UBIGINT(12));
	config.AddExtensionOption("quack_fetch_batch_bytes", "Maximum estimated payload bytes per FETCH response",
	                          LogicalType::UBIGINT, Value::UBIGINT(4 * (1 << 20)));
}

void QuackExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string QuackExtension::Name() {
	return "quack";
}

std::string QuackExtension::Version() const {
#ifdef EXT_VERSION_RPC
	return EXT_VERSION_RPC;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(quack, loader) {
	LoadInternal(loader);
}
}
