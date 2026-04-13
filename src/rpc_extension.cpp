#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"

#include "remote_extension.hpp"
#include "rpc_log_type.hpp"
#include "rpc_scan_function.hpp"
#include "rpc_start_function.hpp"
#include "rpc_storage_extension.hpp"
#include "rpc_uri.hpp"

#include "duckdb/common/types/blob.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "catalog.hpp"

namespace duckdb {

static unique_ptr<Catalog> RpcAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                     AttachedDatabase &db, const string &name, AttachInfo &info,
                                     AttachOptions &attach_options) {
	auto diable_ssl = attach_options.options.find("disable_ssl") != attach_options.options.end() &&
	                  attach_options.options["disable_ssl"].GetValue<bool>();
	return make_uniq<RpcCatalog>(db, RpcUri("remote:" + info.path, !diable_ssl), context);
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
	auto sz = args.size();
	// D_ASSERT(args.size() == 2);
	// D_ASSERT(args.GetTypes()[0].id() == LogicalTypeId::VARCHAR);
	// D_ASSERT(args.GetTypes()[1].id() == LogicalTypeId::VARCHAR);
	// D_ASSERT(result.GetType().id() == LogicalTypeId::BOOLEAN);

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
	// D_ASSERT(args.size() == 2);
	D_ASSERT(args.GetTypes()[0].id() == LogicalTypeId::VARCHAR); // session id
	D_ASSERT(args.GetTypes()[1].id() == LogicalTypeId::VARCHAR); // query
	D_ASSERT(result.GetType().id() == LogicalTypeId::BOOLEAN);

	result.SetValue(0, Value(true)); // choose life
}

 static void RpcVerifySignature(const DataChunk &args, ExpressionState &state, Vector &result) {
      D_ASSERT(args.GetTypes()[0].id() == LogicalTypeId::VARCHAR); // connection id
      D_ASSERT(args.GetTypes()[1].id() == LogicalTypeId::VARCHAR); // query
      D_ASSERT(args.GetTypes()[2].id() == LogicalTypeId::VARCHAR); // signature (base64)
      D_ASSERT(result.GetType().id() == LogicalTypeId::BOOLEAN);

      auto connection_id  = args.GetValue(0, 0).GetValue<string>();
      auto query          = args.GetValue(1, 0).GetValue<string>();
      auto signature_b64  = args.GetValue(2, 0).GetValue<string>();

      Value pubkey_val;
      auto &config = DBConfig::GetConfig(state.GetContext());

	// get the public key
      if (!config.TryGetCurrentSetting("rpc_package_pubkey", pubkey_val)
          || pubkey_val.IsNull()
          || pubkey_val.GetValue<string>().empty()) {
          result.SetValue(0, Value(false));
          return;
      }

      auto pem = pubkey_val.GetValue<string>();

      BIO *bio = BIO_new_mem_buf(pem.data(), (int)pem.size());
      EVP_PKEY *pkey = PEM_read_bio_PUBKEY(bio, nullptr, nullptr, nullptr);
      BIO_free(bio);
      if (!pkey) {
          result.SetValue(0, Value(false));
          return;
      }

      auto signature = Blob::FromBase64(signature_b64);
      string payload  = connection_id + "\n" + query;

      EVP_MD_CTX *ctx = EVP_MD_CTX_new();
      bool ok = EVP_DigestVerifyInit(ctx, nullptr, EVP_sha256(), nullptr, pkey) == 1 &&
                EVP_DigestVerifyUpdate(ctx, payload.data(), payload.size()) == 1 &&
                EVP_DigestVerifyFinal(ctx, (const unsigned char *)signature.data(), signature.size()) == 1;
      EVP_MD_CTX_free(ctx);
      EVP_PKEY_free(pkey);

      result.SetValue(0, Value(ok));
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

	ScalarFunction rpc_verify_signature("rpc_verify_signature",
	  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	  LogicalType::BOOLEAN, RpcVerifySignature);

	loader.RegisterFunction(rpc_verify_signature);

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

	// config.AddExtensionOption("rpc_authorization_function", "Name of a callback function for authorization",
	// 					  LogicalType::VARCHAR, Value("rpc_verify_signature"));

	// TODO make this readonly from SQL?
	config.AddExtensionOption("rpc_default_token", "Authorization token used by default", LogicalType::VARCHAR, Value(),
	                          nullptr, SetScope::GLOBAL);

	config.AddExtensionOption("rpc_signed_plan_pubkey",
						  "PEM public key for verifying signed packages",
						  LogicalType::VARCHAR, Value(""));
}

void RemoteExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string RemoteExtension::Name() {
	return "remote";
}

std::string RemoteExtension::Version() const {
#ifdef EXT_VERSION_RPC
	return EXT_VERSION_RPC;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(remote, loader) {
	LoadInternal(loader);
}
}
