#pragma once

#include "client.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"



namespace duckdb {

class RpcCatalog;

class RpcTransaction : public Transaction {
public:
	RpcTransaction(RpcCatalog &rpc_catalog_p, TransactionManager &manager_p, ClientContext &context_p);
	~RpcTransaction() override;
	// TODO
	void Start();
	void Commit();
	void Rollback();
	//
	// optional_ptr<CatalogEntry> GetCatalogEntry(const string &table_name);
	// void DropEntry(CatalogType type, const string &table_name, bool cascade);
	// void ClearTableEntry(const string &table_name);
	//
	static RpcTransaction &Get(ClientContext &context, Catalog &catalog);

private:
	RpcCatalog &rpc_catalog;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> catalog_entries;
};

class RpcTransactionManager : public TransactionManager {
public:
	RpcTransactionManager(AttachedDatabase &db_p, RpcCatalog &sqlite_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	RpcCatalog &rpc_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<RpcTransaction>> transactions;
};

struct RpcSchemaInfo : CreateSchemaInfo {
	string schema_name;
	string catalog_name;
};

class RpcTableCatalogEntry : public TableCatalogEntry {
public:
	RpcTableCatalogEntry(Catalog &catalog_p, SchemaCatalogEntry &schema_p, CreateTableInfo &info_p)
	    : TableCatalogEntry(catalog_p, schema_p, info_p) {
	}

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
};

class RpcSchemaCatalogEntry : public SchemaCatalogEntry {
public:
	RpcSchemaCatalogEntry(Catalog &catalog_p, CreateSchemaInfo &info_p) : SchemaCatalogEntry(catalog_p, info_p) {
	}

	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;

	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;

	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;

private:
	optional_ptr<CatalogEntry> TryLoadBuiltInFunction(const string &entry_name);
	optional_ptr<CatalogEntry> LoadBuiltInFunction(DefaultTableMacro macro);

private:
	mutex default_function_lock;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> default_function_map;
};

class RpcCatalog : public Catalog {
public:
	explicit RpcCatalog(AttachedDatabase &db_p, const string &server_string);
	~RpcCatalog();

public:
	string GetCatalogType() override {
		return "rpc";
	}
	void Initialize(bool load_builtin) override;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

	unique_ptr<ColumnDataCollection> ExecuteCommand(const string &query);
	const string &GetServerString();
	const string &GetConnectionId();
	const string &GetClientIdString() const;

	RpcClient &GetRawClient();

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;
	string server_string;
	unique_ptr<RpcClient> client;
	string connection_id;
	unordered_map<string, unique_ptr<RpcSchemaCatalogEntry>> schemas;

	string client_id;
};

} // namespace duckdb
