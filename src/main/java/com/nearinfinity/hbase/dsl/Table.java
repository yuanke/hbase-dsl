package com.nearinfinity.hbase.dsl;

public interface Table<QUERY_OP_TYPE extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> {

	SaveRow<QUERY_OP_TYPE, ROW_ID_TYPE> save();

	FetchRow<ROW_ID_TYPE> fetch();

	Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan();

	Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(ROW_ID_TYPE startId);

	Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(ROW_ID_TYPE startId, ROW_ID_TYPE endId);

	DeletedRow<QUERY_OP_TYPE, ROW_ID_TYPE> delete();
}
