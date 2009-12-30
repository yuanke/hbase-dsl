package com.nearinfinity.hbase.dsl;

public interface Table<T extends QueryOps<I>, I> {

	SaveRow<T, I> save();

	FetchRow<I> fetch();

	Scanner<T, I> scan();

	Scanner<T, I> scan(I startId);

	Scanner<T, I> scan(I startId, I endId);

	DeletedRow<T, I> delete();
}
