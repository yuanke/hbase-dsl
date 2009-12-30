package com.nearinfinity.hbase.dsl;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.nearinfinity.hbase.dsl.types.TypeConverter;

/**
 * The HBase object is the main object for the hbase dsl. It can handle multiple
 * tables, and is thread safe.
 * 
 * @author Aaron McCurry
 * 
 * @param <QUERY_OP_TYPE>
 *            QueryOperator Type, allows users to extend QueryOperatorDelegate
 *            and add their own methods.
 * @param <ROW_ID_TYPE>
 *            Type of the Row id (String, Integer, Long, etc.).
 */
public class HBase<QUERY_OP_TYPE extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> {

	private static final Log LOG = LogFactory.getLog(HBase.class);
	private static final int MAX_QUEUE_SIZE = 1024;
	private Map<byte[], Queue<Put>> putsMap = new TreeMap<byte[], Queue<Put>>(Bytes.BYTES_COMPARATOR);
	private Map<byte[], Queue<Delete>> deletesMap = new TreeMap<byte[], Queue<Delete>>(Bytes.BYTES_COMPARATOR);
	private HBaseConfiguration conf = new HBaseConfiguration();
	private HTablePool pool = new HTablePool(conf, 16);
	private Class<?> whereClauseType;
	private TypeDriver typeDriver = new TypeDriver().registerAllKnownTypes();
	private Class<ROW_ID_TYPE> idType;

	public HBase(Class<? extends QueryOps<ROW_ID_TYPE>> whereClauseType, Class<ROW_ID_TYPE> idType) {
		this.whereClauseType = whereClauseType;
		this.idType = idType;
		setupAutoFlushOnShutdown();
	}

	public static <U extends QueryOps<Y>, Y> HBase<U, Y> newHBase(Class<U> queryOpsClass, Class<Y> rowIdTypeClass) {
		return new HBase<U, Y>(queryOpsClass, rowIdTypeClass);
	}

	public static <U extends QueryOps<String>> HBase<U, String> newHBaseWithStringIdType(Class<U> queryOpsClass) {
		return newHBase(queryOpsClass, String.class);
	}

	@SuppressWarnings("unchecked")
	public static HBase<QueryOps<String>, String> newHBaseWithStringIdType() {
		return newHBase(QueryOps.class, String.class);
	}

	/**
	 * Creates a {@link HBase} object with basic {@link QueryOps} class.
	 * 
	 * @param <Y>
	 *            the type of the row id.
	 * @param t
	 *            the class type of the row id.
	 * @return the {@link HBase} instance.
	 */
	@SuppressWarnings("unchecked")
	public static <Y> HBase<QueryOps<Y>, Y> newHBase(Class<Y> t) {
		return newHBase(QueryOps.class, t);
	}

	/**
	 * Creates a {@link HBase} object with basic {@link QueryOps} class and
	 * String class as the row id type.
	 * 
	 * @return the {@link HBase} instance.
	 */
	@SuppressWarnings("unchecked")
	public static HBase<QueryOps<String>, String> newHBase() {
		return newHBase(QueryOps.class, String.class);
	}

	/**
	 * Truncates the given table.
	 * 
	 * @param tableName
	 *            the table name.
	 */
	public void truncateTable(final String tableName) {
		scan(tableName).foreach(new ForEach<Row<ROW_ID_TYPE>>() {
			@Override
			public void process(Row<ROW_ID_TYPE> row) {
				delete(tableName).row(row.getId());
			}
		});
		flush(Bytes.toBytes(tableName));
	}

	public SaveRow<QUERY_OP_TYPE, ROW_ID_TYPE> save(String tableName) {
		LOG.debug("save [" + tableName + "]");
		return new SaveRow<QUERY_OP_TYPE, ROW_ID_TYPE>(this, tableName);
	}

	public FetchRow<ROW_ID_TYPE> fetch(String tableName) {
		flush();
		LOG.debug("fetch [" + tableName + "]");
		return new FetchRow<ROW_ID_TYPE>(this, tableName);
	}

	public TableAdmin defineTable(String tableName) {
		flush();
		LOG.debug("defineTable [" + tableName + "]");
		return new TableAdmin(tableName);
	}

	/**
	 * Creates a scanner for the given table.
	 * 
	 * @param tableName
	 *            the table name.
	 * @return the {@link Scanner}.
	 */
	public Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(String tableName) {
		return scan(tableName, null);
	}

	/**
	 * Creates a scanner for the given table with a starting id.
	 * 
	 * @param tableName
	 *            the table name.
	 * @param startId
	 *            the starting id.
	 * @return the {@link Scanner}.
	 */
	public Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(String tableName, ROW_ID_TYPE startId) {
		return scan(tableName, startId, null);
	}

	/**
	 * Creates a scanner for the given table with a starting id and an ending
	 * id.
	 * 
	 * @param tableName
	 *            the table name.
	 * @param startId
	 *            the starting id.
	 * @param endId
	 *            the ending id.
	 * @return the {@link Scanner}.
	 */
	public Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(String tableName, ROW_ID_TYPE startId, ROW_ID_TYPE endId) {
		flush();
		LOG.debug("scan [" + tableName + "] startId [" + startId + "] endId [" + endId + "]");
		try {
			HTable hTable = new HTable(conf, Bytes.toBytes(tableName));
			return new Scanner<QUERY_OP_TYPE, ROW_ID_TYPE>(this, hTable, startId, endId);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Removes the table.
	 * 
	 * @param tableName
	 *            the table name.
	 */
	public void removeTable(String tableName) {
		flush();
		LOG.debug("removeTable [" + tableName + "]");
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
			byte[] tableNameBytes = Bytes.toBytes(tableName);
			hBaseAdmin.disableTable(tableNameBytes);
			hBaseAdmin.deleteTable(tableNameBytes);
		} catch (TableNotFoundException e) {
			LOG.error("Table [" + tableName + "] does not exist.");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Performs a flush of all pending changes for all tables.
	 */
	public void flush() {
		LOG.debug("flush");
		for (byte[] tableName : putsMap.keySet()) {
			flush(tableName);
		}
	}

	/**
	 * Performs a flush of all pending changes for the given table.
	 * 
	 * @param tableName
	 *            the table name.
	 */
	public void flush(String tableName) {
		flush(Bytes.toBytes(tableName));
	}

	/**
	 * Creates a {@link DeletedRow} object for the given table.
	 * 
	 * @param tableName
	 *            the table name.
	 * @return the {@link DeletedRow}.
	 */
	public DeletedRow<QUERY_OP_TYPE, ROW_ID_TYPE> delete(String tableName) {
		LOG.debug("delete [" + tableName + "]");
		return new DeletedRow<QUERY_OP_TYPE, ROW_ID_TYPE>(this, Bytes.toBytes(tableName));
	}

	/**
	 * Registers a new type converter.
	 * 
	 * @param <U>
	 *            the type (class) for the converter.
	 * @param typeConverter
	 *            the type converter.
	 */
	public <U> void registerTypeConverter(TypeConverter<U> typeConverter) {
		typeDriver.registerType(typeConverter);
	}

	public Table<QUERY_OP_TYPE, ROW_ID_TYPE> table(final String tableName) {
		return new Table<QUERY_OP_TYPE,ROW_ID_TYPE>() {

			@Override
			public DeletedRow<QUERY_OP_TYPE, ROW_ID_TYPE> delete() {
				return HBase.this.delete(tableName);
			}

			@Override
			public FetchRow<ROW_ID_TYPE> fetch() {
				return HBase.this.fetch(tableName);
			}

			@Override
			public SaveRow<QUERY_OP_TYPE, ROW_ID_TYPE> save() {
				return HBase.this.save(tableName);
			}

			@Override
			public Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan() {
				return HBase.this.scan(tableName);
			}

			@Override
			public Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(ROW_ID_TYPE startId) {
				return HBase.this.scan(tableName, startId);
			}

			@Override
			public Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(ROW_ID_TYPE startId, ROW_ID_TYPE endId) {
				return HBase.this.scan(tableName, startId, endId);
			}
		};
	}

	protected void flush(byte[] tableName) {
		LOG.debug("flush [" + tableName + "]");
		Queue<Put> puts = getPuts(tableName);
		if (puts != null) {
			flushPuts(tableName, puts);
		}
		Queue<Delete> deletes = getDeletes(tableName);
		if (deletes != null) {
			flushDeletes(tableName, deletes);
		}
	}

	protected Row<ROW_ID_TYPE> convert(Result result) {
		return new ResultRow<ROW_ID_TYPE>(this, result);
	}

	protected void savePut(byte[] tableName, Put put) {
		Queue<Put> puts = getPuts(tableName);
		if (puts.size() >= MAX_QUEUE_SIZE) {
			flushPuts(tableName, puts);
		}
		puts.add(put);
	}

	protected void saveDelete(byte[] tableName, Delete delete) {
		Queue<Delete> deletes = getDeletes(tableName);
		if (deletes.size() >= MAX_QUEUE_SIZE) {
			flushDeletes(tableName, deletes);
		}
		deletes.add(delete);
	}

	protected byte[] toBytes(Object o) {
		if (o == null) {
			return null;
		}
		return typeDriver.toBytes(o);
	}

	protected <U> U fromBytes(byte[] value, Class<U> c) {
		if (value == null) {
			return null;
		}
		return typeDriver.fromBytes(value, c);
	}

	@SuppressWarnings("unchecked")
	protected QueryOps<ROW_ID_TYPE> createWhereClause(Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> whereScanner, byte[] family, byte[] value) {
		try {
			Constructor<?> constructor = this.whereClauseType.getConstructor(new Class[] { Where.class, byte[].class,
					byte[].class });
			return (QueryOps<ROW_ID_TYPE>) constructor.newInstance(whereScanner, family, value);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected Result getResult(byte[] tableName, Get get) {
		HTable table = pool.getTable(tableName);
		try {
			return table.get(get);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			pool.putTable(table);
		}
	}

	protected Class<ROW_ID_TYPE> getIdType() {
		return (Class<ROW_ID_TYPE>) idType;
	}

	private Queue<Delete> getDeletes(byte[] tableName) {
		Queue<Delete> queue = deletesMap.get(tableName);
		if (queue == null) {
			queue = new ConcurrentLinkedQueue<Delete>();
			deletesMap.put(tableName, queue);
		}
		return queue;
	}

	private void flushDeletes(byte[] tableName, Queue<Delete> deletes) {
		HTable table = pool.getTable(tableName);
		try {
			table.delete(new ArrayList<Delete>(deletes));
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			pool.putTable(table);
		}
		deletes.clear();
	}

	private void flushPuts(byte[] tableName, Queue<Put> puts) {
		HTable table = pool.getTable(tableName);
		try {
			table.put(new ArrayList<Put>(puts));
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			pool.putTable(table);
		}
		puts.clear();
	}

	private Queue<Put> getPuts(byte[] tableName) {
		Queue<Put> queue = putsMap.get(tableName);
		if (queue == null) {
			queue = new ConcurrentLinkedQueue<Put>();
			putsMap.put(tableName, queue);
		}
		return queue;
	}

	private void setupAutoFlushOnShutdown() {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				flush();
			}
		}));
	}
}
