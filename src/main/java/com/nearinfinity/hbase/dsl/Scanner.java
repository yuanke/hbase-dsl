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
import java.util.Iterator;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * 
 * @author Aaron McCurry
 * 
 * @param <T>
 *            QueryOperator Type, allows users to extend QueryOperatorDelegate
 *            and add their own methods.
 * @param <I>
 *            Type of the Row id (String, Integer, Long, etc.).
 */
public class Scanner<T extends QueryOps<I>, I> implements Iterable<Row<I>> {

	private HTable hTable;
	private Scan scan;
	private HBase<T, I> hBase;

	Scanner(HBase<T, I> hBase, HTable hTable, I startId, I endId) {
		this.hTable = hTable;
		this.hBase = hBase;
		if (startId != null && endId != null) {
			this.scan = new Scan(hBase.toBytes(startId), hBase.toBytes(endId));
		} else if (startId != null) {
			this.scan = new Scan(hBase.toBytes(startId));
		} else {
			this.scan = new Scan();
		}
	}

	@Override
	public Iterator<Row<I>> iterator() {
		try {
			ResultScanner scanner = hTable.getScanner(scan);
			final Iterator<Result> iterator = scanner.iterator();
			return new Iterator<Row<I>>() {
				@Override
				public boolean hasNext() {
					return iterator.hasNext();
				}

				@Override
				public Row<I> next() {
					return hBase.convert(iterator.next());
				}

				@Override
				public void remove() {
					throw new RuntimeException("read only");
				}
			};
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * The foreach method allows for iterative processing of each row found in
	 * this where clause.
	 * 
	 * @param forEach
	 *            the {@link ForEach} object provided to perform the processing.
	 */
	public void foreach(ForEach<Row<I>> forEach) {
		for (Row<I> row : this) {
			forEach.process(row);
		}
	}

	public Where<T, I> where() {
		return new Where<T, I>(this);
	}

	protected void setFilter(Filter filter) {
		scan.setFilter(filter);
	}

	protected byte[] toBytes(Object o) {
		return hBase.toBytes(o);
	}

	protected QueryOps<I> createWhereClause(Where<? extends QueryOps<I>, I> whereScanner, byte[] family, byte[] value) {
		return hBase.createWhereClause(whereScanner, family, value);
	}

	public Select<T, I> select() {
		return new Select<T, I>(this);
	}

	protected void addFamily(byte[] family) {
		scan.addFamily(family);
	}

	protected void addColumn(byte[] family, byte[] qualifier) {
		scan.addColumn(family, qualifier);
	}

	protected void setTimestamp(long timestamp) {
		scan.setTimeStamp(timestamp);
	}

	protected void setTimeRange(long minTimestamp, long maxTimestamp) {
		try {
			scan.setTimeRange(minTimestamp, maxTimestamp);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void allVersions() {
		scan.setMaxVersions();
	}

}
