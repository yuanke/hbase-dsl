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

import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author Aaron McCurry
 * 
 * @param <QUERY_OP_TYPE>
 * @param <ROW_ID_TYPE>
 */
public class Select<QUERY_OP_TYPE extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> implements Iterable<Row<ROW_ID_TYPE>> {

	private byte[] family;
	private Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scanner;

	Select(Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scanner) {
		this.scanner = scanner;
	}

	public Where<QUERY_OP_TYPE, ROW_ID_TYPE> where() {
		return new Where<QUERY_OP_TYPE, ROW_ID_TYPE>(scanner);
	}

	public void foreach(ForEach<Row<ROW_ID_TYPE>> forEach) {
		scanner.foreach(forEach);
	}

	public Select<QUERY_OP_TYPE, ROW_ID_TYPE> family(String name) {
		return family(Bytes.toBytes(name));
	}

	public Select<QUERY_OP_TYPE, ROW_ID_TYPE> col(String name) {
		return col(Bytes.toBytes(name));
	}
	
	public Select<QUERY_OP_TYPE, ROW_ID_TYPE> family(byte[] name) {
		family = name;
		scanner.addFamily(name);
		return this;
	}

	public Select<QUERY_OP_TYPE, ROW_ID_TYPE> col(byte[] name) {
		scanner.addColumn(family, name);
		return this;
	}

	@Override
	public Iterator<Row<ROW_ID_TYPE>> iterator() {
		return scanner.iterator();
	}

	public Select<QUERY_OP_TYPE, ROW_ID_TYPE> timestamp(long timestamp) {
		scanner.setTimestamp(timestamp);
		return this;
	}

	public Select<QUERY_OP_TYPE, ROW_ID_TYPE> timerange(long minTimestamp, long maxTimestamp) {
		scanner.setTimeRange(minTimestamp, maxTimestamp);
		return this;
	}

	public Select<QUERY_OP_TYPE, ROW_ID_TYPE> timestamp(Date timestamp) {
		scanner.setTimestamp(timestamp.getTime());
		return this;
	}

	public Select<QUERY_OP_TYPE, ROW_ID_TYPE> timerange(Date minTimestamp, Date maxTimestamp) {
		scanner.setTimeRange(minTimestamp.getTime(), maxTimestamp.getTime());
		return this;
	}

	public Select<QUERY_OP_TYPE, ROW_ID_TYPE> allVersions() {
		scanner.allVersions();
		return this;
	}

}
