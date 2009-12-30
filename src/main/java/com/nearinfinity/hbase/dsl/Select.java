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
 * @param <T>
 * @param <I>
 */
public class Select<T extends QueryOps<I>, I> implements Iterable<Row<I>> {

	private byte[] family;
	private Scanner<T, I> scanner;

	Select(Scanner<T, I> scanner) {
		this.scanner = scanner;
	}

	public Where<T, I> where() {
		return new Where<T, I>(scanner);
	}

	public void foreach(ForEach<Row<I>> forEach) {
		scanner.foreach(forEach);
	}

	public Select<T, I> family(String name) {
		family = Bytes.toBytes(name);
		scanner.addFamily(family);
		return this;
	}

	public Select<T, I> col(String name) {
		scanner.addColumn(family, Bytes.toBytes(name));
		return this;
	}

	@Override
	public Iterator<Row<I>> iterator() {
		return scanner.iterator();
	}

	public Select<T, I> timestamp(long timestamp) {
		scanner.setTimestamp(timestamp);
		return this;
	}

	public Select<T, I> timerange(long minTimestamp, long maxTimestamp) {
		scanner.setTimeRange(minTimestamp, maxTimestamp);
		return this;
	}

	public Select<T, I> timestamp(Date timestamp) {
		scanner.setTimestamp(timestamp.getTime());
		return this;
	}

	public Select<T, I> timerange(Date minTimestamp, Date maxTimestamp) {
		scanner.setTimeRange(minTimestamp.getTime(), maxTimestamp.getTime());
		return this;
	}

	public Select<T, I> allVersions() {
		scanner.allVersions();
		return this;
	}

}
