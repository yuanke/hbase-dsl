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

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author Aaron McCurry
 * 
 * @param <I>
 */
public class ResultRow<I> implements Row<I> {

	private Result result;
	private byte[] id;
	private HBase<? extends QueryOps<I>, I> hBase;

	public ResultRow(HBase<? extends QueryOps<I>, I> hBase, Result result) {
		this.hBase = hBase;
		this.result = result;
		this.id = result.getRow();
	}

	public <U> U value(String family, String qualifier, Class<U> c) {
		byte[] value = null;
		if (family == null) {
			value = result.getValue(Bytes.toBytes(qualifier));
		}
		value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		if (value == null) {
			return null;
		}
		return hBase.fromBytes(value, c);
	}

	@Override
	public I getId() {
		return hBase.fromBytes(id, hBase.getIdType());
	}

	@Override
	public Family family(final String family) {
		return new Family() {
			@Override
			public <U> U value(String qualifier, Class<U> c) {
				return ResultRow.this.value(family, qualifier, c);
			}

			@Override
			public <U> NavigableMap<Long, U> values(String qualifier, Class<U> c) {
				return ResultRow.this.values(family, qualifier, c);
			}

			@Override
			public <U> List<U> valuesAscTimestamp(String qualifier, Class<U> c) {
				return ResultRow.this.valuesAscTimestamp(family, qualifier, c);
			}

			@Override
			public <U> List<U> valuesDescTimestamp(String qualifier, Class<U> c) {
				return ResultRow.this.valuesDescTimestamp(family, qualifier, c);
			}
		};
	}

	@Override
	public <U> NavigableMap<Long, U> values(String family, String qualifier, Class<U> c) {
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowMap = result.getMap();
		if (rowMap == null) {
			return new TreeMap<Long, U>();
		}
		NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = rowMap.get(Bytes.toBytes(family));
		if (familyMap == null) {
			return new TreeMap<Long, U>();
		}
		NavigableMap<Long, byte[]> navigableMap = familyMap.get(Bytes.toBytes(qualifier));
		return convert(navigableMap, c);
	}

	private <U> NavigableMap<Long, U> convert(NavigableMap<Long, byte[]> navigableMap, Class<U> c) {
		TreeMap<Long, U> map = new TreeMap<Long, U>();
		for (Long timestamp : navigableMap.keySet()) {
			map.put(timestamp, hBase.fromBytes(navigableMap.get(timestamp), c));
		}
		return map;
	}

	@Override
	public <U> List<U> valuesAscTimestamp(String family, String qualifier, Class<U> c) {
		NavigableMap<Long, U> values = values(family, qualifier, c);
		List<U> result = new ArrayList<U>();
		for (Long timestamp : values.keySet()) {
			result.add(values.get(timestamp));
		}
		return result;
	}

	@Override
	public <U> List<U> valuesDescTimestamp(String family, String qualifier, Class<U> c) {
		NavigableMap<Long, U> values = values(family, qualifier, c);
		List<U> result = new ArrayList<U>();
		for (Long timestamp : values.descendingKeySet()) {
			result.add(values.get(timestamp));
		}
		return result;
	}
}
