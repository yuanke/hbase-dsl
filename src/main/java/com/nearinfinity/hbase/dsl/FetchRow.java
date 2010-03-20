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

import java.util.Map;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author Aaron McCurry
 * 
 * @param <ROW_ID_TYPE>
 */
public class FetchRow<ROW_ID_TYPE> {

	private static final Log LOG = LogFactory.getLog(FetchRow.class);
	private byte[] currentFamily;
	private Get get = new Get();
	private byte[] tableName;
	private HBase<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> hBase;
	private Result result;

	FetchRow(HBase<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> hBase, byte[] tableName) {
		this.hBase = hBase;
		this.tableName = tableName;
	}

	public Row<ROW_ID_TYPE> row(ROW_ID_TYPE id) {
		get = newGet(id);
		fetch();
		if (result.getRow() == null) {
			return null;
		}
		return new ResultRow<ROW_ID_TYPE>(hBase, result);
	}

	public FetchRow<ROW_ID_TYPE> select() {
		return this;
	}

	public FetchRow<ROW_ID_TYPE> family(String family) {
		return family(Bytes.toBytes(family));
	}
	
	public FetchRow<ROW_ID_TYPE> family(byte[] family) {
		currentFamily = family;
		get.addFamily(currentFamily);
		return this;
	}

	public FetchRow<ROW_ID_TYPE> col(String name) {
		return col(Bytes.toBytes(name));
	}
	
	public FetchRow<ROW_ID_TYPE> col(byte[] name) {
		get.addColumn(currentFamily, name);
		return this;
	}

	private void fetch() {
		if (result == null) {
			LOG.debug("Fetching row with id [" + Bytes.toString(get.getRow()) + "]");
			result = hBase.getResult(tableName, get);
		}
	}

	private Get newGet(ROW_ID_TYPE id) {
		Get newGet = new Get(hBase.toBytes(id));
		Map<byte[], NavigableSet<byte[]>> familyMap = get.getFamilyMap();
		for (byte[] family : familyMap.keySet()) {
			NavigableSet<byte[]> qualifiers = familyMap.get(family);
			if (qualifiers == null) {
				newGet.addFamily(family);
			} else {
				for (byte[] qualifier : qualifiers) {
					newGet.addColumn(family, qualifier);
				}
			}
		}
		return newGet;
	}

}
