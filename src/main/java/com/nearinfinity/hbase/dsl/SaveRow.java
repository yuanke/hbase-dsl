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

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author Aaron McCurry
 * 
 * @param <QUERY_OP_TYPE>
 *            QueryOperator Type, allows users to extend QueryOperatorDelegate
 *            and add their own methods.
 * @param <ROW_ID_TYPE>
 *            Type of the Row id (String, Integer, Long, etc.).
 */
public class SaveRow<QUERY_OP_TYPE extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> {

	private HBase<QUERY_OP_TYPE, ROW_ID_TYPE> hBase;
	private byte[] tableName;

	SaveRow(HBase<QUERY_OP_TYPE, ROW_ID_TYPE> hBase, byte[] tableName) {
		this.hBase = hBase;
		this.tableName = tableName;
	}

	public SaveFamilyCol<QUERY_OP_TYPE, ROW_ID_TYPE> row(ROW_ID_TYPE id) {
		return new SaveFamilyCol<QUERY_OP_TYPE, ROW_ID_TYPE>(tableName, id, this, hBase);
	}

	public <U> SaveRow<QUERY_OP_TYPE, ROW_ID_TYPE> rows(Iterable<U> it, ForEach<U> process) {
		for (U u : it) {
			process.process(u);
		}
		return this;
	}

	public static class SaveFamilyCol<T extends QueryOps<I>, I> {

		private Put put;
		private byte[] currentFamily;
		private SaveRow<T, I> saveRow;
		private HBase<T, I> hBase;
		private byte[] tableName;

		SaveFamilyCol(byte[] tableName, I id, SaveRow<T, I> saveRow, HBase<T, I> hBase) {
			this.put = new Put(hBase.toBytes(id));
			this.saveRow = saveRow;
			this.hBase = hBase;
			this.tableName = tableName;
			this.hBase.savePut(tableName, put);
		}

		public SaveFamilyCol<T, I> family(String name) {
			return family(Bytes.toBytes(name));
		}
		
		public SaveFamilyCol<T, I> family(byte[] name) {
			currentFamily = name;
			return this;
		}
		
		public SaveFamilyCol<T, I> col(String qualifier, Object o) {
			return col(Bytes.toBytes(qualifier),o);
		}

		public SaveFamilyCol<T, I> col(byte[] qualifier, Object o) {
			return col(qualifier,o,(Long)null);
		}
		
		public SaveFamilyCol<T, I> col(String qualifier, Object o, Long timestamp) {
			return col(Bytes.toBytes(qualifier),o,timestamp);
		}
		
		public SaveFamilyCol<T, I> col(byte[] qualifier, Object o, Long timestamp) {
			if (currentFamily == null) {
				throw new RuntimeException("not implemented");
			}
			if (o == null) {
				return this;
			}
			if (timestamp == null) {
				put.add(currentFamily, qualifier, hBase.toBytes(o));
			} else {
				put.add(currentFamily, qualifier, timestamp, hBase.toBytes(o));
			}
			return this;
		}
		
		public SaveFamilyCol<T, I> col(String qualifier, Object o, Date timestamp) {
			return col(Bytes.toBytes(qualifier),o,timestamp);
		}
		
		public SaveFamilyCol<T, I> col(byte[] qualifier, Object o, Date timestamp) {
			if (timestamp == null) {
				return col(qualifier,o);
			}
			return col(qualifier,o,timestamp.getTime());
		}

		public SaveFamilyCol<T, I> row(I id) {
			return saveRow.row(id);
		}

		public SaveFamilyCol<T, I> flush() {
			hBase.flush(tableName);
			return this;
		}
	}
}
