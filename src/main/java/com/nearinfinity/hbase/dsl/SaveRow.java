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
 * @param <T>
 *            QueryOperator Type, allows users to extend QueryOperatorDelegate
 *            and add their own methods.
 * @param <I>
 *            Type of the Row id (String, Integer, Long, etc.).
 */
public class SaveRow<T extends QueryOps<I>, I> {

	private HBase<T, I> hBase;
	private byte[] tableName;

	SaveRow(HBase<T, I> hBase, String tableName) {
		this.hBase = hBase;
		this.tableName = Bytes.toBytes(tableName);
	}

	public SaveFamilyCol<T, I> row(I id) {
		return new SaveFamilyCol<T, I>(tableName, id, this, hBase);
	}

	public <U> SaveRow<T, I> rows(Iterable<U> it, ForEach<U> process) {
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
			currentFamily = Bytes.toBytes(name);
			return this;
		}

		public SaveFamilyCol<T, I> col(String qualifier, Object o) {
			if (o == null) {
				return this;
			}
			if (currentFamily == null) {
				throw new RuntimeException("not implemented");
			} else {
				put.add(currentFamily, Bytes.toBytes(qualifier), hBase.toBytes(o));
			}
			return this;
		}
		
		public SaveFamilyCol<T, I> col(String qualifier, Object o, Long timestamp) {
			if (o == null) {
				return this;
			}
			if (timestamp == null) {
				return col(qualifier,o);
			}
			if (currentFamily == null) {
				throw new RuntimeException("not implemented");
			} else {
				put.add(currentFamily, Bytes.toBytes(qualifier), timestamp, hBase.toBytes(o));
			}
			return this;
		}
		
		public SaveFamilyCol<T, I> col(String qualifier, Object o, Date timestamp) {
			if (o == null) {
				return this;
			}
			if (timestamp == null) {
				return col(qualifier,o);
			}
			if (currentFamily == null) {
				throw new RuntimeException("not implemented");
			} else {
				put.add(currentFamily, Bytes.toBytes(qualifier), timestamp.getTime(), hBase.toBytes(o));
			}
			return this;
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
