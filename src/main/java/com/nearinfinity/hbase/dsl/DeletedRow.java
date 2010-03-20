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

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class is responsible for deletions in the system. You can delete entire
 * rows, or just families, or single columns.
 * 
 * @author Aaron McCurry
 * 
 * @param <QUERY_OP_TYPE>
 *            QueryOperator Type, allows users to extend QueryOperatorDelegate
 *            and add their own methods.
 * @param <ROW_ID_TYPE>
 *            Type of the Row id (String, Integer, Long, etc.).
 */
public class DeletedRow<QUERY_OP_TYPE extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> {

	private HBase<QUERY_OP_TYPE, ROW_ID_TYPE> hBase;
	private byte[] tableName;

	/**
	 * A generic constructor that sets the {@link HBase} object and table name.
	 * 
	 * @param hBase
	 *            the {@link HBase} object.
	 * @param tableName
	 *            the table name.
	 */
	DeletedRow(HBase<QUERY_OP_TYPE, ROW_ID_TYPE> hBase, byte[] tableName) {
		this.hBase = hBase;
		this.tableName = tableName;
	}

	/**
	 * Creates a {@link DeletedRowFamily} object and passes the id provided.
	 * 
	 * @param id
	 *            the id of the row, family(s), or column(s) that you intend to
	 *            delete.
	 * @return the {@link DeletedRowFamily}.
	 */
	public DeletedRowFamily<QUERY_OP_TYPE, ROW_ID_TYPE> row(ROW_ID_TYPE id) {
		return new DeletedRowFamily<QUERY_OP_TYPE, ROW_ID_TYPE>(tableName, id, this, hBase);
	}

	/**
	 * Processes each of the id(s).
	 * 
	 * @param it
	 *            the {@link Iterable} of the id(s).
	 * @return this object.
	 */
	public DeletedRow<QUERY_OP_TYPE, ROW_ID_TYPE> rows(Iterable<ROW_ID_TYPE> it) {
		return rows(it, null);
	}

	/**
	 * Processes each of the id(s), with a {@link ForEach} object to allow for
	 * deletion of families and columns.
	 * 
	 * @param it
	 *            the {@link Iterable} of the id(s).
	 * @param forEach
	 *            the {@link ForEach} object that allow for deletion of families
	 *            and columns.
	 * @return this object.
	 */
	public DeletedRow<QUERY_OP_TYPE, ROW_ID_TYPE> rows(Iterable<ROW_ID_TYPE> it, ForEach<DeletedRowFamily<QUERY_OP_TYPE, ROW_ID_TYPE>> forEach) {
		for (ROW_ID_TYPE id : it) {
			DeletedRowFamily<QUERY_OP_TYPE, ROW_ID_TYPE> deleteRowFamily = row(id);
			if (forEach != null) {
				forEach.process(deleteRowFamily);
			}
		}
		return this;
	}

	public static class DeletedRowFamily<T extends QueryOps<I>, I> {

		private HBase<T, I> hBase;
		private byte[] tableName;
		private Delete delete;
		private DeletedRow<T, I> deleteRow;

		DeletedRowFamily(byte[] tableName, I id, DeletedRow<T, I> deleteRow, HBase<T, I> hBase) {
			this.delete = new Delete(hBase.toBytes(id));
			this.deleteRow = deleteRow;
			this.hBase = hBase;
			this.tableName = tableName;
			this.hBase.saveDelete(this.tableName, delete);
		}

		public DeletedRowFamilyColumn<T, I> family(String name) {
			return family(Bytes.toBytes(name));
		}
		
		public DeletedRowFamilyColumn<T, I> family(byte[] name) {
			return new DeletedRowFamilyColumn<T, I>(hBase, tableName, name, delete, deleteRow);
		}

		public DeletedRowFamily<T, I> row(I id) {
			return deleteRow.row(id);
		}

		public DeletedRowFamily<T, I> flush() {
			hBase.flush(tableName);
			return this;
		}
	}

	public static class DeletedRowFamilyColumn<T extends QueryOps<I>, I> {

		private HBase<T, I> hBase;
		private byte[] tableName;
		private byte[] currentFamily;
		private Delete delete;
		private DeletedRow<T, I> deleteRow;

		DeletedRowFamilyColumn(HBase<T, I> hBase, byte[] tableName, byte[] currentFamily, Delete delete,
				DeletedRow<T, I> deleteRow) {
			this.hBase = hBase;
			this.tableName = tableName;
			this.currentFamily = currentFamily;
			this.delete = delete;
			this.deleteRow = deleteRow;
		}

		public DeletedRowFamilyColumn<T, I> family(String name) {
			return family(Bytes.toBytes(name));
		}
		
		public DeletedRowFamilyColumn<T, I> family(byte[] name) {
			return new DeletedRowFamilyColumn<T, I>(hBase, tableName, name, delete, deleteRow);
		}

		public DeletedRowFamilyColumn<T, I> col(String name) {
			return col(Bytes.toBytes(name));
		}
		
		public DeletedRowFamilyColumn<T, I> col(byte[] name) {
			if (currentFamily == null) {
				throw new RuntimeException("not implemented");
			} else {
				delete.deleteColumn(currentFamily, name);
			}
			return this;
		}

		public DeletedRowFamily<T, I> row(I id) {
			return deleteRow.row(id);
		}

		public DeletedRowFamilyColumn<T, I> flush() {
			hBase.flush(tableName);
			return this;
		}

	}
}
