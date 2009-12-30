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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;

import com.nearinfinity.hbase.dsl.Where.BOOL_EXP;

/**
 * 
 * @author Aaron McCurry
 */
class QueryContext {

	private static Log LOG = LogFactory.getLog(QueryContext.class);

	private class QueryNode {
		BOOL_EXP exp;
		QueryNode parentNode;
		QueryNode leftNode;
		QueryNode rightNode;
		Filter filter;

		public Filter getFilter() {
			if (filter != null) {
				return filter;
			}
			Operator operator;
			if (exp == BOOL_EXP.AND) {
				operator = Operator.MUST_PASS_ALL;
			} else {
				operator = Operator.MUST_PASS_ONE;
			}
			if (rightNode == null) {
				return leftNode.getFilter();
			}
			return new FilterList(operator, Arrays.asList(leftNode.getFilter(), rightNode.getFilter()));
		}

		public String toString() {
			if (filter != null) {
				return QueryContext.toString(filter);
			}
			return "(" + leftNode + " " + exp + " " + rightNode + ")";
		}
	}

	private QueryNode root = new QueryNode();
	private QueryNode currentNode = root;
	private int depth = 0;

	public void addFilter(Filter filter) {
		QueryNode node = new QueryNode();
		node.filter = filter;
		node.parentNode = currentNode;
		if (currentNode.exp == null) {
			currentNode.leftNode = node;
		} else {
			currentNode.rightNode = node;
		}
	}

	public void down() {
		depth++;
		if (currentNode.exp == null) {
			currentNode.leftNode = new QueryNode();
			currentNode.leftNode.parentNode = currentNode;
			currentNode = currentNode.leftNode;
		} else {
			currentNode.rightNode = new QueryNode();
			currentNode.rightNode.parentNode = currentNode;
			currentNode = currentNode.rightNode;
		}
	}

	public void up() {
		depth--;
		currentNode = currentNode.parentNode;
	}

	public void addBooleanExp(BOOL_EXP exp) {
		if (currentNode.exp != null) {
			moveCurrentNodeDown();
		}
		currentNode.exp = exp;
	}

	private void moveCurrentNodeDown() {
		QueryNode currentParentNode = currentNode.parentNode;

		QueryNode newNode = new QueryNode();
		newNode.parentNode = currentParentNode;
		newNode.leftNode = currentNode;

		if (currentNode == currentParentNode.leftNode) {
			currentParentNode.leftNode = newNode;
		} else if (currentNode == currentParentNode.rightNode) {
			currentParentNode.rightNode = newNode;
		} else {
			throw new RuntimeException("This ia a bug, please submit a bug report!");
		}
		currentNode = newNode;
	}

	@Override
	public String toString() {
		return root.toString();
	}

	public Filter getResultingFilter() {
		if (depth != 0) {
			throw new RuntimeException(
					"Query depth is incorrect, this is normally caused by parenthesis not being closed.");
		}
		LOG.info("Get Query [" + root + "]");
		return root.getFilter();
	}

	public static String toString(Object obj) {
		if (obj instanceof SingleColumnValueFilter) {
			return new ToStringSingleColumnValueFilter((SingleColumnValueFilter) obj).toString();
		} else if (obj instanceof BinaryComparator) {
			return new ToStringBinaryComparator((BinaryComparator) obj).toString();
		}
		return obj.toString();
	}

	private static class ToStringBinaryComparator {

		private static Map<String, Field> fields = new HashMap<String, Field>();

		static {
			Field[] declaredFields = BinaryComparator.class.getDeclaredFields();
			for (Field field : declaredFields) {
				field.setAccessible(true);
				fields.put(field.getName(), field);
			}
		}

		private String toString;

		public ToStringBinaryComparator(BinaryComparator obj) {
			try {
				byte[] value = (byte[]) fields.get("value").get(obj);
				toString = "[" + Bytes.toStringBinary(value) + "]";
			} catch (Exception e) {
				LOG.error("unknown", e);
			}
		}

		public String toString() {
			return toString;
		}

	}

	private static class ToStringSingleColumnValueFilter {

		private static Map<String, Field> fields = new HashMap<String, Field>();

		static {
			Field[] declaredFields = SingleColumnValueFilter.class.getDeclaredFields();
			for (Field field : declaredFields) {
				field.setAccessible(true);
				fields.put(field.getName(), field);
			}
		}

		private String toString;

		public ToStringSingleColumnValueFilter(SingleColumnValueFilter filter) {
			try {
				byte[] family = (byte[]) fields.get("columnFamily").get(filter);
				byte[] qualifier = (byte[]) fields.get("columnQualifier").get(filter);
				CompareOp compareOp = (CompareOp) fields.get("compareOp").get(filter);
				WritableByteArrayComparable writableByteArrayComparable = (WritableByteArrayComparable) fields.get(
						"comparator").get(filter);
				boolean foundColumn = (Boolean) fields.get("foundColumn").get(filter);
				boolean matchedColumn = (Boolean) fields.get("matchedColumn").get(filter);
				boolean filterIfMissing = (Boolean) fields.get("filterIfMissing").get(filter);

				toString = Bytes.toString(family) + ":" + Bytes.toString(qualifier) + " " + compareOp + " ("
						+ QueryContext.toString(writableByteArrayComparable) + "," + foundColumn + "," + matchedColumn
						+ "," + filterIfMissing + ")";
			} catch (Exception e) {
				LOG.error("unknown", e);
			}

		}

		public String toString() {
			return toString;
		}

	}
}