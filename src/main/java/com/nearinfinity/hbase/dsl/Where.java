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

import java.util.Iterator;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
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
public class Where<T extends QueryOps<I>, I> implements Iterable<Row<I>> {

	protected enum BOOL_EXP {
		OR, AND
	}

	private Scanner<T, I> scanner;
	private byte[] family;
	private QueryContext context = new QueryContext();

	Where(Scanner<T, I> scanner) {
		this.scanner = scanner;
	}

	public Where<T, I> family(String name) {
		family = Bytes.toBytes(name);
		return this;
	}

	@SuppressWarnings("unchecked")
	public T col(String name) {
		return (T) scanner.createWhereClause(this, family, Bytes.toBytes(name));
	}

	/**
	 * The foreach method allows for iterative processing of each row found in
	 * this where clause.
	 * 
	 * @param forEach
	 *            the {@link ForEach} object provided to perform the processing.
	 */
	public void foreach(ForEach<Row<I>> forEach) {
		Filter filter = context.getResultingFilter();
		scanner.setFilter(filter);
		scanner.foreach(forEach);
	}

	/**
	 * Adds a user provided filter with all the built in functions.
	 * 
	 * @param filter
	 *            the user provided filter.
	 * @return this.
	 */
	public Where<T, I> filter(Filter filter) {
		return addFilter(filter);
	}

	/**
	 * Creates a boolean expression of OR between two filters.
	 * 
	 * @return this.
	 */
	public Where<T, I> or() {
		context.addBooleanExp(BOOL_EXP.OR);
		return this;
	}

	/**
	 * Creates a boolean expression of AND between two filters.
	 * 
	 * @return this.
	 */
	public Where<T, I> and() {
		context.addBooleanExp(BOOL_EXP.AND);
		return this;
	}

	/**
	 * This is the left parenthesis '(' in a query, used to provide scope of
	 * operations.
	 * 
	 * @return this.
	 */
	public Where<T, I> lp() {
		context.down();
		return this;
	}

	/**
	 * This is the right parenthesis ')' in a query, used to provide scope of
	 * operations.
	 * 
	 * @return this.
	 */
	public Where<T, I> rp() {
		context.up();
		return this;
	}

	/**
	 * Turns an object into a byte array. See {@link Scanner} toBytes method.
	 * 
	 * @param o
	 *            the object.
	 * @return byte array.
	 */
	protected byte[] toBytes(Object o) {
		return scanner.toBytes(o);
	}

	/**
	 * Adds a filter to this where clause see {@link Scan} setFilter.
	 * 
	 * @param filter
	 *            the {@link Filter} to be added.
	 * @return this object.
	 */
	protected Where<T, I> addFilter(Filter filter) {
		context.addFilter(filter);
		return this;
	}

	@Override
	public Iterator<Row<I>> iterator() {
		return scanner.iterator();
	}
}
