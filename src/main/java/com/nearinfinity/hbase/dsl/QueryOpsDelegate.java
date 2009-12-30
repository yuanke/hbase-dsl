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

import java.util.Collection;

/**
 * 
 * @author Aaron McCurry
 * 
 * @param <T>
 * @param <I>
 */
@SuppressWarnings("unchecked")
public class QueryOpsDelegate<T extends QueryOps<I>, I> extends QueryOps<I> {

	public QueryOpsDelegate(Where<T, I> whereScanner, byte[] family, byte[] qualifier) {
		super(whereScanner, family, qualifier);
	}

	@Override
	public <U> Where<T, I> betweenEx(U s, U e) {
		return (Where<T, I>) super.betweenEx(s, e);
	}

	@Override
	public <U> Where<T, I> betweenIn(U s, U e) {
		return (Where<T, I>) super.betweenIn(s, e);
	}

	@Override
	public <U> Where<T, I> eq(Collection<U> col) {
		return (Where<T, I>) super.eq(col);
	}

	@Override
	public <U> Where<T, I> eq(U... objs) {
		return (Where<T, I>) super.eq(objs);
	}

	@Override
	public <U> Where<T, I> gt(Collection<U> col) {
		return (Where<T, I>) super.gt(col);
	}

	@Override
	public <U> Where<T, I> gt(U... objs) {
		return (Where<T, I>) super.gt(objs);
	}

	@Override
	public <U> Where<T, I> gte(Collection<U> col) {
		return (Where<T, I>) super.gte(col);
	}

	@Override
	public <U> Where<T, I> gte(U... objs) {
		return (Where<T, I>) super.gte(objs);
	}

	@Override
	public <U> Where<T, I> lt(Collection<U> col) {
		return (Where<T, I>) super.lt(col);
	}

	@Override
	public <U> Where<T, I> lt(U... objs) {
		return (Where<T, I>) super.lt(objs);
	}

	@Override
	public <U> Where<T, I> lte(Collection<U> col) {
		return (Where<T, I>) super.lte(col);
	}

	@Override
	public <U> Where<T, I> lte(U... objs) {
		return (Where<T, I>) super.lte(objs);
	}

	@Override
	public <U> Where<T, I> contains(String pattern) {
		return (Where<T, I>) super.contains(pattern);
	}

	@Override
	public Where<T, I> match(String pattern) {
		return (Where<T, I>) super.match(pattern);
	}

	@Override
	public <U> Where<T, I> ne(Collection<U> col) {
		return (Where<T, I>) super.ne(col);
	}

	@Override
	public <U> Where<T, I> ne(U... objs) {
		return (Where<T, I>) super.ne(objs);
	}
}
