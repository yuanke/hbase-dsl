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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

/**
 * 
 * @author Aaron McCurry
 * 
 * @param <ROW_ID_TYPE>
 */
public class QueryOps<ROW_ID_TYPE> {

	protected Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> whereScanner;
	protected byte[] qualifier;
	protected byte[] family;

	public QueryOps(Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> whereScanner, byte[] family, byte[] qualifier) {
		this.whereScanner = whereScanner;
		this.family = family;
		this.qualifier = qualifier;
	}

	public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> eq(U... objs) {
		return objs == null ? whereScanner : eq(Arrays.asList(objs));
	}

	public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> eq(Collection<U> col) {
		return getCommonFilter(col, CompareOp.EQUAL, true);
	}

	public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> ne(Collection<U> col) {
		return getCommonFilter(col, CompareOp.NOT_EQUAL, false);
	}

	public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> lt(Collection<U> col) {
		return getCommonFilter(col, CompareOp.LESS, true);
	}

	public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> gt(Collection<U> col) {
		return getCommonFilter(col, CompareOp.GREATER, true);
	}

	public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> lte(Collection<U> col) {
		return getCommonFilter(col, CompareOp.LESS_OR_EQUAL, true);
	}

	public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> gte(Collection<U> col) {
		return getCommonFilter(col, CompareOp.GREATER_OR_EQUAL, true);
	}

	public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> contains(String pattern) {
		SubstringComparator comparator = new SubstringComparator(pattern);
		SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(family, qualifier,
				CompareOp.EQUAL, comparator);
		singleColumnValueFilter.setFilterIfMissing(true);
		return whereScanner.addFilter(singleColumnValueFilter);
	}

	public Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> match(String regexPattern) {
		RegexStringComparator regexStringComparator = new RegexStringComparator(regexPattern);
		SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(family, qualifier,
				CompareOp.EQUAL, regexStringComparator);
		singleColumnValueFilter.setFilterIfMissing(true);
		return whereScanner.addFilter(singleColumnValueFilter);
	}

	public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> betweenIn(U s, U e) {
		Filter leftFilter = getSingleValueEqualFilter(whereScanner.toBytes(s), CompareOp.GREATER_OR_EQUAL, true);
		Filter rightFilter = getSingleValueEqualFilter(whereScanner.toBytes(e), CompareOp.LESS_OR_EQUAL, true);
		return whereScanner.addFilter(new FilterList(Operator.MUST_PASS_ALL, Arrays.asList(leftFilter, rightFilter)));
	}

	public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> betweenEx(U s, U e) {
		Filter leftFilter = getSingleValueEqualFilter(whereScanner.toBytes(s), CompareOp.GREATER, true);
		Filter rightFilter = getSingleValueEqualFilter(whereScanner.toBytes(e), CompareOp.LESS, true);
		return whereScanner.addFilter(new FilterList(Operator.MUST_PASS_ALL, Arrays.asList(leftFilter, rightFilter)));
	}

	public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> ne(U... objs) {
		return objs == null ? whereScanner : ne(Arrays.asList(objs));
	}

	public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> lt(U... objs) {
		return objs == null ? whereScanner : lt(Arrays.asList(objs));
	}

	public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> gt(U... objs) {
		return objs == null ? whereScanner : gt(Arrays.asList(objs));
	}

	public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> lte(U... objs) {
		return objs == null ? whereScanner : lte(Arrays.asList(objs));
	}

	public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> gte(U... objs) {
		return objs == null ? whereScanner : gte(Arrays.asList(objs));
	}

	private Filter getSingleValueEqualFilter(byte[] value, CompareOp compareOp, boolean filterIfMissing) {
		SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(family, qualifier, compareOp,
				value);
		singleColumnValueFilter.setFilterIfMissing(filterIfMissing);
		return singleColumnValueFilter;
	}

	protected <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> getCommonFilter(Collection<U> col, CompareOp compareOp,
			boolean filterIfMissing) {
		if (col == null || col.size() == 0) {
			return whereScanner;
		}
		if (col.size() == 1) {
			byte[] value = whereScanner.toBytes(col.iterator().next());
			return whereScanner.addFilter(getSingleValueEqualFilter(value, compareOp, filterIfMissing));
		}
		List<Filter> list = new ArrayList<Filter>();
		for (Object o : col) {
			byte[] value = whereScanner.toBytes(o);
			list.add(getSingleValueEqualFilter(value, compareOp, filterIfMissing));
		}
		return whereScanner.addFilter(new FilterList(Operator.MUST_PASS_ONE, list));
	}
}
