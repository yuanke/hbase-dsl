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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.HTable;
import org.junit.Test;

/**
 * @author Aaron McCurry
 */
public class CustomQueryOperatorTest {

	protected static final String FAM_A = "famA";
	protected static final String FAM_B = "famB";
	protected static final String TABLE = "test";
	protected HBase<MyQueryOps<String>, String> hBase;
	protected HTable hTable;

	@SuppressWarnings("unchecked")
	@Test
	public void myFuncMethod() throws IOException {
//		hBase = new HBase<MyQueryOps<String>, String>((Class<? extends QueryOps<String>>) MyQueryOps.class, String.class);
		hBase = HBase.newHBase(MyQueryOps.class, String.class);
		hBase.defineTable(TABLE).family(FAM_A).family(FAM_B);
		hBase.truncateTable(TABLE);
		hTable = new HTable(TABLE);
		
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", "val1").col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", "val2").col("col2", "val2");
		hBase.save(TABLE).row("1236").family(FAM_A).col("col2", "val2");
		

		final Iterator<String> ids = Arrays.asList("1235").iterator();
		hBase.scan(TABLE).where().family(FAM_A).col("col1").myFunc("val2").foreach(new ForEach<Row<String>>() {
			@Override
			public void process(Row<String> row) {
				assertTrue(ids.hasNext());
				assertEquals(ids.next(), row.getId());
			}
		});
		assertFalse(ids.hasNext());
	}
	
	public static class MyQueryOps<I> extends QueryOpsDelegate<QueryOps<I>, I> {

		public MyQueryOps(Where<QueryOps<I>, I> whereScanner, byte[] family, byte[] qualifier) {
			super(whereScanner, family, qualifier);
			this.whereScanner = whereScanner;
		}
		
		@SuppressWarnings("unchecked")
		public Where<MyQueryOps<I>, I> myFunc(String s) {
			Where<? extends QueryOps<I>, I> eq = super.eq(s);
			return (Where<MyQueryOps<I>, I>) eq;
		}
		
	}

}
