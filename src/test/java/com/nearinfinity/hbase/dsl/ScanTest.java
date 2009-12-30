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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

/**
 * @author Aaron McCurry
 */
public class ScanTest extends BaseTest {

	@Test
	public void scan() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", "val1").col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", "val2").col("col2", "val2");

		final Iterator<String> ids = Arrays.asList("1234", "1235").iterator();
		hBase.scan(TABLE).foreach(new ForEach<Row<String>>() {
			@Override
			public void process(Row<String> row) {
				assertTrue(ids.hasNext());
				assertEquals(ids.next(), row.getId());
			}
		});
		assertFalse(ids.hasNext());
	}

	@Test
	public void scanWithSimpleSelect() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", "val1").col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", "val2").col("col2", "val2");

		final Iterator<String> ids = Arrays.asList("1234", "1235").iterator();

		hBase.scan(TABLE).select().family(FAM_A).col("col1").foreach(new ForEach<Row<String>>() {
			@Override
			public void process(Row<String> row) {
				assertTrue(ids.hasNext());
				assertEquals(ids.next(), row.getId());
				assertNotNull(row.value(FAM_A, "col1", String.class));
				assertNull(row.value(FAM_A, "col2", String.class));
			}
		});
		assertFalse(ids.hasNext());
	}

	@Test
	public void scanWithSimpleWhere() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", "val1").col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", "val2").col("col2", "val2");

		final Iterator<String> ids = Arrays.asList("1235").iterator();

		hBase.scan(TABLE).where().family(FAM_A).col("col1").eq("val2").foreach(new ForEach<Row<String>>() {
			@Override
			public void process(Row<String> row) {
				assertTrue(ids.hasNext());
				assertEquals(ids.next(), row.getId());
				assertEquals("val2", row.value(FAM_A, "col1", String.class));
				assertEquals("val2", row.value(FAM_A, "col2", String.class));
			}
		});
		assertFalse(ids.hasNext());
	}

	@Test
	public void scanWithSimpleWhereAnd() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", "val1").col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", "val2").col("col2", "val2");

		final Iterator<String> ids = Arrays.asList("1235").iterator();

		hBase.scan(TABLE).where().family(FAM_A).col("col1").eq("val2").and().col("col2").eq("val2").foreach(
				new ForEach<Row<String>>() {
					@Override
					public void process(Row<String> row) {
						assertTrue(ids.hasNext());
						assertEquals(ids.next(), row.getId());
						assertEquals("val2", row.value(FAM_A, "col1", String.class));
						assertEquals("val2", row.value(FAM_A, "col2", String.class));
					}
				});
		assertFalse(ids.hasNext());
	}

	@Test
	public void scanWithSimpleWhereOr() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", "val1").col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", "val2").col("col2", "val2");

		final Iterator<String> ids = Arrays.asList("1234", "1235").iterator();
		final Iterator<String> col1s = Arrays.asList("val1", "val2").iterator();
		final Iterator<String> col2s = Arrays.asList("val1", "val2").iterator();

		hBase.scan(TABLE).where().family(FAM_A).col("col1").eq("val1").or().col("col2").eq("val2").foreach(
				new ForEach<Row<String>>() {
					@Override
					public void process(Row<String> row) {
						assertTrue(ids.hasNext());
						assertTrue(col1s.hasNext());
						assertTrue(col2s.hasNext());

						assertEquals(ids.next(), row.getId());
						assertEquals(col1s.next(), row.value(FAM_A, "col1", String.class));
						assertEquals(col2s.next(), row.value(FAM_A, "col2", String.class));
					}
				});
		assertFalse(ids.hasNext());
	}

	@Test
	public void scanWithWhereClauseOfNestedAndOrCase1() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", "val1").col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", "val2").col("col2", "val2");
		hBase.save(TABLE).row("1236").family(FAM_A).col("col1", "val3").col("col2", "val3");
		hBase.save(TABLE).row("1237").family(FAM_A).col("col1", "val4").col("col2", "val4");

		final Iterator<String> ids = Arrays.asList("1234", "1236").iterator();

		hBase.scan(TABLE).where().family(FAM_A).lp().col("col1").eq("val1").and().col("col2").eq("val1").rp().or().col(
				"col1").eq("val3").foreach(new ForEach<Row<String>>() {
			@Override
			public void process(Row<String> row) {
				assertTrue(ids.hasNext());
				assertEquals(ids.next(), row.getId());
			}
		});
		assertFalse(ids.hasNext());
	}

	@Test
	public void scanWithWhereClauseOfNestedAndOrCase2() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", "val1").col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", "val2").col("col2", "val2");
		hBase.save(TABLE).row("1236").family(FAM_A).col("col1", "val3").col("col2", "val3");
		hBase.save(TABLE).row("1237").family(FAM_A).col("col1", "val4").col("col2", "val4");

		final Iterator<String> ids = Arrays.asList("1234", "1236").iterator();

		hBase.scan(TABLE).where().family(FAM_A).col("col1").eq("val3").or().lp().col("col1").eq("val1").and().col(
				"col2").eq("val1").rp().foreach(new ForEach<Row<String>>() {
			@Override
			public void process(Row<String> row) {
				assertTrue(ids.hasNext());
				assertEquals(ids.next(), row.getId());
			}
		});
		assertFalse(ids.hasNext());
	}

	@Test
	public void scanWithWhereAndAllVersions() throws Exception {
		long originalTs = new Date().getTime();
		long ts = originalTs;
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", "val1",ts).col("col2", "val1").flush();
		ts += 3;
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", "val2",ts).col("col2", "val2").flush();
		ts += 3;
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", "val3",ts).col("col2", "val3").flush();
		ts += 3;
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", "val4",ts).col("col2", "val4").flush();

		final Iterator<String> ids = Arrays.asList("1234").iterator();
		
		System.out.println(originalTs);
		
		final Map<Long,String> map = new TreeMap<Long, String>();
		map.put(originalTs + 3, "val2");
		map.put(originalTs + 6, "val3");
		map.put(originalTs + 9, "val4");
		
		final List<String> asc = new ArrayList<String>();
		asc.add("val2");
		asc.add("val3");
		asc.add("val4");
		
		final List<String> desc = new ArrayList<String>();
		desc.add("val4");
		desc.add("val3");
		desc.add("val2");

		hBase.scan(TABLE).
			select().
				allVersions().
			where().
				family(FAM_A).
					col("col1").
						eq("val4").
			foreach(new ForEach<Row<String>>() {
					@Override
					public void process(Row<String> row) {
						assertTrue(ids.hasNext());
						assertEquals(ids.next(), row.getId());
						Family family = row.family(FAM_A);
						assertEquals(map,family.values("col1", String.class));
						assertEquals(asc,family.valuesAscTimestamp("col1", String.class));
						assertEquals(desc,family.valuesDescTimestamp("col1", String.class));
					}
				});
		assertFalse(ids.hasNext());
	}

}
