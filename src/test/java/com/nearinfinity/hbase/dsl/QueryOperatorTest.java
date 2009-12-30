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

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

/**
 * @author Aaron McCurry
 */
public class QueryOperatorTest extends BaseTest {

	@Test
	public void equalsMethod() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", "val1").col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", "val2").col("col2", "val2");
		hBase.save(TABLE).row("1236").family(FAM_A).col("col2", "val2");

		final Iterator<String> ids = Arrays.asList("1235").iterator();
		hBase.scan(TABLE).where().family(FAM_A).col("col1").eq("val2").foreach(new ForEach<Row<String>>() {
			@Override
			public void process(Row<String> row) {
				assertTrue(ids.hasNext());
				assertEquals(ids.next(), row.getId());
			}
		});
		assertFalse(ids.hasNext());
	}

	@Test
	public void notEqualsMethod() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", "val1").col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", "val2").col("col2", "val2");
		hBase.save(TABLE).row("1236").family(FAM_A).col("col2", "val2");

		final Iterator<String> ids = Arrays.asList("1234", "1236").iterator();
		hBase.scan(TABLE).where().family(FAM_A).col("col1").ne("val2").foreach(new ForEach<Row<String>>() {
			@Override
			public void process(Row<String> row) {
				assertTrue(ids.hasNext());
				assertEquals(ids.next(), row.getId());
			}
		});
		assertFalse(ids.hasNext());
	}

	@Test
	public void lessThanMethod() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", 1234).col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", 1235).col("col2", "val2");
		hBase.save(TABLE).row("1236").family(FAM_A).col("col1", 1236);
		hBase.save(TABLE).row("1237").family(FAM_A).col("col2", 1238);

		final Iterator<String> ids = Arrays.asList("1234").iterator();
		hBase.scan(TABLE).where().family(FAM_A).col("col1").lt(1235).foreach(new ForEach<Row<String>>() {
			@Override
			public void process(Row<String> row) {
				assertTrue(ids.hasNext());
				assertEquals(ids.next(), row.getId());
			}
		});
		assertFalse(ids.hasNext());
	}

	@Test
	public void lessThanOrEqualToMethod() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", 1234).col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", 1235).col("col2", "val2");
		hBase.save(TABLE).row("1236").family(FAM_A).col("col1", 1236);
		hBase.save(TABLE).row("1237").family(FAM_A).col("col2", 1238);

		final Iterator<String> ids = Arrays.asList("1234", "1235").iterator();
		hBase.scan(TABLE).where().family(FAM_A).col("col1").lte(1235).foreach(new ForEach<Row<String>>() {
			@Override
			public void process(Row<String> row) {
				assertTrue(ids.hasNext());
				assertEquals(ids.next(), row.getId());
			}
		});
		assertFalse(ids.hasNext());
	}

	@Test
	public void greaterThanMethod() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", 1234).col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", 1235).col("col2", "val2");
		hBase.save(TABLE).row("1236").family(FAM_A).col("col1", 1236);
		hBase.save(TABLE).row("1237").family(FAM_A).col("col2", 1238);

		final Iterator<String> ids = Arrays.asList("1236").iterator();
		hBase.scan(TABLE).where().family(FAM_A).col("col1").gt(1235).foreach(new ForEach<Row<String>>() {
			@Override
			public void process(Row<String> row) {
				assertTrue(ids.hasNext());
				assertEquals(ids.next(), row.getId());
			}
		});
		assertFalse(ids.hasNext());
	}

	@Test
	public void greaterThanOrEqualToMethod() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", 1234).col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", 1235).col("col2", "val2");
		hBase.save(TABLE).row("1236").family(FAM_A).col("col1", 1236);
		hBase.save(TABLE).row("1237").family(FAM_A).col("col2", 1238);

		final Iterator<String> ids = Arrays.asList("1235", "1236").iterator();
		hBase.scan(TABLE).where().family(FAM_A).col("col1").gte(1235).foreach(new ForEach<Row<String>>() {
			@Override
			public void process(Row<String> row) {
				assertTrue(ids.hasNext());
				assertEquals(ids.next(), row.getId());
			}
		});
		assertFalse(ids.hasNext());
	}

	@Test
	public void betweenInMethod() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", 1234).col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", 1235).col("col2", "val2");
		hBase.save(TABLE).row("1236").family(FAM_A).col("col1", 1236);
		hBase.save(TABLE).row("1237").family(FAM_A).col("col1", 1237);
		hBase.save(TABLE).row("1238").family(FAM_A).col("col2", 1238);

		final Iterator<String> ids = Arrays.asList("1235", "1236", "1237").iterator();
		hBase.scan(TABLE).where().family(FAM_A).col("col1").betweenIn(1235, 1237).foreach(new ForEach<Row<String>>() {
			@Override
			public void process(Row<String> row) {
				assertTrue(ids.hasNext());
				assertEquals(ids.next(), row.getId());
			}
		});
		assertFalse(ids.hasNext());
	}

	@Test
	public void betweenExMethod() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", 1234).col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", 1235).col("col2", "val2");
		hBase.save(TABLE).row("1236").family(FAM_A).col("col1", 1236);
		hBase.save(TABLE).row("1237").family(FAM_A).col("col1", 1237);
		hBase.save(TABLE).row("1238").family(FAM_A).col("col2", 1238);

		final Iterator<String> ids = Arrays.asList("1236").iterator();
		hBase.scan(TABLE).where().family(FAM_A).col("col1").betweenEx(1235, 1237).foreach(new ForEach<Row<String>>() {
			@Override
			public void process(Row<String> row) {
				assertTrue(ids.hasNext());
				assertEquals(ids.next(), row.getId());
			}
		});
		assertFalse(ids.hasNext());
	}

	@Test
	public void matchMethod() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", "John doe").col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", "john M. doe").col("col2", "val2");
		hBase.save(TABLE).row("1236").family(FAM_A).col("col1", "john Doe");
		hBase.save(TABLE).row("1237").family(FAM_A).col("col1", "John P. Doe");
		hBase.save(TABLE).row("1238").family(FAM_A).col("col2", 1238);

		final Iterator<String> ids = Arrays.asList("1235", "1236").iterator();
		hBase.scan(TABLE).where().family(FAM_A).col("col1").match("john*").foreach(new ForEach<Row<String>>() {
			@Override
			public void process(Row<String> row) {
				assertTrue(ids.hasNext());
				assertEquals(ids.next(), row.getId());
			}
		});
		assertFalse(ids.hasNext());
	}

	@Test
	public void containsMethod() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("col1", "John doe").col("col2", "val1");
		hBase.save(TABLE).row("1235").family(FAM_A).col("col1", "john M. doe").col("col2", "val2");
		hBase.save(TABLE).row("1236").family(FAM_A).col("col1", "john Doe");
		hBase.save(TABLE).row("1237").family(FAM_A).col("col1", "John P. Doe");
		hBase.save(TABLE).row("1238").family(FAM_A).col("col2", 1238);

		final Iterator<String> ids = Arrays.asList("1234", "1235", "1236", "1237").iterator();
		hBase.scan(TABLE).where().family(FAM_A).col("col1").contains("doe").foreach(new ForEach<Row<String>>() {
			@Override
			public void process(Row<String> row) {
				assertTrue(ids.hasNext());
				assertEquals(ids.next(), row.getId());
			}
		});
		assertFalse(ids.hasNext());
	}

}
