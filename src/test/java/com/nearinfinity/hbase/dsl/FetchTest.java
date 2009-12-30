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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * @author Aaron McCurry
 */
public class FetchTest extends BaseTest {

	@Test
	public void fetch() throws IOException {
		Date d = new Date();
		Put put = new Put(Bytes.toBytes("4321"));
		put.add(Bytes.toBytes(FAM_A), Bytes.toBytes("string"), Bytes.toBytes("1234"));
		put.add(Bytes.toBytes(FAM_A), Bytes.toBytes("int"), Bytes.toBytes(1234));
		put.add(Bytes.toBytes(FAM_A), Bytes.toBytes("long"), Bytes.toBytes(1234l));
		put.add(Bytes.toBytes(FAM_A), Bytes.toBytes("double"), Bytes.toBytes(1234.1234d));
		put.add(Bytes.toBytes(FAM_A), Bytes.toBytes("float"), Bytes.toBytes(1234.1234f));
		put.add(Bytes.toBytes(FAM_A), Bytes.toBytes("short"), Bytes.toBytes((short) 1234));
		put.add(Bytes.toBytes(FAM_A), Bytes.toBytes("boolean"), Bytes.toBytes(true));
		put.add(Bytes.toBytes(FAM_A), Bytes.toBytes("date"), Bytes.toBytes(d.getTime()));
		put.add(Bytes.toBytes(FAM_A), Bytes.toBytes("bigint"), new BigInteger("1234").toByteArray());
		put.add(Bytes.toBytes(FAM_A), Bytes.toBytes("bigdec"), Bytes.toBytes(new BigDecimal("1234.1234")
				.toPlainString()));

		hTable.put(put);
		assertNull(hBase.fetch(TABLE).row("54321"));
		Row<String> row = hBase.fetch(TABLE).row("4321");
		assertNotNull(row);
		assertEquals("1234", row.value(FAM_A, "string", String.class));
		assertEquals(new Integer(1234), row.value(FAM_A, "int", Integer.class));
		assertEquals(new Long(1234), row.value(FAM_A, "long", Long.class));
		assertEquals(new Double(1234.1234), row.value(FAM_A, "double", Double.class));
		assertEquals(new Float(1234.1234), row.value(FAM_A, "float", Float.class));
		assertEquals(new Short((short) 1234), row.value(FAM_A, "short", Short.class));
		assertEquals(Boolean.TRUE, row.value(FAM_A, "boolean", Boolean.class));
		assertEquals(d, row.value(FAM_A, "date", Date.class));
		assertEquals(new BigInteger("1234"), row.value(FAM_A, "bigint", BigInteger.class));
		assertEquals(new BigDecimal("1234.1234"), row.value(FAM_A, "bigdec", BigDecimal.class));
	}

}
