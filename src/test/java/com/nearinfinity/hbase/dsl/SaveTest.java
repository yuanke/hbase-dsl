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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * @author Aaron McCurry
 */
public class SaveTest extends BaseTest {

	@Test
	public void save() throws IOException {
		hBase.save(TABLE).row("1234").family(FAM_A).col("base", "value1").family(FAM_B).col("base", "value2");
		hBase.flush();

		Get get = new Get(Bytes.toBytes("1234"));
		Result result = hTable.get(get);
		NavigableMap<byte[], byte[]> familyMapA = result.getFamilyMap(Bytes.toBytes(FAM_A));
		assertNotNull(familyMapA);
		byte[] value1 = familyMapA.get(Bytes.toBytes("base"));
		assertNotNull(value1);
		assertArrayEquals(Bytes.toBytes("value1"), value1);

		NavigableMap<byte[], byte[]> familyMapB = result.getFamilyMap(Bytes.toBytes(FAM_B));
		assertNotNull(familyMapB);

		byte[] value2 = familyMapB.get(Bytes.toBytes("base"));
		assertNotNull(value2);
		assertArrayEquals(Bytes.toBytes("value2"), value2);
	}

	@Test
	public void saveWithTypes() throws IOException {
		Date date = new Date();
		hBase.save(TABLE).row("1234").
			family(FAM_A).
				col("int", 1234).
				col("boolean", true).
				col("date", date).
				col("double", 1234.1234).
				col("float", 1234.1234f).
				col("short", (short) 1234).
				col("string", "1234").
				col("bigint", new BigInteger("1234")).
				col("bigdec", new BigDecimal("1234.1234")).
				col("long", 1234l).
				col("bytearray", new byte[]{1,2,3,4});

		hBase.flush();

		Get get = new Get(Bytes.toBytes("1234"));
		Result result = hTable.get(get);
		NavigableMap<byte[], byte[]> familyMapA = result.getFamilyMap(Bytes.toBytes(FAM_A));
		assertNotNull(familyMapA);

		assertArrayEquals(Bytes.toBytes(1234), familyMapA.get(Bytes.toBytes("int")));
		assertArrayEquals(Bytes.toBytes(true), familyMapA.get(Bytes.toBytes("boolean")));
		assertArrayEquals(Bytes.toBytes(date.getTime()), familyMapA.get(Bytes.toBytes("date")));
		assertArrayEquals(Bytes.toBytes(1234.1234), familyMapA.get(Bytes.toBytes("double")));
		assertArrayEquals(Bytes.toBytes(1234.1234f), familyMapA.get(Bytes.toBytes("float")));
		assertArrayEquals(Bytes.toBytes((short) 1234), familyMapA.get(Bytes.toBytes("short")));
		assertArrayEquals(Bytes.toBytes("1234"), familyMapA.get(Bytes.toBytes("string")));
		assertArrayEquals(new BigInteger("1234").toByteArray(), familyMapA.get(Bytes.toBytes("bigint")));
		assertArrayEquals(Bytes.toBytes(new BigDecimal("1234.1234").toPlainString()), familyMapA.get(Bytes
				.toBytes("bigdec")));
		assertArrayEquals(Bytes.toBytes(1234l), familyMapA.get(Bytes.toBytes("long")));
		assertArrayEquals(new byte[]{1,2,3,4}, familyMapA.get(Bytes.toBytes("bytearray")));
	}
}
