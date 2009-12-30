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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * @author Aaron McCurry
 */
public class AdminTest extends BaseTest {

	private static final String TEST_FAM = "test-fam";
	private static final String TEST_TABLE = "test-table";

	@Test
	public void defineTable() throws Exception {
		hBase.removeTable(TEST_TABLE);
		hBase.defineTable(TEST_TABLE).family(TEST_FAM);

		HBaseAdmin hBaseAdmin = new HBaseAdmin(new HBaseConfiguration());
		HTableDescriptor tableDescriptor = hBaseAdmin.getTableDescriptor(Bytes.toBytes(TEST_TABLE));
		HColumnDescriptor family = tableDescriptor.getFamily(Bytes.toBytes(TEST_FAM));

		assertFalse(family.isBloomfilter());
		assertFalse(family.isInMemory());
		assertTrue(family.isBlockCacheEnabled()); // default value
	}

	@Test
	public void reconfigureTable() throws Exception {
		hBase.defineTable(TEST_TABLE).family(TEST_FAM).inMemory().enableBloomFilter().disableBlockCache();

		HBaseAdmin hBaseAdmin = new HBaseAdmin(new HBaseConfiguration());
		HTableDescriptor tableDescriptor = hBaseAdmin.getTableDescriptor(Bytes.toBytes(TEST_TABLE));
		HColumnDescriptor family = tableDescriptor.getFamily(Bytes.toBytes(TEST_FAM));

		assertTrue(family.isBloomfilter());
		assertTrue(family.isInMemory());
		assertFalse(family.isBlockCacheEnabled());

		hBase.defineTable(TEST_TABLE).family(TEST_FAM).inMemory().enableBloomFilter().disableBlockCache();

		hBaseAdmin = new HBaseAdmin(new HBaseConfiguration());
		tableDescriptor = hBaseAdmin.getTableDescriptor(Bytes.toBytes(TEST_TABLE));
		family = tableDescriptor.getFamily(Bytes.toBytes(TEST_FAM));

		assertTrue(family.isBloomfilter());
		assertTrue(family.isInMemory());
		assertFalse(family.isBlockCacheEnabled());
	}
}
