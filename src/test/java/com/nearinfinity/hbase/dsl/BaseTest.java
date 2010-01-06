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

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.junit.Before;

/**
 * @author Aaron McCurry
 */
public abstract class BaseTest {

	protected static final String FAM_A = "famA";
	protected static final String FAM_B = "famB";
	protected static final String TABLE = "test";
	protected HBase<QueryOps<String>, String> hBase;
	protected HTable hTable;

	@Before
	public void setUp() throws IOException {
		hBase = new HBase<QueryOps<String>, String>(String.class);
		hBase.defineTable(TABLE).family(FAM_A).family(FAM_B);
		hBase.truncateTable(TABLE);
		hTable = new HTable(TABLE);
	}

}
