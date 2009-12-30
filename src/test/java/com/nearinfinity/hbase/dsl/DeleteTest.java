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

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author Aaron McCurry
 */
public class DeleteTest extends BaseTest {
	
	@Test public void deleteColumn() {
		hBase.save(TABLE).row("1234").family(FAM_A).col("base", "value1").family(FAM_B).col("base", "value2");
		hBase.flush();
		
		Row<String> rowBefore = hBase.fetch(TABLE).row("1234");
		assertEquals("value1",rowBefore.value(FAM_A, "base", String.class));
		
		hBase.delete(TABLE).row("1234").family(FAM_A).col("base");
		Row<String> rowAfter = hBase.fetch(TABLE).row("1234");
		assertNull(rowAfter.value(FAM_A, "base", String.class));
	}

}
