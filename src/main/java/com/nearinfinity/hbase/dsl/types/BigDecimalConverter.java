package com.nearinfinity.hbase.dsl.types;

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

import java.math.BigDecimal;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Aaron McCurry
 */
public class BigDecimalConverter implements TypeConverter<BigDecimal> {

	@Override
	public BigDecimal fromBytes(byte[] t) {
		return new BigDecimal(Bytes.toString(t));
	}

	@Override
	public Class<?>[] getTypes() {
		return new Class[] { BigDecimal.class };
	}

	@Override
	public byte[] toBytes(BigDecimal t) {
		return Bytes.toBytes(t.toPlainString());
	}

}
