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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Aaron McCurry
 */
public class FloatConverter implements TypeConverter<Float> {

	@Override
	public Float fromBytes(byte[] t) {
		return Bytes.toFloat(t);
	}

	@Override
	public Class<?>[] getTypes() {
		return new Class[] { Float.class, Float.TYPE };
	}

	@Override
	public byte[] toBytes(Float t) {
		return Bytes.toBytes(t);
	}

}
