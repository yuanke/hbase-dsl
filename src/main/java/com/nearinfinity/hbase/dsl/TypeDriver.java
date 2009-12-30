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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.nearinfinity.hbase.dsl.types.BigDecimalConverter;
import com.nearinfinity.hbase.dsl.types.BigIntegerConverter;
import com.nearinfinity.hbase.dsl.types.BooleanConverter;
import com.nearinfinity.hbase.dsl.types.ByteArrayConverter;
import com.nearinfinity.hbase.dsl.types.DateConverter;
import com.nearinfinity.hbase.dsl.types.DoubleConverter;
import com.nearinfinity.hbase.dsl.types.FloatConverter;
import com.nearinfinity.hbase.dsl.types.IntConverter;
import com.nearinfinity.hbase.dsl.types.LongConverter;
import com.nearinfinity.hbase.dsl.types.ShortConverter;
import com.nearinfinity.hbase.dsl.types.StringConverter;
import com.nearinfinity.hbase.dsl.types.TypeConverter;

/**
 * This class is responsible for converting objects to binary and back again.
 * Internally it uses a hash map, so it is not thread safe while changing the
 * types. Once setup though, it is thread safe.
 * 
 * @author Aaron McCurry
 */
@SuppressWarnings("unchecked")
public class TypeDriver {

	private static final Log LOG = LogFactory.getLog(TypeDriver.class);
	private Map<Class<?>, TypeConverter<?>> converters = new HashMap<Class<?>, TypeConverter<?>>();

	/**
	 * This method registers all know type converters.
	 * 
	 * @return this.
	 */
	public TypeDriver registerAllKnownTypes() {
		registerType(new BigDecimalConverter());
		registerType(new BigIntegerConverter());
		registerType(new StringConverter());
		registerType(new IntConverter());
		registerType(new DoubleConverter());
		registerType(new BooleanConverter());
		registerType(new FloatConverter());
		registerType(new LongConverter());
		registerType(new ShortConverter());
		registerType(new DateConverter());
		registerType(new ByteArrayConverter());
		return this;
	}

	/**
	 * Converts the object to binary.
	 * 
	 * @param o
	 *            the object, can not be null.
	 * @return the binary.
	 */
	public byte[] toBytes(Object o) {
		TypeConverter<Object> typeConverter = (TypeConverter<Object>) converters.get(o.getClass());
		if (typeConverter == null) {
			for (Class<?> c : converters.keySet()) {
				LOG.info("Known type [" + c + "]");
			}
			throw new RuntimeException("No type found for class [" + o.getClass() + "]");
		}
		return typeConverter.toBytes(o);
	}

	/**
	 * Convert the binary back into an object.
	 * 
	 * @param <U>
	 *            the type of the new object.
	 * @param value
	 *            the binary.
	 * @param cl
	 *            the class type of the new object.
	 * @return the new object.
	 */
	public <U> U fromBytes(byte[] value, Class<U> cl) {
		TypeConverter<U> typeConverter = (TypeConverter<U>) converters.get(cl);
		if (typeConverter == null) {
			for (Class<?> c : converters.keySet()) {
				LOG.info("Known type [" + c + "]");
			}
			throw new RuntimeException("No type found for class [" + cl + "]");
		}
		return typeConverter.fromBytes(value);
	}

	/**
	 * Registers a single type converter into this driver.
	 * 
	 * @param converter
	 *            the new converter.
	 * @return this.
	 */
	public TypeDriver registerType(TypeConverter<?> converter) {
		for (Class<?> c : converter.getTypes()) {
			LOG.info("Registering type [" + c + "]");
			converters.put(c, converter);
		}
		return this;
	}

}
