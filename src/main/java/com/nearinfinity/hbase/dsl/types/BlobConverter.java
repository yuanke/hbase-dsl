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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialException;

/**
 * @author Aaron McCurry
 */
public class BlobConverter implements TypeConverter<Blob> {

	@Override
	public Blob fromBytes(byte[] t) {
		try {
			return new SerialBlob(t);
		} catch (SerialException e) {
			throw new RuntimeException(e);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Class<?>[] getTypes() {
		return new Class[] { Blob.class };
	}

	@Override
	public byte[] toBytes(Blob t) {
		InputStream binaryStream;
		try {
			binaryStream = t.getBinaryStream();
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream((int) t.length());
			byte[] buffer = new byte[1024];
			int num = -1;
			while ((num = binaryStream.read(buffer)) != -1) {
				outputStream.write(buffer, 0, num);
			}
			binaryStream.close();
			return outputStream.toByteArray();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
