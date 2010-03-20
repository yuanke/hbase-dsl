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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Table admin is responsible for altering existing tables. For example if you
 * want to add a new family to a table you can simply call the family method,
 * and the method will disable the table and add the new family only if it does
 * not already exist.
 * 
 * @author Aaron McCurry
 * 
 */
public class TableAdmin {

	private static final Log LOG = LogFactory.getLog(HBase.class);
	private HBaseAdmin admin;
	private byte[] tableName;
	private HTableDescriptor desc;

	TableAdmin(byte[] tableName) {
		this.tableName = tableName;
		try {
			admin = new HBaseAdmin(new HBaseConfiguration());
			if (!admin.tableExists(tableName)) {
				desc = new HTableDescriptor(this.tableName);
				admin.createTable(desc);
			} else {
				LOG.info("table [" + tableName + "] already exists");
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public FamilyAdmin family(String name) {
		byte[] familyName = Bytes.toBytes(name);
		try {
			desc = admin.getTableDescriptor(tableName);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		if (desc.getFamiliesKeys().contains(familyName)) {
			LOG.info("family [" + name + "] already exists");
			return new FamilyAdmin(this, admin, tableName, desc, familyName);
		}
		try {
			LOG.info("Adding family [" + Bytes.toString(familyName) + "] to table [" + Bytes.toString(tableName) + "]");
			admin.disableTable(tableName);
			HColumnDescriptor family = new HColumnDescriptor(familyName);
			desc.addFamily(family);
			admin.modifyTable(tableName, desc);
			admin.enableTable(tableName);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return new FamilyAdmin(this, admin, tableName, desc, familyName);
	}

	public static class FamilyAdmin {

		private TableAdmin tableAdmin;
		private HBaseAdmin admin;
		private byte[] tableName;
		private HTableDescriptor desc;
		private byte[] familyName;

		FamilyAdmin(TableAdmin tableAdmin, HBaseAdmin admin, byte[] tableName, HTableDescriptor desc, byte[] familyName) {
			this.tableAdmin = tableAdmin;
			this.admin = admin;
			this.tableName = tableName;
			this.desc = desc;
			this.familyName = familyName;
		}

		public FamilyAdmin family(String name) {
			return tableAdmin.family(name);
		}

		public FamilyAdmin inMemory() {
			HColumnDescriptor columnDescriptor = desc.getFamily(familyName);
			if (!columnDescriptor.isInMemory()) {
				LOG.info("Setting family [" + Bytes.toString(familyName) + "] to be in memory family.");
				disableTable();
				columnDescriptor.setInMemory(true);
				desc.addFamily(columnDescriptor);
				return modifyAndEnable();
			}
			LOG.info("Family [" + Bytes.toString(familyName) + "] is in memory family.");
			return this;
		}
		


		public FamilyAdmin enableBloomFilter() {
			HColumnDescriptor columnDescriptor = desc.getFamily(familyName);
			if (!columnDescriptor.isBloomfilter()) {
				LOG.info("Enable Bloom Filter for family [" + Bytes.toString(familyName) + "].");
				disableTable();
				columnDescriptor.setBloomfilter(true);
				desc.addFamily(columnDescriptor);
				return modifyAndEnable();
			}
			LOG.info("Bloom Filter for family [" + Bytes.toString(familyName) + "] enabled.");
			return this;
		}
		
		public FamilyAdmin disableBlockCache() {
			HColumnDescriptor columnDescriptor = desc.getFamily(familyName);
			if (columnDescriptor.isBlockCacheEnabled()) {
				LOG.info("Disable Bloom Cache for family [" + Bytes.toString(familyName) + "].");
				disableTable();
				columnDescriptor.setBlockCacheEnabled(false);
				desc.addFamily(columnDescriptor);
				return modifyAndEnable();
			}
			LOG.info("Bloom Cache for family [" + Bytes.toString(familyName) + "] disabled.");
			return this;
		}
		
		private void disableTable() {
			try {
				admin.disableTable(tableName);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		
		private FamilyAdmin modifyAndEnable() {
			try {
				admin.modifyTable(tableName, desc);
				admin.enableTable(tableName);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			return this;
		}
		
//		family.setBlocksize(s);
//		family.setBloomfilter(onOff);
//		family.setCompressionType(type);
//		family.setInMemory(inMemory);
//		family.setMapFileIndexInterval(interval);
//		family.setTimeToLive(timeToLive);
	}

}
