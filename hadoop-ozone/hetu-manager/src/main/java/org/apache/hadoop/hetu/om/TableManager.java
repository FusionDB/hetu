/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hetu.om;

import org.apache.hadoop.ozone.om.helpers.OmTableArgs;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;

import java.io.IOException;
import java.util.List;

/**
 * TableManager handles all the table level operations.
 */
public interface TableManager extends IOzoneAcl {
  /**
   * Creates a table.
   * @param tableInfo - OmTableInfo for creating table.
   */
  void createTable(OmTableInfo tableInfo) throws IOException;


  /**
   * Returns Table Information.
   * @param databaseName - Name of the Database.
   * @param tableName - Name of the Table.
   */
  OmTableInfo getTableInfo(String databaseName, String tableName)
      throws IOException;

  /**
   * Sets table property from args.
   * @param args - TableArgs.
   * @throws IOException
   */
  void setTableProperty(OmTableArgs args) throws IOException;

  /**
   * Deletes an existing empty table from database.
   * @param databaseName - Name of the Database.
   * @param tableName - Name of the Table.
   * @throws IOException
   */
  void deleteTable(String databaseName, String tableName) throws IOException;

  /**
   * Returns a list of tables represented by {@link OmTableInfo}
   * in the given database.
   *
   * @param databaseName
   *   Required parameter database name determines tables in which database
   *   to return.
   * @param startTable
   *   Optional start table name parameter indicating where to start
   *   the table listing from, this key is excluded from the result.
   * @param tablePrefix
   *   Optional start key parameter, restricting the response to tables
   *   that begin with the specified name.
   * @param maxNumOfTables
   *   The maximum number of tables to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of tables.
   * @throws IOException
   */
  List<OmTableInfo> listTables(String databaseName,
      String startTable, String tablePrefix, int maxNumOfTables)
      throws IOException;

}
