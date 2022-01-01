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
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;

import java.io.IOException;
import java.util.List;

/**
 * OM volume manager interface.
 */
public interface DatabaseManager extends IHetuAccess{

  /**
   * Create a new volume.
   * @param args - Volume args to create a volume
   */
  void createDatabase(OmDatabaseArgs args)
      throws IOException;

  /**
   * Changes the owner of a volume.
   *
   * @param database - Name of the volume.
   * @param owner - Name of the owner.
   * @throws IOException
   */
  void setOwner(String database, String owner)
      throws IOException;

  /**
   * Gets the volume information.
   * @param database - Database name.
   * @return VolumeArgs or exception is thrown.
   * @throws IOException
   */
  OmDatabaseArgs getDatabaseInfo(String database) throws IOException;

  /**
   * Deletes an existing empty volume.
   *
   * @param database - Name of the database.
   * @throws IOException
   */
  void deleteDatabase(String database) throws IOException;

  /**
   * Returns a list of databases owned by a given user; if user is null,
   * returns all databases.
   *
   * @param userName
   *   database owner
   * @param prefix
   *   the database prefix used to filter the listing result.
   * @param startKey
   *   the start database name determines where to start listing from,
   *   this key is excluded from the result.
   * @param maxKeys
   *   the maximum number of databases to return.
   * @return a list of {@link OmDatabaseArgs}
   * @throws IOException
   */
  List<OmDatabaseArgs> listDatabase(String userName, String prefix,
                                    String startKey, int maxKeys) throws IOException;

}
