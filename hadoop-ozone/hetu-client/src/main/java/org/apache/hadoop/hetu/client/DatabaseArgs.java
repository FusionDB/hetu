/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hetu.client;

import org.apache.hadoop.ozone.OzoneConsts;

import java.util.HashMap;
import java.util.Map;

/**
 * This class encapsulates the arguments that are
 * required for creating a volume.
 */
public final class DatabaseArgs {

  private final String admin;
  private final String owner;
  private final long quotaInBytes;
  private final int quotaInTable;
  private Map<String, String> metadata;

  /**
   * Private constructor, constructed via builder.
   * @param admin Administrator's name.
   * @param owner Volume owner's name
   * @param quotaInBytes Database quota in bytes.
   * @param quotaInTable Database quota in counts.
   * @param metadata Metadata of volume.
   */
  private DatabaseArgs(String admin,
                       String owner,
                       long quotaInBytes,
                       int quotaInTable,
                       Map<String, String> metadata) {
    this.admin = admin;
    this.owner = owner;
    this.quotaInBytes = quotaInBytes;
    this.quotaInTable = quotaInTable;
    this.metadata = metadata;
  }

  /**
   * Returns the Admin Name.
   * @return String.
   */
  public String getAdmin() {
    return admin;
  }

  /**
   * Returns the owner Name.
   * @return String
   */
  public String getOwner() {
    return owner;
  }

  /**
   * Returns Volume Quota in bytes.
   * @return quotaInBytes.
   */
  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  /**
   * Returns Database Quota in table counts.
   * @return quotaInNamespace.
   */
  public int getQuotaInTable() {
    return quotaInTable;
  }

  /**
   * Return custom key value map.
   *
   * @return metadata
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  /**
   * Returns new builder class that builds a OmVolumeArgs.
   *
   * @return Builder
   */
  public static DatabaseArgs.Builder newBuilder() {
    return new DatabaseArgs.Builder();
  }

  /**
   * Builder for OmVolumeArgs.
   */
  public static class Builder {
    private String adminName;
    private String ownerName;
    private long quotaInBytes;
    private int quotaInTable;
    private Map<String, String> metadata = new HashMap<>();

    /**
     * Constructs a builder.
     */
    public Builder() {
      quotaInBytes = OzoneConsts.HETU_QUOTA_RESET;
      quotaInTable = OzoneConsts.HETU_TABLE_QUOTA_RESET;
    }

    public DatabaseArgs.Builder setAdmin(String admin) {
      this.adminName = admin;
      return this;
    }

    public DatabaseArgs.Builder setOwner(String owner) {
      this.ownerName = owner;
      return this;
    }

    public DatabaseArgs.Builder setQuotaInBytes(long quota) {
      this.quotaInBytes = quota;
      return this;
    }

    public DatabaseArgs.Builder setQuotaInTable(int quota) {
      this.quotaInTable = quota;
      return this;
    }

    public DatabaseArgs.Builder addMetadata(String key, String value) {
      metadata.put(key, value);
      return this;
    }

    /**
     * Constructs a CreateVolumeArgument.
     * @return CreateVolumeArgs.
     */
    public DatabaseArgs build() {
      return new DatabaseArgs(adminName, ownerName, quotaInBytes,
          quotaInTable, metadata);
    }
  }

}
