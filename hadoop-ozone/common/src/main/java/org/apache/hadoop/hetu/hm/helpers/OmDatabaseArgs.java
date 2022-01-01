/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hetu.hm.helpers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.WithObjectID;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DatabaseInfo;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * A class that encapsulates the OmDatabaseArgs Args.
 */
public final class OmDatabaseArgs extends WithObjectID implements Auditable {
  private final String adminName;
  private String ownerName;
  private final String name;
  private long creationTime;
  private long modificationTime;
  private long quotaInBytes;
  private int quotaInTable;
  private long usedNamespace;

  /**
   * Private constructor, constructed via builder.
   * @param adminName  - Administrator's name.
   * @param ownerName  - Database owner's name
   * @param database - database name
   * @param quotaInBytes - Database Quota in bytes.
   * @param quotaInTable - Table Quota in counts.
   * @param usedNamespace - Database Table Quota Usage in counts.
   * @param metadata - metadata map for custom key/value data.
   * @param creationTime - Volume creation time.
   * @param objectID - ID of this object.
   * @param updateID - A sequence number that denotes the last update on this
   * object. This is a monotonically increasing number.
   */
  @SuppressWarnings({"checkstyle:ParameterNumber", "This is invoked from a " +
      "builder."})
  private OmDatabaseArgs(String adminName, String ownerName, String database,
                         long quotaInBytes, int quotaInTable, long usedNamespace,
                         Map<String, String> metadata, long creationTime,
                         long modificationTime, long objectID, long updateID) {
    this.adminName = adminName;
    this.ownerName = ownerName;
    this.name = database;
    this.quotaInBytes = quotaInBytes;
    this.quotaInTable = quotaInTable;
    this.usedNamespace = usedNamespace;
    this.metadata = metadata;
    this.creationTime = creationTime;
    this.modificationTime = modificationTime;
    this.objectID = objectID;
    this.updateID = updateID;
  }


  public void setOwnerName(String newOwner) {
    this.ownerName = newOwner;
  }

  public void setQuotaInBytes(long quotaInBytes) {
    this.quotaInBytes = quotaInBytes;
  }

  public void setQuotaInTable(int quotaInTable) {
    this.quotaInTable= quotaInTable;
  }

  public void setCreationTime(long time) {
    this.creationTime = time;
  }

  public void setModificationTime(long time) {
    this.modificationTime = time;
  }

  /**
   * Returns the database name
   * @return
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the Admin Name.
   * @return String.
   */
  public String getAdminName() {
    return adminName;
  }

  /**
   * Returns the owner Name.
   * @return String
   */
  public String getOwnerName() {
    return ownerName;
  }

  /**
   * Returns creation time.
   * @return long
   */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * Returns modification time.
   * @return long
   */
  public long getModificationTime() {
    return modificationTime;
  }

  /**
   * Returns Quota in Bytes.
   * @return long, Quota in bytes.
   */
  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  /**
   * Returns Quota in counts.
   * @return long, Quota in counts.
   */
  public int getQuotaInTable() {
    return quotaInTable;
  }

  /**
   * increase used table namespace by n.
   */
  public void incrUsedNamespace(long n) {
    usedNamespace += n;
  }

  /**
   * Returns used table namespace.
   * @return usedNamespace
   */
  public long getUsedNamespace() {
    return usedNamespace;
  }

  /**
   * Returns new builder class that builds a OmVolumeArgs.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.ADMIN, this.adminName);
    auditMap.put(OzoneConsts.OWNER, this.ownerName);
    auditMap.put(OzoneConsts.DATABASE, this.name);
    auditMap.put(OzoneConsts.CREATION_TIME, String.valueOf(this.creationTime));
    auditMap.put(OzoneConsts.MODIFICATION_TIME,
        String.valueOf(this.modificationTime));
    auditMap.put(OzoneConsts.QUOTA_IN_BYTES, String.valueOf(this.quotaInBytes));
    auditMap.put(OzoneConsts.QUOTA_IN_NAMESPACE,
        String.valueOf(this.quotaInTable));
    auditMap.put(OzoneConsts.USED_NAMESPACE,
        String.valueOf(this.usedNamespace));
    auditMap.put(OzoneConsts.OBJECT_ID, String.valueOf(this.getObjectID()));
    auditMap.put(OzoneConsts.UPDATE_ID, String.valueOf(this.getUpdateID()));
    return auditMap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OmDatabaseArgs that = (OmDatabaseArgs) o;
    return Objects.equals(this.objectID, that.objectID);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.objectID);
  }

  /**
   * Builder for OmVolumeArgs.
   */
  public static class Builder {
    private String adminName;
    private String ownerName;
    private String name;
    private long creationTime;
    private long modificationTime;
    private long quotaInBytes;
    private int  quotaInTable;
    private long  usedNamespace;
    private Map<String, String> metadata;
    private List<OzoneAcl> acls;
    private long objectID;
    private long updateID;

    /**
     * Sets the Object ID for this Object.
     * Object ID are unique and immutable identifier for each object in the
     * System.
     * @param id - long
     */
    public Builder setObjectID(long id) {
      this.objectID = id;
      return this;
    }

    /**
     * Sets the update ID for this Object. Update IDs are monotonically
     * increasing values which are updated each time there is an update.
     * @param id - long
     */
    public Builder setUpdateID(long id) {
      this.updateID = id;
      return this;
    }

    /**
     * Constructs a builder.
     */
    public Builder() {
      metadata = new HashMap<>();
      quotaInBytes = OzoneConsts.HETU_QUOTA_RESET;
      quotaInTable = OzoneConsts.HETU_TABLE_QUOTA_RESET;
    }

    public Builder setAdminName(String admin) {
      this.adminName = admin;
      return this;
    }

    public Builder setOwnerName(String owner) {
      this.ownerName = owner;
      return this;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setCreationTime(long createdOn) {
      this.creationTime = createdOn;
      return this;
    }

    public Builder setModificationTime(long modifiedOn) {
      this.modificationTime = modifiedOn;
      return this;
    }

    public Builder setQuotaInBytes(long quotaBytes) {
      this.quotaInBytes = quotaBytes;
      return this;
    }

    public Builder setQuotaInTable(int quotaTable) {
      this.quotaInTable = quotaTable;
      return this;
    }

    public Builder setUsedNamespace(long tableUsage) {
      this.usedNamespace = tableUsage;
      return this;
    }

    public Builder addMetadata(String key, String value) {
      metadata.put(key, value); // overwrite if present.
      return this;
    }

    public Builder addAllMetadata(Map<String, String> additionalMetaData) {
      if (additionalMetaData != null) {
        metadata.putAll(additionalMetaData);
      }
      return this;
    }

    /**
     * Constructs a CreateVolumeArgument.
     * @return CreateVolumeArgs.
     */
    public OmDatabaseArgs build() {
      Preconditions.checkNotNull(adminName);
      Preconditions.checkNotNull(ownerName);
      Preconditions.checkNotNull(name);
      return new OmDatabaseArgs(adminName, ownerName, name, quotaInBytes,
          quotaInTable, usedNamespace, metadata, creationTime,
          modificationTime, objectID, updateID);
    }
  }

  public DatabaseInfo getProtobuf() {
    return DatabaseInfo.newBuilder()
        .setAdminName(adminName)
        .setOwnerName(ownerName)
        .setName(name)
        .setQuotaInBytes(quotaInBytes)
        .setQuotaInTable(quotaInTable)
        .setUsedNamespace(usedNamespace)
        .addAllMetadata(KeyValueUtil.toProtobuf(metadata))
        .setCreationTime(
            creationTime == 0 ? System.currentTimeMillis() : creationTime)
        .setModificationTime(modificationTime)
        .setObjectID(objectID)
        .setUpdateID(updateID)
        .build();
  }

  public static OmDatabaseArgs getFromProtobuf(DatabaseInfo databaseInfo)
      throws OMException {
    return new OmDatabaseArgs(
        databaseInfo.getAdminName(),
        databaseInfo.getOwnerName(),
        databaseInfo.getName(),
        databaseInfo.getQuotaInBytes(),
        databaseInfo.getQuotaInTable(),
        databaseInfo.getUsedNamespace(),
        KeyValueUtil.getFromProtobuf(databaseInfo.getMetadataList()),
        databaseInfo.getCreationTime(),
        databaseInfo.getModificationTime(),
        databaseInfo.getObjectID(),
        databaseInfo.getUpdateID());
  }

  @Override
  public String getObjectInfo() {
    return "OmDatabaseArgs{" +
        "name='" + name + '\'' +
        ", admin='" + adminName + '\'' +
        ", owner='" + ownerName + '\'' +
        ", creationTime='" + creationTime + '\'' +
        ", quotaInBytes='" + quotaInBytes + '\'' +
        ", quotaInTable='" + quotaInTable + '\'' +
        ", usedNamespace='" + usedNamespace + '\'' +
        '}';
  }

  /**
   * Return a new copy of the object.
   */
  public OmDatabaseArgs copyObject() {
    Map<String, String> cloneMetadata = new HashMap<>();
    if (metadata != null) {
      metadata.forEach((k, v) -> cloneMetadata.put(k, v));
    }

    return new OmDatabaseArgs(adminName, ownerName, name, quotaInBytes,
        quotaInTable, usedNamespace, cloneMetadata,
        creationTime, modificationTime, objectID, updateID);
  }
}