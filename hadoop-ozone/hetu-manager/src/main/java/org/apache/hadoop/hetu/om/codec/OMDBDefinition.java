/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hetu.om.codec;

import org.apache.hadoop.hdds.utils.TransactionInfoCodec;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hetu.hm.helpers.OmDatabaseArgs;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.hetu.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.codec.HmDatabaseArgsCodec;
import org.apache.hadoop.ozone.om.codec.OmBucketInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmKeyInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmMultipartKeyInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmPartitionInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmPrefixInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmTableInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmTabletInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmVolumeArgsCodec;
import org.apache.hadoop.ozone.om.codec.RepeatedOmKeyInfoCodec;
import org.apache.hadoop.ozone.om.codec.RepeatedOmTabletInfoCodec;
import org.apache.hadoop.ozone.om.codec.S3SecretValueCodec;
import org.apache.hadoop.ozone.om.codec.TokenIdentifierCodec;
import org.apache.hadoop.ozone.om.codec.UserDatabaseInfoCodec;
import org.apache.hadoop.ozone.om.codec.UserVolumeInfoCodec;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmPartitionInfo;
import org.apache.hadoop.ozone.om.helpers.OmTableArgs;
import org.apache.hadoop.ozone.om.helpers.OmTableInfo;
import org.apache.hadoop.ozone.om.helpers.OmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmTabletInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;

import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;

/**
 * Class defines the structure and types of the om.db.
 */
public class OMDBDefinition implements DBDefinition {

  public static final DBColumnFamilyDefinition<String, RepeatedOmKeyInfo>
            DELETED_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.DELETED_TABLE,
                    String.class,
                    new StringCodec(),
                    RepeatedOmKeyInfo.class,
                    new RepeatedOmKeyInfoCodec(true));

  public static final DBColumnFamilyDefinition<String,
            OzoneManagerStorageProtos.PersistedUserVolumeInfo>
            USER_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.USER_TABLE,
                    String.class,
                    new StringCodec(),
                    OzoneManagerStorageProtos.PersistedUserVolumeInfo.class,
                    new UserVolumeInfoCodec());

  public static final DBColumnFamilyDefinition<String, OmVolumeArgs>
            VOLUME_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.VOLUME_TABLE,
                    String.class,
                    new StringCodec(),
                    OmVolumeArgs.class,
                    new OmVolumeArgsCodec());

  public static final DBColumnFamilyDefinition<String, OmKeyInfo>
            OPEN_KEY_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.OPEN_KEY_TABLE,
                    String.class,
                    new StringCodec(),
                    OmKeyInfo.class,
                    new OmKeyInfoCodec(true));

  public static final DBColumnFamilyDefinition<String, OmKeyInfo>
            KEY_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.KEY_TABLE,
                    String.class,
                    new StringCodec(),
                    OmKeyInfo.class,
                    new OmKeyInfoCodec(true));

  public static final DBColumnFamilyDefinition<String, OmBucketInfo>
            BUCKET_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.BUCKET_TABLE,
                    String.class,
                    new StringCodec(),
                    OmBucketInfo.class,
                    new OmBucketInfoCodec());

  public static final DBColumnFamilyDefinition<String, OmMultipartKeyInfo>
            MULTIPART_INFO_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.MULTIPARTINFO_TABLE,
                    String.class,
                    new StringCodec(),
                    OmMultipartKeyInfo.class,
                    new OmMultipartKeyInfoCodec());

  public static final DBColumnFamilyDefinition<String, OmPrefixInfo>
            PREFIX_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.PREFIX_TABLE,
                    String.class,
                    new StringCodec(),
                    OmPrefixInfo.class,
                    new OmPrefixInfoCodec());

  public static final DBColumnFamilyDefinition<OzoneTokenIdentifier, Long>
            DTOKEN_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.DELEGATION_TOKEN_TABLE,
                    OzoneTokenIdentifier.class,
                    new TokenIdentifierCodec(),
                    Long.class,
                    new LongCodec());

  public static final DBColumnFamilyDefinition<String, S3SecretValue>
            S3_SECRET_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.S3_SECRET_TABLE,
                    String.class,
                    new StringCodec(),
                    S3SecretValue.class,
                    new S3SecretValueCodec());

  public static final DBColumnFamilyDefinition<String, TransactionInfo>
            TRANSACTION_INFO_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.TRANSACTION_INFO_TABLE,
                    String.class,
                    new StringCodec(),
                    TransactionInfo.class,
                    new TransactionInfoCodec());

  public static final DBColumnFamilyDefinition<String, OmDatabaseArgs>
          DATABASE_TABLE =
          new DBColumnFamilyDefinition<>(
                  OmMetadataManagerImpl.DATABASE_TABLE,
                  String.class,
                  new StringCodec(),
                  OmDatabaseArgs.class,
                  new HmDatabaseArgsCodec());

  public static final DBColumnFamilyDefinition<String, OmTableInfo>
          META_TABLE =
          new DBColumnFamilyDefinition<>(
                  OmMetadataManagerImpl.META_TABLE,
                  String.class,
                  new StringCodec(),
                  OmTableInfo.class,
                  new OmTableInfoCodec());

  public static final DBColumnFamilyDefinition<String, OmPartitionInfo>
          PARTITION_TABLE =
          new DBColumnFamilyDefinition<>(
                  OmMetadataManagerImpl.PARTITION_TABLE,
                  String.class,
                  new StringCodec(),
                  OmPartitionInfo.class,
                  new OmPartitionInfoCodec());

  public static final DBColumnFamilyDefinition<String, OmTabletInfo>
          OPEN_TABLET_TABLE =
          new DBColumnFamilyDefinition<>(
                  OmMetadataManagerImpl.OPEN_TABLET_TABLE,
                  String.class,
                  new StringCodec(),
                  OmTabletInfo.class,
                  new OmTabletInfoCodec(true));

  public static final DBColumnFamilyDefinition<String, OmTabletInfo>
          TABLET_TABLE =
          new DBColumnFamilyDefinition<>(
                  OmMetadataManagerImpl.TABLET_TABLE,
                  String.class,
                  new StringCodec(),
                  OmTabletInfo.class,
                  new OmTabletInfoCodec(true));

  public static final DBColumnFamilyDefinition<String, RepeatedOmTabletInfo>
          DELETED_TABLET =
          new DBColumnFamilyDefinition<>(
                  OmMetadataManagerImpl.DELETED_TABLET,
                  String.class,
                  new StringCodec(),
                  RepeatedOmTabletInfo.class,
                  new RepeatedOmTabletInfoCodec(true));

  public static final DBColumnFamilyDefinition<String,
          OzoneManagerStorageProtos.PersistedUserDatabaseInfo>
          USER_TABLE_DB =
          new DBColumnFamilyDefinition<>(
                  OmMetadataManagerImpl.USER_TABLE_DB,
                  String.class,
                  new StringCodec(),
                  OzoneManagerStorageProtos.PersistedUserDatabaseInfo.class,
                  new UserDatabaseInfoCodec());

  @Override
  public String getName() {
    return OzoneConsts.OM_DB_NAME;
  }

  @Override
  public String getLocationConfigKey() {
    return OMConfigKeys.OZONE_OM_DB_DIRS;
  }

  @Override
  public DBColumnFamilyDefinition[] getColumnFamilies() {
    return new DBColumnFamilyDefinition[] {DELETED_TABLE, USER_TABLE,
        VOLUME_TABLE, OPEN_KEY_TABLE, KEY_TABLE,
        BUCKET_TABLE, MULTIPART_INFO_TABLE, PREFIX_TABLE, DTOKEN_TABLE,
        DATABASE_TABLE, META_TABLE, PARTITION_TABLE, OPEN_TABLET_TABLE,
        TABLET_TABLE, DELETED_TABLET, USER_TABLE_DB,
        S3_SECRET_TABLE, TRANSACTION_INFO_TABLE};
  }
}
