/*
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

package org.apache.hadoop.ozone.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HetuStore class is responsible for the client operations that can be
 * performed on Hetu Store.
 */
public class HetuStore {

  /**
   * The proxy used for connecting to the cluster and perform
   * client operations.
   */
  // TODO: remove rest api and client
  private final ClientProtocol proxy;

  /**
   * Cache size to be used for listDatabase calls.
   */
  private int listCacheSize;

  private String databaseName;

  /**
   * Creates an instance of HetuStore.
   * @param conf Configuration object.
   * @param proxy ClientProtocol proxy.
   */
  public HetuStore(ConfigurationSource conf, ClientProtocol proxy, String databaseName) {
    this.proxy = TracingUtil.createProxy(proxy, ClientProtocol.class, conf);
    this.listCacheSize = HddsClientUtils.getListCacheSize(conf);
    this.databaseName = databaseName;
  }

  @VisibleForTesting
  protected HetuStore(String databaseName) {
    // For the unit test
    OzoneConfiguration conf = new OzoneConfiguration();
    this.databaseName = databaseName;
    proxy = null;
  }

  @VisibleForTesting
  public ClientProtocol getClientProxy() {
    return proxy;
  }

  /**
   * Creates the database with default values.
   * @param databaseName Name of the database to be created.
   * @throws IOException
   */
  public void createDatabase(String databaseName) throws IOException {
    proxy.createDatabase(databaseName);
  }

  /**
   * Creates the volume.
   * @param databaseName Name of the database to be created.
   * @param databaseArgs Database properties.
   * @throws IOException
   */
  public void createDatabase(String databaseName, DatabaseArgs databaseArgs)
      throws IOException {
    proxy.createDatabase(databaseName, databaseArgs);
  }

  /**
   * Returns the database information.
   * @param databaseName Name of the database.
   * @return HetuDatabase
   * @throws IOException
   */
  public HetuDatabase getDatabase(String databaseName) throws IOException {
    HetuDatabase database = proxy.getDatabaseDetails(databaseName);
    return database;
  }

  /**
   * Returns Iterator to iterate over all the volumes in object store.
   * The result can be restricted using volume prefix, will return all
   * volumes if volume prefix is null.
   *
   * @param databasePrefix Database prefix to match
   * @return {@code Iterator<OzoneVolume>}
   */
  public Iterator<? extends HetuDatabase> listDatabase(String databasePrefix)
      throws IOException {
    return listDatabase(databasePrefix, null);
  }

  /**
   * Returns Iterator to iterate over all the volumes after prevVolume in object
   * store. If prevVolume is null it iterates from the first volume.
   * The result can be restricted using volume prefix, will return all
   * volumes if volume prefix is null.
   *
   * @param databasePrefix Database prefix to match
   * @param prevDatabase Database will be listed after this database name
   * @return {@code Iterator<HetuDatabase>}
   */
  public Iterator<? extends HetuDatabase> listDatabase(String databasePrefix,
      String prevDatabase) throws IOException {
    return new DatabaseIterator(null, databasePrefix, prevDatabase);
  }

  /**
   * Returns Iterator to iterate over the list of volumes after prevVolume
   * accessible by a specific user. The result can be restricted using volume
   * prefix, will return all volumes if volume prefix is null. If user is not
   * null, returns the volume of current user.
   *
   * Definition of accessible:
   * When ACL is enabled, accessible means the user has LIST permission.
   * When ACL is disabled, accessible means the user is the owner of the volume.
   * See {@code OzoneManager#listVolumeByUser}.
   *
   * @param user User Name
   * @param databasePrefix Volume prefix to match
   * @param prevDatabase Volumes will be listed after this volume name
   * @return {@code Iterator<OzoneVolume>}
   */
  public Iterator<? extends HetuDatabase> listDatabaseByUser(String user,
      String databasePrefix, String prevDatabase)
      throws IOException {
    if(Strings.isNullOrEmpty(user)) {
      user = UserGroupInformation.getCurrentUser().getUserName();
    }
    return new DatabaseIterator(user, databasePrefix, prevDatabase);
  }

  /**
   * Deletes the database.
   * @param databaseName Name of the database.
   * @throws IOException
   */
  public void deleteDatabase(String databaseName) throws IOException {
    proxy.deleteDatabase(databaseName);
  }

  /**
   * Deletes the database.
   * @param hetuDatabase Name of the database.
   * @throws IOException
   */
  public HetuDatabase updateDatabase(HetuDatabase hetuDatabase) throws IOException {
    return proxy.updateDatabase(hetuDatabase);
  }

  /**
   * An Iterator to iterate over {@link OzoneVolume} list.
   */
  private class DatabaseIterator implements Iterator<HetuDatabase> {

    private String user = null;
    private String databasePrefix = null;

    private Iterator<HetuDatabase> currentIterator;
    private HetuDatabase currentDatabase;

    /**
     * Creates an Iterator to iterate over all volumes after
     * prevVolume of the user. If prevVolume is null it iterates from the
     * first volume. The returned volumes match volume prefix.
     * @param user user name
     * @param databasePrefix database prefix to match
     */
    DatabaseIterator(String user, String databasePrefix, String prevDatabase) {
      this.user = user;
      this.databasePrefix = databasePrefix;
      this.currentDatabase = null;
      this.currentIterator = getNextListOfDatabase(prevDatabase).iterator();
    }

    @Override
    public boolean hasNext() {
      // IMPORTANT: Without this logic, remote iteration will not work.
      // Removing this will break the listVolume call if we try to
      // list more than 1000 (ozone.client.list.cache ) volumes.
      if (!currentIterator.hasNext() && currentDatabase != null) {
        currentIterator = getNextListOfDatabase(currentDatabase.getName())
            .iterator();
      }
      return currentIterator.hasNext();
    }

    @Override
    public HetuDatabase next() {
      if(hasNext()) {
        currentDatabase = currentIterator.next();
        return currentDatabase;
      }
      throw new NoSuchElementException();
    }

    /**
     * Returns the next set of volume list using proxy.
     * @param prevDatabase previous database, this will be excluded from the result
     * @return {@code List<OzoneVolume>}
     */
    private List<HetuDatabase> getNextListOfDatabase(String prevDatabase) {
      try {
        //if user is null, we do list of all volumes.
        if(user != null) {
          return proxy.listDatabase(user, databasePrefix, prevDatabase, listCacheSize);
        }
        return proxy.listDatabase(databasePrefix, prevDatabase, listCacheSize);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Get a valid Delegation Token.
   *
   * @param renewer the designated renewer for the token
   * @return Token<OzoneDelegationTokenSelector>
   * @throws IOException
   */
  public Token<OzoneTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return proxy.getDelegationToken(renewer);
  }

  /**
   * Renew an existing delegation token.
   *
   * @param token delegation token obtained earlier
   * @return the new expiration time
   * @throws IOException
   */
  public long renewDelegationToken(Token<OzoneTokenIdentifier> token)
      throws IOException {
    return proxy.renewDelegationToken(token);
  }

  /**
   * Cancel an existing delegation token.
   *
   * @param token delegation token
   * @throws IOException
   */
  public void cancelDelegationToken(Token<OzoneTokenIdentifier> token)
      throws IOException {
    proxy.cancelDelegationToken(token);
  }

  /**
   * @return canonical service name of ozone delegation token.
   */
  public String getCanonicalServiceName() {
    return proxy.getCanonicalServiceName();
  }

}
