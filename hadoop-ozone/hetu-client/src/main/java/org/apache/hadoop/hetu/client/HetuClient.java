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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hetu.client.protocol.ClientProtocol;

import java.io.Closeable;
import java.io.IOException;

/**
 * OzoneClient connects to Ozone Cluster and
 * perform basic operations.
 */
public class HetuClient implements Closeable {

  /*
   * HetuClient connects to Hetu Cluster and
   * perform basic operations.
   *
   * +-------------+     +---+   +-------------------------------------+
   * | HetuClient  | --> | C |   | Hetu Store                          |
   * |_____________|     | l |   |  +-------------------------------+  |
   *                     | i |   |  | Database(s)                   |  |
   *                     | e |   |  |   +------------------------+  |  |
   *                     | n |   |  |   | Table(s)               |  |  |
   *                     | t |   |  |   |   +------------------+ |  |  |
   *                     |   |   |  |   |   | partition(s) ->  | |  |  |
   *                     | P |-->|  |   |   |    Value (s)     | |  |  |
   *                     |   |   |  |   |   |    -> tablet(s)  | |  |  |
   *                     | r |   |  |   |   |__________________| |  |  |
   *                     | o |   |  |   |                        |  |  |
   *                     | t |   |  |   |________________________|  |  |
   *                     | o |   |  |                               |  |
   *                     | c |   |  |_______________________________|  |
   *                     | o |   |                                     |
   *                     | l |   |_____________________________________|
   *                     |___|
   * Example:
   * HetuStore store = client.getHetuStore();
   * store.createDatabase(“database_one”, DatabaseArgs);
   * database.setQuota(“10 GB”);
   * OzoneDatabase database = store.getDatabase(“database_one”);
   * database.createTable(“table_one”, TableArgs);
   * table.setVersioning(true);
   * OzoneTable table = store.getTable("table_one");
   * table.createPartition("p1_20210120", PartitionArgs);
   * OzonePartition partition = store.getPartition("p1_20210120");
   * HetuOutputStream os = partition.createTablet(“tablet_one”, 1024);
   * os.write(byte[]);
   * os.close();
   * HetuInputStream is = partition.readTablet(“tablet_one”);
   * is.read();
   * is.close();
   * partition.deleteTablet(“tablet_one”);
   * table.deletePartition("p1_20210120");
   * database.deleteTable(“table_one”);
   * store.deleteDatabase(“database_one”);
   * client.close();
   */

  private final ClientProtocol proxy;
  private final HetuStore hetuStore;
  private  ConfigurationSource conf;

  /**
   * Creates a new OzoneClient object, generally constructed
   * using {@link HetuClientFactory}.
   * @param conf Configuration object
   * @param proxy ClientProtocol proxy instance
   */
  public HetuClient(ConfigurationSource conf, ClientProtocol proxy) {
    this.proxy = proxy;
    this.hetuStore = new HetuStore(conf, this.proxy);
    this.conf = conf;
  }

  @VisibleForTesting
  protected HetuClient(HetuStore hetuStore) {
    this.hetuStore = hetuStore;
    this.proxy = null;
    // For the unit test
    this.conf = new OzoneConfiguration();
  }
  /**
   * Returns the object store associated with the Ozone Cluster.
   * @return ObjectStore
   */
  public HetuStore getObjectStore() {
    return hetuStore;
  }

  /**
   * Returns the configuration of client.
   * @return ConfigurationSource
   */
  public ConfigurationSource getConfiguration() {
    return conf;
  }

  /**
   * Closes the client and all the underlying resources.
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    proxy.close();
  }
}
