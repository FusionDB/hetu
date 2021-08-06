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
package org.apache.hadoop.ozone.om.helpers;

/**
 * This class represents a open tablet "session". A session here means a tablet is
 * opened by a specific client, the client sends the handler to server, such
 * that servers can recognize this client, and thus know how to close the tablet.
 */
public class OpenTabletSession {
  private final long id;
  private final OmTabletInfo tabletInfo;
  // the version of the tablet when it is being opened in this session.
  // a block that has a create version equals to open version means it will
  // be committed only when this open session is closed.
  private long openVersion;

  public OpenTabletSession(long id, OmTabletInfo info, long version) {
    this.id = id;
    this.tabletInfo = info;
    this.openVersion = version;
  }

  public long getOpenVersion() {
    return this.openVersion;
  }

  public OmTabletInfo getTabletInfo() {
    return tabletInfo;
  }

  public long getId() {
    return id;
  }
}
