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

package org.apache.hadoop.hetu.om.block;

import org.apache.hadoop.hetu.om.IOzoneAcl;
import org.apache.hadoop.ozone.om.helpers.*;

import java.io.IOException;
import java.util.List;

/**
 * Ozone Manager Tablet interface.
 */
public interface OzoneManagerTablet extends IOzoneAcl {

  /**
   * Get block status for a Tablet.
   *
   * @param args          the args of the tablet provided by client.
   * @return block status.
   * @throws IOException if tablet or partition or table or database does not exist
   */
  OzoneTabletStatus getTabletStatus(OmTabletArgs args) throws IOException;

  /**
   * Get block status for a tablet.
   *
   * @param args          the args of the tablet provided by client.
   * @param clientAddress a hint to tablet manager, order the datanode in returned
   *                      pipeline by distance between client and datanode.
   * @return block status.
   * @throws IOException if tablet or partition or table or database does not exist
   */
  OzoneTabletStatus getTabletStatus(OmTabletArgs args, String clientAddress)
          throws IOException;

  OpenTabletSession createTablet(OmTabletArgs args, boolean isOverWrite)
          throws IOException;

  /**
   * Look up a block. Return the info of the tablet to client side.
   *
   * @param args the args of the tablet provided by client.
   * @param clientAddress a hint to tablet manager, order the datanode in returned
   *                      pipeline by distance between client and datanode.
   * @return a OmTabletInfo instance client uses to talk to container.
   * @throws IOException
   */
  OmTabletInfo lookupTablet(OmTabletArgs args, String clientAddress) throws IOException;

  /**
   * List the status for a tablet and its contents.
   *
   * @param tabletArgs       the args of the tablet provided by client.
   * @param startTablet      Tablet from which listing needs to start. If startTablet
   *                      exists its status is included in the final list.
   * @param numEntries    Number of entries to list from the start tablet
   * @return list of tablet status
   * @throws IOException if tablet or partition or table or database does not exist
   */
  List<OzoneTabletStatus> listStatus(OmTabletArgs tabletArgs,
                                   String startTablet, long numEntries)
          throws IOException;

  /**
   * List the status for a tablet and its contents.
   *
   * @param tabletArgs       the args of the tablet provided by client.
   * @param startTablet      tablet from which listing needs to start. If startTablet
   *                      exists its status is included in the final list.
   * @param numEntries    Number of entries to list from the start tablet
   * @param clientAddress a hint to tablet manager, order the datanode in returned
   *                      pipeline by distance between client and datanode.
   * @return list of tablet status
   * @throws IOException if tablet or partition or table or database does not exist
   */
  List<OzoneTabletStatus> listStatus(OmTabletArgs tabletArgs,
      String startTablet, long numEntries, String clientAddress)
          throws IOException;
}
