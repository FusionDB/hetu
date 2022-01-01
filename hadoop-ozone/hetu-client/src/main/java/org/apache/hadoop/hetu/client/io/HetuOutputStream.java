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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hetu.client.io;

import org.apache.hadoop.crypto.CryptoOutputStream;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;

import java.io.IOException;
import java.io.OutputStream;

/**
 * OzoneOutputStream is used to write data into Ozone.
 * It uses SCM's {@link TabletOutputStream} for writing the data.
 */
public class HetuOutputStream extends OutputStream {

  private final OutputStream outputStream;

  /**
   * Constructs OzoneOutputStream with TabletOutputStream.
   *
   * @param outputStream
   */
  public HetuOutputStream(OutputStream outputStream) {
    this.outputStream = outputStream;
  }

  @Override
  public void write(int b) throws IOException {
    outputStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    outputStream.write(b, off, len);
  }

  @Override
  public synchronized void flush() throws IOException {
    outputStream.flush();
  }

  @Override
  public synchronized void close() throws IOException {
    //commitKey can be done here, if needed.
    outputStream.close();
  }

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    if (outputStream instanceof TabletOutputStream) {
      return ((TabletOutputStream) outputStream).getCommitUploadPartInfo();
    } else  if (outputStream instanceof CryptoOutputStream) {
      OutputStream wrappedStream =
          ((CryptoOutputStream) outputStream).getWrappedStream();
      if (wrappedStream instanceof TabletOutputStream) {
        return ((TabletOutputStream) wrappedStream).getCommitUploadPartInfo();
      }
    }
    // Otherwise return null.
    return null;
  }

  public OutputStream getOutputStream() {
    return outputStream;
  }
}
