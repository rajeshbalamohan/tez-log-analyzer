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

package org.apache.tez.log.analyzer;

import java.io.IOException;

public class VersionInfo extends BaseAnalyzer {
  private StringBuilder sb = new StringBuilder();
  private static final String VERSION_INFO = "versionInfo=";
  private static final String CLIENT_VERSION = "Comparing client version with AM version";

  @Override
  public void process(String line) throws IOException {
    if (line.contains(VERSION_INFO)) {
      sb.append("line:").append(getCurrentLineNumber()).append(" ").append(line).append("\n");
    }
    if (line.contains(CLIENT_VERSION)) {
      sb.append("line:").append(getCurrentLineNumber()).append(" ").append(line).append("\n");
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    return sb.toString();
  }

  @Override
  public String getName() {
    return "Version Details";
  }

  @Override
  public Object getResult() {
    return sb.toString();
  }
}
