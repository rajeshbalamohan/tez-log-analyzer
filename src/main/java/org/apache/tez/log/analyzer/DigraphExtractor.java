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

/**
 * Extract digraph from app logs. In case applogs are huge
 * it is difficult to extract.
 *
 * Latest versions support getting only AM logs. But sometimes
 * we get huge files and its painful to get digraph
 */
public class DigraphExtractor extends BaseAnalyzer {
  private StringBuilder sb = new StringBuilder();

  private static final String STARTING = "digraph";
  private static final String ENDING = "LogType";

  // To track when to start and end
  private boolean started = false;

  @Override
  public void process(String line) throws IOException {
    if (line.startsWith(STARTING)) {
      started = true;
      sb.append("Line Number:" + getCurrentLineNumber()).append("\n");
    }
    if (started) {
      sb.append(line).append("\n");
    }
    if (line.startsWith(ENDING)) {
      started = false; //reset back
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    return sb.toString();
  }

  @Override
  public String getName() {
    return "Digraph";
  }

  @Override
  public Object getResult() {
    return sb.toString();
  }
}
