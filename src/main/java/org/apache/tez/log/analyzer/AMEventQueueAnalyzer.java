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

import com.google.common.base.Joiner;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Get details on AM event queue. Sometimes for very large DAGs, this could be a problem.
 * Need to ensure that tez.am.use.concurrent-dispatcher is enabled.
 */
public class AMEventQueueAnalyzer extends BaseAnalyzer {
  private final List<String> queue = new LinkedList();

  @Override
  public void process(String line) throws IOException {
    if (line.contains("Size of event-queue is")) {
        queue.add(line);
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    return Joiner.on('\n').join(queue);
  }

  @Override
  public String getName() {
    return "AM Event Queue";
  }

  @Override
  public Object getResult() {
    return Collections.unmodifiableList(queue);
  }
}
