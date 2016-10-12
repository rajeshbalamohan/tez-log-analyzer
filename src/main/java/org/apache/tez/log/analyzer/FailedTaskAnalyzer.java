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
import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;
import org.apache.tez.log.IAnalyzer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FailedTaskAnalyzer extends BaseAnalyzer {

  private final Pattern FailedTaskPattern =
      Pattern.compile("Attempt failed on node: (.*):(.*) TA: "
          + "(.*) failed: true container: (.*) numFailedTAs: (\\d+)");

  private final Map<String, FailedTask> failedTaskMap = Maps.newTreeMap();

  @Override
  public void process(String line) throws IOException {
    Matcher matcher = FailedTaskPattern.matcher(line);
    while (matcher.find()) {

      String machine = matcher.group(1);
      String attempt = matcher.group(3);
      String container = matcher.group(4);
      int numFailedTAs = Integer.parseInt(matcher.group(5));
      FailedTask failedTask = new FailedTask(machine, attempt, container, numFailedTAs);
      failedTaskMap.put(attempt, failedTask);
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    Joiner.MapJoiner joiner = Joiner.on('\n').withKeyValueSeparator("=");
    return joiner.join(failedTaskMap);
  }

  @Override
  public String getName() {
    return "Failed Task Analyzer";
  }

  @Override
  public Object getResult() {
    return null;
  }

  public static final class FailedTask {
    private final String machine;
    private final String container;
    private final String attempt;
    private final int noOfFailures;

    FailedTask(String machine, String container, String attempt, int noOfFailures) {
      this.machine = machine;
      this.container = container;
      this.attempt = attempt;
      this.noOfFailures = noOfFailures;
    }

    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("machine", machine)
          .add("container", container)
          .add("attempt", attempt)
          .add("noOfFailures", noOfFailures).toString();
    }
  }
}
