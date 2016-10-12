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
import com.google.common.collect.Maps;
import org.apache.tez.log.IAnalyzer;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Print all tasks which have started but not finished
 */
public class StuckTaskAnalyzer extends BaseAnalyzer {
  private Map<String, String> startedTasks = Maps.newTreeMap();
  private Map<String, String> finishedTasks = Maps.newTreeMap();

  private static final String TASK_STARTED = "TASK_STARTED";
  private static final String TASK_FINISHED = "TASK_FINISHED";

  private static final Pattern startPattern =
      Pattern.compile("vertexName=(.*), taskId=(.*), scheduledTime=(.*)");
  private static final Pattern finishPattern =
      Pattern.compile("vertexName=(.*), taskId=(.*), startTime=(.*)");

  @Override
  public void process(String line) throws IOException {
    if (line.contains(TASK_STARTED)) {
      match(line, startPattern, startedTasks);
    } else if (line.contains(TASK_FINISHED)) {
      match(line, finishPattern, finishedTasks);
    }
  }

  private void match(String line, Pattern pattern, Map<String, String> map) {
    Matcher matcher = pattern.matcher(line);
    while (matcher.find()) {
      String vertexName = matcher.group(1).trim();
      String taskId = matcher.group(2).trim();
      map.put(taskId, vertexName);
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    Map<String, String> diffMap = Maps.newLinkedHashMap();
    for (Map.Entry<String, String> entry : startedTasks.entrySet()) {
      if (finishedTasks.get(entry.getKey()) == null) {
        diffMap.put(entry.getKey(), entry.getValue());
      }
    }
    Joiner.MapJoiner joiner = Joiner.on('\n').withKeyValueSeparator("=");
    return joiner.join(diffMap);
  }

  @Override
  public String getName() {
    return "Stuck Task Analyzer (replace task by attempt & search in "
        + "Task Attempt Started to known node details)";
  }

  @Override public Object getResult() {
    return null;
  }
}
