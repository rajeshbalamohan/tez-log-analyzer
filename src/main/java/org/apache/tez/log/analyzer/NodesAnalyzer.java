package org.apache.tez.log.analyzer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tez.log.IAnalyzer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class NodesAnalyzer extends BaseAnalyzer {

  // machines --> task --> Container
  private Map<String, Map<String, String>> nodes = Maps.newTreeMap();

  private static Pattern nodesPattern =
      Pattern.compile("Assigning container to task: containerId=(.*), task=(.*), containerHost=(.*), containerPriority");

  @Override
  public void process(String line) throws IOException {
    if (line.contains("Assigning container to task")) {
      Matcher matcher = nodesPattern.matcher(line);
      while (matcher.find()) {
        String containerId = matcher.group(1);
        String task = matcher.group(2);
        //Get dagId from task attempt_1478350923850_0006_1_00_000022_0 (hardcoded now)
        String dagId = task.substring(0, 29);
        String node = matcher.group(3);
        Map<String, String> taskMap = nodes.get(node);
        if (taskMap == null) {
          taskMap = Maps.newHashMap();
          nodes.put(node, taskMap);
        }
        taskMap.put(task, containerId);
      }
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Map<String, String>> entry : nodes.entrySet()) {
      sb.append(entry.getKey());
      sb.append("\n");
    }
    sb.append("\nContainer Mapping\n");
    //container mapping
    for (Map.Entry<String, Map<String, String>> entry : nodes.entrySet()) {
      for(Map.Entry<String, String> innerEntry : entry.getValue().entrySet()) {
        sb.append(entry.getKey() + " : " + innerEntry.getKey() + " : " + innerEntry.getValue());
        sb.append("\n");
      }
    }
    return sb.toString();
  }

  @Override
  public String getName() {
    return "Publish set of nodes participating in the job";
  }

  @Override
  public Object getResult() {
    return Collections.unmodifiableMap(nodes);
  }
}
