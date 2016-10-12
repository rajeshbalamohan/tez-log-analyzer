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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tez.log.IAnalyzer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SplitsAnalyzer extends BaseAnalyzer {

  private Map<String, String> splitsMap = Maps.newLinkedHashMap();
  private List<String> debugList = Lists.newLinkedList();
  private static final Pattern pattern = Pattern.compile("(.*)\\{(.*)\\} (.*) number of splits ("
      + ".*)");
  private static final Pattern oldPattern = Pattern.compile("(.*)\\[(.*)\\] (.*) number of splits ("
      + ".*)");

  private static final Pattern splitGroupPattern = Pattern.compile("\\{(.*)\\}(.*)");

  //reducer pattern
  private static final Pattern redPattern =
      Pattern.compile("Creating (\\d+) tasks for vertex: (.*) \\[(.*)\\]");

  @Override
  public void process(String line) throws IOException {
    if (line.contains("number of splits ")) {
      if (!populate(pattern, line)) {
        populate(oldPattern, line);
      }
    }
    if (line.contains("tasks for vertex:")) {
      Matcher matcher = redPattern.matcher(line);
      while (matcher.find()) {
        String splits = matcher.group(1).trim();
        String vertexID = matcher.group(2).trim();
        String vertexName = matcher.group(3).trim();
        splitsMap.put(vertexName + ", " + vertexID, splits);
      }
    }

    if (line.contains("SplitsGrouper") ||
        line.contains("HiveSplitGenerator") ||
        line.contains("SplitGrouper")) {
      debugList.add("line:" + getCurrentLineNumber() + " " + line);
    }
  }

  private boolean populate(Pattern pattern, String line) {
    Matcher matcher = pattern.matcher(line);
    while (matcher.find()) {
      String time = matcher.group(1).trim();
      String vertexName = matcher.group(2).trim();
      String splits = matcher.group(4).trim();
      splitsMap.put(time + "," + vertexName, splits);
      return true;
    }
    return false;
  }

  @Override
  public String getAnalysis() throws IOException {
    Joiner.MapJoiner joiner = Joiner.on('\n').withKeyValueSeparator("=");
    return joiner.join(splitsMap) + Joiner.on('\n').join(debugList);
  }

  @Override
  public String getName() {
    return "Split Details (in case of multiple DAGs, analyze by time stamp. E.g Dag1 and Dag10 "
        + "can have Map 1)";
  }

  @Override
  public Object getResult() {
    return Collections.unmodifiableMap(splitsMap);
  }
}
