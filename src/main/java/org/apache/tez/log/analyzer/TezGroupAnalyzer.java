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

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * Print Tez grouper details
 */
public class TezGroupAnalyzer extends BaseAnalyzer {

  // tez grouping can't do anything when original splits is much less than desired
  private final String canNotDoAnythingInTez = "Using original number of splits";
  private final String memoryRequested = "App total resource memory";

  private final List<String> interestedLines = Lists.newLinkedList();
  private final List<String> cantDoAnythingList = Lists.newLinkedList();

  @Override
  public void process(String line) throws IOException {
    if (line.contains("Grouper")) {
      interestedLines.add(line);
      if (line.contains(canNotDoAnythingInTez)) {
        cantDoAnythingList.add(line);
      }
    }
    if (line.contains(memoryRequested)) {
      interestedLines.add(line);
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    StringBuilder sb = new StringBuilder();
    addListToString(interestedLines, sb);
    sb.append("\n\n Can't do anything in tez for the following..i.e can't change parallelism\n\n");
    addListToString(cantDoAnythingList, sb);
    return sb.toString();
  }

  void addListToString(List<String> list, StringBuilder sb) {
    for (String l : list) {
      sb.append(l).append("\n");
    }
  }

  @Override
  public String getName() {
    return "Tez Group Analyzer";
  }

  @Override
  public Object getResult() {
    List<String> result = Lists.newLinkedList();
    result.addAll(interestedLines);
    result.addAll(cantDoAnythingList);
    return result;
  }
}
