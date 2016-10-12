
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
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigAnalyzer extends BaseAnalyzer {

  private static final List<String> interestingPatterns = Lists.newLinkedList();
  static {
    interestingPatterns.add("Initializing task");
    interestingPatterns.add("MergerManager");
    interestingPatterns.add(" Setting up");
    interestingPatterns.add(" Shutting down");
    interestingPatterns.add("io.sort.mb");
  }
  private static final Pattern VertexParallismPattern = Pattern.compile("VertexName: (.*), "
      + "VertexParallelism: (.*), TaskAttemptID:(.*),");

  private List<String> interestingLines = Lists.newLinkedList();
  private Map<String, String> vertexMap = Maps.newLinkedHashMap();

  @Override
  public void process(String line) throws IOException {
    for(String p : interestingPatterns) {
      if (line.contains(p)) {
        interestingLines.add("line:" + getCurrentLineNumber() + " " + line);
      }
    }

    //Hardcoding vertex parallelism
    if (line.contains("VertexParallelism")) {
      Matcher matcher = VertexParallismPattern.matcher(line);
      while (matcher.find()) {
        String vertexName = matcher.group(1); //vertexName
        String parallelism = matcher.group(2); //parallelism
        vertexMap.put(vertexName, parallelism);
      }
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    return "Parallelism details : " + vertexMap + "\n"
        + Joiner.on('\n').join(interestingLines).toString() + "\n";
  }

  @Override public String getName() {
    return "Config Analyzer";
  }

  @Override public Object getResult() {
    return null;
  }
}
