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
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VertexMappingAnalyzer extends BaseAnalyzer {

  //vertexID, vertexName
  private Map<String, String> vertexMap = Maps.newLinkedHashMap();

  //2016-06-24 18:33:13,935 [INFO] [Dispatcher thread {Central}] |impl.VertexImpl|: Setting vertexManager to RootInputVertexManager for vertex_1462638223520_58911_1_09 [Map 12]
  private static final Pattern vertexMappingPattern =
      Pattern.compile("Setting vertexManager.* for (.*) \\[(.*)\\]");

  private static final Pattern redPattern =
      Pattern.compile("Creating (\\d+) tasks for vertex: (.*) \\[(.*)\\]");

  private static final String VERTEX_IMPL = "VertexImpl";

  @Override
  public void process(String line) throws IOException {
    if (line.contains(VERTEX_IMPL)) {
      Matcher matcher = vertexMappingPattern.matcher(line);
      while (matcher.find()) {
        String vertexID = matcher.group(1).trim();
        String vertexName = matcher.group(2).trim();
        vertexMap.put(vertexID, vertexName);
      }

      matcher = redPattern.matcher(line);
      while (matcher.find()) {
        String vertexID = matcher.group(2).trim();
        String vertexName = matcher.group(3).trim();
        vertexMap.put(vertexID, vertexName);
      }
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    Joiner.MapJoiner joiner = Joiner.on('\n').withKeyValueSeparator("=");
    return joiner.join(vertexMap);
  }

  @Override
  public String getName() {
    return "Vertex Mapping";
  }

  @Override
  public Object getResult() {
    return Collections.unmodifiableMap(vertexMap);
  }
}
