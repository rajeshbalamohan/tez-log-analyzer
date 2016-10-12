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
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extract rack details. Sometimes useful for checking if all failures
 * are from same rack or not.
 */
public class RackResolverExtractor extends BaseAnalyzer {

  private static Pattern ResolverPattern = Pattern.compile("Resolved (.*) to (.*)");
  private Map<String, String> rackMap = new TreeMap();

  @Override
  public void process(String line) throws IOException {
    Matcher matcher = ResolverPattern.matcher(line);
    while (matcher.find()) {

      String machine = matcher.group(1); //machine
      String rack = matcher.group(2); //rack
      rackMap.put(machine, rack);
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    Joiner.MapJoiner joiner = Joiner.on('\n').withKeyValueSeparator("=");
    return "Number of nodes (as per job) : " + rackMap.size() + "\n" + joiner.join(rackMap);
  }

  @Override
  public String getName() {
    return "Rack Resolver";
  }

  @Override
  public Object getResult() {
    return Collections.unmodifiableMap(rackMap);
  }
}
