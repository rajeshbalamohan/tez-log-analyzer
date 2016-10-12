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

import com.google.common.collect.Maps;
import org.apache.tez.log.IAnalyzer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ShuffleBlamedForAnalyzer extends BaseAnalyzer {
  private final Pattern BLAMED_FOR = Pattern.compile("TaskAttemptImpl:(.*) blamed for read error "
      + "from (" + ".*) at");

  //HDP 2.3.4 (as the log file format changed)
  private final Pattern NEW_BLAMED_FOR = Pattern.compile("TaskAttemptImpl\\|:(.*) blamed for read "
      + "error from (.*) at");

  private final Pattern HOST_PATTERN = Pattern.compile("task=(.*), containerHost=(.*), "
      + "localityMatchType");

  private final Map<String, String> hostMap = Maps.newHashMap();
  private final Map<String, Integer> fetcherFailure = Maps.newHashMap();

  private final Set<String> srcMachines = new HashSet<String>();
  private final Set<String> fetcherMachines = new HashSet<String>();

  @Override
  public void process(String line) throws IOException {
    if (line.contains("task") && line.contains("containerHost")) {
      Matcher matcher = HOST_PATTERN.matcher(line);
      while (matcher.find()) {
        String attempt = matcher.group(1).trim();
        String host = matcher.group(2).trim();
        fetcherFailure.put(attempt, 0); //Just initializing
        hostMap.put(attempt, host);
      }
    }

    if (line.contains("blamed for read error")) {
      if (!populate(BLAMED_FOR, line)) {
        populate(NEW_BLAMED_FOR, line);
      }
    }
  }

  private boolean populate(Pattern pattern, String line) {
    Matcher matcher = pattern.matcher(line);
    while (matcher.find()) {
      String srcAttempt = matcher.group(1).trim();
      String fetcherAttempt = matcher.group(2).trim();
      fetcherFailure.put(fetcherAttempt, fetcherFailure.get(fetcherAttempt) + 1);
      if (hostMap.get(srcAttempt) == null) {
        System.out.println("ISSUE");
      }
      srcMachines.add(hostMap.get(srcAttempt.trim()));
      fetcherMachines.add(hostMap.get(fetcherAttempt.trim()));
      return true;
    }
    return false;
  }

  @Override
  public String getAnalysis() throws IOException {
    StringBuilder sb = new StringBuilder();
    //Summary
    sb.append("Source Machines being blamed for \n");
    for (String src : srcMachines) {
      sb.append("\t" + src + "\n");
    }

    sb.append("Fetcher Machines \n");
    for (String fetcher : fetcherMachines) {
      sb.append("\t" + fetcher + "\n");
    }

    return sb.toString();
  }

  @Override
  public String getName() {
    return "Shuffle Blamed For";
  }

  @Override
  public Object getResult() {
    return new ShuffleBlamedForResult(Collections.unmodifiableSet(srcMachines), Collections
        .unmodifiableSet(fetcherMachines));
  }

  public static class ShuffleBlamedForResult {
    private final Set<String> srcMachines;
    private final Set<String> fetcherMachines;

    public ShuffleBlamedForResult(Set<String> src, Set<String> fetcher) {
      this.srcMachines = src;
      this.fetcherMachines = fetcher;
    }

    public Set<String> getSrcMachines() {
      return srcMachines;
    }

    public Set<String> getFetcherMachines() {
      return fetcherMachines;
    }
  }
}
