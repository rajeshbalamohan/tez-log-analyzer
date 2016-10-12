package org.apache.tez.log.analyzer;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.tez.log.IAnalyzer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
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
public class HashTableAnalyzer extends BaseAnalyzer {
  private static final Pattern loadHashTable =
      Pattern.compile("method=LoadHashtable start=(\\d+) end=(\\d+) duration=(\\d+) ");
  private static final Pattern memory =
      Pattern.compile("Memory manager allocates (\\d+) bytes for the loading hashtable");
  private List<String> loadTimes = Lists.newLinkedList();

  @Override
  public void process(String line) throws IOException {
    Matcher matcher = loadHashTable.matcher(line);
    while (matcher.find()) {
      //loadTimes.add(matcher.group(3));
      //add entire line
      loadTimes.add(line);
    }
    matcher = memory.matcher(line);
    while (matcher.find()) {
      loadTimes.add("line:" + getCurrentLineNumber() + " " + line);
    }
  }

  @Override
  public String getAnalysis() throws IOException {
    Collections.sort(loadTimes);
    return Joiner.on("\n").join(loadTimes);
  }

  @Override
  public String getName() {
    return "HashTable load times";
  }

  @Override
  public Object getResult() {
    return Collections.unmodifiableList(loadTimes);
  }
}
