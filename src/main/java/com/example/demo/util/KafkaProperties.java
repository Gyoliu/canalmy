/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.demo.util;

public interface KafkaProperties
{
  String topic = "testlx";
  String bootstrapServers = "yksp020008.youkeshu.com:6667,yksp020007.youkeshu.com:6667,yksp020006.youkeshu.com:6667";
  String groupId = "DemoConsumer";
  String enableAutoCommit = "true";
  String autoCommitIntervalMs = "1000";
  String sessionTimeoutMs = "30000";
  //String keyDeserializer = "org.apache.kafka.common.serialization.IntegerDeserializer";
  //String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";


  String keySerializer = "org.apache.kafka.common.serialization.IntegerSerializer";
  String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";

}
