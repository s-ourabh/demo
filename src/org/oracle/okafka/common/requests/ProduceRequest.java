/*
** OKafka Java Client version 0.8.
**
** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * 04/20/2020: This file is modified to support Kafka Java Client compatability to Oracle Transactional Event Queues.
 *
 */

package org.oracle.okafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiMessage;
import org.oracle.okafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.AbstractResponse;
import org.oracle.okafka.common.requests.AbstractRequest;

public class ProduceRequest extends AbstractRequest{
	//private Map<String, Map<TopicPartition, MemoryRecords>> produceRecordsByTopic;
	private final short acks;
    private final int timeout;
    //private final Map<TopicPartition, MemoryRecords> partitionRecords;
    private final TopicPartition topicPartition;
    private final MemoryRecords memoryRecords;
    
	
	public ProduceRequest(TopicPartition topicPartition, MemoryRecords memoryRecords, short acks, int timeout) {
		super(ApiKeys.PRODUCE, (short)1);
		this.topicPartition = topicPartition;
    	this.memoryRecords = memoryRecords;
    	this.acks = acks;
    	this.timeout = timeout;
	}

	/*public Map<String, Map<TopicPartition, MemoryRecords>> getproduceRecordsByTopic() {
		return produceRecordsByTopic;
	}*/
	
	public TopicPartition getTopicpartition() {
		return topicPartition;
	}
	
	public MemoryRecords getMemoryRecords() {
		return  memoryRecords;
	}
	
	public static class Builder extends AbstractRequest.Builder<ProduceRequest> {
        private final short acks;
        private final int timeout;
        //private final Map<TopicPartition, MemoryRecords> partitionRecords;
        private final TopicPartition topicPartition;
        private final MemoryRecords memoryRecords;
        
        public Builder(TopicPartition topicPartition, MemoryRecords memoryRecords, short acks, int timeout) {
        	super(ApiKeys.PRODUCE);
        	this.topicPartition = topicPartition;
        	this.memoryRecords = memoryRecords;
        	this.acks = acks;
        	this.timeout = timeout;

        }
        
        @Override
        public ProduceRequest build() {
            return new ProduceRequest(topicPartition, memoryRecords, acks, timeout);
        }

		@Override
		public ProduceRequest build(short version) {
			return new ProduceRequest(topicPartition, memoryRecords, acks, timeout);
		}
	}

	@Override
	public ApiMessage data() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
