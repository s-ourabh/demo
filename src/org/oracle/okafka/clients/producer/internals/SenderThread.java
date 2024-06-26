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

package org.oracle.okafka.clients.producer.internals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.oracle.okafka.clients.KafkaClient;
import org.oracle.okafka.clients.Metadata;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.oracle.okafka.common.requests.ProduceRequest;
import org.oracle.okafka.clients.producer.KafkaProducer;
import org.oracle.okafka.clients.producer.ProducerConfig;
import org.oracle.okafka.clients.producer.internals.ProducerBatch;
import org.oracle.okafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.SenderMetricsRegistry;
import org.oracle.okafka.common.requests.ProduceResponse;
import org.oracle.okafka.common.utils.MessageIdConverter;
import org.oracle.okafka.common.Node;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;
/**
 * The background thread that handles the sending of produce requests to the Kafka cluster. This thread makes metadata
 * requests to renew its view of the cluster and then sends produce requests to the appropriate nodes.
 */
public class SenderThread implements Runnable {

	private final Logger log;
    /* the record accumulator that batches records */
    private final RecordAccumulator accumulator;

    private final KafkaClient client;
    
	/* the metadata for the client */
	private final Metadata metadata;
    
	/*
	 * the flag indicating whether the producer should guarantee the message order
	 * on the broker or not.
	 */
	private final boolean guaranteeMessageOrder;

    /* the client id used to identify this client in requests to the server */
    private final String clientId;

    /* the current correlation id to use when sending requests to servers */
    private int correlation;
    
    private final int maxRequestSize;
    
    /* the number of times to retry a failed request before giving up */
	private final int retries;

    /* the number of acknowledgements to request from the server */
    private final short acks;

    /* the clock instance used for getting the time */
    private final Time time;
    
    /* the max time to wait for the server to respond to the request */
	private final int requestTimeoutMs;

	/* The max time to wait before retrying a request which has failed */
	private final long retryBackoffMs;


    /* true while the sender thread is still running */
    private volatile boolean running;

    /* true when the caller wants to ignore all unsent/inflight messages and force close.  */
    private volatile boolean forceClose;
    
    private final ProducerConfig config;
    
    public SenderThread(LogContext logContext,String clientId, KafkaClient client, Metadata metadata, RecordAccumulator accumulator,
			boolean guaranteeMessageOrder, ProducerConfig pConfig, short acks, int retries,
			SenderMetricsRegistry metricsRegistry, Time time) {
		this.log = logContext.logger(SenderThread.class);
		this.clientId = clientId;
        this.accumulator = accumulator;
        this.client = client;
        this.metadata = metadata;
        this.guaranteeMessageOrder = guaranteeMessageOrder;
        this.config = pConfig;
        this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
        this.correlation = 0;
        this.running = true;
        this.acks = acks;
        this.time = time;
        this.retries = retries;
        this.requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
		this.retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
    }

    /**
     * The main run loop for the sender thread
     */
    public void run() {
    	log.debug("Starting Kafka producer I/O thread.");
        // main loop, runs until close is called
        while (running) {
            try {
                run(time.milliseconds());              
            } catch (Exception e) {
            	log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }
        log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");
        // okay we stopped accepting requests but there may still be
        // requests in the accumulator or waiting for acknowledgment,
        // wait until these are completed.
        while (!forceClose && this.accumulator.hasUndrained()) {
            try {
                run(time.milliseconds());
            } catch (Exception e) {
            	log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }
        if (forceClose) {
            // We need to fail all the incomplete batches and wake up the threads waiting on
            // the futures.
        	log.debug("Aborting incomplete batches due to forced shutdown");
            this.accumulator.abortIncompleteBatches();
        }
        try {
        	this.client.close();
        } catch(Exception ex) {
        	log.error("failed to close AQ producer", ex);
        }
        
        log.debug("Shutdown of Kafka producer I/O thread has completed.");

    }

    /**
     * Run a single iteration of sending
     *
     * @param now The current POSIX time in milliseconds
     */
    void run(long now) {   
        long pollTimeOut = sendProducerData(now);
        client.maybeUpdateMetadata(now);
        
        try {
        	long sleepTime = pollTimeOut;
        	if (sleepTime == Long.MAX_VALUE)
        	{
        		sleepTime =(int) Math.min(config.getLong(ProducerConfig.LINGER_MS_CONFIG), 5000);
        		
        	}
        	if(sleepTime > 0)
        	{
        		Thread.sleep(sleepTime);
        	}
        	
        }catch(Exception e) {} 
    }

    private long sendProducerData(long now) {
        // get the list of partitions with data ready to send
        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(metadata.fetch(), now);
        
        if (!result.unknownLeaderTopics.isEmpty()) {
            // The set of topics with unknown leader contains topics with leader election pending as well as
            // topics which may have expired. Add the topic again to metadata to ensure it is included
            // and request metadata update, since there are messages to send to the topic.
            for (String topic : result.unknownLeaderTopics)
                this.metadata.add(topic);

            log.debug("Requesting metadata update due to unknown leader topics from the batched records: {}", result.unknownLeaderTopics);

            this.metadata.requestUpdate();
        }
        //log.debug("ReadyNodes Size " + result.readyNodes.size());
        // remove any nodes we aren't ready to send to
        Iterator<org.apache.kafka.common.Node> iter = result.readyNodes.iterator();
 		long notReadyTimeout = Long.MAX_VALUE;
 		while (iter.hasNext()) {
 			Node node = (org.oracle.okafka.common.Node)iter.next();
 			if (!this.client.ready(node, now)) { 				
 				iter.remove();
 				notReadyTimeout = Math.min(notReadyTimeout, this.client.pollDelayMs(node, now));
 			}
 		}
 		
        // create produce requests
        Map<Integer, List<ProducerBatch>> batches = this.accumulator.drain(metadata.fetch(), result.readyNodes, maxRequestSize, now);
        if (guaranteeMessageOrder) {
			// Mute all the partitions drained
			for (List<ProducerBatch> batchList : batches.values()) {
				for (ProducerBatch batch : batchList)
					this.accumulator.mutePartition(batch.topicPartition);
			}
        }
        
        //List<ProducerBatch> expiredBatches = this.accumulator.expiredBatches(this.requestTimeoutMs, now);
        
        List<ProducerBatch> expiredBatches = this.accumulator.expiredBatches(this.requestTimeoutMs);
        
        // Reset the producer id if an expired batch has previously been sent to the broker. Also update the metrics
        // for expired batches. see the documentation of @TransactionState.resetProducerId to understand why
        // we need to reset the producer id here.
        if (!expiredBatches.isEmpty())
            log.trace("Expired {} batches in accumulator", expiredBatches.size());
        for (ProducerBatch expiredBatch : expiredBatches) {
        	 String errorMessage = "Expiring " + expiredBatch.recordCount + " record(s) for " + expiredBatch.topicPartition
                     + ":" + (now - expiredBatch.createdMs) + " ms has passed since batch creation";
            //failBatch(expiredBatch, NO_TIMESTAMP, new ArrayList<String>() { { add("ID:00000000000000000000000000000000"); }}, new TimeoutException(errorMessage));
        	 List<String> msgIdList = new ArrayList<String>();
            failBatch(expiredBatch, -1l, NO_TIMESTAMP, msgIdList, (RuntimeException)(new TimeoutException(errorMessage)));
        }
        
        // If we have any nodes that are ready to send + have sendable data, poll with 0 timeout so this can immediately
        // loop and try sending more data. Otherwise, the timeout is determined by nodes that have partitions with data
        // that isn't yet sendable (e.g. lingering, backing off). Note that this specifically does not include nodes
        // with sendable data that aren't ready to send since they would cause busy looping.
        long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
        if (!result.readyNodes.isEmpty()) {
        	log.trace("Instances with data ready to send: {}", result.readyNodes);
            // if some partitions are already ready to be sent, the select time would be 0;
            // otherwise if some partition already has some data accumulated but not ready yet,
            // the select time will be the time difference between now and its linger expiry time;
            // otherwise the select time will be the time difference between now and the metadata expiry time;
            pollTimeout = 0;
        }   
        sendProduceRequests(batches, pollTimeout);
        return pollTimeout;
    }

    /**
     * Start closing the sender (won't actually complete until all data is sent out)
     */
    public void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
        this.accumulator.close();
        this.running = false;
    }

    /**
     * Closes the sender without sending out any pending messages.
     */
    public void forceClose() {
        this.forceClose = true;
        initiateClose();
    }


    /**
     * Transfer the record batches into a list of produce requests on a per-node basis
     */
    private void sendProduceRequests(Map<Integer, List<ProducerBatch>> collated, long pollTimeout) {
    
    	for (Map.Entry<Integer, List<ProducerBatch>> entry : collated.entrySet())  
            sendProduceRequest(metadata.getNodeById(entry.getKey()), entry.getValue()); 
    }

    /**
     * Create a produce request from the given record batch
     */
    private void sendProduceRequest(Node node, List<ProducerBatch> batches) {
        if (batches.isEmpty()) 
       	  return;
        //Map<String, Map<TopicPartition, MemoryRecords>> produceRecordsByTopic = new HashMap<>();
    	//Map<TopicPartition, ProducerBatch> batchesByPartition = new HashMap<>();
    	for (ProducerBatch batch : batches) {
            /*TopicPartition tp = batch.topicPartition;
            MemoryRecords records = batch.records();
            if(!produceRecordsByTopic.containsKey(tp.topic())) {
            	produceRecordsByTopic.put(tp.topic(), new HashMap<TopicPartition, MemoryRecords>()); 	
            }
            produceRecordsByTopic.get(tp.topic()).put(tp, records);
            batchesByPartition.put(new TopicPartition(tp.topic(), tp.partition()), batch);*/
    		RequestCompletionHandler callback = new RequestCompletionHandler() {
        		@Override
    			public void onComplete(ClientResponse response) {
    				handleProduceResponse(response, batch, time.milliseconds());
    			}
    		};
    		
            ClientRequest request = client.newClientRequest(node, new ProduceRequest.Builder(batch.topicPartition, batch.records(), (short)1, -1), time.milliseconds(), true, -1, callback);
        	send(request, batch);  
        }
    }
    
    /**
     * Send produce request to destination
     * Handle response generated from send.
     */
    public void send(ClientRequest request, ProducerBatch batch)  {
    	/*for(Map.Entry<String, Map<TopicPartition, MemoryRecords>> produceRecordsByPartition : request.getproduceRecordsByTopic().entrySet()) {
    		for(Map.Entry<TopicPartition, MemoryRecords> partitionRecords : produceRecordsByPartition.getValue().entrySet()) {
    		}
    	}*/
		ClientResponse response = client.send(request, time.milliseconds()); 
		log.info("Batch Send complete, evaluating response " + batch.topicPartition);
		ProduceResponse pResponse = (ProduceResponse)response.responseBody();
		ProduceResponse.PartitionResponse partitionResponse = pResponse.getPartitionResponse();
		if(response.wasDisconnected()) {
			log.info("Connection to oracle database node " + response.destination() +" was broken. Retry again");
			accumulator.reenqueue(batch, System.currentTimeMillis());
			//Request for MetaData update since the Database instance has went down.
			this.metadata.requestUpdate();
		}
		else if(partitionResponse.exception != null)
		{
			RuntimeException producerException = partitionResponse.exception;
			if(producerException instanceof  NotLeaderForPartitionException) {
				
				log.info("No Owner for Topic Partition " +batch.topicPartition +" retrying.");
				this.metadata.requestUpdate();
			}
			if(producerException instanceof InvalidTopicException) {
				log.info(producerException.getMessage());
				completeResponse(response);
			}
			else {
				log.info("Exception while sending batch for partiton " +batch.topicPartition +". " + producerException);
			}
			accumulator.reenqueue(batch, System.currentTimeMillis());
		}
		else {
			log.trace("No Exception from send. Completing the batch");
			completeResponse(response);
		}
    }
    
    /**
     * Handle response using callback in a request
     */
    private void completeResponse(ClientResponse response) {
			response.onComplete();
    }
    
	/**
	 * Handle a produce response
	 */
	private void handleProduceResponse(ClientResponse response, ProducerBatch batch, long now) {
		if(response.wasDisconnected()) {
	    	client.disconnected(metadata.getNodeById(Integer.parseInt(response.destination())), now);
	    	metadata.requestUpdate();	    	
	    }
		long receivedTimeMs = response.receivedTimeMs();
		int correlationId = response.requestHeader().correlationId();
		/*if (response.wasDisconnected()) {
			log.trace("Cancelled request with header {} due to node {} being disconnected", requestHeader,
					response.destination());
			for (ProducerBatch batch : batches.values())
				completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NETWORK_EXCEPTION), correlationId,
						now, 0L);
		}  else {
			log.trace("Received produce response from node {} with correlation id {}", response.destination(),
					correlationId);
			 if we have a response, parse it
				for (Map.Entry<TopicPartition, ProduceResponse.PartitionResponse> entry : response.responses().entrySet()) {
					TopicPartition tp = entry.getKey();
					ProduceResponse.PartitionResponse partResp = entry.getValue();
					ProducerBatch batch = batches.get(tp);
					completeBatch(batch, partResp, correlationId, now,
							receivedTimeMs + response.throttleTimeMs());
				}
				this.sensors.recordLatency(response.destination(), response.requestLatencyMs());
			} else {
				 this is the acks = 0 case, just complete all requests
				for (ProducerBatch batch : batches.values()) {
					completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NONE), correlationId, now, 0L);
				}
			}*/
		ProduceResponse produceResponse = (ProduceResponse)response.responseBody();
		ProduceResponse.PartitionResponse partResp = produceResponse.getPartitionResponse();
		completeBatch(batch, partResp, correlationId, now,
				receivedTimeMs + produceResponse.throttleTimeMs());       
	}
	
	/**
	 * Complete or retry the given batch of records.
	 *
	 * @param batch         The record batch
	 * @param response      The produce response
	 * @param correlationId The correlation id for the request
	 * @param now           The current POSIX timestamp in milliseconds
	 */
	private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response, long correlationId,
			long now, long throttleUntilTimeMs) {
		
		RuntimeException exception = response.exception();

		if(exception != null) {
			if(canRetry(batch, response)) {
				reenqueueBatch(batch, now);
			} else failBatch(batch, response, exception);
			
			if( exception instanceof InvalidMetadataException) {
				metadata.requestUpdate();
			}
		} else completeBatch(batch, response);
	    
		// Unmute the completed partition.
		if (guaranteeMessageOrder)
		{
			//this.accumulator.unmutePartition(batch.topicPartition, throttleUntilTimeMs);
			this.accumulator.unmutePartition(batch.topicPartition);
		}
		
	}
	
	private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response) {
		
		if (batch.done(response.subPartitionId * MessageIdConverter.DEFAULT_SUBPARTITION_SIZE, response.logAppendTime, response.msgIds, null))
			this.accumulator.deallocate(batch);
	}
	
	private void failBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response, RuntimeException exception) {
		failBatch(batch, response.subPartitionId, response.logAppendTime,  response.msgIds, exception);
	}

	private void failBatch(ProducerBatch batch, long baseOffSet, long logAppendTime, List<String> msgIds, RuntimeException exception) {
		if (batch.done(baseOffSet, logAppendTime, msgIds, exception))
			this.accumulator.deallocate(batch);
	}

	private void reenqueueBatch(ProducerBatch batch, long currentTimeMs) {
		this.accumulator.reenqueue(batch, currentTimeMs);
	}

	/**
	 * We can retry a send if the error is transient and the number of attempts
	 * taken is fewer than the maximum allowed.
	 */
	private boolean canRetry(ProducerBatch batch, ProduceResponse.PartitionResponse response) {
		return batch.attempts() < this.retries && ((response.exception instanceof RetriableException));
	}
	
	public void wakeup()
	{
		// No OP. 
	}
	  public boolean isRunning() {
	        return running;
	    }
	

}
