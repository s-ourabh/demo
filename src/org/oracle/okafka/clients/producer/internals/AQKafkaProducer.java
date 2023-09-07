/*
 ** OKafka Java Client version 0.8.
 **
 ** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
 ** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
 */

package org.oracle.okafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.TopicPublisher;

import oracle.jdbc.driver.OracleConnection;
import oracle.jms.AQjmsBytesMessage;
import oracle.jms.AQjmsConnection;
import oracle.jms.AQjmsException;
import oracle.jms.AQjmsProducer;
import oracle.jms.AQjmsSession;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.oracle.okafka.clients.Metadata;
import org.oracle.okafka.clients.NetworkClient;
import org.oracle.okafka.clients.producer.ProducerConfig;
import org.oracle.okafka.common.Node;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.header.Header;
import org.oracle.okafka.common.network.AQClient;
import org.oracle.okafka.common.protocol.ApiKeys;
import org.oracle.okafka.common.requests.ProduceRequest;
import org.oracle.okafka.common.requests.ProduceResponse;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.oracle.okafka.common.utils.ConnectionUtils;
import org.oracle.okafka.common.utils.MessageIdConverter;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

/**
 * This class sends messages to AQ
 */
public final class AQKafkaProducer extends AQClient {

	//Holds TopicPublishers of each node. Each TopicPublisher can contain a connection to corresponding node, session associated with that connection and topic publishers associated with that session
	private final Map<Node, TopicPublishers> topicPublishersMap;
	private final ProducerConfig configs;
	private final Time time;
	private Metadata metadata; 
	private final int DLENGTH_SIZE = 4;
	public AQKafkaProducer(LogContext logContext, ProducerConfig configs, Time time, Metadata _metadata)
	{   
		super(logContext.logger(AQKafkaProducer.class), configs);
		this.configs = configs;
		this.time = time;
		this.topicPublishersMap = new HashMap<Node, TopicPublishers>();
		this.metadata = _metadata;
	}

	public void connect(Node node) throws JMSException {
		TopicPublishers nodePublishers = null;
		try {
			log.debug("Creating new connection for node " + node);
			nodePublishers = new TopicPublishers(node);
			topicPublishersMap.put(node, nodePublishers);
			log.debug("CONNECTED NODES: "+topicPublishersMap.keySet());
		}catch(JMSException e) {
			close(node, nodePublishers);
			throw e;
		}	          	
	}

	public boolean isChannelReady(Node node) {
		if(topicPublishersMap.containsKey(node)) {
			return true;
		}
		return false;
	}

	public ClientResponse send(ClientRequest request) {
		return parseRequest(request, ApiKeys.convertToOracleApiKey(request.apiKey()));	
	}

	/**
	 * Determines the type of request and calls appropriate method for handling request
	 * @param request request to be sent
	 * @param key uniquely identifies type of request.
	 * @return response for given request
	 */
	private ClientResponse parseRequest( ClientRequest request, ApiKeys key) {
		if(key == ApiKeys.PRODUCE) 
			return publish(request);
		if(key == ApiKeys.METADATA)
			return getMetadata(request);
		return null;

	}

	/**
	 *Unwraps memory records of a producer batch into records. 
	 *Then translates each record into AQjmsBytesMessage and sends them to database instance as AqjmsBytesMessage array.
	 *Returns response for all messages in a memory records.
	 */
	private ClientResponse publish(ClientRequest request) {
		ProduceRequest.Builder builder = (ProduceRequest.Builder)request.requestBuilder();
		ProduceRequest produceRequest = builder.build();
		Node node = metadata.getNodeById(Integer.parseInt(request.destination()));
		TopicPartition topicPartition = produceRequest.getTopicpartition();
		MemoryRecords memoryRecords = produceRequest.getMemoryRecords();
		TopicPublishers nodePublishers = null;
		AQjmsBytesMessage[] msgs =null;
		ProduceResponse.PartitionResponse partitionResponse = null;
		boolean disconnected = false;
		TopicPublishers allPublishers = null;
		TopicPublisher publisher = null;
		int retryCnt = 2; 
		AQjmsBytesMessage byteMessage  = null;
		do
		{
			disconnected = false;
			retryCnt--;
			try  {
				nodePublishers = topicPublishersMap.get(node);
				if(nodePublishers == null)
				{
					throw new NullPointerException("No publishers created for node " + node);
				}
				TopicSession session = nodePublishers.getSession();
				final List<AQjmsBytesMessage> messages = new ArrayList<>();	
				Iterator<MutableRecordBatch> mutableRecordBatchIterator = memoryRecords.batchIterator();
				while(mutableRecordBatchIterator.hasNext()) {
					Iterator<Record>  recordIterator = mutableRecordBatchIterator.next().iterator();
					while(recordIterator.hasNext()) {
						Record record = recordIterator.next();
						byteMessage = createBytesMessage(session, topicPartition, record.key(), record.value(), record.headers());
						messages.add(byteMessage);	
					}
				}

				allPublishers = topicPublishersMap.get(node);
				publisher = allPublishers.getTopicPublisher(topicPartition.topic());
				msgs = messages.toArray(new AQjmsBytesMessage[0]);
				log.trace("sending messages to topic : {} with partition: {}, number of messages: {}", topicPartition.topic(), topicPartition.partition(), msgs.length);	
				sendToAQ(msgs, publisher);
				log.trace("Messages sent successfully to topic : {} with partition: {}, number of messages: {}", topicPartition.topic(), topicPartition.partition(), msgs.length);
				retryCnt = 0;
			}
			/*	catch(NullPointerException nlp)
			{
				partitionResponse =  createResponses(topicPartition, (RuntimeException)nlp, msgs);
				retryCnt = 0;
				disconnected = true;
				break;
			}*/
			catch(Exception e) {
				log.error("Exception while sending records for topic partition " + topicPartition + " no node " + node , e);

				if ( e instanceof JMSException) {
					log.info(" Encountered JMS Exception:" + e.getMessage() );

					if( (e instanceof AQjmsException ) && ((AQjmsException)e).getErrorNumber() == 25348 )
					{
						log.debug("Causing NotLeaderForPartitionException ");
						partitionResponse =  createResponses(topicPartition, new NotLeaderForPartitionException(e), msgs);  
						retryCnt =0;
						this.metadata.requestUpdate();
						break;
					}					
				}				
				if(nodePublishers != null)
				{
					boolean connected = nodePublishers.isConnected();
					if(!connected)
					{
						try {

							nodePublishers.close();

							if(retryCnt > 0)
							{
								log.info("Reconnecting to node " + node);
								nodePublishers.reCreate();
							}else {
								disconnected = true;
								log.info("Disconnected. Failing the batch");
							}

						}catch(Exception reConnException)
						{
							log.error("Exception while reconnecting to node " + node , reConnException);
							disconnected = true;
							retryCnt = 0;
							try {
								// Close again just to be sure that we are not leaking connections.
								nodePublishers.close();  
							}catch(Exception ignoreExcp) {}

							topicPublishersMap.remove(node);
							log.trace("Connection with node {} is closed", request.destination());
							String exceptionMsg = "Database instance not reachable: " + node;
							org.apache.kafka.common.errors.DisconnectException disconnExcp = new org.apache.kafka.common.errors.DisconnectException(exceptionMsg,e);
							partitionResponse =  createResponses(topicPartition, disconnExcp, msgs);
						}
					}
					else {
						log.info("Connection to node is fine. Retrying to publish = "  + ((retryCnt > 0 )?"TRUE":"FALSE"));
						// ToDo: Check for duplicate. Enforce idempotency.
					}
				}
			}
		}while(retryCnt > 0);

		if( partitionResponse == null)
			partitionResponse = createResponses(topicPartition, null, msgs);	

		return createClientResponse(request, topicPartition, partitionResponse, disconnected);

	}

	private ClientResponse createClientResponse(ClientRequest request, TopicPartition topicPartition, ProduceResponse.PartitionResponse partitionResponse, boolean disconnected) {
		return  new ClientResponse(request.makeHeader((short)1), request.callback(), request.destination(), 
				request.createdTimeMs(), time.milliseconds(), disconnected, null,null,
				new ProduceResponse(topicPartition, partitionResponse));
	}

	/**
	 * Bulk send messages to AQ.
	 * @param messages array of AQjmsBytesmessage to be sent
	 * @param publisher topic publisher used for sending messages
	 * @throws JMSException throws JMSException
	 */
	private void sendToAQ(AQjmsBytesMessage[] messages, TopicPublisher publisher) throws JMSException {
		//Sends messages in bulk using topic publisher;
		log.info("In BulkSend: #messages = " + messages.length);
		((AQjmsProducer)publisher).bulkSend(publisher.getTopic(), messages);
	}

	/**
	 * Creates AQjmsBytesMessage from ByteBuffer's key, value and headers
	 */
	private AQjmsBytesMessage createBytesMessage(TopicSession session, TopicPartition topicPartition, 
			ByteBuffer key, ByteBuffer value, Header[] headers, boolean obsolete ) throws JMSException {
		AQjmsBytesMessage msg=null;
		msg = (AQjmsBytesMessage)(session.createBytesMessage());

		if(key!=null) {
			byte[] keyByteArray  = new byte[key.limit()];
			key.get(keyByteArray);
			msg.setJMSCorrelationID(new String(keyByteArray));
		}
		byte[] payload = new byte[value.limit()];
		value.get(payload);
		msg.writeBytes(payload);
		payload = null;
		msg.setStringProperty("topic", topicPartition.topic());
		msg.setStringProperty(AQClient.PARTITION_PROPERTY, Integer.toString(topicPartition.partition()*2));

		return msg;
	}

	/*
	 * Construct Byte Payload in below format:
	 * | KEY LENGTH (4 Bytes Fixed)          | KEY   |
	 * | VALUE LENGTH (4 BYTES FIXED)        | VALUE |
	 * | HEADER NAME LENGTH(4 BYTES FIXED)   | HEADER NAME |
	 * | HEADER VALUE LENGTH (4 BYTES FIXED) | HEADER VALUE |
	 * | HEADER NAME LENGTH(4 BYTES FIXED)   | HEADER NAME |
	 * | HEADER VALUE LENGTH (4 BYTES FIXED) | HEADER VALUE |
	 * 
	 * For records with null key , KEY LENGTH is set to 0.
	 * For records with null value, VALUE LENGTH is set to 0.
	 * Number of headers are set in property "AQINTERNAL_HEADERCOUNT"
	 * 
	 * 	*/
	
	private AQjmsBytesMessage createBytesMessage(TopicSession session, TopicPartition topicPartition, 
			ByteBuffer key, ByteBuffer value, Header[] headers) throws JMSException {

		AQjmsBytesMessage msg=null;
		int keyLen = 0;
		int valueLen =0;

		int hKeysLen[] = null;
		int hValuesLen[] = null;
		
		byte[] keyByteArray  = null;
		byte[] valueByteArray = null;
		
		
		if(headers != null)
		{
			hKeysLen = new int[headers.length];
			hValuesLen = new int[headers.length];
		}

		msg = (AQjmsBytesMessage)(session.createBytesMessage());

		int totalSize = 0;
		if(key != null) {
			
			keyByteArray =  new byte[key.limit()];
			key.get(keyByteArray);
			keyLen = keyByteArray.length;
		}
	
		totalSize += (keyLen + DLENGTH_SIZE );
		
		if(value != null) {
			valueByteArray = new byte[value.limit()];
			value.get(valueByteArray);
			valueLen = valueByteArray.length;
			
		}
		totalSize += (valueLen + DLENGTH_SIZE);
		
		if(headers != null) {
			int hIndex = 0;
			for(Header h:headers)
			{
				int hKeyLen = h.key().getBytes().length;
				totalSize += (hKeyLen + DLENGTH_SIZE);
				hKeysLen[hIndex] = hKeyLen;
				int hValueLength = h.value().length;
				totalSize += (hValueLength +DLENGTH_SIZE);
				hValuesLen[hIndex++] = hValueLength;
			}
		}
		ByteBuffer pBuffer = ByteBuffer.allocate(totalSize);

		//If Key is null Put Length = 0
		pBuffer.put(ConnectionUtils.convertTo4Byte(keyLen));
		if(keyLen > 0) {
			pBuffer.put(keyByteArray);
			msg.setJMSCorrelationID(new String(keyByteArray));
		}
		//If Value is null then put length = 0
		pBuffer.put(ConnectionUtils.convertTo4Byte(valueLen));
		if(valueLen > 0)
		{
			pBuffer.put(valueByteArray);
		}

		if(headers != null)
		{
			int hIndex = 0;
			for(Header h : headers)
			{
				pBuffer.put(ConnectionUtils.convertTo4Byte(hKeysLen[hIndex]));
				pBuffer.put(h.key().getBytes());
				pBuffer.put(ConnectionUtils.convertTo4Byte(hValuesLen[hIndex++]));
				pBuffer.put(h.value());
			}
		}
		
		pBuffer.rewind();
		byte[] payload = new byte[pBuffer.limit()];
		pBuffer.get(payload);
		msg.writeBytes(payload);
		payload = null;
		msg.setStringProperty(PARTITION_PROPERTY, Integer.toString(topicPartition.partition()*2));
		if(headers !=null)
		{
			msg.setIntProperty(HEADERCOUNT_PROPERTY, headers.length);
		}
		msg.setBooleanProperty(PARSEPAYLOAD_PROPERTY, true);

		return msg;
	}

	/**
	 * Creates response for records in a producer batch from each corresponding AQjmsBytesMessage data updated after send is done.
	 */
	private ProduceResponse.PartitionResponse createResponses(TopicPartition tp, RuntimeException exception, AQjmsBytesMessage[] msgs) {
		int iter=0;
		//Map<TopicPartition, ProduceResponse.PartitionResponse> responses = new HashMap<>();
		ProduceResponse.PartitionResponse response =new ProduceResponse.PartitionResponse(exception);

		if(exception == null) {
			response.msgIds = new ArrayList<>();
			//response.logAppendTime = new ArrayList<>();
			String msgId = null;
			long timeStamp = -1;
			long subPartitionId = -1;
			while(iter<msgs.length) {
				try {
					msgId = msgs[iter].getJMSMessageID(); 
					timeStamp = msgs[iter].getJMSTimestamp();
					if(subPartitionId == -1) {
						subPartitionId = MessageIdConverter.getOKafkaOffset(msgId).subPartitionId();
					}
				} catch(Exception excp) {
					msgId = null;
					timeStamp = -1;
				}
				response.msgIds.add(msgId);
				//response.logAppendTime.add(timeStamp);
				iter++;
			}
			response.logAppendTime = timeStamp;
			response.subPartitionId = subPartitionId;
		}
		//responses.put(new TopicPartition(tp.topic(), tp.partition()), response);		
		return response;
	}

	private ClientResponse getMetadata(ClientRequest request) {
		Connection conn = null;
		Node node = null;
		//Cluster used for this metadata is still a bootstrap cluster and does not have all necessary information
		//Pick any random node from the bootstrap nodes and send metadata request.
		if(metadata.isBootstrap())
		{
			Cluster cluster = metadata.fetch();
			List<Node> clusterNodes = NetworkClient.convertToOracleNodes(cluster.nodes());
			// Check if we have a node where connection already exists
			Set<Node> nodesWithConn = topicPublishersMap.keySet();
			for(Node nodeNow: clusterNodes)
			{
				//System.out.println("Cluster Node "+ nodeNow);
				for(Node connectedNode : nodesWithConn)
				{
					if(connectedNode.equals(nodeNow))
					{
						//Found a node with a connection to database.
						node = nodeNow;
						break;
					}
				}
			}
			if(node == null)
			{
				//No node with connection yet. Pick the first bootstrap node.
				node = clusterNodes.get(0);
				log.info("No Connected Node Found. Picked first of bootstrap nodes.: " + node);
			}
		}
		else
		{
			node = (org.oracle.okafka.common.Node)metadata.getNodeById(Integer.parseInt(request.destination()));
		}
		try {
			TopicPublishers tpNode  = topicPublishersMap.get(node);
			if(tpNode != null)
			{
				conn = ((AQjmsSession)topicPublishersMap.get(node).getSession()).getDBConnection();
			}else {
				// Designated node does not have a connection. Find alternative. 
				for(TopicPublishers tPublishers: topicPublishersMap.values())
				{
					if(tPublishers.isConnected())
					{
						conn = ((AQjmsSession)tPublishers.getSession()).getDBConnection();
					}
				}

				if(conn == null)
				{
					log.info("Sender not connected to any node. Re-connecting.");
					List<Node> clusterNodes = NetworkClient.convertToOracleNodes(metadata.fetch().nodes());
					for(Node n : clusterNodes)
					{
						try {
							this.connect(n);
							log.info("Attempting to connect to " + n);
							conn = ((AQjmsSession)topicPublishersMap.get(n).getSession()).getDBConnection();
							log.info("Connected to node " + n);
							node = n;
							break;
						}catch(Exception e)
						{
							log.info(" Node {} not rechable", n);
						}
					}
				}
				/*if(conn == null)
					metadata.requestUpdate(); */
			}
		} catch(JMSException jms) {			
			try {
				log.trace("Unexcepted error occured with connection to node {}, closing the connection", request.destination());
				topicPublishersMap.get(metadata.getNodeById(Integer.parseInt(request.destination()))).getConnection().close();
				log.trace("Connection with node {} is closed", request.destination());
			} catch(JMSException jmsEx) {
				log.trace("Failed to close connection with node {}", request.destination());
			}
		}


		ClientResponse response = getMetadataNow(request, conn, node, metadata.updateRequested());
		if(response.wasDisconnected()) {
			topicPublishersMap.remove(metadata.getNodeById(Integer.parseInt(request.destination())));
			metadata.requestUpdate();
		}
		return response;
	}

	/**
	 * Closes AQKafkaProducer
	 */
	public void close() {
		for(Map.Entry<Node, TopicPublishers> nodePublishers : topicPublishersMap.entrySet()) {
			close(nodePublishers.getKey(), nodePublishers.getValue());
		}
		topicPublishersMap.clear();
	}

	public void close(Node node) {

	}

	/**
	 * Closes all connections, session associated with each connection  and all topic publishers associated with session.
	 */
	private void close(Node node, TopicPublishers publishers) {
		if( node == null || publishers == null)
			return ;
		for(Map.Entry<String, TopicPublisher> topicPublisher : publishers.getTopicPublisherMap().entrySet()) {
			try {
				topicPublisher.getValue().close();
			}catch(JMSException jms) {
				log.error("failed to close topic publisher for topic {} ", topicPublisher.getKey());
			}
		}
		try {
			publishers.getSession().close();
		} catch(JMSException jms) {
			log.error("failed to close session {} associated with connection {} and node {}  ",publishers.getSession(), publishers.getConnection(), node );
		}
		try {
			publishers.getConnection().close();
		} catch(JMSException jms) {
			log.error("failed to close connection {} associated with node {}  ",publishers.getConnection(), node );
		}
	}

	/**This class is used to create and manage connection to database instance.
	 * Also creates, manages session associated with each connection and topic publishers associated with each session
	 */
	private final class TopicPublishers {
		private Node node;
		private TopicConnection conn;
		private TopicSession sess;
		private Map<String, TopicPublisher> topicPublishers = null;
		private int sessionAckMode =  javax.jms.Session.AUTO_ACKNOWLEDGE;

		private boolean isAlive = false;
		PreparedStatement pingStmt = null;
		private final String PING_QUERY = "SELECT banner FROM v$version where 1<>1";

		public TopicPublishers(Node node) throws JMSException {
			this(node, TopicSession.AUTO_ACKNOWLEDGE);
		}
		public TopicPublishers(Node _node,int mode) throws JMSException {
			this.node = _node;
			this.sessionAckMode = mode;

			createPublishers();
			/*conn = createTopicConnection(node);
			sess = createTopicSession(mode);*/

			try {
				Connection oConn = ((AQjmsSession)sess).getDBConnection();
				int instId = Integer.parseInt(((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("AUTH_INSTANCE_NO"));
				String serviceName = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("SERVICE_NAME");
				String instanceName = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("INSTANCE_NAME");
				String user = oConn.getMetaData().getUserName();

				try {
					String sessionId = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("AUTH_SESSION_ID");
					String serialNum = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("AUTH_SERIAL_NUM");
					String serverPid = ((oracle.jdbc.internal.OracleConnection)oConn).getServerSessionInfo().getProperty("AUTH_SERVER_PID");

					log.info("Database Producer Session Info: "+ sessionId +","+serialNum+". Process Id " + serverPid +" Instance Name "+instanceName);
				}catch(Exception ignoreE)
				{
				}

				node.setId(instId);
				node.setService(serviceName);
				node.setInstanceName(instanceName);
				node.setUser(user);
				node.updateHashCode();
				pingStmt = oConn.prepareStatement(PING_QUERY);
				pingStmt.setQueryTimeout(1);
				isAlive = true;
			}catch(Exception e)
			{
				log.error("Exception while getting instance id from conneciton " + e, e);
			}
			topicPublishers = new HashMap<>();
		}
		private boolean createPublishers() throws JMSException {
			conn = createTopicConnection(node);
			sess = createTopicSession(sessionAckMode);

			return true;
		}
		/**
		 * Creates topic connection to node
		 * @param node destination to which connection is needed
		 * @return established topic connection
		 * @throws JMSException
		 */
		public TopicConnection createTopicConnection(Node node) throws JMSException {
			conn = ConnectionUtils.createTopicConnection(node, configs, log);
			return conn;
		}

		public TopicPublisher getTopicPublisher(String topic) throws JMSException {
			TopicPublisher publisher = topicPublishers.get(topic);
			if(publisher == null) {
				publisher = createTopicPublisher(topic);
				topicPublishers.put(topic, publisher);
			}
			return publisher;
		}

		/**
		 * Creates topic session from established connection
		 * @param mode mode of acknowledgement with which session has to be created
		 * @return created topic session
		 * @throws JMSException
		 */
		public TopicSession createTopicSession(int mode) throws JMSException {		
			if(sess != null) 
				return sess;			
			sess= ConnectionUtils.createTopicSession(conn, mode, false);
			conn.start();
			return sess;

		}

		/**
		 * Creates topic publisher for given topic
		 */
		private TopicPublisher createTopicPublisher(String topic) throws JMSException {
			Topic dest = ((AQjmsSession)sess).getTopic((node!=null&&node.user()!=null)?node.user():ConnectionUtils.getUsername(configs), topic);
			TopicPublisher publisher = sess.createPublisher(dest);
			return publisher;

		}
		public TopicConnection getConnection() {
			return conn;
		}

		public TopicSession getSession() {
			return sess;
		}

		public Map<String, TopicPublisher> getTopicPublisherMap() {
			return topicPublishers;
		}

		public boolean isConnected()
		{
			if(isAlive)
			{
				try
				{			
					pingStmt.executeQuery();
				}catch(Exception e)
				{
					log.error("Publishers to node {} Failed to connect.", node.toString());
					isAlive = false;
				}
			}
			return isAlive;
		}

		public boolean reCreate() throws JMSException
		{
			close();
			createPublishers();
			try {
				Map<String, TopicPublisher> topicPublishersNew = new HashMap<String, TopicPublisher>();
				for(String topic:  topicPublishers.keySet())
				{
					try {
						TopicPublisher tpNew = createTopicPublisher(topic);
						topicPublishersNew.put(topic, tpNew);
					} catch(Exception e) 
					{
						log.error("Exception "+ e +" while re-creating publishers for topic " +topic + " for node" + node );		
					}
				}
				topicPublishers.clear();
				topicPublishers = topicPublishersNew;
			}catch(Exception e)
			{
				log.error("Exception "+ e +" while re-creating publishers for topic for node" + node );
			}
			isAlive = true;

			return isAlive;
		}

		public void close()
		{
			try {
				if(pingStmt != null)
				{
					pingStmt.close();
				}
				if(sess != null)
				{
					sess.close();
				}
				if(conn != null)
				{
					conn.close();
				}
				isAlive = false;
			}catch(Exception e)
			{
				isAlive = false;
				log.error("Failed to Close publishers for node " + node);
			}
		}

	}
}
