/*
** OKafka Java Client version 0.8.
**
** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.common.network;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.oracle.okafka.clients.CommonClientConfigs;
import org.oracle.okafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.AbstractConfig;
import org.oracle.okafka.common.requests.MetadataRequest;
import org.oracle.okafka.common.requests.MetadataResponse;
import org.oracle.okafka.common.requests.CreateTopicsRequest.TopicDetails;
import org.oracle.okafka.common.utils.ConnectionUtils;
import org.oracle.okafka.common.utils.CreateTopics;
import org.slf4j.Logger;

import javax.jms.JMSException;
import oracle.jdbc.OracleTypes;
import oracle.jdbc.driver.OracleConnection;
import oracle.jms.AQjmsSession;

public abstract class AQClient {
	
	protected final Logger log ;
	private final AbstractConfig configs;
	
	public static final String PARTITION_PROPERTY = "AQINTERNAL_PARTITION";
	public static final String HEADERCOUNT_PROPERTY = "AQINTERNAL_HEADERCOUNT";
	public static final String MESSAGE_VERSION = "AQINTERNAL_MESSAGEVERSION";
	
	public AQClient(Logger log, AbstractConfig configs) {
		this.log = log;
		this.configs = configs;
	}
	public abstract ClientResponse send(ClientRequest request);
	
	public abstract boolean isChannelReady(Node node);
	
	public abstract void connect(Node node) throws JMSException;
	
	public abstract void close(Node node);
	
	public abstract void close();
	
	public ClientResponse getMetadataNow(ClientRequest request, Connection con) {
		
		log.debug("Getting Metadata now");
		
		MetadataRequest.Builder builder= (MetadataRequest.Builder)request.requestBuilder();
		MetadataRequest metadataRequest = builder.build();	
		List<Node> nodes = new ArrayList<>();
		List<PartitionInfo> partitionInfo = new ArrayList<>();
		Map<String, Exception> errorsPerTopic = new HashMap<>();
		List<String> metadataTopics = new ArrayList<String>(metadataRequest.topics());
		boolean disconnected = false;
		String clusterId = "";
		try {
			if(con == null)
			{
				throw new NullPointerException("Database connection to fetch metadata is null");
			}
			//Database Name to be set as Cluster ID
			clusterId = ((oracle.jdbc.internal.OracleConnection)con).getServerSessionInfo().getProperty("DATABASE_NAME");
			//Get Instances
			getNodes(nodes, con); 
			log.debug("Exploring hosts of the cluster. #Nodes " + nodes.size());
			for(Node nodeNow : nodes)
			{	
				log.debug("DB Instance: " + nodeNow);
			}
			
		/*	This should not happen. Handle it outside this class where we have handle to cluster object.
		 * Need to do cluster.getNodeById(request.destination());
		 * if(nodes.size() == 0)		 * 
			  nodes.add(request.destination());*/
			
			if(nodes.size() > 0)					
			    getPartitionInfo(metadataRequest.topics(), metadataTopics, con, nodes, metadataRequest.allowAutoTopicCreation(), partitionInfo, errorsPerTopic);
			
		} catch(Exception exception) {
			log.error("Exception while getting metadata "+ exception.getMessage() );
			exception.printStackTrace();
			
			if(exception instanceof SQLException) 
				if(((SQLException)exception).getErrorCode() == 6550) {
					log.error("execute on dbms_aqadm is not assigned", ((SQLException)exception).getMessage());
	        		log.info("create session, execute on dbms_aqin, dbms_aqadm , dbms_aqjms privileges required for producer or consumer to work");
				}
			if(exception instanceof SQLSyntaxErrorException)
				log.trace("Please grant select on gv_$instnce , gv_$listener_network, user_queues and user_queue_shards.");
			for(String topic : metadataTopics) {
				errorsPerTopic.put(topic, exception);
			}
			disconnected = true;			
			try {
				log.trace("Unexcepted error occured with connection to node {}, closing the connection", request.destination());
				if(con != null)
					con.close();
				
				log.trace("Connection with node {} is closed", request.destination());
			} catch(SQLException sqlEx) {
				log.trace("Failed to close connection with node {}", request.destination());
			}
       }
		return  new ClientResponse(request.makeHeader((short)1),
				request.callback(), request.destination(), request.createdTimeMs(),
				System.currentTimeMillis(), disconnected, null,null, new MetadataResponse(clusterId, nodes, partitionInfo, errorsPerTopic));
		
	}
	
	
	private void getNodes(List<Node> nodes, Connection con) throws SQLException {
		Statement stmt = null;
		ResultSet result = null;
		String user = "";
		try {
			user = con.getMetaData().getUserName();
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			String query = "select inst_id, instance_name from gv$instance";
			result = stmt.executeQuery(query);
			Map<Integer, String> instances = new HashMap<>();
			while(result.next()) {
				int instId = result.getInt(1);
				String instName = result.getString(2);
				
				instances.put(instId, instName);
			}
			result.close();
			result = null;
			
			if (instances.size()==1)
			{

				//Connected Node is :
				Node connectedNode = getNodeToThisConnection(con);
				// NON-RAC env. We are connected to it.
				if(connectedNode != null) {
					nodes.add(connectedNode);
					return;
				}
			}

			//int connectedInst = connectedNode !=null?connectedNode.id():-1;
			
			query = "select inst_id, TYPE, value from gv$listener_network order by inst_id";
			result = stmt.executeQuery(query);
			Map<Integer, ArrayList<String>> services = new HashMap<>();
			Map<Integer,ArrayList<String>> localListenersMap = new HashMap<>();
			
			String security = configs.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
			String preferredService = configs.getString(CommonClientConfigs.ORACLE_SERVICE_NAME);
			
			boolean plainText = security.equalsIgnoreCase("PLAINTEXT")?true:false;
			
			while(result.next()) {
				int instId = result.getInt(1);
				String type = result.getString(2);
				String value = result.getString(3);
				
				if(type.equalsIgnoreCase("SERVICE NAME")) {
					ArrayList<String> servicesList = services.get(instId);
					if(servicesList == null)
					{
						servicesList = new ArrayList<String>();
						services.put(instId,servicesList);
					}
					if(preferredService != null && value.equalsIgnoreCase(preferredService))
					{
						log.info("Found Preferred Services " + value);
						servicesList.add(0, value);
					}
					else {
						servicesList.add(value);
					}
				}
				else if(type.equalsIgnoreCase("LOCAL LISTENER"))
				{
					ArrayList<String> localListenerList =  localListenersMap.get(instId);
					if(localListenerList == null)
					{
						localListenerList = new ArrayList<String>();
						localListenersMap.put(instId, localListenerList);
					}
					localListenerList.add(value);
				}
			} //Result set Parsed
			result.close();
			result = null;
			
			for(Integer instIdNow : instances.keySet())
			{
				/*if( instIdNow.intValue() == connectedInst)
					continue; */
				
				log.debug("Processing metadata for instance: " + instIdNow);
				
				ArrayList<String> localListenerList = localListenersMap.get(instIdNow);
				for(String localListenerNow : localListenerList)
				{
					log.debug("Processing Local Listener " + localListenerNow);
					String str = localListenerNow;
					//AdHoc processing of LISTENER STRING 
					StringBuilder sb = new StringBuilder();
					
					for(int ind = 0;ind < str.length(); ind++)
						if(str.charAt(ind) != ' ')
							sb.append(str.charAt(ind));
					
					str = sb.toString();
					String protocolNow = getProperty(str,"PROTOCOL");
					log.debug("Protocol used by this local listener " + protocolNow);
					
					if( (plainText && protocolNow.equalsIgnoreCase("TCP")) || 
						(!plainText && protocolNow.equalsIgnoreCase("TCPS")))
					{
						String host = getProperty(str, "HOST");;
						Integer port = Integer.parseInt(getProperty(str, "PORT"));
						log.debug("Hot:PORT " + host +":"+port);
						
						// ToDo: Assign Service List instead of a single Service
						Node newNode =new Node(instIdNow, host, port, services.get(instIdNow).get(0), instances.get(instIdNow));
						newNode.setUser(user);
						log.debug("New Node created: " + newNode);
						newNode.updateHashCode();
						nodes.add(newNode);
					}
				}
			}
		}
		catch(Exception e)
		{
			log.error("Exception while updating metadata " ,e);
		} finally {
			try {
				if(result != null)
					result.close();
				
				if(stmt != null)
				stmt.close();
			} catch(SQLException sqlEx) {
				//do nothing
			}
		}
	}
	
	private Node getNodeToThisConnection(Connection con)
	{
		Node node = null;
		try {
			 oracle.jdbc.internal.OracleConnection oracleInternalConn = (oracle.jdbc.internal.OracleConnection)con;
			 String instanceName = oracleInternalConn.getServerSessionInfo().getProperty("INSTANCE_NAME");
		     int instanceNum = Integer.parseInt(oracleInternalConn.getServerSessionInfo().getProperty("AUTH_INSTANCE_NO"));
		     String dbServiceName = oracleInternalConn.getServerSessionInfo().getProperty("SERVICE_NAME");
		     String userName = con.getMetaData().getUserName();
		     
		     List<String> bootStrapServers = this.configs.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
		     
		     String firstHost = bootStrapServers.get(0);
		     StringTokenizer stn = new StringTokenizer(firstHost,":");
		     String dbHost = stn.nextToken();
		     String dbPort = stn.nextToken();
		     
		     node = new Node(instanceNum, dbHost,Integer.parseInt(dbPort), dbServiceName, instanceName);
		     node.setUser(userName);
		     node.updateHashCode();
		}
		catch(Exception e)
		{
			log.error("Exception while creating node from JDBC Connection", e );
		}
		return node;
	}
	
private void getPartitionInfo(List<String> topics, List<String> topicsRem, Connection con, List<Node> nodes, boolean allowAutoTopicCreation, List<PartitionInfo> partitionInfo, Map<String, Exception> errorsPerTopic) throws Exception {
		
	if(nodes.size() <= 0 || topics == null || topics.isEmpty())
		return;

	//String queryQShard = "select shard_id, enqueue_instance from user_queue_shards where  name = ? ";
	String queryQShard = "select SHARD_ID, ENQUEUE_INSTANCE from user_queue_shards where  QUEUE_ID = (select qid from user_queues where name = upper(?)) ";
	PreparedStatement stmt1 = null;
	try {
		stmt1 = con.prepareStatement(queryQShard);
		int nodeIndex = 0 ;
		int nodesSize = nodes.size();
		ResultSet result1 = null;
		Node[] nodesArray = null;
		if(nodesSize > 1) {
			int max = -1;
			for(Node nodeNew : nodes)  {
				if(nodeNew.id() > max)
					max = nodeNew.id();
			}
			
			nodesArray = new Node[max];
			for(Node nodeNew : nodes) 
				nodesArray[nodeNew.id()-1] = nodeNew;
		}
		
		for(String topic : topics) {
			boolean topicDone = false;
			int partCnt = 0;
			try {
				//Get number of partitions
				partCnt = getPartitions(ConnectionUtils.enquote(topic), con);
			} catch(SQLException sqlE) {
				int errorNo = sqlE.getErrorCode();
				if(errorNo == 24010)  {
					//Topic does not exist, it will be created
					continue;
				}
			}catch(Exception excp) {
				// Topic May or may not exists. We will not attempt to create it again
				topicsRem.remove(topic);
				continue;
			}
			
			boolean partArr[] =  new boolean[partCnt];
			for(int i =0; i < partCnt ;i++)
				partArr[i] = false;

			// If more than one RAC node then check who is owner Node for which partition
			if(nodes.size()  > 1) {
				
				stmt1.clearParameters();
				stmt1.setString(1, topic);
				result1 = stmt1.executeQuery(); 
				// If any row exist 
				if(result1.isBeforeFirst()) {
					while(result1.next() ) {
					int partNum = result1.getInt(1)/2;
					int nodeNum = result1.getInt(2);
					partitionInfo.add(new PartitionInfo(topic, partNum , nodesArray[nodeNum-1], new Node[0], new Node[0]));	
					partArr[partNum] = true;
				 }
					
				 result1.close();
				 // For the partitions not yet mapped to an instance 
				 for(int i = 0; i < partCnt ; i++) {
					if( partArr[i] == false ) {
						partitionInfo.add(new PartitionInfo(topic, i , nodes.get(nodeIndex++%nodesSize), null, null));	
					}
				 }
				 topicDone = true;
				} // Entry Existed in USER_QUEUE_SHARD
			}// Nodes > 1
		
			// No Record in USER_QUEUE_SHARD or Node =1 check if topic exist		   	
			if(!topicDone){
				for(int i = 0; i < partCnt ; i++) {
					partitionInfo.add(new PartitionInfo(topic, i , nodes.get(nodeIndex++%nodesSize), null, null));
				}
				topicDone =true;
			}
			if(topicDone)
			  topicsRem.remove(topic);
			} // For all Topics
	
		if(allowAutoTopicCreation && topicsRem.size() > 0) {
			Map<String, TopicDetails> topicDetails = new HashMap<String, TopicDetails>();
			for(String topicRem : topicsRem) {
				topicDetails.put(topicRem, new TopicDetails(1, (short)0 , Collections.<String, String>emptyMap()));
			}
			Map<String, Exception> errors= CreateTopics.createTopics(con, topicDetails);
			for(String topicRem : topicsRem) {
				if(errors.get(topicRem) == null) {
		    		partitionInfo.add(new PartitionInfo(topicRem, 0, nodes.get(nodeIndex++%nodesSize), null, null));
		    	} else {
		    		errorsPerTopic.put(topicRem, errors.get(topicRem));
		    	}
			}
		}
		} finally {
			try {
				if(stmt1 != null) 
					stmt1.close();		
			} catch(Exception ex) {
			//do nothing
			}
		}
	}

	private int getPartitions(String topic, Connection con) throws Exception {
		   if(topic == null) return 0;
		   String query = "begin dbms_aqadm.get_queue_parameter(?,?,?); end;";
		   CallableStatement cStmt = null;
		   int part = 1;
		   try {
	       cStmt = con.prepareCall(query);
	       cStmt.setString(1, topic);
		   cStmt.setString(2, "SHARD_NUM");
		   cStmt.registerOutParameter(3, OracleTypes.NUMBER);
		   cStmt.execute();
		   part = cStmt.getInt(3);
		   } 
		   catch(SQLException ex) {
			   throw ex;
		   }
		   finally {
			   try {
				   if(cStmt != null)
					   cStmt.close();
			   } catch(Exception ex) {
				   //Do Nothing
			   }
		   }		   
		   return part;
	   }  
	
	public static String getProperty(String str, String property) {
		String tmp = str.toUpperCase();
		int index = tmp.indexOf(property.toUpperCase());
		if(index == -1)
			return null;
        int index1 = tmp.indexOf("=", index);
        if(index1 == -1)
        	return null;
        int index2 = tmp.indexOf(")", index1);
        if(index2 == -1)
        	return null;
        return str.substring(index1 + 1, index2).trim();
    }
	
}
