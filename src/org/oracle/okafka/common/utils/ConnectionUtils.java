/*
** OKafka Java Client version 0.8.
**
** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.common.utils;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.TopicSession;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

import org.oracle.okafka.clients.CommonClientConfigs;
import org.oracle.okafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.oracle.okafka.common.config.SslConfigs;
import org.oracle.okafka.common.errors.ConnectionException;
import org.slf4j.Logger;

import oracle.jdbc.driver.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;
import oracle.jms.AQjmsFactory;
import oracle.jms.AQjmsSession;

public class ConnectionUtils {
    
    public static String createUrl(Node node, AbstractConfig configs) {
    	
    	if( !configs.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG).equalsIgnoreCase("PLAINTEXT")) {
		  return "jdbc:oracle:thin:@" + configs.getString(SslConfigs.TNS_ALIAS); // + "?TNS_ADMIN=" + configs.getString(SslConfigs.ORACLE_NET_TNS_ADMIN); 
        }
    	StringBuilder urlBuilder =new StringBuilder("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + Integer.toString(node.port())+")(HOST=" + node.host() +"))");
		urlBuilder.append("(CONNECT_DATA=(SERVICE_NAME=" + node.serviceName() + ")");
		if(node.instanceName()!=null && node.instanceName().length()>0)
		{
			urlBuilder.append("(INSTANCE_NAME=" + node.instanceName() + ")");
		}
		urlBuilder.append("))");
		String url = urlBuilder.toString();
		//System.out.println("TEQAssignor: ORACLE URL TO CONNECT TO " + url);
    	return url;
    }
    public static Connection createJDBCConnection(Node node, AbstractConfig configs) throws SQLException{
    	OracleDataSource s=new OracleDataSource();
    	String dbUrl = createUrl(node, configs);
		s.setURL(dbUrl);
	    Connection conn =  s.getConnection();
	    try {
    		int instId = Integer.parseInt(((oracle.jdbc.internal.OracleConnection)conn).getServerSessionInfo().getProperty("AUTH_INSTANCE_NO"));
    		String serviceName = ((oracle.jdbc.internal.OracleConnection)conn).getServerSessionInfo().getProperty("SERVICE_NAME");
    		String instanceName = ((oracle.jdbc.internal.OracleConnection)conn).getServerSessionInfo().getProperty("INSTANCE_NAME");
    		String userName = conn.getMetaData().getUserName();
    		node.setId(instId);
    		node.setService(serviceName);
    		node.setInstanceName(instanceName);
    		node.setUser(userName);
    		node.updateHashCode();
    	}catch(Exception e)
    	{
    		System.out.println("Exception while connecting to database with connection string " + dbUrl +":" + e);
    		e.printStackTrace();
    		//log.error("Exception while setting new instance ids " + e);
    		throw e;
    	}
	    return conn;
    }
    
    public static TopicConnection createTopicConnection(Node node,AbstractConfig configs, Logger log) throws JMSException {
    	if(node==null) 
    		throw new ConnectionException("Invalid argument: Node cannot be null");
    	
    	String url = createUrl(node, configs);
    	log.info("Connecting to Oracle Database : "+ url);
    	OracleDataSource dataSource;
    	try {
    		dataSource =new OracleDataSource();
    		dataSource.setURL(url);	
    	}
    	catch(SQLException sql) {
    		throw new JMSException(sql.toString());
    	}
    	TopicConnectionFactory connFactory = AQjmsFactory.getTopicConnectionFactory(dataSource);
    	TopicConnection conn = connFactory.createTopicConnection();
        conn.setClientID(configs.getString(CommonClientConfigs.CLIENT_ID_CONFIG));
        return conn;  	
    }
    
    public static TopicSession createTopicSession(TopicConnection conn, int mode, boolean transacted) throws JMSException {
    	if(conn == null)
    		throw new ConnectionException("Invalid argument: Connection cannot be null");
    	TopicSession sess = conn.createTopicSession(transacted, mode);
    	//ToDo: Validate if caching of dequeue statement helps or not
    	((AQjmsSession)sess).setDeqStmtCachingFlag(true);
    	return sess;
    }
    
    public static String getUsername(AbstractConfig configs) {
    	File file = null;
    	FileReader fr = null;
    	try {
    	file = new File(configs.getString(CommonClientConfigs.ORACLE_NET_TNS_ADMIN)+"/ojdbc.properties");
    	fr = new FileReader(file);
    	Properties prop = new Properties();
    	prop.load(fr);
    	return prop.getProperty("user").trim();
    	} catch( Exception exception) {
    		//do nothing
    	} finally {
    		try {
    			if(fr != null)
  				  fr.close();
    		}catch (Exception e) {
    			
    		}	

    	}
    	return null;
    }
    public static String enquote(String name) throws IllegalArgumentException{
		if( !name.contains("'")) {
		    if(!name.contains("\"")) return "\"" + name + "\"";
		    if(name.indexOf("\"") == 0 && name.indexOf("\"", 1) == name.length() -1 )
		       return name;
		   
		}
	    throw new IllegalArgumentException("Invalid argument provided: " + name);	
	}
    
    public static String getDBVersion(Connection conn) throws Exception
    {
    	String dbVersionQuery = "select version_full from PRODUCT_COMPONENT_VERSION where product like  'Oracle Database%'";
    	String dbVersionStr = "";
    	PreparedStatement dbVerStmt =  null;
    	ResultSet rs = null;
    	try {
    		dbVerStmt = conn.prepareStatement(dbVersionQuery);
    		dbVerStmt.execute();
    		rs = dbVerStmt.getResultSet();
    		if(rs.next()) {
    			dbVersionStr = rs.getString(1);
    		}
    	}catch(Exception e)
    	{
    		throw e;
    	}
    	finally {
    		if(rs != null)
    			rs.close();
    		
    		if(dbVerStmt != null)
    			dbVerStmt.close();
    	}
    	return dbVersionStr;
    }

}
