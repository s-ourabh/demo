/*
 ** OKafka Java Client version 0.8.
 **
 ** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
 ** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
 */

package org.oracle.okafka.common.utils;

import org.apache.kafka.common.TopicPartition;
import org.oracle.okafka.common.errors.InvalidMessageIdException;

public class MessageIdConverter {

	public static final int DEFAULT_SUBPARTITION_SIZE = 20000;
	
	public static long getOffset(String msgId) {
		if(msgId.length() != 35) 
			throw new InvalidMessageIdException("Length of message Id  is not 35");
		try {
			String endian = msgId.substring(29, 31);
			byte endianIndex ;
			//Get endian of message id
			switch(endian) {
			//big endian
			case "FF" : 
				endianIndex = 0;
				break; 
				//little endian    
			case "66" : 
				endianIndex = 1;
				break;
				//PDP endian
			case "99" : 
				endianIndex = 2;
				break;
			default : 
				endianIndex = -1;
			}
			if(endianIndex == -1) 
				throw new InvalidMessageIdException("Endian of message Id is not valid");
			long baseOffset = getOffset(msgId.substring(3, 19), endianIndex);
			long relOffset = getOffset(msgId.substring(31, 35), endianIndex);  
			//return ((baseOffset << 16) + relOffset);
			return baseOffset*20000 + relOffset;

		} catch(RuntimeException e) {
			throw e;
		}

	}
	public static int getRelativeOffset(String msgId)
	{
		OKafkaOffset okOffset = getOKafkaOffset(msgId, false, false);
		return okOffset.sequenceNo;
	}
	public static OKafkaOffset getOKafkaOffset(String msgId)
	{
		// Most common use. Get sub Partition Id (base offset)  and sequence number ( relative offset)
		return getOKafkaOffset(msgId, false, true);
	}

	public static OKafkaOffset getOKafkaOffset(String msgId, boolean getPartId, boolean getSubPartId) {
		
		 if(msgId == null)
			return  new OKafkaOffset(-1, -1, -1); 
		
		if(msgId.length() != 35) 
			throw new InvalidMessageIdException("Length of message Id  is not 35");
		try {
			String endian = msgId.substring(29, 31);
			byte endianIndex ;
			//Get Endian of message id
			switch(endian) {
			//big Endian
			case "FF" : 
				endianIndex = 0;
				break; 
				//little Endian    
			case "66" : 
				endianIndex = 1;
				break;
				//PDP Endian
			case "99" : 
				endianIndex = 2;
				break;
			default : 
				endianIndex = -1;
			}
			if(endianIndex == -1) 
				throw new InvalidMessageIdException("Endian of message Id '" + endian +"' is not valid");
			
			int relOffset = (int)getOffset(msgId.substring(31, 35), endianIndex);

			long baseOffset = -1;
			if(getSubPartId)
				baseOffset = getOffset(msgId.substring(3, 19), endianIndex);
			
			long partId = -1;
			if (getPartId)
			{
				partId= getOffset(msgId.substring(19,27), endianIndex);
			}

			OKafkaOffset okOffset = new OKafkaOffset(partId, baseOffset, relOffset);
			return okOffset;
		} catch(RuntimeException e) {
			throw e;
		}
	}

	/**
	 * Converts hexadecimal string which is in specific endian format to decimal number
	 * @param data hexadecimal string representing either subshard or sequence number in a subshard.
	 * @param endianIndex index representing either of big, little and pdp endian.
	 * @return decimal representation of hexadecimal string.
	 */
	private static long getOffset(String data, byte endianIndex) {
		String builderString = null;
		switch(endianIndex) {
		case 0 : 
			builderString = data;
			break;
		case 1 :
			builderString = reverse(data);
			break;
		case 2 :
			builderString = swap(data);
		}
		if(builderString != null) 
			return Long.parseLong(builderString, 16);
		return -1;
	}

	/**
	 * convert hexadecimal string in little endian to big endian 
	 * @param data hexadecimal string representing either subshard or sequence number in a subshard.
	 * @return hexadecimal string in big endian
	 */
	private static String reverse(String data) {
		char[] builderArray = new char[data.length()];
		int length = data.length();
		for(int i = length-2; i >= 0 ;  i= i-2) {
			builderArray[length -2 -i] = data.charAt(i);
			builderArray[length -1 -i] = data.charAt(i+1);
		}
		return new String(builderArray);
	}

	/**
	 * convert hexadecimal string in pdp endian to big endian 
	 * @param data hexadecimal string representing either subshard or sequence number in a subshard.
	 * @return hexadecimal string in big endian
	 */
	private static String swap(String data) {
		StringBuilder sb= new StringBuilder();
		int length = data.length();
		for(int i = 0; i < length; i = i+4) {
			sb.append(data.substring(i+2, i+4));
			sb.append(data.substring(i, i+2));
		}
		return sb.toString();	
	}
	public static String getMsgId(TopicPartition tp, long offset, String endian, int priority) {
        
		StringBuilder sb = new StringBuilder("");
		/*String subpartition = String.format("%16s", Long.toHexString(offset >>> 16)).replace(' ', '0');     	
   	    String partition =  String.format("%8s",Integer.toHexString(tp.partition())).replace(' ', '0');   	
    	String seq = String.format("%4s", Long.toHexString(offset & 65535)).replace(' ', '0');
		 */
        String subpartition = String.format("%16s", Long.toHexString((int)(offset/20000))).replace(' ', '0');     	
		String partition =  String.format("%8s",Integer.toHexString(2*tp.partition())).replace(' ', '0');   	
		String seq = String.format("%4s", Long.toHexString(offset % 20000)).replace(' ', '0');
        

		if(endian.equals("66")) {
			sb.append(reverse(subpartition));
			sb.append(reverse(partition));
			sb.append("0"+priority+"66");
			sb.append(reverse(seq));   	
		} else if (endian.equals("FF")) {
			sb.append(swap(subpartition));
			sb.append(swap(partition));
			sb.append("0"+priority+"FF");
			sb.append(swap(seq));
		}
		return sb.toString();
	}

	public static class OKafkaOffset
	{
		long partitionId;
		long subPartitionId;
		int sequenceNo;

		public OKafkaOffset(long partId, long subPartId, int seqNo)
		{
			partitionId = partId;
			subPartitionId = subPartId;
			sequenceNo = seqNo;
		}
		
		public long partitionId() {
			return partitionId;
		}
		public long subPartitionId()
		{
			return subPartitionId;
		}
		public int sequenceNo()
		{
			return sequenceNo;
		}
	}

}
