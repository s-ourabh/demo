/*
** OKafka Java Client version 0.8.
**
** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.common.requests;

import java.util.Map;

import javax.jms.JMSException;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.oracle.okafka.common.protocol.ApiKeys;

public class SubscribeResponse extends AbstractResponse {
    private final JMSException exception;
	private final String topic;
	
	public SubscribeResponse(String topic, JMSException exception) {
		super(ApiKeys.SUBSCRIBE);
		this.topic = topic;
		this.exception = exception;
	}
	
	public String getTopic() {
		return this.topic;
		
	}
	
	public JMSException getException() {
		return this.exception;
	}

	@Override
	public ApiMessage data() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Errors, Integer> errorCounts() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int throttleTimeMs() {
		// TODO Auto-generated method stub
		return 0;
	}

}
