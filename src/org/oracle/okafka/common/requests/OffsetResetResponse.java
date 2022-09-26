/*
** OKafka Java Client version 0.8.
**
** Copyright (c) 2019, 2020 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
*/

package org.oracle.okafka.common.requests;

import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.oracle.okafka.common.protocol.ApiKeys;

public class OffsetResetResponse extends AbstractResponse {
	private final Map<TopicPartition, Exception> offsetResetResponse;
	private final Exception exception ;
	public OffsetResetResponse(Map<TopicPartition, Exception> offsetResetResponse, Exception exception) {
		super(ApiKeys.OFFSETRESET);
		this.offsetResetResponse = offsetResetResponse;
		this.exception = exception;
	}
	
	public Map<TopicPartition, Exception> offsetResetResponse() {
		return offsetResetResponse;
	}

	public Exception getException() {
		return exception;
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