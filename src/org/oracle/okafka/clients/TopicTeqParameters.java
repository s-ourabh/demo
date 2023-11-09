package org.oracle.okafka.clients;

public class TopicTeqParameters {

	int keyBased;
	int stickyDeq;
	int shardNum;
	int dbMajorVersion;
	int dbMinorVersion;
	int msgVersionSupported;

	public void setkeyBased(int keyBased)
	{
		this.keyBased = keyBased;
	}

	public void setstickyDeq(int stickyDeq)
	{
		this.stickyDeq = stickyDeq;
	}

	public void setshardNum(int shardNum)
	{
		this.shardNum = shardNum;
	}

	public int getkeyBased()
	{
		return this.keyBased;
	}
	
	public int getstickyDeq()
	{
		return this.stickyDeq;
	}
	
	public int getshardNum()
	{
		return this.shardNum;
	}

}
