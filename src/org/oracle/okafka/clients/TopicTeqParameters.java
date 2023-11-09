package org.oracle.okafka.clients;


import java.util.HashMap;

public class TopicTeqParameters {

	int keyBased;
	int stickyDeq;
	int shardNum;
	int dbMajorVersion;
	int dbMinorVersion;
	int msgVersion;


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

	public void setmsgVersion() {
		if(getstickyDeq()!=2) {
			this.msgVersion = 1;
		}
		else {
			this.msgVersion = 2;
		}
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

	public int getmsgVersion() {
		return this.msgVersion;
	}

}
