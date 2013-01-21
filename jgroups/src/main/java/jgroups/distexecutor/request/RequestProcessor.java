package jgroups.distexecutor.request;

import jgroups.distexecutor.DistributedExecutors;

import org.jgroups.Message;

public interface RequestProcessor {
	public void processMessage(Message msg,DistributedExecutors executor);
}
