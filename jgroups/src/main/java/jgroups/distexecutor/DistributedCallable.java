package jgroups.distexecutor;

import java.io.Serializable;
import java.util.concurrent.Callable;


public interface DistributedCallable<V> extends Callable<V>, Serializable {
	public int getTaskId();	
	public void setTaskId(int taskId);
}
