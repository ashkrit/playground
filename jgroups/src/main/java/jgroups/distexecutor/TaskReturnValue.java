package jgroups.distexecutor;

import java.io.Serializable;

public class TaskReturnValue<V> implements Serializable
{
	private static final long serialVersionUID = 1L;
	private V value;
	private int taskId;
	
	public V getValue() {
		return value;
	}
	
	public void setValue(V value) {
		this.value = value;
	}
	
	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}
	
	public int getTaskId() {
		return taskId;
	}
	
}
