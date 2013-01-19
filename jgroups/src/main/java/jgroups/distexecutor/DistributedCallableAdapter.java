package jgroups.distexecutor;


public abstract class DistributedCallableAdapter<V> implements DistributedCallable<V> {

	private static final long serialVersionUID = -7827855571411686504L;
	private int taskId;
	
	public int getTaskId() {		
		return taskId;
	}
	
	public void setTaskId(int taskId) {
		this.taskId=taskId;		
	}

}
