package jgroups.distexecutor.request;

import jgroups.distexecutor.DistributedCallable;
import jgroups.distexecutor.DistributedExecutors;
import jgroups.distexecutor.DistributedFutureTask;
import jgroups.distexecutor.DistributedTaskFuture;
import jgroups.distexecutor.TaskReturnValue;

import org.jgroups.Address;
import org.jgroups.Message;

public enum RequestType implements RequestProcessor {
	Execute {
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public void processMessage(Message msg, DistributedExecutors executor) {
			Object value = msg.getObject();
			DistributedCallable<?> callable = ((DistributedCallable<?>) value);
			Address nodeToProcess = executor.identitfyNode(callable);
			if (nodeToProcess.equals(executor.getChannel().getAddress())) {
				executor.submit(new DistributedFutureTask(callable, msg
						.getSrc(), executor.getChannel()));
			}
		}
	},
	Response {
		
		public void processMessage(Message msg, DistributedExecutors executor) {
			Object value = msg.getObject();
			TaskReturnValue<?> v = (TaskReturnValue<?>)value;
			DistributedTaskFuture<?> future  = executor.removeSubmittedTask(v.getTaskId());
			if(future!=null)
			{
				future.setValue(v);						
			}	
		}
	},
	TaskCount{
		
		public void processMessage(Message msg, DistributedExecutors executor) {
			Object value = msg.getObject();		
			executor.updateClusterLoad(msg.getSrc(), (Integer)value);			
		}
	};

	public void processMessage(Message msg, DistributedExecutors executor) {

	}
}
