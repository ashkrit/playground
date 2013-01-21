package jgroups.distexecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import jgroups.distexecutor.request.RequestType;
import jgroups.distexecutor.request.RequestTypeHeader;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;

public class DistributedFutureTask<V> extends FutureTask<V> {

	private Address source;
	private JChannel channel;
	private DistributedCallable<V> callable;

	public DistributedFutureTask(Callable<V> callable,Address source,JChannel channel) {
		super(callable);
		this.source = source;
		this.channel = channel;
		this.callable = (DistributedCallable<V>) callable;
		
	}
	
	public DistributedCallable<V> getCallable() {
		return callable;
	}
	
	public Address getSource() {
		return source;
	}
	
	@Override
	protected void done() {		
		try 
		{
			TaskReturnValue<V> valueToReturn = new TaskReturnValue<V>();
			valueToReturn.setValue(get());
			valueToReturn.setTaskId(callable.getTaskId());
			Message msg = new Message(source,valueToReturn);			
			msg.putHeader(DistributedExecutors.requestTypeClassId, new RequestTypeHeader((byte)RequestType.Response.ordinal()));			
			
			synchronized (channel) {
				channel.send(msg);
			}
			
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
