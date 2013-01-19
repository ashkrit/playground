package jgroups.distexecutor;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jgroups.Address;
import org.jgroups.Header;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

public class DistributedExecutors extends ThreadPoolExecutor{

	public static short requestTypeClassId=103;
	private String clusterName;
	private JChannel channel;
	private ConcurrentHashMap<Integer, DistributedTaskFuture<?>> submittedTask = new ConcurrentHashMap<Integer, DistributedTaskFuture<?>>();
	private ConcurrentHashMap<DistFutureKey, Future<?>> inProgressTask = new ConcurrentHashMap<DistFutureKey, Future<?>>();
	private ConcurrentHashMap<Address, Integer> clusterStatus = new ConcurrentHashMap<Address, Integer>();
	private AtomicInteger noOfRequest = new AtomicInteger();
	
	public enum RequestType
	{
		Execute,
		Response,
		TaskCount
	}	
	
	public DistributedExecutors(String clusterName,int poolSize) {
		super(poolSize, poolSize,0L, TimeUnit.MILLISECONDS,new LinkedBlockingQueue<Runnable>());
		this.clusterName = clusterName;		
	}
	
	public DistributedExecutors(String clusterName) {
		this(clusterName, Runtime.getRuntime().availableProcessors());
	}
	
	
	public void start() throws Exception
	{
		channel=new JChannel();		
	    channel.setReceiver( new ReceiverAdapterImpl());
		channel.connect(clusterName);	    
	    
	}	
	
	public <T> Future<T> submitAndGetFuture(DistributedCallable<T> callable) throws Exception
	{
		callable.setTaskId(noOfRequest.getAndIncrement());
		Message m = new Message(null, callable);		
		m.putHeader(requestTypeClassId, new RequestTypeHeader((byte)RequestType.Execute.ordinal()));
		DistributedTaskFuture<T> future = new DistributedTaskFuture<T>();
		submittedTask.put(callable.getTaskId(),future);
		channel.send(m);	
		return future;
	}
	
	private class ReceiverAdapterImpl extends ReceiverAdapter
	{		
		private volatile View view;
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public void receive(Message msg) {			
			Object value = msg.getObject();			
			Header header = msg.getHeader(requestTypeClassId);
			if(header!=null)
			{
				RequestTypeHeader requestType = (RequestTypeHeader)header;
				if(requestType.type == (byte)RequestType.Execute.ordinal())
				{
					DistributedCallable<?> callable = ((DistributedCallable<?>)value);
					Address nodeToProcess = identitfyNode(callable);
					
					if(nodeToProcess.equals(channel.getAddress()))
					{
						submit(new DistributedFutureTask(callable, msg.getSrc(),channel));
					}
				}
				else if(requestType.type == (byte)RequestType.Response.ordinal())
				{
					TaskReturnValue<?> v = (TaskReturnValue<?>)value;
					DistributedTaskFuture<?> future  = submittedTask.remove(v.getTaskId());
					if(future!=null)
					{
						future.setValue(v);						
					}				
					//System.out.println(String.format("Reply Msg %s, value %s" , msg,v.getTaskId()));
				}
				else if(requestType.type == (byte)RequestType.TaskCount.ordinal())
				{
					
					clusterStatus.put(msg.getSrc(), (Integer)value);
					//System.out.println("Cluster:" + clusterStatus);
				}
				
			}
			
        }

		private Address identitfyNode(DistributedCallable<?> callable) 
		{
			Address nodeToProcess = null;
			if(!clusterStatus.isEmpty())
			{
				Map.Entry<Address, Integer> entry = Collections.min(clusterStatus.entrySet(), new LeastLoadedNode());
				if(entry!=null)
				{
					nodeToProcess = entry.getKey();
				}
			}			
			if(nodeToProcess==null)
			{
				int clusterIndex = callable.getTaskId() % view.getMembers().size();
				nodeToProcess = view.getMembers().get(clusterIndex);
			}
			return nodeToProcess;
		}
		
		public void viewAccepted(View view) {		
			this.view = view; 
	    }
	}
	
	public JChannel getChannel() {
		return channel;
	}	
		
	@Override
	protected void afterExecute(Runnable r, Throwable t) { 
		DistributedFutureTask<?> task = (DistributedFutureTask<?>)r;
		inProgressTask.remove(new DistFutureKey(task.getSource(), task.getCallable().getTaskId()));
		sendTaskCount();
	}
	
	@Override
	protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
		@SuppressWarnings("unchecked")
		DistributedFutureTask<T> callableTask = (DistributedFutureTask<T>)runnable;
		inProgressTask.put( new DistFutureKey(callableTask.getSource(), callableTask.getCallable().getTaskId()), callableTask);
		return callableTask;
    }
	
	public static class RequestTypeHeader extends Header
	{
		public RequestTypeHeader(){}
		
		public void setType(byte type) {
			this.type = type;
		}
		
		public byte getType() {
			return type;
		}
		
		private byte type;
		public RequestTypeHeader(byte type)
		{
			this.type = type;
		}
		public void writeTo(DataOutput out) throws Exception {
			out.writeByte(type);				
		}
		
		public void readFrom(DataInput in) throws Exception {
			type = in.readByte();				
		}
		
		@Override
		public int size() {
			return 1;
		}
	}
	
	public static class DistFutureKey
	{
		private Address address;
		private int requestId;
		public DistFutureKey(Address source, int requestId) {
			super();
			this.address = source;
			this.requestId = requestId;
		}
		
		@Override
        public int hashCode() {
            int result = 0;
            result += ((address == null) ? 0 : address.hashCode());
            result +=  (int) (requestId ^ (requestId >>> 32));
            return result;
        }

        // @see java.lang.Object#equals(java.lang.Object)
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            DistFutureKey other = (DistFutureKey) obj;
            if (address == null) {
                if (other.address != null) return false;
            }
            else if (!address.equals(other.address)) return false;
            if (requestId != other.requestId) return false;
            return true;
        }	
	}
	
	private void sendTaskCount()
	{
		try 
		{
			Message m = new Message(null,inProgressTask.size());		
			m.putHeader(requestTypeClassId, new RequestTypeHeader((byte)RequestType.TaskCount.ordinal()));
			channel.send(m);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private class LeastLoadedNode implements Comparator<Entry<Address, Integer>>
	{
		
		public int compare(Entry<Address, Integer> o1,Entry<Address, Integer> o2) {
			return o1.getValue().compareTo(o2.getValue());
		}
		
	}
	
}
