package jgroups.distexecutor;

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

import jgroups.distexecutor.request.RequestType;
import jgroups.distexecutor.request.RequestTypeHeader;

import org.jgroups.Address;
import org.jgroups.Header;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

public class DistributedExecutors extends ThreadPoolExecutor {

	public static short requestTypeClassId = 103;
	private String clusterName;
	private JChannel channel;
	private ConcurrentHashMap<Integer, DistributedTaskFuture<?>> submittedTask = new ConcurrentHashMap<Integer, DistributedTaskFuture<?>>();
	private ConcurrentHashMap<DistributedFutureKey, Future<?>> inProgressTask = new ConcurrentHashMap<DistributedFutureKey, Future<?>>();
	private ConcurrentHashMap<Address, Integer> clusterLoad = new ConcurrentHashMap<Address, Integer>();
	private AtomicInteger noOfRequest = new AtomicInteger();
	private volatile View view;

	public DistributedExecutors(String clusterName, int poolSize) {
		super(poolSize, poolSize, 30, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>());
		this.clusterName = clusterName;
	}

	public DistributedExecutors(String clusterName) {
		this(clusterName, Runtime.getRuntime().availableProcessors());
	}

	public void start() throws Exception {
		channel = new JChannel();
		channel.setReceiver(new ReceiverAdapterImpl());
		channel.connect(clusterName);

	}

	public <T> Future<T> submitAndGetFuture(DistributedCallable<T> callable)
			throws Exception {
		callable.setTaskId(noOfRequest.getAndIncrement());
		Message m = new Message(null, callable);
		m.putHeader(requestTypeClassId, new RequestTypeHeader(
				(byte) RequestType.Execute.ordinal()));
		DistributedTaskFuture<T> future = new DistributedTaskFuture<T>();
		submittedTask.put(callable.getTaskId(), future);
		channel.send(m);
		return future;
	}

	private class ReceiverAdapterImpl extends ReceiverAdapter {

		public void receive(Message msg) {

			Header header = msg.getHeader(requestTypeClassId);
			if (header != null) {
				RequestTypeHeader requestType = (RequestTypeHeader) header;
				RequestType processor = RequestType.values()[requestType
						.getType()];
				processor.processMessage(msg, DistributedExecutors.this);
			}

		}

		public void viewAccepted(View view) {
			DistributedExecutors.this.view = view;
		}
	}

	public JChannel getChannel() {
		return channel;
	}

	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		DistributedFutureTask<?> task = (DistributedFutureTask<?>) r;
		inProgressTask.remove(new DistributedFutureKey(task.getSource(), task.getCallable().getTaskId()));
		sendTaskCount();
	}

	@Override
	protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
		@SuppressWarnings("unchecked")
		DistributedFutureTask<T> callableTask = (DistributedFutureTask<T>) runnable;
		inProgressTask.put(new DistributedFutureKey(callableTask.getSource(),callableTask.getCallable().getTaskId()), callableTask);
		return callableTask;
	}

	private void sendTaskCount() {
		try 
		{
			//TODO : Better way to find load on cluster
			Message m = new Message(null, inProgressTask.size());
			m.putHeader(requestTypeClassId, new RequestTypeHeader((byte) RequestType.TaskCount.ordinal()));
			channel.send(m);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private class LeastLoadedNode implements
			Comparator<Entry<Address, Integer>> {

		public int compare(Entry<Address, Integer> o1,
				Entry<Address, Integer> o2) {
			return o1.getValue().compareTo(o2.getValue());
		}

	}

	public Address identitfyNode(DistributedCallable<?> callable) {
		Address nodeToProcess = null;
		if (!clusterLoad.isEmpty()) {
			Map.Entry<Address, Integer> entry = Collections.min(clusterLoad.entrySet(), new LeastLoadedNode());
			if (entry != null) {
				nodeToProcess = entry.getKey();
			}
		}
		if (nodeToProcess == null) {
			int clusterIndex = callable.getTaskId() % view.getMembers().size();
			nodeToProcess = view.getMembers().get(clusterIndex);
		}
		return nodeToProcess;
	}

	public DistributedTaskFuture<?> removeSubmittedTask(int taskId) {
		return submittedTask.remove(taskId);
	}

	public void updateClusterLoad(Address node, Integer load) {
		clusterLoad.put(node, load);
	}
}
