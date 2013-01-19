package jgroups.distexecutor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DistributedTaskFuture<V> implements Future<V> {

	private CountDownLatch latch = new CountDownLatch(1);
	private TaskReturnValue<V> value;
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	public boolean isCancelled() {
		return false;
	}

	public boolean isDone() {
		return false;
	}

	public V get() throws InterruptedException, ExecutionException {
		latch.await();
		return value.getValue();
	}

	public V get(long timeout, TimeUnit unit) throws InterruptedException,ExecutionException, TimeoutException {		
		latch.await(timeout, unit);
		return value.getValue();
	}
	
	@SuppressWarnings("unchecked")
	public void setValue(TaskReturnValue<?> value)
	{
		this.value = (TaskReturnValue<V>)value;
		latch.countDown();
	}
}
