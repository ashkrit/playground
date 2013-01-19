package jgroups.distexecutor.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import jgroups.distexecutor.DistributedCallableAdapter;
import jgroups.distexecutor.DistributedExecutors;


public class DistributedClient {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		DistributedExecutors ex = new DistributedExecutors("disttp");
		ex.start();
		List<Future<String>> ftList = new ArrayList<Future<String>>();
		
		for(int x=0;x<10;x++)
		{
			ftList.add(ex.submitAndGetFuture(new SimpleDistCallable(ex.getChannel().getAddressAsString() + "-" + x)));
		}
		
		for(Future<?> f : ftList)
		{
			System.out.println("Got Response for  :" + f.get());
		}
	}
	
	
	private static class SimpleDistCallable extends DistributedCallableAdapter<String>
	{
		private static final long serialVersionUID = 1L;
		private String taskName;
		
		public SimpleDistCallable(String taskName)
		{
			this.taskName = taskName;
		}
		
		public String call() throws Exception {
			System.out.println("Process ..." + taskName);
			//Thread.sleep(1000*10);
			return taskName;
		}
		
		
	}

}
