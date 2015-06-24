package trumanz.yarnExample;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class ApplicationMaster {
	private final AtomicInteger sleepSeconds = new AtomicInteger(0);
	private class LaunchContainerTask implements Runnable {
		Container container;
		public LaunchContainerTask(Container container) {
			this.container = container;
		}
		public void run() {
			List<String> commands = new LinkedList<String>();
			commands.add("sleep " + sleepSeconds.addAndGet(1));
			ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
					null, null, commands, null, null, null);
			amNMClient.startContainerAsync(container, ctx);
		}
	}

	private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
		
		

		public void onContainersCompleted(List<ContainerStatus> statuses) {
			for (ContainerStatus status : statuses) {
				LOG.info("Container Completed: " + status.getContainerId().toString() 
						+ " exitStatus="+ status.getExitStatus());
				if (status.getExitStatus() != 0) {
					// restart
				}
				ContainerId id = status.getContainerId();
				runningContainers.remove(id);
				numCompletedConatiners.addAndGet(1);
			}
		}

		public void onContainersAllocated(List<Container> containers) {
			for (Container c : containers) {
				LOG.info("Container Allocated"
						+ ", id=" + c.getId() 
						+ ", containerNode=" + c.getNodeId());
				exeService.submit(new LaunchContainerTask(c));
				runningContainers.put(c.getId(), c);
			}
		}

		public void onShutdownRequest() {
		}

		public void onNodesUpdated(List<NodeReport> updatedNodes) {

		}

		public float getProgress() {
			float progress = 0;
			return progress;
		}

		public void onError(Throwable e) {
			amRMClient.stop();
		}

	}

	private class NMCallbackHandler implements NMClientAsync.CallbackHandler {

		public void onContainerStarted(ContainerId containerId,
				Map<String, ByteBuffer> allServiceResponse) {
			LOG.info("Container Stared " + containerId.toString());

		}

		public void onContainerStatusReceived(ContainerId containerId,
				ContainerStatus containerStatus) {

		}

		public void onContainerStopped(ContainerId containerId) {
			// TODO Auto-generated method stub

		}

		public void onStartContainerError(ContainerId containerId, Throwable t) {
			// TODO Auto-generated method stub

		}

		public void onGetContainerStatusError(ContainerId containerId,
				Throwable t) {
			// TODO Auto-generated method stub

		}

		public void onStopContainerError(ContainerId containerId, Throwable t) {
			// TODO Auto-generated method stub

		}

	}


	
	
	@SuppressWarnings("rawtypes")
	AMRMClientAsync amRMClient = null;
	NMClientAsyncImpl amNMClient = null;
	
	AtomicInteger numTotalContainers = new AtomicInteger(10);
	AtomicInteger numCompletedConatiners = new AtomicInteger(0);
	ExecutorService exeService = Executors.newCachedThreadPool();
	Map<ContainerId, Container> runningContainers = new ConcurrentHashMap<ContainerId, Container>();
	
	private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

	@SuppressWarnings("unchecked")
	void run() throws YarnException, IOException {

		logInformation();
		Configuration conf = new Configuration();

		// 1. create amRMClient
		
		amRMClient = AMRMClientAsync.createAMRMClientAsync(
				1000, new RMCallbackHandler());
		amRMClient.init(conf);
		amRMClient.start();
		// 2. Create nmClientAsync
		amNMClient = new NMClientAsyncImpl(new NMCallbackHandler());
		amNMClient.init(conf);
		amNMClient.start();

		// 3. register with RM and this will heartbeating to RM
		RegisterApplicationMasterResponse response = amRMClient
				.registerApplicationMaster(NetUtils.getHostname(), -1, "");

		// 4. Request containers
		response.getContainersFromPreviousAttempts();
		int numContainers = 10;

		for (int i = 0; i < numTotalContainers.get(); i++) {
			ContainerRequest containerAsk = new ContainerRequest(
					//100*10M + 1vcpu
					Resource.newInstance(100, 1), null, null,
					Priority.newInstance(0));
			amRMClient.addContainerRequest(containerAsk);
		}
	}
	
	void waitComplete() throws YarnException, IOException{
		while(numTotalContainers.get() != numCompletedConatiners.get()){
			try{
				Thread.sleep(200);
			} catch (InterruptedException ex){}
		}
		exeService.shutdown();
		amNMClient.stop();
		
		amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "dummy Message", null);
		
		amRMClient.stop();
	}

	void logInformation() {
		System.out.println("This is System.out.println");
		System.err.println("This is System.err.println");

		String containerIdStr = System
				.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());

		LOG.info("containerIdStr " + containerIdStr);

		ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
		ApplicationAttemptId appAttemptId = containerId
				.getApplicationAttemptId();
		LOG.info("appAttemptId " + appAttemptId.toString());
	}

	/*
	 * 
	 * UserGroupInformation ugi; final TimelineClient timelineClient;
	 * 
	 * TimelineEntity entity = new TimelineEntity();
	 * entity.setEntityId(appAttemptId.toString());
	 * entity.setEntityType(DSEntity.DS_APP_ATTEMPT.toString());
	 * entity.setDomainId(domainId); entity.addPrimaryFilter("user",
	 * ugi.getShortUserName()); TimelineEvent event = new TimelineEvent();
	 * event.setEventType(appEvent.toString());
	 * event.setTimestamp(System.currentTimeMillis()); entity.addEvent(event);
	 * 
	 * try{ ugi.doAs(new PrivilegedExceptionAction<TimelinePutResponse>(){
	 * public TimelinePutResponse run() throws Exception { return
	 * timelineClient.putEntities(entity); } }); } catch (Exception e){
	 * LOG.error(e); }
	 * 
	 * }
	 */

	public static void main(String[] args) throws Exception {
		ApplicationMaster am = new ApplicationMaster();
		am.run();
		am.waitComplete();
	}
}