package trumanz.yarnExample;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;

public class ApplicationMaster {
	private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
	
	public static void main(String[] args ) throws InterruptedException{
		
		System.out.println("This is System.out.println");
		System.err.println("This is System.err.println");

		String  containId = System.getenv(Environment.CONTAINER_ID.name());
		//System.out.println("containId " + containId);
		LOG.warn("This is LOG " +  containId);
		Thread.sleep(1000*1000);
		/*
		if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
		      if (cliParser.hasOption("app_attempt_id")) {
		        String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
		        appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
		      } else {
		        throw new IllegalArgumentException(
		            "Application Attempt Id not set in the environment");
		      }
		      */
		
	}
}