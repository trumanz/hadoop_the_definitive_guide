package trumanz.yarnExample;


import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Logger;

/**
 * Hello world!
 *
 */
public class Client 
{
	static private  Logger logger = Logger.getLogger("trumanz");
    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Hello World!" );
        
       
        
        Configuration conf = new Configuration();
        
        //1. create and start a yarnClient
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        
        //2. create an application
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse  appResponse = app.getNewApplicationResponse();
        logger.info("appRespons.getMaximumResourceCapability() :"
        		+ appResponse.getMaximumResourceCapability().toString());
        
        //3. set the application submission context
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        logger.info("appId:"+ appId.toString());
        
      
        appContext.setKeepContainersAcrossApplicationAttempts(false);
        
        //4. add local resources
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        FileSystem fs = FileSystem.get(conf);
        String thisJar = ClassUtil.findContainingJar(Client.class);
        String thisJarBaseName = FilenameUtils.getName(thisJar);
        logger.info("thisJar is " + thisJar);
        
        addToLocalResources(fs, thisJar, thisJarBaseName, appId.toString(), localResources);
        
        //5. set env , CLASSPTH
        Map<String, String> env  = new HashMap<String, String>();
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$());
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
        classPathEnv.append("./*");
      for (String c : conf.getStrings(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
        classPathEnv.append(c.trim());
      }
      
      if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
        classPathEnv.append(':');
        classPathEnv.append(System.getProperty("java.class.path"));
      }

      env.put(Environment.CLASSPATH.name(), classPathEnv.toString());
        
        //6. set java executable command
        List<String> commands = new LinkedList<String>();
        commands.add(Environment.JAVA_HOME.$$() + "/bin/java" + 
        		" trumanz.yarnExample.ApplicationMaster");
        //commands.add("sleep 1000");
        
        //application master
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
        		localResources, env, commands, null, null, null);
        		
        if(UserGroupInformation.isSecurityEnabled()){
        	throw new Exception("SecurityEnabled , not support");
        }
        
        //100m 1cpu
        Resource capability = Resource.newInstance(100, 1);
        appContext.setApplicationName("truman.ApplicationMaster");
        appContext.setResource(capability);
        
        appContext.setAMContainerSpec(amContainer);
        
        //submit 
        appContext.setPriority(Priority.newInstance(0));
        appContext.setQueue("default");
        yarnClient.submitApplication(appContext);
        
        
        ApplicationReport report = yarnClient.getApplicationReport(appId);
        
        monitorApplicationReport(report);
        
    
    }
     private static void addToLocalResources(FileSystem fs, String fileSrcPath, String fileDstPath,
    		String appId, Map<String, LocalResource> localResources) 
    				throws IllegalArgumentException, IOException{
    	String suffix = "mytest" + "/" + appId + "/" + fileDstPath;
    	Path dst = new Path(fs.getHomeDirectory(), suffix);
    	logger.info("hdfs copyFromLocalFile " + fileSrcPath   + " =>" + dst);
    	fs.copyFromLocalFile(new Path(fileSrcPath), dst);
    	FileStatus scFileStatus = fs.getFileStatus(dst);
    	LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromPath(dst), 
    			LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
    			scFileStatus.getLen(), scFileStatus.getModificationTime());
    	
    	localResources.put(fileDstPath, scRsrc);
 
    }
     
     private static void  monitorApplicationReport(ApplicationReport report)
     {
    	 while(true){
    		 try{
    			 Thread.sleep(5*1000);
    		 } catch (InterruptedException e){
    			 
    		 }
    		 
    	 logger.info("Got application report "
    	          + ", clientToAMToken=" + report.getClientToAMToken()
    	          + ", appDiagnostics=" + report.getDiagnostics()
    	          + ", appMasterHost=" + report.getHost()
    	          + ", appQueue=" + report.getQueue()
    	          + ", appMasterRpcPort=" + report.getRpcPort()
    	          + ", appStartTime=" + report.getStartTime()
    	          + ", yarnAppState=" + report.getYarnApplicationState().toString()
    	          + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
    	          + ", appTrackingUrl=" + report.getTrackingUrl()
    	          + ", appUser=" + report.getUser());
    	 }
     }
}
