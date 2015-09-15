package com.cloudera.ktaneja;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MiniMRClientCluster;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory;
/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
	private static  MiniDFSCluster dfsCluster = null;
	private static MiniMRClientCluster mrCluster = null;
	private static Log logger = LogFactory.getLog(com.cloudera.ktaneja.AppTest.class);

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class )  {
        	protected void setup() throws Exception {
        	
        	}
        	protected void tearDown() throws Exception {
        		
        	}
        };
        
    }

    /**

     * @throws IOException 
     * @throws IllegalArgumentException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    public void testApp() throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
    {
    	// clean up input and output directories. 
    	// If they exist then delete and re-create them
    	instantiateUnitTestFileSystem();
    	
    	
		startMRCluster();
		
    	Job job = Job.getInstance(mrCluster.getConfig(), "Word Count Unit Test");

        //job.setJarByClass(WordCounterClass.class);
        //job.setJobName("Word Count Unit Test");
        FileInputFormat.setInputPaths(job, new Path("/tmp/hdapp1/input"));
        FileOutputFormat.setOutputPath(job, new Path("/tmp/hdapp1/output"));
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        logger.info("Submitting MR Task");
        boolean success = job.waitForCompletion(true);
        //System.exit(success ? 0 : 1);
        logger.info("MR TASK Finished");
        
       stopMRCluster();
    	
        assertTrue( success );
    }
    
    public void testDFSApp() {
    	
    	
    	assertTrue(true);
    }

	private void instantiateUnitTestFileSystem() throws IOException {
		String input_path = "/tmp/hdapp1/input/";
		String output_path = "/tmp/hdapp1/output/";
		
		delete(output_path,true);
		delete(input_path,true);
		
		File input_dir = new File(input_path);
		
		input_dir.mkdirs();
		//output_dir.mkdirs();
		DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("/tmp/hdapp1/input/Data.txt")));
		out.writeUTF("The quick brown fox jumps over the lazy dog");
		out.close();
	
	}
	
	 public static boolean delete(String filePath, boolean recursive) {
	      File file = new File(filePath);
	      if (!file.exists()) {
	          return true;
	      }

	      if (!recursive || !file.isDirectory())
	          return file.delete();

	      String[] list = file.list();
	      for (int i = 0; i < list.length; i++) {
	          if (!delete(filePath + File.separator + list[i], true))
	              return false;
	      }
	      return file.delete();
	  }
	 
	 public static void startMRCluster() throws IOException {
		 logger.info("Starting the MiniCluster");
	    	class internalClass { };
			mrCluster = MiniMRClientClusterFactory.create(internalClass.class, 1, new Configuration());
	 }
	 
	 public static void stopMRCluster() throws IOException {
		logger.info("Stopping the MiniCluster");
		 mrCluster.stop();
	 }
	 
	 public static void startDFSCluster() {
		 //File baseDir = new File("./target/hdfs/" + testName).getAbsoluteFile();
		 //Configuration conf = new Configuration();
		  //conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
		  //MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		  //MiniDFSCluster hdfsCluster = builder.build();
		  //String hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort()} + "/";
	 }
		
}
    
    
    

