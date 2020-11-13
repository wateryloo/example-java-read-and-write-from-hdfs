//java -jar example-java-read-and-write-from-hdfs-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://10.20.0.228:9000 /user/hive/warehouse/ssb_2_orc.db/lineorder/ 000000_0

package io.saagie.example.hdfs;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.logging.Logger;

public class Main
{

//  static
//  {
//    System.load("/home/spark/source/zjs_workspace/jni_read_profile/lib.so");
//  }
//
//  private static native void cMethod(byte[] arr);

  private static final Logger logger = Logger.getLogger("io.saagie.example.hdfs.Main");

  public static void main(String[] args) throws Exception
  {
    //HDFS URI
    String hdfsuri = args[0];

    String path = args[1];
    String fileName = args[2];

    // ====== Init HDFS File System Object
    Configuration conf = new Configuration();
    // Set FileSystem URI
    conf.set("fs.defaultFS", hdfsuri);
    // Because of Maven
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    // Set HADOOP user
    System.setProperty("HADOOP_USER_NAME", "hdfs");
    System.setProperty("hadoop.home.dir", "/");
    //Get the filesystem - HDFS
    FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);

    //==== Read file
    logger.info("Read file into hdfs");
    //Create a path
    Path hdfsreadpath = new Path(path + "/" + fileName);
    //Init input stream
    long t1 = System.currentTimeMillis();
    FSDataInputStream inputStream = fs.open(hdfsreadpath);
    //Classical input stream usage
    String s = IOUtils.toString(inputStream);
    inputStream.close();
    System.out.println(s);
    long t2 = System.currentTimeMillis();
//    cMethod(arr);
//    long t3 = System.currentTimeMillis();
//    logger.info("Passing data to JNI complete.");
//    String s0 = "java -jar example-java-read-and-write-from-hdfs-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://10.20.0.228:9000 /tmp/ssb/10_new_transformed/lineorder/ data-m-00001.txt\nThe size is approx. 530MB according to HADOOP web UI.";
//    logger.info(s0);
    String s1 = String.format("Time to read from HDFS to Java: %f sec.", (t2 - t1) / 1000.0);
    logger.info(s1);
//    String s2 = String.format("Time to read from Java to JNI: %f sec.", (t3 - t2) / 1000.0);
//    logger.info(s2);
//    String s3 = String.format("Total time: %f sec.", (t3 - t1) / 1000.0);
//    logger.info(s3);
    fs.close();

  }
}
