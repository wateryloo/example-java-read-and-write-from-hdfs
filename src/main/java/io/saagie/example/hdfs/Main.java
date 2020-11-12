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

  static
  {
    System.loadLibrary("lib.so");
  }

  private static native void cMethod(byte[] arr);

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
    FSDataInputStream inputStream = fs.open(hdfsreadpath);
    //Classical input stream usage
    byte[] arr = IOUtils.toByteArray(inputStream);
    inputStream.close();
    cMethod(arr);
    logger.info("Read file complete.");
    fs.close();

  }
}
