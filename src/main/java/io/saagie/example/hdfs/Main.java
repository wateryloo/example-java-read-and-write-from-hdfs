//java -jar example-java-read-and-write-from-hdfs-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://10.20.0.228:9000 /user/hive/warehouse/ssb_2_orc.db/lineorder/ 000000_0

package io.saagie.example.hdfs;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.logging.Logger;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

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
//    logger.info("Read file into hdfs");
    //Create a path
    Path hdfsReadPath = new Path(path + "/" + fileName);
    //Init input stream
    long t1 = System.currentTimeMillis();
    FSDataInputStream inputStream = fs.open(hdfsReadPath);
    //Classical input stream usage
    inputStream.close();
    long t2 = System.currentTimeMillis();
    String s1 = String.format("Time to read from HDFS to Java: %f sec.", (t2 - t1) / 1000.0);
    fs.close();

    Reader reader = OrcFile.createReader(hdfsReadPath, OrcFile.readerOptions(conf));
    TypeDescription typeDescription = reader.getSchema();
//    System.out.println(typeDescription.toJson());
    RecordReader recordReader = reader.rows();
    TypeDescription schema = reader.getSchema();

    List<TypeDescription> typeDescriptions = schema.getChildren();
    Iterator<TypeDescription> iterator = typeDescriptions.stream().iterator();
    ArrayList<String> typeNames = new ArrayList<>();
    while (iterator.hasNext())
    {
      TypeDescription description = iterator.next();
      typeNames.add(description.toString());
    }
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    while (recordReader.nextBatch(batch))
    {
      String col = batch.stringifyColumn(0);
      ColumnVector[] columnVectors = batch.cols;
      for (int i = 0; i < columnVectors.length; ++i)
      {
        switch (typeNames.get(i))
        {
          case "int":
          case "bigint":
          {
            LongColumnVector longColumnVector = (LongColumnVector) columnVectors[i];
            long[] data = longColumnVector.vector;
            break;
          }
          case "double":
          {
            DoubleColumnVector doubleColumnVector = (DoubleColumnVector) columnVectors[i];
            double[] data = doubleColumnVector.vector;
            break;
          }
          case "string":
          {
            BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVectors[i];
            byte[][] data = bytesColumnVector.vector;
            break;
          }
          default:
            logger.info("Known type!!!");
            System.exit(-1);
        }
      }
    }
//    System.out.println(batch.toString());
//    recordReader.nextBatch(batch);
//    System.out.println(batch.toString());
//    while (recordReader.nextBatch(batch))
//    {
//      System.out.println(batch.toString());
//    }
    recordReader.close();
    reader.close();
  }
}
