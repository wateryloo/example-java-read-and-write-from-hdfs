//java -jar example-java-read-and-write-from-hdfs-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://10.20.0.228:9000 /user/hive/warehouse/ssb_2_orc.db/lineorder/ 000000_0

package io.saagie.example.hdfs;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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

  static
  {
    System.load("/home/spark/source/zjs_workspace/jni_read_profile/lib.so");
  }

  private static native void cMethod(Object[] arr);

  private static final Logger logger = Logger.getLogger("io.saagie.example.hdfs.Main");

  public static void main(String[] args) throws Exception
  {
    //HDFS URI
    String hdfsUri = args[0];

    String path = args[1];
    String fileName = args[2];

    // ====== Init HDFS File System Object
    Configuration conf = new Configuration();
    // Set FileSystem URI
    conf.set("fs.defaultFS", hdfsUri);
    // Because of Maven
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    // Set HADOOP user
    System.setProperty("HADOOP_USER_NAME", "hdfs");
    System.setProperty("hadoop.home.dir", "/");
    //Get the filesystem - HDFS
    FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);

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
    logger.info(s1);
    fs.close();
    Reader reader = OrcFile.createReader(hdfsReadPath, OrcFile.readerOptions(conf));
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
      ColumnVector[] columnVectors = batch.cols;
      Object[] objects = new Object[columnVectors.length];
      for (int i = 0; i < columnVectors.length; ++i)
      {
        switch (typeNames.get(i))
        {
          case "int":
          case "bigint":
          {
            LongColumnVector longColumnVector = (LongColumnVector) columnVectors[i];
            long[] data = longColumnVector.vector;
            objects[i] = data;
            break;
          }
          case "double":
          {
            DoubleColumnVector doubleColumnVector = (DoubleColumnVector) columnVectors[i];
            double[] data = doubleColumnVector.vector;
            objects[i] = data;
            break;
          }
          case "string":
          {
            BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVectors[i];
            byte[][] data = bytesColumnVector.vector;
            objects[i] = data;
            break;
          }
          default:
            logger.info("Unknown type!!!");
            System.exit(-1);
        }
      }
      cMethod(objects);
    }

    long t3 = System.currentTimeMillis();
    recordReader.close();
    reader.close();
    String s2 = String.format("Time to pass all data to JNI: %f sec.", (t3 - t2) / 1000.0);
    logger.info(s2);
    String s3 = String.format("Total time: %f sec.", (t3 - t1) / 1000.0);
    logger.info(s3);
  }
}