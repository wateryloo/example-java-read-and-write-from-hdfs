//java -jar example-java-read-and-write-from-hdfs-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://10.20.0.228:9000 /user/hive/warehouse/ssb_2_orc.db/lineorder/ 000000_0

package io.saagie.example.hdfs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;
import org.apache.commons.io.IOUtils;

public class Main
{

  /*
  static
  {
    System.load("/home/spark/source/zjs_workspace/jni_read_profile/lib.so");
  }
  private static native void cMethod(byte[] arr);
   */

  /**
   * Get a file system from uri of HDFS.
   *
   * @param hdfsUri The uri fo HDFS.
   * @return A file system of HDFS.
   * @throws IOException IOException.
   */
  private static FileSystem getFs(String hdfsUri) throws IOException
  {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", hdfsUri);
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    System.setProperty("HADOOP_USER_NAME", "hdfs");
    System.setProperty("hadoop.home.dir", "/");
    return FileSystem.get(URI.create(hdfsUri), conf);
  }

  /**
   * Read from HDFS.
   *
   * @param fs       The file system.
   * @param hdfsPath The path of text file in hdfs.
   * @return A string as output.
   * @throws IOException IOException.
   */
  private static String readTextFromHdfs(FileSystem fs, String hdfsPath) throws IOException
  {
    FSDataInputStream inputStream = fs.open(new Path(hdfsPath));
    return IOUtils.toString(inputStream);
  }

  /**
   * Read from HDFS.
   *
   * @param hdfsUri  The URI of HDFS.
   * @param hdfsPath The path of file.
   * @throws IOException IOException.
   */
  private static void testReadFromHdfs(String hdfsUri, String hdfsPath) throws IOException
  {
    //HDFS URI

    FileSystem fs = getFs(hdfsUri);
    System.out.println(readTextFromHdfs(fs, hdfsPath));
  }

  /**
   * TODO: Read ORC from HDFS. The format of input and return is to be determined.
   *
   * @param fs       The file system.
   * @param hdfsPath The path of ORC file.
   * @throws IOException IOException.
   */
  private static void readOrcFromHdfs(FileSystem fs, String hdfsPath) throws IOException
  {

  }

  /*
  private static void readHrcFromHdfs() throws Exception
  {
    //HDFS URI
    String hdfsUri = "";

    String path = "";
    String fileName = "";

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
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    while (recordReader.nextBatch(batch))
    {
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
            for (long val : data)
            {
              dataOutputStream.writeLong(val);
            }
            break;
          }
          case "double":
          {
            DoubleColumnVector doubleColumnVector = (DoubleColumnVector) columnVectors[i];
            double[] data = doubleColumnVector.vector;
            for (double val : data)
            {
              dataOutputStream.writeDouble(val);
            }
            break;
          }
          case "string":
          {
            BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVectors[i];
            byte[][] data = bytesColumnVector.vector;
            for (byte[] val : data)
            {
              if (val != null)
              {
                dataOutputStream.write(val);
              }
              else
              {
                dataOutputStream.write('\0');
              }
            }
            break;
          }
          default:
            logger.info("Unknown type!!!");
            System.exit(-1);
        }
      }
      dataOutputStream.flush();
    }
    dataOutputStream.close();
    byte[] bytes = byteArrayOutputStream.toByteArray();
    byteArrayOutputStream.flush();
    byteArrayOutputStream.close();
    long t3 = System.currentTimeMillis();
    String s3 = String.format("Time to prepare bytes array: %f sec.", (t3 - t2) / 1000.0);
    logger.info(s3);
    long t4 = System.currentTimeMillis();
    String s2 = String.format("Time to transfer data: %f sec.", (t4 - t3) / 1000.0);
    logger.info(s2);
    String s4 = String
        .format("Total data: %d bytes. Total time %f sec.", bytes.length, (t4 - t1) / 1000.0);
    logger.info(s4);
    recordReader.close();
    reader.close();
  }
   */

  public static void main(String[] args)
  {
    System.out.println("START");
    try
    {
      String hdfsUri = "hdfs://10.20.0.228:9000";
      String hdfsPath = "/user/hdfs/example/hdfs/hello.csv";
      testReadFromHdfs(hdfsUri, hdfsPath);
    } catch (IOException exception)
    {
      exception.printStackTrace();
    } finally
    {
      System.out.println("END");
    }
  }
}
