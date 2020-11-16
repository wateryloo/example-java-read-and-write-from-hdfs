//java -jar example-java-read-and-write-from-hdfs-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://10.20.0.228:9000 /user/hive/warehouse/ssb_2_orc.db/lineorder/ 000000_0

package io.saagie.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

public class Main
{

  static
  {
    System.load("/home/spark/source/zjs_workspace/jni_read_profile/lib.so");
  }

  private static native void cMethod(long[] arr);

  public static void main(String[] args) throws Exception
  {
    //HDFS URI
    long init = System.currentTimeMillis();
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

    //==== Read file
    Path hdfsReadPath = new Path(path + "/" + fileName);
    long[] times = new long[7];
    times[0] = System.currentTimeMillis();
    Reader reader = OrcFile.createReader(hdfsReadPath, OrcFile.readerOptions(conf));
    times[1] = System.currentTimeMillis();
    RecordReader recordReader = reader.rows();
    times[2] = System.currentTimeMillis();
    long rowNum = reader.getNumberOfRows();
    System.out.println(rowNum);
    long[] cols = new long[4 * (int) rowNum];
    VectorizedRowBatch batch = reader.getSchema().createRowBatch((int) rowNum);
    int length = 0;
//    int count = 0;
    times[3] = System.currentTimeMillis();
    while (recordReader.nextBatch(batch))
    {
      for (int i = 0; i < 4; ++i)
      {
        System.arraycopy(((LongColumnVector) batch.cols[0]).vector, 0, cols,
            i * (int) rowNum + length, batch.size);
      }
      length += batch.size;
//      ++count;
    }
    times[4] = System.currentTimeMillis();
    cMethod(cols);
    times[5] = System.currentTimeMillis();
//    System.out.println(length);
//    System.out.println(count);
    recordReader.close();
    reader.close();
    times[6] = System.currentTimeMillis();
    System.out.println((times[0] - init) / 1000.0);
    for (int i = 1; i < times.length; ++i)
    {
      System.out.println((times[i] - times[i - 1]) / 1000.0);
    }
  }
}

//java -jar example-java-read-and-write-from-hdfs-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://10.20.0.228:9000 /user/hive/warehouse/ssb_2_orc.db/lineorder/ 000000_0
