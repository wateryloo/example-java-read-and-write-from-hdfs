//java -jar example-java-read-and-write-from-hdfs-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://10.20.0.228:9000 /tmp/ zjs_demo_orc

package io.saagie.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

public class Main
{

//  static
//  {
//    System.load("/home/spark/source/zjs_workspace/jni_read_profile/lib.so");
//  }
//
//  private static native void cMethod(Object[] arr);

  public static void main(String[] args) throws Exception
  {
    //HDFS URI
    String hdfsUri = args[0];

    String path = args[1];
    String fileName = args[2];

    // ====== Init HDFS File System Object
    Configuration conf = new Configuration();
    TypeDescription schema = TypeDescription.fromString("struct<col:bigint>");
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
//    logger.info("Read file into hdfs");
    //Create a path
    Path hdfsReadPath = new Path(path + "/" + fileName);

    //Init input stream
    //Classical input stream usage

    VectorizedRowBatch inputRowBatch = new VectorizedRowBatch(1, 256 * 1024 * 1024);
    inputRowBatch.selectedInUse = false;
    inputRowBatch.selected = new int[256 * 1024 * 1024];
    for (int i = 0; i < inputRowBatch.selected.length; ++i)
    {
      inputRowBatch.selected[i] = i;
    }
    LongColumnVector vector = new LongColumnVector();
    vector.vector = new long[256 * 1024 * 1024];
    for (int i = 0; i < vector.vector.length; ++i)
    {
      vector.vector[i] = (long) (100000.0 * Math.random());
    }
    inputRowBatch.cols[0] = vector;
    Writer writer = OrcFile
        .createWriter(hdfsReadPath, OrcFile.writerOptions(conf).setSchema(schema));
    writer.addRowBatch(inputRowBatch);
    inputRowBatch.reset();
    writer.close();

  }
}
