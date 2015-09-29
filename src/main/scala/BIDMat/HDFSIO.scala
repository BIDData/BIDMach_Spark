package BIDMat;
import java.io.File
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.SnappyCodec;

object HDFSIO {
  
  def readMat(fname:String):Mat = {
     val conf = new Configuration();
     val path = new Path(fname);
     var reader = new Reader(conf, Reader.file(path));
     val key = new IntWritable;
     val value = new MatIO;
     reader.next(key, value);
     IOUtils.closeStream(reader);
     value.mat;
  }
  
  def readND(fname:String):ND = {
     val conf = new Configuration();
     val path = new Path(fname);
     var reader = new Reader(conf, Reader.file(path));
     val key = new IntWritable;
     val value = new NDIO;
     reader.next(key, value);
     IOUtils.closeStream(reader);
     value.nd;
  }
  
  def writeMat(fname:String, mat:Mat):Mat = {
     val conf = new Configuration();
     val fs = FileSystem.get(conf);
     val path = new Path(fname);
     val key = new IntWritable;
     key.set(0);
     val value = new MatIO;
     value.mat = mat;
     var writer = SequenceFile.createWriter(conf, 
         Writer.file(path),
         Writer.keyClass(key.getClass()),
         Writer.valueClass(value.getClass()));
/*         Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size",4096)), 
         Writer.blockSize(1073741824),
         Writer.compression(SequenceFile.CompressionType.BLOCK, new DefaultCodec()),
         Writer.progressable(null),
         Writer.metadata(new Metadata()));      */

     writer.append(key, value);
     IOUtils.closeStream(writer);
     mat;
  }
  
   def writeND(fname:String, mat:ND):ND = {
     val conf = new Configuration();
     val fs = FileSystem.get(conf);
     val path = new Path(fname);
     val key = new IntWritable;
     key.set(0);
     val value = new NDIO;
     value.nd = mat;
     var writer = SequenceFile.createWriter(conf, 
         Writer.file(path),
         Writer.keyClass(key.getClass()),
         Writer.valueClass(value.getClass()));

     writer.append(key, value);
     IOUtils.closeStream(writer);
     mat;
  }
}