package BIDMat;

import java.io.File
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;


class HDFSIO {

	def getCompressor(compress:Int):CompressionCodec = {
			import org.apache.hadoop.io.compress._;
			compress match {
			case 0 => new DefaultCodec();
			case 1 => new GzipCodec();
			case 2 => new BZip2Codec();
			case 3 => new SnappyCodec();
			}
	}

	def readMat(fname:String, omat:Mat):Mat = {
		val value = new MatIO;
		value.mat = omat;
		readThing(fname, value);
		value.mat;
	}
	
	def readND(fname:String, ond:ND):ND = {
		val value = new NDIO;
		value.nd = ond;
		readThing(fname, value);
		value.nd;
	}
	
	def readThing(fname:String, value:Writable) = {
		val conf = new Configuration();
		val path = new Path(fname);
		var reader = new Reader(conf, Reader.file(path));
		val key = new Text;
		reader.next(key, value);
		IOUtils.closeStream(reader);
	}

	def writeMat(fname:String, mat:Mat, compress:java.lang.Integer) = {
		val value = new MatIO;
		value.mat = mat;
		writeThing(fname, value, compress);
	}
	
  def writeND(fname:String, mat:ND, compress:java.lang.Integer) = {
		val value = new NDIO;
		value.nd = mat;
		writeThing(fname, value, compress);
  }

	def writeThing(fname:String, value:Writable, compress:Int) = {
		val conf = new Configuration();
		val path = new Path(fname);
		val codec = getCompressor(compress);
		val key = new Text;
		key.set(fname);
		var writer = SequenceFile.createWriter(conf, 
				Writer.file(path),
				Writer.keyClass(key.getClass()),
				Writer.valueClass(value.getClass()),
				Writer.compression(SequenceFile.CompressionType.BLOCK, codec));

		writer.append(key, value);
		IOUtils.closeStream(writer);
	}
}