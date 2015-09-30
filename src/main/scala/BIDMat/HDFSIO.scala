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
import org.apache.hadoop.io.IntWritable;
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
			val conf = new Configuration();
			val path = new Path(fname);
			var reader = new Reader(conf, Reader.file(path));
			val key = new IntWritable;
			val value = new MatIO;
      value.mat = omat;
			reader.next(key, value);
			IOUtils.closeStream(reader);
			value.mat;
	}

	def readND(fname:String, ond:ND):ND = {
			val conf = new Configuration();
			val path = new Path(fname);
			var reader = new Reader(conf, Reader.file(path));
			val key = new IntWritable;
			val value = new NDIO;
      value.nd = ond;
			reader.next(key, value);
			IOUtils.closeStream(reader);
			value.nd;
	}

	def writeMat(fname:String, mat:Mat, compress:java.lang.Integer):Mat = {
			val conf = new Configuration();
			val path = new Path(fname);
			val codec = getCompressor(compress);
			val key = new IntWritable;
			key.set(0);
			val value = new MatIO;
			value.mat = mat;
			var writer = SequenceFile.createWriter(conf, 
					Writer.file(path),
					Writer.keyClass(key.getClass()),
					Writer.valueClass(value.getClass()),
					Writer.compression(SequenceFile.CompressionType.BLOCK, codec));
			/*         Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size",4096)), 
         Writer.blockSize(1073741824),
         Writer.progressable(null),
         Writer.metadata(new Metadata()));      */

			writer.append(key, value);
			IOUtils.closeStream(writer);
			mat;
	}

	def writeND(fname:String, mat:ND, compress:java.lang.Integer):ND = {
		val conf = new Configuration();
		val path = new Path(fname);
		val codec = getCompressor(compress);
		val key = new IntWritable;
		key.set(0);
		val value = new NDIO;
		value.nd = mat;
		var writer = SequenceFile.createWriter(conf, 
				Writer.file(path),
				Writer.keyClass(key.getClass()),
				Writer.valueClass(value.getClass()),
				Writer.compression(SequenceFile.CompressionType.BLOCK, codec));

		writer.append(key, value);
		IOUtils.closeStream(writer);
		mat;
	}
}