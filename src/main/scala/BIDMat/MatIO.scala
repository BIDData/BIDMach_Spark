package BIDMat

import org.apache.hadoop.io.Writable
import java.io.IOException
import java.io.DataOutput
import java.io.DataInput

class MatIO extends Writable {
  
  var contents : Mat = null
  
  def mat = contents
  def mat_=(m: Mat) {
	contents = m
  }

  override def write(out: DataOutput):Unit = {
    contents match {
      case fM:FMat => {out.writeInt(MatTypeTag.FMat); HMat.saveFMat(out, fM);}
    }
  }
  
  override def readFields(in: DataInput):Unit = {
    val matType : Int = in.readInt();
    matType match {
      case MatTypeTag.FMat => mat = HMat.loadFMat(in, mat);
    }
  } 
  
}

class NDIO extends Writable {

  var contents : ND = null
  
  def nd = contents
  def nd_=(m: ND) {
	  contents = m
  }

  override def write(out: DataOutput):Unit = {
    contents match {
      case _ => throw new RuntimeException("NDwrite: type not matched");
    }
  }
  
  override def readFields(in: DataInput):Unit = {
    val matType : Int = in.readInt();
    matType match {
      case _ => throw new RuntimeException("NDread: type not matched");
    }
  } 
  
}

object MatTypeTag {
  final val IMat = 110;
  final val LMat = 120;
  final val FMat = 130;
  final val DMat = 140;
  final val SMat = 231;
  final val SMatX = 331; // compressed indices
  final val SDMat = 241;
  final val SDMatX = 341; // compressed indices
  final val SBMat = 201;
  final val SBMatX = 301;
  final val CSMat = 202;
  final val CSMatX = 302;
}
