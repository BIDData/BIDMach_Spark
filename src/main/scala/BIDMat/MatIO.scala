package BIDMat

import org.apache.hadoop.io.Writable
import java.io.IOException
import java.io.DataOutput
import java.io.DataInput

class MatIO extends Writable with MatIOtrait {
  
  var mats : Array[Mat] = null;
  def mat = mats(0);
  def len = mats.length
  def get:Array[Mat] = mats
  def mat_=(m:Mat) = {
    if (mats == null) {
      mats = new Array[Mat](1);
    }
    mats(0) = m;
  }
  
  def mkmats(n:Int) = {
	  if (mats == null || mats.length != n) {
		  mats = new Array[Mat](n);
	  }
  }

  override def write(out: DataOutput):Unit = {
    out.writeInt(mats.length);
    for (i <- 0 until mats.length) {
    	mats(i) match {
    	case fM:FMat => {out.writeInt(MatTypeTag.FMat); HMat.saveFMat(out, fM);}
    	case iM:IMat => {out.writeInt(MatTypeTag.IMat); HMat.saveIMat(out, iM);}
    	case lM:LMat => {out.writeInt(MatTypeTag.LMat); HMat.saveLMat(out, lM);}
    	case dM:DMat => {out.writeInt(MatTypeTag.DMat); HMat.saveDMat(out, dM);}
    	case sM:SMat => {out.writeInt(MatTypeTag.SMat); HMat.saveSMat(out, sM);}
    	case sdM:SDMat => {out.writeInt(MatTypeTag.SDMat); HMat.saveSDMat(out, sdM);}
    	case sbM:SBMat => {out.writeInt(MatTypeTag.SBMat); HMat.saveSBMat(out, sbM);}
    	case csM:CSMat => {out.writeInt(MatTypeTag.CSMat); HMat.saveCSMat(out, csM);}
    	}
    }
  }
  
  override def readFields(in: DataInput):Unit = {
    val nmats : Int = in.readInt();
    mats = new Array[Mat](nmats);
    for (i <- 0 until nmats) {
    	val matType : Int = in.readInt();
      matType match {
      case MatTypeTag.FMat => mats(i) = HMat.loadFMat(in, mats(i));
      case MatTypeTag.IMat => mats(i) = HMat.loadIMat(in, mats(i));
      case MatTypeTag.LMat => mats(i) = HMat.loadLMat(in, mats(i));
      case MatTypeTag.DMat => mats(i) = HMat.loadDMat(in, mats(i));
      case MatTypeTag.SMat => mats(i) = HMat.loadSMat(in, mats(i));
      case MatTypeTag.SDMat => mats(i) = HMat.loadSDMat(in, mats(i));
      case MatTypeTag.SBMat => mats(i) = HMat.loadSBMat(in, mats(i));
      case MatTypeTag.CSMat => mats(i) = HMat.loadCSMat(in, mats(i));
      }
    } 
  }  
}

class NDIO extends Writable {
  
  var nds : Array[ND] = null;
  def nd = nds(0);
  def nd_=(m:ND) = {
    if (nds == null) {
      nds = new Array[ND](1);
    }
    nds(0) = m;
  }
  
  def mknds(n:Int) = {
    if (nds == null || nds.length != n) {
      nds = new Array[ND](n);
    }
  }

  override def write(out: DataOutput):Unit = {
	  out.writeInt(nds.length);
	  for (i <- 0 until nds.length) {
		  nds(i) match {
		  case fM:FND => {out.writeInt(MatTypeTag.FND); HMat.saveFND(out, fM);}
		  case _ => throw new RuntimeException("NDwrite: type not matched");
		  }
    }
  }
  
  override def readFields(in: DataInput):Unit = {
    val nnds : Int = in.readInt();
    nds = new Array[ND](nnds);
    for (i <- 0 until nnds) {
    	val matType : Int = in.readInt();
      matType match {
      case MatTypeTag.FND => nds(i) = HMat.loadFND(in, nds(i));
      case _ => throw new RuntimeException("NDread: type not matched");
      }
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
  final val FND = 630;
}
