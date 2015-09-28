/**
 * This code was originally written by Derrick Cheng in 2013.
 */

import org.apache.hadoop.io.Writable
import java.io.IOException
import java.io.DataOutput
import java.io.DataInput
import BIDMat.{Mat,FMat,IMat,SMat,BMat,DenseMat,SparseMat}
import MatType._

class MatIO extends Writable{
  
  // include implicit empty constructor
  var contents : Mat = null
  
  def mat = contents
  def mat_=(m: Mat) {
	contents = m
  }

  
  
  override def write(out: DataOutput):Unit = {
    contents match {
      case fM : FMat => writeDenseMat(fM, MatType.FMAT, (v:Float)=>out.writeFloat(v) ,out)
      case iM : IMat => writeDenseMat(iM, MatType.IMAT, (v:Int)=>out.writeInt(v) ,out)
      case sM : SMat => writeSparseMat(sM, MatType.SMAT,(v:Float)=>out.writeFloat(v) , out)
      case bM : BMat => writeSparseMat(bM, MatType.BMAT,(v:Byte)=>out.writeByte(v) , out)
    }
  }
  
  override def readFields( in: DataInput):Unit = {
    val matType : Int = in.readInt();
    matType match {
      case MatType.FMAT => readDenseMat( (nr: Int, nc:Int, data0:Array[Float])=>FMat(nr,nc,data0), ()=>in.readFloat() , in)
      case MatType.IMAT => readDenseMat( (nr: Int, nc:Int, data0:Array[Int])=>IMat(nr,nc,data0), ()=>in.readInt() , in)
      case MatType.SMAT => readSparseMat( (nr: Int, nc: Int, nnz: Int, ir : Array[Int], jc : Array[Int], data : Array[Float])=>SMat(nr, nc, nnz, ir, jc, data), ()=>in.readFloat(), in)
      case MatType.BMAT => readSparseMat( (nr: Int, nc: Int, nnz: Int, ir : Array[Int], jc : Array[Int], data : Array[Byte])=>BMat(nr, nc, nnz, ir, jc, data), ()=>in.readByte(), in)

    }
  }
  
  
  
  private def writeDenseMat[T](dM : DenseMat[T] , matType : Int , writeT: T => Unit , out : DataOutput) {
      out.writeInt(matType)
	  out.writeInt(dM.nrows)
	  out.writeInt(dM.ncols)
	  var i : Int = 0
	  while (i < dM.data.length)
	  {
	    writeT(dM.data(i))
	    i = i+1
	  }
  }
  
  // http://www.scala-lang.org/node/133
  // (x: Int, y: Int) => "(" + x + ", " + y + ")"
  // multiparam for constructMat
  // pass new DMat....
  private def readDenseMat[T : Manifest](constructMat: (Int,Int,Array[T]) => DenseMat[T] , readT: () => T, in : DataInput)
  {
    val nrows : Int = in.readInt()
    val ncols : Int = in.readInt()
    val data : Array[T] = new Array[T](nrows*ncols)
    var i : Int = 0
    while (i < data.length)
    {
      data(i)=readT()
      i = i+1
	}
    contents = constructMat(nrows,ncols,data)
  }
  
  private def writeSparseMat[T](sM : SparseMat[T], matType : Int , writeT: T => Unit, out : DataOutput) {
    out.writeInt(matType)
    out.writeInt(sM.nrows)
    out.writeInt(sM.ncols)
    out.writeInt(sM.nnz)
    var i, j, k = 0;
    while (i < sM.ir.length)
    {
      out.writeInt(sM.ir(i))
      i = i + 1;
    }
    while (j < sM.jc.length)
    {
      out.writeInt(sM.jc(j))
      j = j + 1;
    }
    while (k < sM.data.length)
    {
      writeT(sM.data(k))
      k = k + 1;
    }
  }
  
  private def readSparseMat[T : Manifest](constructMat: (Int,Int,Int,Array[Int],Array[Int],Array[T]) => SparseMat[T] , readT: () => T, in : DataInput)
  {
    val nrows : Int = in.readInt()
    val ncols : Int = in.readInt()
    var nnz : Int = in.readInt()
    var ir = new Array[Int](nrows*ncols) // should be nnz but is nrows*ncols?
    var jc = new Array[Int](ncols+1)
    val data : Array[T] = new Array[T](nrows*ncols) // should be nnz but is ncols + 1?
    
    var i, j, k = 0;
    while (i < ir.length)
    {
      ir(i) = in.readInt()
      i = i + 1;
    }
    while (j < jc.length)
    {
      jc(j) = in.readInt();
      j = j + 1;
    }
    while (k < data.length)
    {
      data(k) = readT()
      k = k + 1;
    }
    contents = constructMat(nrows,ncols,nnz,ir,jc,data)
  }

  
  
  
  
  
  
  
  
  
  
  
  
  
}
