package matrix_factorization

import breeze.linalg._
import breeze.optimize._
import org.apache.spark.rdd.RDD


object test{
  def main(args: Array[String]): Unit = {

//    val f = new DiffFunction[DenseVector[Double]] {
//                    def calculate(x: DenseVector[Double]) = {
//                       (norm((x - 3d) :^ 2d,1d),(x * 2d) - 6d);
//                    }
//                  }
    val dm = DenseMatrix((1.0,1.0,1.0),
                         (2.0,2.0,2.0),
                         (3.0,3.0,3.0))

    val idx = Iterable[Long](0,2)
    val rates = Iterable[Int](2,0)
    print(pinv(computeTransposeProductSum(idx,dm)))
//    print(computeRatingProduct(idx,rates,dm))
//    val lambda = 1

//    val m = DenseMatrix.rand[Double](3,3)
//    val u = DenseMatrix.rand[Double](3,3)
////
//    println(m.rows, m.cols)
//    println(m)
//    m(4,::) := DenseVector(1,2,3,4,5).t
//    println(m)
//    print(computeTransposeProductSum(idx,dm))
  }

  def computeGradient(P: DenseMatrix[Double],Q: DenseMatrix[Double],R: RDD[(Long,(Long,Int))]): Unit = {
    /*
    @params
    P : DenseMatrix[Double]   : Broadcasted dense latent factor matrix
    Q: DenseMatrix[Double]    : Broadcasted dense latent factor matrix
    R: RDD[(Long,(Long,Int))] : Input RDD of the dense representation of the rating matrix

    Note : Possible addition of a Lambda param.

    Computes the gradient step and updates for each latent factor matrix.
    */

    val lambda = 1.0
    var temp = R.groupByKey()
    val P_u = temp.mapValues(values => values.unzip)
    P_u.mapValues{ case (colList, rateList) =>
      pinv(computeTransposeProductSum(colList,Q) + lambda *:* DenseMatrix.eye[Double](Q.rows))* computeRatingProduct(colList, rateList ,Q) }
    }

  def computeTransposeProductSum(columns: Iterable[Long], M : DenseMatrix[Double]): DenseMatrix[Double]= {
    /*
    @params
    columns: Iterable[Long] : list of indices associated with given user/item.
    M: DenseMatrix[Double]    : Broadcasted dense latent factor matrix

    */
    var LatentFactorSum = DenseMatrix.zeros[Double](M.rows,M.rows)
    for (i <- columns.toList){
      val C = M(::, i.toInt)
      val ColProd = C * C.t
      LatentFactorSum :+=  ColProd }
    return LatentFactorSum
  }

  def computeRatingProduct(columns: Iterable[Long], ratings: Iterable[Int], M : DenseMatrix[Double]): DenseMatrix[Double]= {
    /*
    @params
    columns: Iterable[Long] : list of indices associated with given user/item.
    ratings: Iterable[Int]
    M: DenseMatrix[Double]    : Broadcasted dense latent factor matrix

*/
    var RatingProdFactorSum = DenseVector.zeros[Double](M.rows)
    val TList = ratings.toList.zip(columns.toList)
    for (i <- TList) {
      val r_ui = i._1.toDouble
      val C = M(::, i._2.toInt)
      val result = r_ui *:* C
      RatingProdFactorSum :+= result
    }
    return RatingProdFactorSum.toDenseMatrix.t
  }
}
