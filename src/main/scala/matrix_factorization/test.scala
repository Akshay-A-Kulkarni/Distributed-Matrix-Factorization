package matrix_factorization

import breeze.linalg._
import breeze.optimize._


object test{
  def main(args: Array[String]): Unit = {

//    val f = new DiffFunction[DenseVector[Double]] {
//                    def calculate(x: DenseVector[Double]) = {
//                       (norm((x - 3d) :^ 2d,1d),(x * 2d) - 6d);
//                    }
//                  }

    val m = DenseMatrix.zeros[Int](5,5)
    print(m.rows, m.cols)
    print(m)
    m(4,::) := DenseVector(1,2,3,4,5).t
  }
}
