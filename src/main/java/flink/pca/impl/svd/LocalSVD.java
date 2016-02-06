package flink.pca.impl.svd;

import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.NotConvergedException;
import no.uib.cipr.matrix.SVD;

/**
 * Class for SVD computation on locally (in-memory) stored matrix.
 * Doesn't do a deep copy of given matrix.
 * Compute decomposition of matrix A, so that A = U*S*V.
 *
 */
public  class LocalSVD implements flink.pca.impl.svd.SVD {
	
	private DenseMatrix 		matrix;
	private DenseMatrix 		U;
	private double[] 			S;
	private DenseMatrix 		V;
	
	/**
	 * @param vals - matrix in a form of array of arrays
	 */
	public LocalSVD(double[][] vals) {
		matrix = new DenseMatrix(vals);
	}
	
	/**
	 * @param rows - number of rows
	 * @param columns - number of columns
	 * @param vals - 1-D array of size rows * columns, stored column-wise
	 */
	public LocalSVD(int rows, int columns, double[] vals) {
		matrix = new DenseMatrix(rows, columns, vals, false);
	}
	
	/**
	 * Computes SVD
	 * 
	 * @throws NotConvergedException - when iterative process lacks convergence
	 */
	public void compute() throws NotConvergedException {
		SVD svd = new SVD(matrix.numRows(), matrix.numColumns());
		SVD s = svd.factor(matrix);
		U = s.getU();
		S = s.getS();
		V = s.getVt();
	}
	
	public DenseMatrix getU(){
		return U;
	}
	
	public double[] getSigmas(){
		return S;
	}
	
	public DenseMatrix getV(){
		return V;
	}
}
