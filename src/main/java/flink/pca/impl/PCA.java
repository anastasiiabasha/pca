package flink.pca.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import flink.pca.impl.svd.ArpackSVD;
import flink.pca.impl.svd.LocalSVD;
import flink.pca.impl.svd.SVD;

public class PCA {
	
	/**
	 * @param k - number of principal components to compute
	 * @param n - number of features/columns in matrix
	 * @param dataset - Flink DataSet<double[]> row-representation of matrix
	 * @param mode - AUTO, DIST or LOCAL. If AUTO is given - decides the mode based on n and k parameters
	 * @return  Flink DataSet<double[]> original matrix projected on principal components
	 * @throws Exception - if Flink job is disrupted
	 */
	public DataSet<double[]> project(int k, int n, DataSet<double[]> dataset, PCAmode mode) 
			throws Exception {
		DenseMatrix V = computeSVD(k, n, dataset, mode);
		DataSet<double[]> result = dataset.map(new MatrixMult(V.getData(), V.numRows(), V.numColumns()));
		return result;
	}
	
	/**
	 * 
	 * Method to compute the SVD of the datamatrix.
	 * If the computation mode isn't given, decided the mode based on the size of the matrix 
	 * @param k - number of principal components to compute
	 * @param n - number of features/columns in the matrix
	 * @param dataset - Flink DataSet<double[]> row-representation of matrix
	 * @param mode - AUTO, DIST or LOCAL. If AUTO is given - decides the mode based on n and k parameters
	 * @return U matrix from 
	 * @throws Exception if flink job was disrupted
	 */
	@SuppressWarnings("incomplete-switch")
	private DenseMatrix computeSVD(int k, int n, DataSet<double[]> dataset, PCAmode mode) 
			throws Exception {
		
		if (n > 65535) {
			throw new Exception("Number of columns too big for computation");
		}
		
		if (n > 15000) {
			if (k > n/2)
				System.out.println("WARNING: At least " + n * n / 250000 + " MB of memory required!");
			else 
				System.out.println("WARNING: At least " + n * 2 * k / 250000 + " MB of memory required!");
		}
		
		if (mode == PCAmode.AUTO) {
			if (n < 15000) {
				mode = PCAmode.LOCAL;
			} else {
				mode = PCAmode.DIST;
			}
		}
		
		//Computing the mean values of each columns in order
		//to center the data matrix
		List<Tuple3<Integer, Double, Integer>> meansList = dataset
				.mapPartition(new AverageMap())
				.groupBy(0)
				.sum(1)
				.andSum(2)
				.collect();
		
		double[] means = new double[n];
		int m = 0;
		for (Tuple3<Integer, Double, Integer> tuple : meansList) {
			means[tuple.f0] = tuple.f1/tuple.f2;
			m = tuple.f2;
		}
		
		
		SVD svd = null;
		DenseMatrix U = null;
		double[] sigmas = null;
		
		//Use different computation model depending on the mode
		switch (mode) {
			case LOCAL: {
				DenseMatrix gramian = computeCovarianceMatrix(dataset, n, means, m);
				svd = new LocalSVD(gramian.numRows(), gramian.numColumns(), gramian.getData());
			}
			break;
		case DIST: {
				double tol = 1e-10;
				int maxIter = Math.max(300, k * 3);
				svd = new ArpackSVD(k, n, m, tol, maxIter, dataset, means);
			}
			break;
		}
		
		svd.compute();
		U = svd.getU();
		sigmas = svd.getSigmas();
		
		//Discard the vectors if eigenvalues are smaller than (1e-9 * (the biggest eigenvalue))
		double sigma0 = sigmas[0];
		double rCond = 1e-9;
		double threshold = rCond * sigma0;
		int i = 0;
		
		// sigmas might have a length smaller than k, if some Ritz values do not satisfy the convergence
		// criterion specified by tol after max number of iterations.
		// Thus use i < min(k, sigmas.length) instead of i < k.
		if (sigmas.length < k) {
			System.out.println("Requested "+ k + " singular values but only found " 
						+ sigmas.length+ " converged.");
		}
	    
		while (i < Math.min(k, sigmas.length) && sigmas[i] >= threshold) {
			i++;
		}
	    
		int sk = i;

		if (sk < k) {
			System.out.println("Requested "+ k + " singular values but only found " 
					+ sk + " nonzeros.");
		}

		DenseMatrix V = new DenseMatrix(n, sk, Arrays.copyOfRange(U.getData(), 0, n * sk), false); 
		return V;
	}
	
	/**
	 * Covariance matrix computation
	 * @param dataset
	 * @param n - number of features/columns
	 * @param means - array of per-column mean values
	 * @param m - number of samples/rows in data matrix
	 * @return covariance matrix
	 * @throws Exception - if flink computation was disrupted
	 */
	private DenseMatrix computeCovarianceMatrix(DataSet<double[]> dataset, int n, double[] means, int m)
			throws Exception {
		
		DataSet<Tuple3<Integer, Integer,Double>> result = dataset
					.mapPartition(new MapMatrix(means, m, n)).groupBy(0, 1).sum(2);
		List<Tuple3<Integer, Integer,Double>> values = result.collect();
		DenseMatrix matrix = new DenseMatrix(n, n);
		for (Tuple3<Integer, Integer, Double> tuple : values) {
			matrix.set(tuple.f0, tuple.f1, tuple.f2);
			matrix.set(tuple.f1, tuple.f0, tuple.f2);
		}
		return matrix;
	}
	
	/**
	 * Class used for Matrix multiplication
	 * Assumes the broadcast of principal components projection matrix
	 *
	 */
	private static final class MatrixMult implements MapFunction<double[], double[]> {
		
		private static final long serialVersionUID = 1L;
		
		private double[] 		pc;
		private int 			n;
		private int 			k;
		
		public MatrixMult(double[] principalComponents, int n, int k) {
			this.pc = principalComponents;
			this.n = n;
			this.k = k;
		}

		@Override
		public double[] map(double[] v) throws Exception {
			DenseVector res = new DenseVector(new double[k]);
			DenseMatrix mat = new DenseMatrix(n, k, pc, false);
			mat.transMult(new DenseVector(v), res);
			return res.getData();
		}
	}
	
	/**
	 * MapPartitionFunction to compute the average of all columns
	 * Pre-computes sums and counts of a single partition for later aggregation
	 */
	public static class AverageMap implements MapPartitionFunction<double[], 
											Tuple3<Integer, Double, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void mapPartition(Iterable<double[]> values,
				Collector<Tuple3<Integer, Double, Integer>> out)
				throws Exception {
			
			HashMap<Integer, Tuple2<Double, Integer>> hash = new HashMap<Integer, 
														Tuple2<Double, Integer>>();
			for (double[] vector : values) {
				for (int i = 0; i < vector.length; i++) {
					Tuple2<Double, Integer> tuple = hash.get(i);
					if (tuple == null) {
						hash.put(i, new Tuple2<Double, Integer>(vector[i], 1));
					} else {
						hash.put(i, new Tuple2<Double, Integer>(vector[i] + tuple.f0, tuple.f1 + 1));
					}
				}
			}
			for (Entry<Integer, Tuple2<Double, Integer>> entry : hash.entrySet()) {
				out.collect(new Tuple3<Integer, Double, Integer>(entry.getKey(), 
						entry.getValue().f0, entry.getValue().f1));
			}
		}
		
	}
	
	/**
	 * MapPartitionFunction used for computation of covariance matrix
	 *
	 */
	public static class MapMatrix implements MapPartitionFunction<double[], 
											Tuple3<Integer, Integer, Double>> {
		
		private static final long 		serialVersionUID = 1L;
		private double[] 				means;
		private int 					m;
		private int 					n;
		
		/**
		 * @param means - the vector holding the per-column mean values
		 * @param m - number of samples/rows in matrix
		 * @param n - number of features/column in matrix
		 */
		public MapMatrix(double[] means, int m, int n) {
			this.means = means;
			this.m = m;
			this.n = n;
		}

		@Override
		public void mapPartition(
				Iterable<double[]> values,
				Collector<Tuple3<Integer, Integer, Double>> out)
				throws Exception {
			//pre-aggregate the partial dot-products in a hash
			HashMap<Tuple2<Integer, Integer>, Double> hash = new HashMap<Tuple2<Integer, Integer>, Double>();
			
			for (double[] vector : values) {
				for (int i = 0; i < n; i++) {
					//compute only the upper triangle of the matrix
					for (int j = i; j < n; j++) {
						Tuple2<Integer, Integer> tuple = new Tuple2<Integer, Integer>(
								i, j);

						if (hash.containsKey(tuple)) {
							Double previous = hash.get(tuple);
							//use the centered values from the matrix
							hash.put(tuple, previous +(vector[i] - means[i]) * (vector[j] - means[j])
									/ m);
						} else {
							hash.put(tuple, (vector[i] - means[i]) * (vector[j] - means[j]) / m);
						}
					}
				}
			}
			for (Entry<Tuple2<Integer, Integer>, Double> entry : hash.entrySet()) {
				out.collect(new Tuple3<Integer, Integer, Double>(entry.getKey().f0, 
							entry.getKey().f1, entry.getValue()));
			}
		}
	}
}
