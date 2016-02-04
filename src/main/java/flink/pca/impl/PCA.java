package flink.pca.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import flink.pca.impl.mult.DistMatrixVectorMultiplicator;
import flink.pca.impl.mult.LocalMatrixVectorMultiplicator;
import flink.pca.impl.svd.ArpackSVD;
import flink.pca.impl.svd.LocalSVD;

public class PCA {
	
	public DataSet<double[]> project(int k, int n, DataSet<double[]> dataset, PCAmode mode) throws Exception {
		DenseMatrix V = computeSVD(k, n, dataset, mode);
		
		DataSet<double[]> result = dataset.map(new MatrixMult(V.getData(), V.numRows(), V.numColumns()));
		return result;
	}
	
	private DenseMatrix computeSVD(int k, int n, DataSet<double[]> dataset, PCAmode mode) throws Exception {
		
		if (mode == PCAmode.AUTO) {
			if (n < 100 || (k > n / 2 && n <= 15000)) {
				if (k < n / 3) {
					mode = PCAmode.LOCALARPACK;
				} else {
					mode = PCAmode.LOCAL;
				}
			} else {
				mode = PCAmode.DIST;
			}
		}
		DenseMatrix res = null;
		double[] sigmas = null;
		List<Tuple3<Integer, Double, Integer>> meansList = dataset
				.flatMap(new AverageFlatMap())
				.groupBy(0)
				.reduceGroup(new AverageGroupReduce()).collect();
		
		double[] means = new double[n];
		int m = 0;
		for (Tuple3<Integer, Double, Integer> tuple : meansList) {
			means[tuple.f0] = tuple.f1/tuple.f2;
			m = tuple.f2;
		}
		
		switch (mode) {
			case LOCAL: {
				DenseMatrix gramian = computeGramian(dataset, n, means, m);
				LocalSVD svd = new LocalSVD(gramian.numRows(), gramian.numColumns(), gramian.getData());
				svd.calculateSVD();
				res = svd.getU();
				sigmas = svd.getS();
			}
			break;
		case LOCALARPACK: {
				DenseMatrix gramian = computeGramian(dataset, n, means, m);
				double tol = 1e-10;
				int maxIter = Math.max(300, k * 3);
				ArpackSVD arp = new ArpackSVD();
				arp.symmetricEigs(new LocalMatrixVectorMultiplicator(gramian), k, n, tol, maxIter);
				res = arp.getU();
				sigmas = arp.getSigmas();
			}
			break;
		case DIST: {
				double tol = 1e-10;
				int maxIter = Math.max(300, k * 3);
				ArpackSVD arp = new ArpackSVD();
				arp.symmetricEigs(new DistMatrixVectorMultiplicator(dataset, means, m), k, n, tol, maxIter);
				res = arp.getU();
				sigmas = arp.getSigmas();
			}
			break;
		}
		
		double sigma0 = sigmas[0];
		double rCond = 1e-9;
	    double threshold = rCond * sigma0;
	    int i = 0;
	    // sigmas might have a length smaller than k, if some Ritz values do not satisfy the convergence
	    // criterion specified by tol after max number of iterations.
	    // Thus use i < min(k, sigmas.length) instead of i < k.
	    if (sigmas.length < k) {
	    	System.out.println("Requested "+ k + " singular values but only found " + sigmas.length+ " converged.");
	    }
	    
	    while (i < Math.min(k, sigmas.length) && sigmas[i] >= threshold) {
	    	i++;
	    }
	    
	    int sk = i;

	    if (sk < k) {
	    	System.out.println("Requested "+ k + " singular values but only found " + sk + " nonzeros.");
	    }

	    DenseMatrix V = new DenseMatrix(n, sk, Arrays.copyOfRange(res.getData(), 0, n * sk), false); 
	    
	    return V;
	}
	
	private DenseMatrix computeGramian(DataSet<double[]> dataset, int n, double[] means, int m) throws Exception {
		
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
	
	private static final class MatrixMult implements MapFunction<double[], double[]> {
		
		private static final long serialVersionUID = 1L;
		
		private double[] pc;
		private int n;
		private int k;
		
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
	
	private static final class AverageFlatMap implements FlatMapFunction<double[], Tuple2<Integer, Double>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(double[] value,
				Collector<Tuple2<Integer, Double>> out) throws Exception {
			for (int i = 0; i < value.length; i++) {
				out.collect(new Tuple2<Integer, Double>(i, value[i]));
			}
		}
	}
	
	private static final class AverageGroupReduce implements GroupReduceFunction<Tuple2<Integer,Double>,Tuple3<Integer, Double, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<Tuple2<Integer, Double>> values,
				Collector<Tuple3<Integer, Double, Integer>> out)
				throws Exception {
			double sum = 0.;
			int count = 0;
			int index = 0;
			for (Tuple2<Integer, Double> tuple : values) {
				index = tuple.f0;
				sum += tuple.f1;
				count++;
			}
			out.collect(new Tuple3<Integer, Double, Integer>(index, sum, count));
		}
	}
	
	public static class MapMatrix implements MapPartitionFunction<double[], Tuple3<Integer, Integer, Double>> {
		
		private static final long serialVersionUID = 1L;
		private double[] means;
		private int m;
		private int n;
		
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
			boolean[] added = new boolean[n];
			HashMap<Tuple2<Integer, Integer>, Double> hash = new HashMap<Tuple2<Integer, Integer>, Double>();
			for (double[] vector : values) {
				for (int i = 0; i < n; i++) {
					for (int j = i + 1; j < n; j++) {
						if (!added[i]) {
							Tuple2<Integer, Integer> tuple = new Tuple2<Integer, Integer>(
									i, i);
							if (hash.containsKey(tuple)) {
								Double previous = hash.get(tuple);
								hash.put(tuple, previous + (vector[i] - means[i])
										* (vector[i] - means[i]) / m);
							} else {
								hash.put(tuple, (vector[i] - means[i]) * (vector[i] - means[i]) / m);
							}
							added[i] = true;
						}

						if (!added[j]) {
							Tuple2<Integer, Integer> tuple = new Tuple2<Integer, Integer>(
									j, j);
							if (hash.containsKey(tuple)) {
								Double previous = hash.get(tuple);
								hash.put(tuple, previous + (vector[j] - means[j])
										* (vector[j] - means[j]) / m);
							} else {
								hash.put(tuple, (vector[j] - means[j]) * (vector[j] - means[j]) / m);
							}
							added[j] = true;
						}

						Tuple2<Integer, Integer> tuple = new Tuple2<Integer, Integer>(
								i, j);

						if (hash.containsKey(tuple)) {
							Double previous = hash.get(tuple);
							hash.put(tuple, previous +(vector[i] - means[i]) * (vector[j] - means[j])
									/ m);
						} else {
							hash.put(tuple, (vector[i] - means[i]) * (vector[j] - means[j]) / m);
						}
					}
				}
				Arrays.fill(added, false);
			}
			for (Entry<Tuple2<Integer, Integer>, Double> entry : hash.entrySet()) {
				out.collect(new Tuple3<Integer, Integer, Double>(entry.getKey().f0, entry.getKey().f1, entry.getValue()));
			}
		}
	}
}
