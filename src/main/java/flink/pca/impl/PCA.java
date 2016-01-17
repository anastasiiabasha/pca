package flink.pca.impl;

import java.util.List;

import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;


public class PCA {
	
	public void computeSVD(int k, int n, DataSet<double[]> dataset, PCAmode mode) throws Exception {
		
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
		
		switch (mode) {
			case LOCAL: {
				DenseMatrix gramian = computeGramian(dataset, n);
				LocalSVD svd = new LocalSVD(gramian.numRows(), gramian.numColumns(), gramian.getData());
				svd.calculateSVD();
			}
			break;
		case LOCALARPACK: {
				DenseMatrix gramian = computeGramian(dataset, n);
				double tol = 1e-10;
				int maxIter = Math.max(300, k * 3);
				DenseMatrix res = Arpack.symmetricEigs(new LocalMatrixVectorMultiplicator(gramian), k, n, tol, maxIter);
			}
			break;
		case DIST: {
				double tol = 1e-10;
				int maxIter = Math.max(300, k * 3);
				DenseMatrix res = Arpack.symmetricEigs(new DistMatrixVectorMultiplicator(dataset, n), k, n, tol, maxIter);
			}
			break;
		}
	}
	
	private class LocalMatrixVectorMultiplicator implements Multiplicator {
		
		private DenseMatrix gramian;
		
		public LocalMatrixVectorMultiplicator(DenseMatrix matrix) {
			gramian = matrix;
		}

		@Override
		public DenseVector multipy(DenseVector v) {
			DenseVector res = new DenseVector(new double[gramian.numColumns()]);
			gramian.mult(v, res);
			return res;
		}
	}
	
	private class DistMatrixVectorMultiplicator implements Multiplicator {
		
		public DistMatrixVectorMultiplicator(DataSet<double[]> dataset, int n) {
			
		}

		@Override
		public DenseVector multipy(DenseVector v) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	private DenseMatrix computeGramian(DataSet<double[]> dataset, int n) throws Exception {
		
		DataSet<Tuple3<Integer, Integer,Double>> result = dataset
				.flatMap(new MapMatrix())
				.groupBy(0,1)
				.reduceGroup(new ReduceMatrix());
		List<Tuple3<Integer, Integer,Double>> values = result.collect();
		DenseMatrix matrix = new DenseMatrix(n, n);
		for (Tuple3<Integer, Integer, Double> tuple : values) {
			matrix.set(tuple.f0, tuple.f1, tuple.f2);
			matrix.set(tuple.f1, tuple.f0, tuple.f2);
		}
		
		return matrix;
	}
	
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//input data
		DataSet<double[]> text = env.fromElements(
			new double[]{1,2,3},
			new double[]{1,2,3}
		);
		
		new PCA().computeSVD(2, 3, text, PCAmode.LOCALARPACK);
	}
	
    private static final class MapMatrix implements FlatMapFunction<double[], Tuple4<Integer, Integer, Double, Double>> {

		private static final long serialVersionUID = 1L;

		public void flatMap(double[] vector,
				Collector<Tuple4<Integer, Integer, Double, Double>> out) {

			for (int i = 0; i < vector.length; i++) {
				for (int j = i + 1; j < vector.length; j++) {
					out.collect(new Tuple4<Integer, Integer, Double, Double>(i, j, vector[i], vector[j]));
				}
			}
		}
	}
    
	private static class ReduceMatrix implements
			GroupReduceFunction<Tuple4<Integer, Integer, Double, Double>, Tuple3<Integer, Integer, Double>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(
				Iterable<Tuple4<Integer, Integer, Double, Double>> inTuple,
				Collector<Tuple3<Integer, Integer, Double>> outTuple)
				throws Exception {
			int i = 0;
			int j = 0;
			double x = 0.;
			double y = 0.;
			double innerProduct = 0;
			for (Tuple4<Integer, Integer, Double, Double> tuple : inTuple) {
				i = tuple.f0;
				j = tuple.f1;
				x = x + tuple.f2 * tuple.f2;
				y = y + tuple.f3 * tuple.f3;
				innerProduct = innerProduct + (tuple.f2 * tuple.f3);
			}

			outTuple.collect(new Tuple3<Integer, Integer, Double>(i, i, x));
			outTuple.collect(new Tuple3<Integer, Integer, Double>(j, j, y));
			outTuple.collect(new Tuple3<Integer, Integer, Double>(i, j, innerProduct));
		}
	}

}
