package flink.pca.impl.mult;

import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import no.uib.cipr.matrix.DenseVector;

public class DistMatrixVectorMultiplicator implements Multiplicator {

	private DataSet<double[]> dataset;
	private double[] means;
	private int m;
	
	public DistMatrixVectorMultiplicator(DataSet<double[]> dataset, double[] means, int m) {
		this.dataset = dataset;
		this.means = means;
		this.m = m;
		
	}

	@Override
	public DenseVector multipy(DenseVector v) {
		DataSet<Tuple2<Integer,Double>> gramianMatrixWithVector = dataset
				.flatMap(new DistMultMapMatrix(means))
				.groupBy(0)
				.reduceGroup(new DistMultReduceMatrix(m, v.getData()));
				
		try {
			List<Tuple2<Integer, Double>> res = gramianMatrixWithVector.collect();
			DenseVector vector = new DenseVector(new double[res.size()]);
			for (Tuple2<Integer, Double> tuple : res) {
				vector.set(tuple.f0, tuple.f1);
			}
			return vector;
		} catch (Exception e) {
			//TODO: how to handle this
			e.printStackTrace();
			return null;
		}
	}
	
	private static final class DistMultMapMatrix implements FlatMapFunction<double[], Tuple4<Integer, Integer, Double, Double>> {

		private static final long serialVersionUID = 1L;
		
		private double[] means;
		
		public DistMultMapMatrix(double[] means) {
			this.means = means;
		}

		public void flatMap(double[] vector, Collector<Tuple4<Integer, Integer, Double, Double>> out) {
			for(int i = 0; i < vector.length; i++) {
			  for(int j = 0; j < vector.length; j++){
				  out.collect(new Tuple4<Integer, Integer, Double, Double>(i, j, vector[i] - means[i], vector[j] - means[j]));
			   } 
			}
		}
	}
	
	private static class DistMultReduceMatrix implements
				GroupReduceFunction<Tuple4<Integer, Integer, Double, Double>, Tuple2<Integer, Double>> {

		private static final long serialVersionUID = 1L;
		private double[] vectorV;
		private int m;

		public DistMultReduceMatrix(int m, double[] vectorV) {
			this.vectorV = vectorV;
			this.m = m;
		}

		@Override
		public void reduce(
				Iterable<Tuple4<Integer, Integer, Double, Double>> inTuple,
				Collector<Tuple2<Integer, Double>> outTuple) throws Exception {
			int x = 0;
			int y = 0;
			double innerProduct = 0;

			for (Tuple4<Integer, Integer, Double, Double> tuple : inTuple) {
				x = tuple.f0;
				y = tuple.f1;
				innerProduct = innerProduct
						+ (tuple.f2 * tuple.f3 * vectorV[y]);
			}
			outTuple.collect(new Tuple2<Integer, Double>(x, innerProduct / m));
		}
	}
}
