package flink.pca.impl.mult;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
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
					.mapPartition(new DistMultMapMatrix(means, v.getData(), m))
					.groupBy(0).sum(1);
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
	
	private static final class DistMultMapMatrix implements MapPartitionFunction<double[], Tuple2<Integer, Double>> {

		private static final long serialVersionUID = 1L;
		
		private double[] means;
		private double[] vectorV;
		private int m;
		
		public DistMultMapMatrix(double[] means, double[] v, int m) {
			this.means = means;
			this.vectorV = v;
			this.m = m;
		}

		@Override
		public void mapPartition(Iterable<double[]> values,
				Collector<Tuple2<Integer, Double>> out)
				throws Exception {
			HashMap<Integer, Double> hash = new HashMap<Integer, Double>();
			for (double[] vector : values) {
				for(int i = 0; i < vector.length; i++) {
					for(int j = 0; j < vector.length; j++){
						if (hash.containsKey(i)) {
							hash.put(i, hash.get(i) + (vector[i] - means[i]) * (vector[j] - means[j]) * vectorV[j] / m);
						} else {
							hash.put(i, (vector[i] - means[i]) * (vector[j] - means[j]) * vectorV[j] / m);
						}
					}
				}
			}
			for (Entry<Integer, Double> entry : hash.entrySet()) {
				out.collect(new Tuple2<Integer, Double>(entry.getKey(), entry.getValue()));
			}
		}
	}
}
