package flink.pca.impl.mult;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import no.uib.cipr.matrix.DenseVector;

/**
 * Class used for distributed matrix-vector multiplication
 * for ARPACK computation mode.
 * Given data matrix A, computes A'*A*v without materializing covariance matrix A'*A.
 * Also, centers the original A matrix in order to compute covariance matrix.
 *
 */
public class DistMatrixVectorMultiplicator implements Multiplicator {

	private DataSet<double[]> 		dataset;
	private double[] 				means;
	private int						m;
	
	/**
	 * @param dataset - original dataset in Flink DataSet<double[]> format
	 * @param means - the vector of pre-computed per-column mean values
	 * @param m - the number of samples/rows in dataset
	 */
	public DistMatrixVectorMultiplicator(DataSet<double[]> dataset, double[] means, int m) {
		this.dataset = dataset;
		this.means = means;
		this.m = m;
		
	}

	/* (non-Javadoc)
	 * @see flink.pca.impl.mult.Multiplicator#multipy(no.uib.cipr.matrix.DenseVector)
	 */
	@Override
	public DenseVector multipy(DenseVector v) throws Exception {
		
		DataSet<Tuple2<Integer,Double>> covarianceMatrixWithVector = dataset
					.mapPartition(new DistMultMapMatrix(means, v.getData(), m))
					.groupBy(0).sum(1);
		
		List<Tuple2<Integer, Double>> res = covarianceMatrixWithVector.collect();
		DenseVector vector = new DenseVector(new double[res.size()]);
		for (Tuple2<Integer, Double> tuple : res) {
			vector.set(tuple.f0, tuple.f1);
		}
		return vector;
	}
	
	private static final class DistMultMapMatrix implements MapPartitionFunction<double[], Tuple2<Integer, Double>> {

		private static final long 	serialVersionUID = 1L;
		
		private double[] 			means;
		private double[] 			vectorV;
		private int 				m;
		
		public DistMultMapMatrix(double[] means, double[] v, int m) {
			this.means = means;
			this.vectorV = v;
			this.m = m;
		}

		@Override
		public void mapPartition(Iterable<double[]> values,
				Collector<Tuple2<Integer, Double>> out)
				throws Exception {
			
			//Pre-compute the dot-product of matrix row, matrix column and given vector
			//in this partition of data
			HashMap<Integer, Double> hash = new HashMap<Integer, Double>();
			for (double[] vector : values) {
				for(int i = 0; i < vector.length; i++) {
					for(int j = 0; j < vector.length; j++){
						if (hash.containsKey(i)) {
							//also, center the matrix with a given means vector
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
