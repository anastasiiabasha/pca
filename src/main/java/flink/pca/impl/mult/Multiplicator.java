package flink.pca.impl.mult;

import no.uib.cipr.matrix.DenseVector;


public interface Multiplicator {
	/**
	 * Multiplication of covariance matrix n x n with n x 1 vector
	 * @param v - vector
	 * @return resulting  n x 1 vector
	 * @throws Exception - when the Flink job was disrupted
	 */
	DenseVector multipy(DenseVector v) throws Exception;
}
