package flink.pca.impl.mult;

import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;

public class LocalMatrixVectorMultiplicator implements Multiplicator {

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
