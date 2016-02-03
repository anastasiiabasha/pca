package flink.pca.impl.mult;

import no.uib.cipr.matrix.DenseVector;


public interface Multiplicator {
	DenseVector multipy(DenseVector v);
}
