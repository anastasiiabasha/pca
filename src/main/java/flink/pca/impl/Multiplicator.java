package flink.pca.impl;

import no.uib.cipr.matrix.DenseVector;


public interface Multiplicator {
	DenseVector multipy(DenseVector v);
}
