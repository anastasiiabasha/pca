package flink.pca.impl.svd;

import no.uib.cipr.matrix.DenseMatrix;

public interface SVD {
	
	void compute() throws Exception;
	DenseMatrix getU();
	double[] getSigmas();

}
