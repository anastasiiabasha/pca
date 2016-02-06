package flink.pca.impl;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.NotConvergedException;
import no.uib.cipr.matrix.SVD;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.Test;

public class PCATestSuite {

	@Test
	public void testWithLocalMode() throws Exception {
		testWithMode(PCAmode.LOCAL);
	}
	
	@Test
	public void testWithLocalArpackMode() throws Exception {
		testWithMode(PCAmode.LOCALARPACK);
	}
	
	@Test
	public void testWithDistMode() throws Exception {
		testWithMode(PCAmode.DIST);
	}
	
	@Test
	public void test1DWithLocalMode() throws Exception {
		testWith1D_data(PCAmode.LOCAL);
	}
	
	@Test
	public void test1DWithLocalArpackMode() throws Exception {
		testWith1D_data(PCAmode.LOCALARPACK);
	}
	
	@Test
	public void test1DWithDistMode() throws Exception {
		testWith1D_data(PCAmode.DIST);
	}
	
	private void testWith1D_data(PCAmode mode) throws Exception {
		
		//Data, which has only 1 important principal component, 1-D data
		//[a, 2*a, 3*a, 4*a, 5*a]
		double[][] matrix = new double[][]{
				{1.0,  2.0,  3.0,  4.0,  5.0},
				{2.0,  4.0,  6.0,  8.0,  10.0},
				{21.0, 42.0, 63.0, 84.0, 105.0},
				{11.0, 22.0, 33.0, 44.0, 55.0},
				{8.0,  16.0, 24.0, 32.0, 40.0},
				{4.0,  8.0,  12.0, 16.0, 20.0}};
		
		//ask to compute 4 principal components
		int computed = runTest(matrix, 4, mode);
		
		//check if recognized, that data contains only 1 important principal component
		assertEquals(computed, 1);
	}
	
	private void testWithMode(PCAmode mode) throws Exception {
		
		//Data matrix
		double[][] matrix = new double[][]{
				{4.0, 7.0, 9.0, 1.0, 0.0},
				{2.0, 0.0, 3.0, 4.0, 5.0},
				{4.0, 7.0, 0.0, 6.0, 1.0},
				{1.0, 0.0, 5.0, 0.0, 0.0},
				{8.0, 1.0, 1.0, 9.0, 4.0},
				{9.0, 5.0, 0.0, 2.0, 8.0}};
		
		runTest(matrix, 0, mode);
	}
	
	//returns the amount of principal components actually computed
	private int runTest(double[][] matrix, int testk, PCAmode mode) throws Exception {
		DenseMatrix dmatrix = new DenseMatrix(matrix);
		
		//Number of principal components to compute
		if (testk == 0)
			testk = dmatrix.numColumns()/2 + 1;
		
		//Locally computed projection of matrix onto testk principal components
		DenseMatrix expected = computeLocalProjection(dmatrix, testk);
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		//Projection computed by implemented method
		DataSet<double[]> matrixDataset = env.fromElements(matrix);
		
		List<double[]> result = new PCA().project(testk, dmatrix.numColumns(), matrixDataset, mode).collect();
		
		DenseMatrix resulting = new DenseMatrix(result.toArray(new double[0][0]));
		
		//Compare the expected and the received results
		assertEquals(expected.numColumns(), resulting.numColumns());
		assertEquals(expected.numRows(), resulting.numRows());
		
		double eps = 1e-2;
		
		for (int i = 0; i < expected.numRows(); i++) {
			for (int j = 0; j < expected.numColumns(); j++) {
				assertTrue((Math.abs(expected.get(i, j)) - Math.abs(resulting.get(i, j))) <= eps);
			}
		}
		return expected.numColumns();
	}
	
	private DenseMatrix computeLocalProjection(DenseMatrix dmatrix, int testk) throws NotConvergedException {
		
		//Compute the mean value of each column
		double[] m = new double[dmatrix.numRows()];
		Arrays.fill(m, 1);
		DenseVector ones = new DenseVector(m);
		DenseVector means = new DenseVector(new double[dmatrix.numColumns()]);
		dmatrix.transMult(1./dmatrix.numRows(), ones, means);
		
		//Center the matrix
		DenseMatrix centeredMatrix = dmatrix.copy();
		
		for (int i = 0; i < dmatrix.numColumns(); i++) {
			for (int j = 0; j < dmatrix.numRows(); j++) {
				centeredMatrix.set(j, i, (dmatrix.get(j, i) - means.get(i)));
			}
		}
		
		//Compute the covariance matrix
		DenseMatrix covarianceMatrix = new DenseMatrix(centeredMatrix.numColumns(), centeredMatrix.numColumns());
		centeredMatrix.transAmult(1./centeredMatrix.numRows(), centeredMatrix, covarianceMatrix);
		
		//Compute the SVD decomposition
		SVD svd = new SVD(covarianceMatrix.numColumns(), covarianceMatrix.numColumns());
		SVD s = svd.factor(covarianceMatrix);
		
		//Eigenvectors
		DenseMatrix evectors = s.getU();
		//Eigenvalues
		double[] sigmas = s.getS();
		
		//Discard the vectors if eigenvalues are smaller than (1e-9 * (the biggest eigenvalue))
		double sigma0 = sigmas[0];
		double rCond = 1e-9;
	    double threshold = rCond * sigma0;
	    
	    int i = 0;
	    
	    while (i < Math.min(testk, sigmas.length) && sigmas[i] >= threshold) {
	    	i++;
	    }
	    
	    int sk = i;

	    //Warning if the amount of principal components is smaller than expected
	    if (sk < testk) {
	    	System.out.println("Requested "+ testk + " singular values but only found " + sk + " nonzeros.");
	    }

	    DenseMatrix pc = new DenseMatrix(covarianceMatrix.numColumns(), sk, 
	    		Arrays.copyOfRange(evectors.getData(), 0, covarianceMatrix.numColumns() * sk), false); 
	    
	    //Project the initial data matrix onto the principal components
	    DenseMatrix projectedDataMatrix = new DenseMatrix(dmatrix.numRows(), sk);
	    dmatrix.mult(pc, projectedDataMatrix);
	    
	    return projectedDataMatrix;
	}

}
