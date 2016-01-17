package flink.pca.impl;

import java.util.Arrays;

import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.DenseVector;

import org.netlib.util.doubleW;
import org.netlib.util.intW;

import com.github.fommil.netlib.ARPACK;

public class Arpack {
	
	private static class EigenPair implements Comparable<EigenPair> {
		
		double value;
		double[] vector; 
		
		public EigenPair(double value, double[] vector) {
			this.value = value;
			this.vector = vector;
		}
		
		@Override
		public int compareTo(EigenPair another) {
			if (value < another.value) {
				return 1;
			} else if (value > another.value) {
				return -1;
			} else {
				return 0;
			}
		}
		
		
	}

	public static DenseMatrix symmetricEigs(Multiplicator mul, int k, int n, double tol, int maxIter) {
		
		ARPACK arpack = ARPACK.getInstance();
		
		// tolerance used in stopping criterion
		doubleW tolW = new doubleW(tol);
		
		// number of desired eigenvalues, 0 < nev < n
		intW nev = new intW(k);
		
		// nev Lanczos vectors are generated in the first iteration
	    // ncv-nev Lanczos vectors are generated in each subsequent iteration
	    // ncv must be smaller than n
	    int ncv = Math.min(2 * k, n);
	    
	    // "I" for standard eigenvalue problem, "G" for generalized eigenvalue problem
        String bmat = "I";
        // "LM" : compute the NEV largest (in magnitude) eigenvalues
        String which = "LM";
        
        int[] iparam = new int[11];
        
	    // use exact shift in each iteration
	    iparam[0] = 1;
	    // maximum number of Arnoldi update iterations, or the actual number of iterations on output
	    iparam[2] = maxIter;
	    // Mode 1: A*x = lambda*x, A symmetric
	    iparam[6] = 1;

	    intW ido = new intW(0);
	    intW info = new intW(0);
	    double[] resid = new double[n];
	    double[] v = new double[n * ncv];
	    double[] workd = new double[n * 3];
	    double[] workl = new double[ncv * (ncv + 8)];
	    int[] ipntr = new int[11];

	    // call ARPACK's reverse communication, first iteration with ido = 0
	    arpack.dsaupd(ido, bmat, n, which, nev.val, tolW, resid, ncv, v, n, iparam, ipntr, workd,
	      workl, workl.length, info);
	    
	    // ido = 99 : done flag in reverse communication
	    while (ido.val != 99) {
	    	if (ido.val != -1 && ido.val != 1) {
		        throw new IllegalStateException("ARPACK returns ido = " + ido.val +
		            " This flag is not compatible with Mode 1: A*x = lambda*x, A symmetric.");
	    	}
	    	// multiply working vector with the matrix
	    	int inputOffset = ipntr[0] - 1;
	    	int outputOffset = ipntr[1] - 1;
	    	DenseVector x = new DenseVector(Arrays.copyOfRange(workd, inputOffset, inputOffset + n));
	    	DenseVector y = mul.multipy(x);
	    	System.arraycopy(y.getData(), 0, workd, outputOffset, n);
	    	
	    	// call ARPACK's reverse communication
	    	arpack.dsaupd(ido, bmat, n, which, nev.val, tolW, resid, ncv, v, n, iparam, ipntr,
	    			workd, workl, workl.length, info);
	    }

	    if (info.val != 0) {
	    	switch(info.val) {
	          case 1:
				  throw new IllegalStateException("ARPACK returns non-zero info = " + info.val +
				  " Maximum number of iterations taken. (Refer ARPACK user guide for details)");
			case 3: throw new IllegalStateException("ARPACK returns non-zero info = " + info.val +
	              " No shifts could be applied. Try to increase NCV. " +
	              "(Refer ARPACK user guide for details)");
			default: 
	        	  throw new IllegalStateException("ARPACK returns non-zero info = " + info.val +
	              " Please refer ARPACK user guide for error message.");
	    	}
	    }

	    double[] d = new double[nev.val];
	    boolean[] select = new boolean[ncv];
  
	    // copy the Ritz vectors
	    double[] z = Arrays.copyOfRange(v, 0, nev.val * n);

	    // call ARPACK's post-processing for eigenvectors
	    arpack.dseupd(true, "A", select, d, z, n, 0.0, bmat, n, which, nev, tol, resid, ncv, v, n,
	    		iparam, ipntr, workd, workl, workl.length, info);

	    // number of computed eigenvalues, might be smaller than k
	    int computed = iparam[4];

	    double[] eigenValues = Arrays.copyOfRange(d, 0, computed);
	    EigenPair[] eigenPairs = new EigenPair[computed];
	    
	    for (int i = 0; i < eigenValues.length; i++) {
	    	eigenPairs[i] = new EigenPair(eigenValues[i], Arrays.copyOfRange(z, i * n, i * n + n));
	    }

	    // sort the eigen-pairs in descending order
	    Arrays.sort(eigenPairs);
	    
	    DenseMatrix sortedU = new DenseMatrix(computed, n);
	    for (int i = 0; i < computed; i++) {
	    	for (int j = 0; j < n; j++) {
	    		sortedU.set(i, j, eigenPairs[i].vector[j]);
	    	}
	    }
	    return sortedU;
	}
}
