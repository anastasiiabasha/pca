package flink.pca.impl.svd;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
//import java.util.HashMap;
import java.util.List;
//import java.util.Map.Entry;


import java.util.Map.Entry;

import no.uib.cipr.matrix.DenseMatrix;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.netlib.util.doubleW;
import org.netlib.util.intW;

import com.github.fommil.netlib.ARPACK;

/**
 * Class for SVD computation.
 * Compute decomposition of matrix A, so that A = U*S*V
 */
public class ArpackSVD implements SVD {

	//Used for sorting eigenvectors according to eigenvalue magnitude
	private static class EigenPair implements Comparable<EigenPair> {

		double 			value;
		double[] 		vector;

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

	private DenseMatrix         U;
	private double[]            sigmas;
	private int                 k;
	private int                 n;
	private double              tol;
	private int                 maxIter;
	private DataSet<double[]>   matrix;
	private double[]            means;
	private int                 m;
	
	/**
	 * @param mul - the function, which provides the multiplication of data matrix 
	 * with a given vector
	 * @param k - the number of principal components to compute
	 * @param n - the initial number of features/columns
	 * @param tol - termination tolerance for ARPACK
	 * @param maxIter - maximal number of iterations for ARPACK
	 */
	public ArpackSVD(int k, int n, int m, double tol, int maxIter, DataSet<double[]> matrix, double[] means) {
		this.matrix = matrix;
		this.means = means;
		this.m = m;
		this.k = k;
		this.n = n;
		this.tol = tol;
		this.maxIter = maxIter;
	}

	public DenseMatrix getU() {
		return U;
	}

	public double[] getSigmas() {
		return sigmas;
	}
	
	private static final class DistMultMapMatrix extends RichMapPartitionFunction<double[], Tuple3<Integer, Double, Integer>> {

		private static final long 	serialVersionUID = 1L;
		
		private double[] 			means;
		private double[] 			vectorV;
		private int 				m;
		private int                 n;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			Collection<ArpackContext> collection = getRuntimeContext().getBroadcastVariable("context");
			for (ArpackContext value : collection) {
				int inputOffset = value.getIpntr()[0] - 1;
				vectorV = Arrays.copyOfRange(value.getWorkd(),
						inputOffset, inputOffset + n);
			}
		}
		
		public DistMultMapMatrix(double[] means, int m, int n) {
			this.means = means;
			this.m = m;
			this.n = n;
		}

		@Override
		public void mapPartition(Iterable<double[]> values,
				Collector<Tuple3<Integer, Double, Integer>> out)
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
				out.collect(new Tuple3<Integer, Double, Integer>(entry.getKey(), entry.getValue(), 0));
			}
		}
	}
	
	
	private static class ArpackReduce extends RichGroupReduceFunction<Tuple3<Integer,Double, Integer>, ArpackContext> {
		
		private int              k;
		private int              n;
		private ArpackContext    arpackContext;
		
		public ArpackReduce(int n, int k) {
			this.k = k;
			this.n = n;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			Collection<ArpackContext> collection = getRuntimeContext().getBroadcastVariable("context");
			for (ArpackContext value : collection) {
				this.arpackContext = value;
			}
		}
		
		@Override
		public void reduce(Iterable<Tuple3<Integer, Double, Integer>> values,
				Collector<ArpackContext> out) throws Exception {
			
			ARPACK arpack = ARPACK.getInstance();
			
			int ncv = Math.min(2 * k, n);
			String bmat = "I";
			String which = "LM";
			
			int oldIdo = arpackContext.getIdo();
			double oldTol = arpackContext.getTol();
			double[] oldResid = new double[arpackContext.getResid().length];
			System.arraycopy(arpackContext.getResid(), 0, oldResid, 0, arpackContext.getResid().length);
			double[] oldV = new double[arpackContext.getV().length];
			System.arraycopy(arpackContext.getV(), 0, oldV, 0, arpackContext.getV().length);
			
			int[] oldIparam = new int[arpackContext.getIparam().length];
			System.arraycopy(arpackContext.getIparam(), 0, oldIparam, 0, arpackContext.getIparam().length);
			int[] oldIpntr = new int[arpackContext.getIpntr().length];
			System.arraycopy(arpackContext.getIpntr(), 0, oldIpntr, 0, arpackContext.getIpntr().length);
			int oldInfo = arpackContext.getInfo();
			double[] oldWorkd = new double[arpackContext.getWorkd().length];
			System.arraycopy(arpackContext.getWorkd(), 0, oldWorkd, 0, arpackContext.getWorkd().length);
			double[] oldWorkl = new double[arpackContext.getWorkl().length];
			System.arraycopy(arpackContext.getWorkl(), 0, oldWorkl, 0, arpackContext.getWorkl().length);
			
			ArpackContext newArpackContext = new ArpackContext(oldIdo, oldTol, oldResid, oldV, oldIparam, oldIpntr, oldInfo, oldWorkd, oldWorkl);
			
			int outputOffset = newArpackContext.getIpntr()[1] - 1;
			
			double[] y = new double[n];
			for (Tuple3<Integer, Double, Integer> tuple : values) {
				y[tuple.f0] = tuple.f1;
			}
			
			System.arraycopy(y, 0, newArpackContext.getWorkd(), outputOffset, n);
			
			intW ido = new intW(newArpackContext.getIdo());
			doubleW tol = new doubleW(newArpackContext.getTol());
			intW info = new intW(newArpackContext.getInfo());

			// call ARPACK's reverse communication
			arpack.dsaupd(ido, bmat, n, which, k, tol, newArpackContext.getResid(), ncv, newArpackContext.getV(), n, 
					newArpackContext.getIparam(), newArpackContext.getIpntr(), newArpackContext.getWorkd(), newArpackContext.getWorkl(), 
					newArpackContext.getWorkl().length, info);
			
			newArpackContext.setIdo(ido.val);
			newArpackContext.setTol(tol.val);
			newArpackContext.setInfo(info.val);
			out.collect(newArpackContext);
		}
	}

	private class EpsilonFilter implements  FilterFunction<ArpackContext> {
		
		@Override
		public boolean filter(ArpackContext value) throws Exception {
			if (value.getIdo() != 99) {
				if (value.getIdo() != -1 && value.getIdo() != 1) {
					throw new IllegalStateException(
							"ARPACK returns ido = "
									+ value.getIdo()
									+ " This flag is not compatible with Mode 1: A*x = lambda*x, A symmetric.");
				}
				return true;
			}
			return false;
		}
	};
	
	public void compute() throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		ARPACK arpack = ARPACK.getInstance();

		//stopping criterion
		doubleW tolW = new doubleW(tol);

		intW nev = new intW(k);

		// nev Lanczos vectors are generated in the first iteration
		// ncv-nev Lanczos vectors are generated in each subsequent iteration
		// ncv must be smaller than n
		int ncv = Math.min(2 * k, n);

		// "I" for standard eigenvalue problem
		String bmat = "I";
		// "LM" : compute the NEV largest (in magnitude) eigenvalues
		String which = "LM";

		int[] iparam = new int[11];

		// use exact shift in each iteration
		iparam[0] = 1;
		// maximum number of Arnoldi update iterations, or the actual number of
		// iterations on output
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
		arpack.dsaupd(ido, bmat, n, which, nev.val, tolW, resid, ncv, v, n, 
				iparam, ipntr, workd, workl, workl.length, info);
		
		if (ido.val != -1 && ido.val != 1) {
			throw new IllegalStateException(
					"ARPACK returns ido = "
							+ ido.val
							+ " This flag is not compatible with Mode 1: A*x = lambda*x, A symmetric.");
		}

		DataSet<ArpackContext> arpackData = env.fromElements(new ArpackContext(ido.val, tolW.val, resid, v, iparam, ipntr, info.val, workd, workl));
		
		IterativeDataSet<ArpackContext> loop = arpackData.iterate(maxIter);
		DataSet<ArpackContext> covarianceMatrixWithVector = matrix
				.mapPartition(new DistMultMapMatrix(means, m, n)).withBroadcastSet(loop, "context")
				.groupBy(0).sum(1).and(Aggregations.MAX, 2).groupBy(2)
				.reduceGroup(new ArpackReduce(n, k)).withBroadcastSet(loop, "context");
		
		DataSet<ArpackContext> finalVector = loop
				.closeWith(covarianceMatrixWithVector,
						covarianceMatrixWithVector.filter(new EpsilonFilter()));
		
		List<ArpackContext> result = finalVector.collect();
		ArpackContext finalArpackContext = null;
		for (ArpackContext value : result) {
			finalArpackContext = value;
		}

		info     = new intW(finalArpackContext.getInfo());
		v        = finalArpackContext.getV();
		resid    = finalArpackContext.getResid();
		iparam   = finalArpackContext.getIparam();
		ipntr    = finalArpackContext.getIpntr();
		workd    = finalArpackContext.getWorkd();
		workl    = finalArpackContext.getWorkl();
		
		if (info.val != 0) {
			switch (info.val) {
			case 1:
				throw new IllegalStateException(
						"ARPACK returns non-zero info = "
								+ info.val
								+ " Maximum number of iterations taken. (Refer ARPACK user guide for details)");
			case 3:
				throw new IllegalStateException(
						"ARPACK returns non-zero info = "
								+ info.val
								+ " No shifts could be applied. Try to increase NCV. "
								+ "(Refer ARPACK user guide for details)");
			default:
				throw new IllegalStateException(
						"ARPACK returns non-zero info = "
								+ info.val
								+ " Please refer ARPACK user guide for error message.");
			}
		}

		double[] d = new double[nev.val];
		boolean[] select = new boolean[ncv];

		// copy the Ritz vectors
		double[] z = Arrays.copyOfRange(v, 0, nev.val * n);

		// call ARPACK's post-processing for eigenvectors
		arpack.dseupd(true, "A", select, d, z, n, 0.0, bmat, n, which, nev,
				finalArpackContext.getTol(), resid, ncv, v, n, iparam, ipntr, workd, workl,
				workl.length, info);

		// number of computed eigenvalues, might be smaller than k
		int computed = iparam[4];

		double[] eigenValues = Arrays.copyOfRange(d, 0, computed);
		EigenPair[] eigenPairs = new EigenPair[computed];

		for (int i = 0; i < eigenValues.length; i++) {
			eigenPairs[i] = new EigenPair(eigenValues[i], Arrays.copyOfRange(z, 
					i * n, i * n + n));
		}

		// sort the eigenpairs in descending order
		Arrays.sort(eigenPairs);

		U = new DenseMatrix(n, computed);
		sigmas = new double[computed];
		for (int i = 0; i < computed; i++) {
			sigmas[i] = eigenPairs[i].value;
			for (int j = 0; j < n; j++) {
				U.set(j, i, eigenPairs[i].vector[j]);
			}
		}
	}
}
