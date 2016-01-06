package flink.pca.impl;

import org.netlib.util.booleanW;

import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.SVD;

public  class LocalSVD {
	
	int rows;
	int columns;
	double[][] vals;
	DenseMatrix U;
	double[] S ;
	DenseMatrix Vt;
	
	
	public LocalSVD(int _rows, int _columns, double[][] _vals) {
		rows=_rows;
		columns = _columns;
		vals=_vals;
	}
	
	public boolean calculateSVD(){
	
		try {	
			DenseMatrix matA = new DenseMatrix(vals);
			SVD svd = new SVD(matA.numRows(),matA.numColumns());
			SVD s = svd.factor(matA);
			 U = s.getU();
			 S = s.getS();
			 Vt = s.getVt();
			
			return true;
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return false;
		}
		
	}
	
	public DenseMatrix getU(){
		return U;
	}
	
	public double[] getS(){
		return S;
	}
	
	public DenseMatrix getVt(){
		return Vt;
	}
	
	public void printU(){
		System.out.println("\nSVD Matrix U  ");
		for (int i = 0; i < U.numRows(); i++) {
			for (int j = 0; j < U.numColumns(); j++) {
				System.out.print(U.get(i, j) +"  " );
			}
			System.out.print("\n" );
			}	
	}
	
	public void printS(){
		System.out.println("\nSVD Matrix S  ");
		for ( int i =0 ; i < S.length ; i++){
			System.out.print(S[i] +"  " );
		}
		System.out.print("\n" );
	}
	
	public void printVt(){
		System.out.println("\nSVD Matrix Vt  ");
		for (int i = 0; i < Vt.numRows(); i++) {
			for (int j = 0; j < Vt.numColumns(); j++) {
				System.out.print(Vt.get(i, j) +"  " );
			}
			System.out.print("\n" );
			}	
	}
	
	public void printMainMat(){
		System.out.println("\nMatrix:    ");
		for (int i = 0; i < rows; i++) {
			for (int j = 0; j < columns; j++) {
				System.out.print(vals[i][j] +"  " );
			}
			System.out.print("\n" );
			}	
	}

}
