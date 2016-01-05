package flink.pca.impl;

import no.uib.cipr.matrix.NotConvergedException;

/**
 * Hello world!
 *
 */
public class App 
{
	
	
	
	
    public static void main( String[] args ) throws NotConvergedException
    {
        System.out.println( "Hello World!" );
        
        double[][] vals = {  {3,2,2},         
        		             {2,3,-2}
        				  };
        
        LinearAlgebraOps.calculateSVD(2, 3, vals);

    }
}
