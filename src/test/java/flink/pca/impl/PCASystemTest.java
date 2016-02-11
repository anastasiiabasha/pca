package flink.pca.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class PCASystemTest {
	
public static void main(String[] args) throws Exception {
		
		//String corePath = "D:\\Class_Lectures\\Masters_TUB_3rd_Sem\\IMPRO3\\Datasets\\Linear Dataset\\";
		String corePath = args[0];
		String fileName = "Ins_10000_Fea_";
		int n = 0, k = 0;
		int[] numOfComponents = {20,50,80};
		
		//Starting value of n (Can be 0 till 900 in steps of 100)
		n = Integer.parseInt(args[1]);
		//total number of tests to be run (Max 10 - as we have 10 datasets) 
		int totalTest  = Integer.parseInt(args[2]);
		//Result file name
		String resultFileName =  args[3] + "ResultFile_Variable_Features" ; 
		//Maximum number of Iterations (We take it as 3)
		int maxIterations = Integer.parseInt(args[4]);
		
		File file = new File(resultFileName);

		if (!file.exists()) {
			file.createNewFile();
		}

		for(int i = 0; i < totalTest ; i++)
		{
			n = n + 100;
			for (int j = 0 ; j < numOfComponents.length;j++)
			{
				String datasetPath = fileName + n;
				k = numOfComponents[j]*(n/100);
				
				for(int testNum = 0; testNum < maxIterations ; testNum++)
				{
					FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
					BufferedWriter bw = new BufferedWriter(fw);
					System.out.println("Dataset:" + datasetPath + "k" + k + " Iteration:" + testNum + "\n");
					runFlinkJob(corePath,datasetPath, n, k, testNum, bw);
					bw.close();
				}
			}
		}
	}
	
	private static void runFlinkJob(String corePath,String datasetPath, int n, int k, int testNum, BufferedWriter bw) throws IOException
	{

		
	    long start = System.currentTimeMillis();

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		//env.setParallelism(4);
		
		String fullName = corePath+datasetPath;

		DataSet<double[]> values = env.readTextFile(fullName).map(new DataMapper());
		
		
		PCA pca = new PCA();
		try {
			DataSet<double[]> principleComponents = pca.project(k, n, values, PCAmode.AUTO);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	    long stop = System.currentTimeMillis();
	    
	    double stopInSec = (stop-start)/(double)1000;
	    
	    String tempRes =  datasetPath + "\t"  + k  + "\t" + (testNum+1) + "\t" +   stopInSec  ;
		try {
			bw.write(tempRes);
			bw.newLine();
			env.startNewSession();

		} catch (Exception e) {
			bw.flush();
			bw.close();
			e.printStackTrace();
		}

	}
	
    public static class DataMapper implements MapFunction<String, double[]> {

		private static final long serialVersionUID = 1L;

		@Override
		public double[] map(String input) throws Exception {
			String[] inputValues = input.split(" ");
			double[] values = new double[inputValues.length];
			for(int i = 0; i<inputValues.length; i++)
			{
				values[i] = Double.parseDouble(inputValues[i]);
			}
			return values;
		}
    }


}
