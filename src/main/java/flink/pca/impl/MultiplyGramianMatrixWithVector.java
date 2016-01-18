package flink.pca.impl;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class MultiplyGramianMatrixWithVector {

	public static void main( String[] args ) throws Exception
    {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//input data
		DataSet<String> text = env.fromElements(
				"3,1,2",
				"4,2,1"
				);
		
		String vectorV = "1,2,3";
		
		DataSet<Tuple2<Integer,Integer>> gramianMatrixWithVector = text
																.flatMap(new MapMatrix())
																.groupBy(0)
																.reduceGroup(new ReduceMatrixVector(vectorV));
		gramianMatrixWithVector.print();
		
    }
    
    
    public static final class MapMatrix implements FlatMapFunction<String, Tuple4<Integer, Integer,Integer, Integer>> {

		private static final long serialVersionUID = 1L;

		public void flatMap(String input, Collector<Tuple4<Integer, Integer,Integer, Integer>> out) {
			String[] tokens = input.split(",");
			  
			for(int i=0; i < tokens.length; i++) {
			  for(int j=0; j < tokens.length; j++){
			    
				  out.collect(new Tuple4<Integer, Integer,Integer, Integer>(i,j,Integer.parseInt(tokens[i]), Integer.parseInt(tokens[j])));
			   
			   } 
			}
		}
	}

    
    public static class ReduceMatrixVector implements GroupReduceFunction<Tuple4<Integer, Integer,Integer, Integer>,Tuple2<Integer,Integer>>
    {
    	String vectorV;
    	public ReduceMatrixVector(String vectorV)
    	{
    		this.vectorV = vectorV;
    	}
		@Override
		public void reduce(Iterable<Tuple4<Integer, Integer, Integer, Integer>> inTuple,
				Collector<Tuple2<Integer, Integer>> outTuple) throws Exception {
			int x = 0;
			int y = 0;
			int innerProduct = 0;
			String[] vectorTokens = vectorV.split(",");

			for(Tuple4<Integer, Integer, Integer, Integer> tuple:inTuple)
			{
				x = tuple.f0;
				y = tuple.f1;
				innerProduct = innerProduct + (tuple.f2*tuple.f3*Integer.parseInt(vectorTokens[y]));
			}
			outTuple.collect(new Tuple2<Integer, Integer>(x,innerProduct));
		}
    }

}
