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

import flink.pca.impl.MultiplyGramianMatrixWithVector.MapMatrix;
import flink.pca.impl.MultiplyGramianMatrixWithVector.ReduceMatrixVector;

public class GramianMatrixCalculation {

		public static void main( String[] args ) throws Exception
	    {
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			//input data
			DataSet<String> text = env.fromElements(
					"3,1,2",
					"4,2,1"
					);


			DataSet<Tuple3<Integer, Integer,Integer>> gramianMatrix = text
																		.flatMap(new MapMatrix())
																		.groupBy(0,1)
																		.reduceGroup(new ReduceMatrix());
			

			gramianMatrix.print();
			
	    }
	    
	    
	    public static final class MapMatrix implements FlatMapFunction<String, Tuple4<Integer, Integer,Integer, Integer>> {

			private static final long serialVersionUID = 1L;

			public void flatMap(String input, Collector<Tuple4<Integer, Integer,Integer, Integer>> out) {
				String[] tokens = input.split(",");
				  
				for(int i=0; i < tokens.length; i++) {
				  for(int j=i; j < tokens.length; j++){
				    
					  out.collect(new Tuple4<Integer, Integer,Integer, Integer>(i,j,Integer.parseInt(tokens[i]), Integer.parseInt(tokens[j])));
				   
				   } 
				  }
			}
		}
	    
	    public static class ReduceMatrix implements GroupReduceFunction<Tuple4<Integer, Integer,Integer, Integer>,Tuple3<Integer, Integer,Integer>>
	    {

			private static final long serialVersionUID = 1L;

			@Override
			public void reduce(Iterable<Tuple4<Integer, Integer, Integer, Integer>> inTuple,
					Collector<Tuple3<Integer, Integer, Integer>> outTuple) throws Exception {
				int x = 0;
				int y = 0;
				int innerProduct = 0;
				for(Tuple4<Integer, Integer, Integer, Integer> tuple:inTuple)
				{
					x = tuple.f0;
					y = tuple.f1;
					innerProduct = innerProduct + (tuple.f2*tuple.f3);
				}

				outTuple.collect(new Tuple3<Integer, Integer, Integer>(x,y,innerProduct));
				if(x!=y)
				{
					outTuple.collect(new Tuple3<Integer, Integer, Integer>(y,x,innerProduct));
				}
			}	
	    }
}
