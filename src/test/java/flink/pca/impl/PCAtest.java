package flink.pca.impl;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.Test;

public class PCAtest {

	@Test
	public void test() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<double[]> text = env.fromElements(
				new double[]{4.0, 7.0, 9.0, 1.0, 0.0},
				new double[]{2.0, 0.0, 3.0, 4.0, 5.0},
				new double[]{4.0, 7.0, 0.0, 6.0, 1.0},
				new double[]{1.0, 0.0, 5.0, 0.0, 0.0},
				new double[]{8.0, 1.0, 1.0, 9.0, 4.0},
				new double[]{9.0, 5.0, 0.0, 2.0, 8.0}
		);
		
		//choose the mode
		DataSet<double[]> rew = new PCA().project(3, 5, text, PCAmode.AUTO);
		List<double[]> res = rew.collect();
		System.out.println();
		for (int i = 0; i < res.size(); i++) {
			for (int j = 0; j < 3; j++) {
				System.out.print(res.get(i)[j] + " ");
			}
			System.out.println();
		}
	}

}
