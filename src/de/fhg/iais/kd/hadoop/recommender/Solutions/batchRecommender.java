package de.fhg.iais.kd.hadoop.recommender.Solutions;

import cascading.flow.Flow;
import de.fraunhofer.iais.kd.livlab.bda.config.BdaConstants;

public class batchRecommender {

	public static void main(String[] args) {

		// String infile =
		// "/home/livlab/last.fm-data/lastfm-dataset-1K/sample.tsv";
		String infile = BdaConstants.SAMPLE_FILE;
		String workDir = "recommender_flowtest/";

		// Flow utilMatrix = UserSetFlow.getUserSetFlow(infile, workDir);
		// Flow utilMatrix = UserSetMatrixFlow.getUserSetMatrixFlow(infile,
		// workDir);

		Flow utilMatrix = UserSetCluster.getUserSetClusterFlow(infile, workDir);

		utilMatrix.complete();

	}

}
