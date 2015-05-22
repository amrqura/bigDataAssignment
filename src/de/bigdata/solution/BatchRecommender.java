package de.bigdata.solution;

import cascading.flow.Flow;
import de.fraunhofer.iais.kd.livlab.bda.config.BdaConstants;
/**
 * the main class that we used to run our program.
 * @author
 *
 */
public class BatchRecommender {

	public static void main(String[] args) {

		String infile = BdaConstants.SAMPLE_FILE;
		String workDir = "recommender_flowtest/";

		Flow userSetflow = UserSetFlow.getUserSetFlow(infile, workDir);
		userSetflow.complete();
		Flow userMatrix = UserSetMatrix.getUserSetMatrix(infile, workDir);
		userMatrix.complete();
		Flow artistCluster = UserSetCluster.getArtistCluster(infile, workDir);
		artistCluster.complete();

	}
}
