package de.fhg.iais.kd.hadoop.recommender.Solutions;

import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModel;
import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModelFactory;

public class UserSetClusterModel {

	String findClosestCluster(Userset userset)
	// Returns the clusterid of the cluster whos metroid is closest to the input
	{
		double resultDouble = 10.0;
		String resultString = "";
		ClusterModel computedClusterModel = ClusterModelFactory
				.readFromCsvResource("/resources/centers_1000_iter_2_50.csv");

		for (String tmpCluster : computedClusterModel.getKeys()) {
			String userSetRepresenation = computedClusterModel.get(tmpCluster);

			Userset tmpUserSet = new Userset(userSetRepresenation);
			if (userset.distanceTo(tmpUserSet) < resultDouble) {
				resultDouble = userset.distanceTo(tmpUserSet);
				resultString = userSetRepresenation;

			}
		}

		return resultString;
	}
}
