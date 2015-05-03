package de.fhg.iais.kd.hadoop.recommender.Solutions;

import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModel;

public class UserSetClusterModel {
	private final ClusterModel myClusterModel;

	String findClosestCluster(Userset userset)
	// Returns the clusterid of the cluster whos metroid is closest to the input
	{
		double resultDouble = 1000.0;
		String clusterID = "";
		// ClusterModel computedClusterModel = ClusterModelFactory
		// .readFromCsvResource("/resources/centers_1000_iter_2_50.csv");

		for (String tmpCluster : myClusterModel.getKeys()) {
			String userSetRepresenation = myClusterModel.get(tmpCluster);

			Userset tmpUserSet = new Userset(userSetRepresenation);
			if (userset.distanceTo(tmpUserSet) <= resultDouble) {
				resultDouble = userset.distanceTo(tmpUserSet);
				clusterID = tmpCluster;

			}
		}

		return clusterID;

	}

	public UserSetClusterModel(ClusterModel param) {
		// TODO Auto-generated constructor stub
		this.myClusterModel = param;

	}

}
