package de.bigdata.solution;

import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModel;
/**
 * task 4
 * @author
 *
 */
public class UserSetClusterModel {
	private final ClusterModel clusterModel;


	public UserSetClusterModel(ClusterModel clusterModel) {
		this.clusterModel = clusterModel;
	}

	//Returns the clusterid of the cluster whose metroid is closest to the input
	public String findClosestCluster(UserSet userset) {
		Double minDistance = Double.MAX_VALUE;
		String minClusterId = "-1";

		for(String clusterId : clusterModel. getKeys()) {
			String curClusterData = clusterModel.get(clusterId);
			UserSet other = constructUserSet(curClusterData);
			double curDistance = userset.distanceTo(other);
			if(curDistance < minDistance) {
				minDistance = curDistance;
				minClusterId = clusterId;
			}
		}
		return minClusterId;
	}

	/**
	 * construct a user set from a bit array representation of a cluster
	 * @param userSet
	 * @return
	 */
	private UserSet constructUserSet(String userSetStr) {
		UserSet userSet = new UserSet();
		String [] userSetArr = userSetStr.split(",");
		for(int i = 0; i < userSetArr.length; i++) {
			userSet.add(i+1);
		}
		return userSet;
	}



}
