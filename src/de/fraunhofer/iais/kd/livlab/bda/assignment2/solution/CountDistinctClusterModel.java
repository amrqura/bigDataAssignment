package de.fraunhofer.iais.kd.livlab.bda.assignment2.solution;

import java.io.Serializable;

import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModel;

public class CountDistinctClusterModel implements Serializable {

	private static ClusterModel clusterModel = new ClusterModel();

	// constructor
	public CountDistinctClusterModel(ClusterModel clusertModel) {
		// TODO Auto-generated constructor stub
		this.clusterModel = clusertModel;

	}

	public String getClosest(CountDistinctSketch sketch) {

		// Returns the clusterid of the cluster who’s metroid is closest to the
		// input

		Double minDistance = Double.MAX_VALUE;
		String minClusterId = "-1";
		for (String clusterId : clusterModel.getKeys()) {

			String metroID = clusterModel.get(clusterId);
			CountDistinctSketch other = constructSketch(metroID);
			if (sketch.distanceTo(other) < minDistance) {
				minClusterId = clusterId;
				minDistance = sketch.distanceTo(other);

			}

		}
		return minClusterId;

	}

	/**
	 * construct a seketch from a bit array representation of a cluster
	 *
	 * @param userSet
	 * @return
	 */
	private CountDistinctSketch constructSketch(String metroID) {
		// int sketchSize = 2 * metroID.split(",").length / 3; // the sketch
		// size
		int sketchSize = 10;
		// should be less
		// than the the real
		// set

		CountDistinctSketch sketch = new CountDistinctSketch(sketchSize);
		String[] userSetArr = metroID.split(" ");
		for (int i = 0; i < userSetArr.length; i++) {
			if (userSetArr[i].toLowerCase().trim().equals("1")) { // not sure
				// about
				// this
				int k = i + 1;
				sketch.addUser("user_" + k); // just to make sure that the hash
				// value
				// will have different values
			}
		}
		return sketch;
	}
}
