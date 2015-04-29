package de.fhg.iais.kd.hadoop.recommender.Solutions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class userSet {

	private List<String> myUserSet;

	public List<String> getMyUserSet() {
		return myUserSet;
	}

	public void add(String username)
	// add a user to the userset
	{
		if (myUserSet == null) {
			myUserSet = new ArrayList<String>();
		}
		myUserSet.add(username);
	}

	public double distanceTo(userSet other)
	// compute Jaccard-Distance to another userset
	{
		if (other == null || other.getMyUserSet().size() == 0) {
			return 0;
		}
		if (myUserSet == null || myUserSet.size() == 0) {
			return 0;
		}

		Set<String> unionXY = new HashSet<String>(myUserSet);
		unionXY.addAll(other.getMyUserSet());

		Set<String> intersectionXY = new HashSet<String>(myUserSet);
		intersectionXY.retainAll(other.getMyUserSet());

		return (double) intersectionXY.size() / (double) unionXY.size();

	}
}
