package de.bigdata.solution;

import java.util.HashSet;
import java.util.Set;

/**
 * Task 1
 * This class represents a set of usernames with listening events of an artist
 * In other words: UserSet = names of users listening to an artist
 * @author
 *
 */

public class UserSet {

	private String artistId;

	private final Set <Integer> usersMatrix = new HashSet<Integer>();

	public void setArtistId(String artistId) {
		this.artistId = artistId;
	}

	public String getArtistId() {
		return artistId;
	}


	// add a user to the user set
	public void add(String username) {
		int userNameid = Integer.parseInt(username.split("_")[1]);
		usersMatrix.add(userNameid);
	}

	public void add(int userId) {
		usersMatrix.add(userId);
	}
	// compute Jaccard-Distance to another userset
	public double distanceTo(UserSet other) {
		int intersectSize = getSetInterSize(other.getUsers());
		// the union size equals summing both sets' elements minus their intersection size
		int unionSize = this.usersMatrix.size() + other.getUsers().size() - intersectSize;
		double similarity = (double) intersectSize / unionSize;
		double distance = 1 - similarity;
		return distance;
	}

	public Set<Integer> getUsers() {
		return usersMatrix;
	}
	// return the intersection size of the 2 user sets.
	private int getSetInterSize(Set<Integer> otherUsers) {
		int intersectCount = 0;
		Set<Integer> smallerSet;
		Set<Integer> largerSet;
		if(otherUsers.size() >= this.usersMatrix.size()) {
			smallerSet = this.usersMatrix;
			largerSet = otherUsers;
		}
		else {
			largerSet = this.usersMatrix;
			smallerSet = otherUsers;
		}

		for(Integer curUser : smallerSet) {
			if(largerSet.contains(curUser)) {
				++intersectCount;
			}
		}
		return intersectCount;
	}
}
