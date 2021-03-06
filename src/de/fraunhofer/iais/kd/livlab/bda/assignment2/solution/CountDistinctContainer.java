package de.fraunhofer.iais.kd.livlab.bda.assignment2.solution;

public class CountDistinctContainer {

	private final String ArtName;

	private boolean isIncreased = false;

	public void setIncreased(boolean isIncreased) {
		this.isIncreased = isIncreased;
	}

	private final CountDistinctSketch sketch = new CountDistinctSketch(10);

	// sketch of userNames with listening events of specific artist name
	public CountDistinctContainer(String ArtName) {
		this.ArtName = ArtName;
	}

	// add user
	public void addUser(String username) {
		isIncreased = false;
		int oldEstimation = sketch.getEstimate();

		sketch.addUser(username);
		if (sketch.getEstimate() > oldEstimation) {
			isIncreased = true;
		} // test
	}

	public int getCount() {
		return sketch.getEstimate();

	}

	public boolean isIncreased() {
		return isIncreased;
	}

	public CountDistinctSketch getSketch() {
		return sketch;
	}

	public String getArtName() {
		return ArtName;
	}
}
