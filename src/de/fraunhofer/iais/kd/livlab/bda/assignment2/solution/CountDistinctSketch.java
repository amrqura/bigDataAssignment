package de.fraunhofer.iais.kd.livlab.bda.assignment2.solution;

import java.util.BitSet;

import de.fraunhofer.iais.kd.livlab.bda.countdistinct.Sketch;

// compact synopsis for set of distinct entities
public class CountDistinctSketch {

	Sketch sketch;

	public CountDistinctSketch() {

	}

	public CountDistinctSketch(int sketchsize) {
		sketch = new Sketch(sketchsize);
		// TODO Auto-generated constructor stub
	}

	public CountDistinctSketch(BitSet sketch) {
		this.sketch = new Sketch(sketch.length());
		this.sketch.setSketch(sketch);
	} // test

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	public void addUser(String username) {

		int index = Math.abs(username.hashCode() % sketch.getSketchsize()); // make sure not negative
		sketch.getSketch().set(index);

	}

	/**
	 * get estimation of how many distinct users are added in the sketch
	 *
	 * @return
	 */
	public int getEstimate() {
		// -m*ln(US/m)

		int m = sketch.getSketchsize();
		int US = sketch.getSketchsize() - sketch.getSketch().cardinality();
		double result = -1 * m * Math.log(US / m);
		return (int) result;

	}

	/**
	 *
	 * // compute Jaccard-Distance to another CountDistinctSketch
	 *
	 * @param other
	 * @return
	 */
	public double distanceTo(CountDistinctSketch other) {

		/*
		 * BitSet unionSet = new BitSet( other.getSketchsize() > getSketchsize()
		 * ? other.getSketchsize() : getSketchsize()); unionSet.set(0,
		 * unionSet.length() - 1, false); unionSet.or(getSketch());
		 * unionSet.or(other.getSketch()); CountDistinctSketch
		 * unionDistinctSketch = new CountDistinctSketch( unionSet);
		 */
		CountDistinctSketch unionDistinctSketch = new CountDistinctSketch(
				sketch.copy().getSketch());
		unionDistinctSketch.sketch.orSketch(other.sketch);

		double similarity = getEstimate() + other.getEstimate()
				- unionDistinctSketch.getEstimate();
		similarity = similarity / unionDistinctSketch.getEstimate();

		return 1 - similarity;

	}

}
