package de.fraunhofer.iais.kd.livlab.bda.assignment2.solution;

import java.util.BitSet;

import de.fraunhofer.iais.kd.livlab.bda.countdistinct.Sketch;

public class CountDistinctSketch extends Sketch {

	public CountDistinctSketch(int sketchsize) {
		super(sketchsize);
		// TODO Auto-generated constructor stub
	}

	public CountDistinctSketch(BitSet sketch) {
		super(sketch.length());
		setSketch(sketch);
	} // test

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	public void addUser(String username) {

		int index = username.hashCode() % getSketchsize();
		getSketch().set(index);

	}

	/**
	 * get estimation of how many distinct users are added in the sketch
	 *
	 * @return
	 */
	public int getEstimate() {
		// -m*ln(US/m)

		int m = getSketch().cardinality();
		int US = getSketchsize() - m;
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

		BitSet unionSet = new BitSet(
				other.getSketchsize() > getSketchsize() ? other.getSketchsize()
						: getSketchsize());
		unionSet.set(0, unionSet.length() - 1, false);
		unionSet.or(getSketch());
		unionSet.or(other.getSketch());
		CountDistinctSketch unionDistinctSketch = new CountDistinctSketch(
				unionSet);

		double similarity = getEstimate() + other.getEstimate()
				- unionDistinctSketch.getEstimate();
		similarity = similarity / unionDistinctSketch.getEstimate();

		return 1 - similarity;

	}

}
