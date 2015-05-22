package de.bigdata.solution.aggregators;

import java.util.ArrayList;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * A custom aggregator for task 3, which aggregates the users list into a set to be grouped
 * by an artist.
 *
 * @author
 *
 */
public class MatrixListAggregator extends
BaseOperation<MatrixListAggregator.Context> implements
Aggregator<MatrixListAggregator.Context> {
	/**
	 *
	 */
	private static final long serialVersionUID = 3845917947755151639L;
	/**
	 *
	 */

	private final static int MAX_USERS_COUNT = 1000;
	private final static String USER_PREFIX = "user_";

	public static class Context {
		List<Integer> value = new ArrayList<Integer>(MAX_USERS_COUNT){

			/**
			 *
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String toString() {
				StringBuilder out = new StringBuilder();
				for(int i = 0 ; i < MAX_USERS_COUNT; i++) {
					if(get(i) == 1) {
						out.append("1,");
					} else {
						out.append("0,");
					}
				}
				return out.toString().substring(0, out.length()-1);
			}
		};

		public Context() {
			for (int i = 0; i < MAX_USERS_COUNT; i++) {
				value.add(i, 0);
			}
		}
	}

	public MatrixListAggregator() {
		// expects 1 argument, fail otherwise
		super(1, new Fields("userset"));
	}

	public MatrixListAggregator(Fields fieldDeclaration) {
		// expects 1 argument, fail otherwise
		super(1, fieldDeclaration);
	}

	@Override
	public void start(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		// set the context object, starting at zero
		aggregatorCall.setContext(new Context());
	}

	@Override
	public void aggregate(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		TupleEntry arguments = aggregatorCall.getArguments();
		Context context = aggregatorCall.getContext();

		// add the current argument value to the current sum
		String username = arguments.getString(1);
		int userId = Integer.parseInt(username.split("_")[1]);
		context.value.set(userId - 1, 1);
	}

	@Override
	public void complete(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		Context context = aggregatorCall.getContext();

		// create a Tuple to hold our result values
		Tuple result = new Tuple();

		// set the sum
		result.add(context.value);

		// return the result Tuple
		aggregatorCall.getOutputCollector().add(result);
	}
}