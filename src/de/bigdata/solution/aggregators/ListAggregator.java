package de.bigdata.solution.aggregators;

import java.util.HashSet;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * A custom aggregator for task 2, which aggregates the users list into a set to be
 * grouped by an artist.
 * @author
 *
 */
public class ListAggregator extends BaseOperation<ListAggregator.Context>
implements Aggregator<ListAggregator.Context> {
	/**
	 *
	 */
	private static final long serialVersionUID = -1219305190023089328L;

	public static class Context {
		Set<String> value = new HashSet<String>();
	}

	public ListAggregator() {
		// expects 1 argument, fail otherwise
		super(1, new Fields("list"));
	}

	public ListAggregator(Fields fieldDeclaration) {
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
		context.value.add(arguments.getString(1));
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