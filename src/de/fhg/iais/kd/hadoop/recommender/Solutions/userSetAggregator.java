package de.fhg.iais.kd.hadoop.recommender.Solutions;

import java.util.ArrayList;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class userSetAggregator extends BaseOperation<userSetAggregator.Context>
		implements Aggregator<userSetAggregator.Context> {
	public static class Context {
		List<String> userNames;
	}

	public userSetAggregator() {
		// expects 1 argument, fail otherwise
		super(1, new Fields("user_id"));
	}

	public userSetAggregator(Fields fieldDeclaration) {
		// expects 1 argument, fail otherwise
		super(1, fieldDeclaration);
	}

	@Override
	public void start(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		// set the context object
		aggregatorCall.setContext(new Context());
	}

	@Override
	public void aggregate(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		TupleEntry arguments = aggregatorCall.getArguments();
		Context context = aggregatorCall.getContext();

		// add the current argument value to the current sum
		// we aggregate on the userName , so we have to aggregate on the second
		// field
		if (context.userNames == null) {
			context.userNames = new ArrayList<String>();
		}

		context.userNames.add(arguments.getString(1));

	}

	@Override
	public void complete(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		Context context = aggregatorCall.getContext();

		// create a Tuple to hold our result values
		Tuple result = new Tuple();

		// set the the list of users
		result.add(context.userNames);

		// return the result Tuple
		aggregatorCall.getOutputCollector().add(result);
	}
}