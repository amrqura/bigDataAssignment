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

public class UserSetMatrixAggregator extends
		BaseOperation<UserSetMatrixAggregator.Context> implements
		Aggregator<UserSetMatrixAggregator.Context> {
	static int numOfUsers = 1000;

	public static class Context {
		List<String> userNames = new ArrayList<String>();

		// for(int i=0;i<numOfUsers;o++)
		public Context() {
			for (int i = 0; i < numOfUsers; i++) {
				userNames.add("0");
			}

		}

	}

	public UserSetMatrixAggregator() {
		// expects 1 argument, fail otherwise
		super(1, new Fields("user_id"));
	}

	public UserSetMatrixAggregator(Fields fieldDeclaration) {
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
		String userId = arguments.getString(1);
		int userIDIndex = Integer.parseInt(userId.split("_")[1]);

		// if the user exists , transfer it to 1
		context.userNames.set(userIDIndex - 1, "1");

	}

	@Override
	public void complete(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		Context context = aggregatorCall.getContext();

		// create a Tuple to hold our result values
		Tuple result = new Tuple();

		// set the the list of users
		// result.add(context.userNames);
		StringBuilder resultString = new StringBuilder();
		for (int i = 0; i < context.userNames.size(); i++) {
			if (i == context.userNames.size() - 1) {
				resultString.append(context.userNames.get(i));
			} else {
				resultString.append(context.userNames.get(i) + ",");
			}

		}

		// remove the last comma

		result.add(resultString);

		// return the result Tuple

		aggregatorCall.getOutputCollector().add(result);

	}
}