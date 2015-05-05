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
import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModel;
import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModelFactory;
import de.fraunhofer.iais.kd.livlab.bda.config.BdaConstants;

public class userSetClusterAggregator extends
		BaseOperation<userSetClusterAggregator.Context> implements
		Aggregator<userSetClusterAggregator.Context> {

	public static class Context {
		List<String> userNames;
	}

	public userSetClusterAggregator() {
		// expects 1 argument, fail otherwise
		super(1, new Fields("user_id"));
	}

	public userSetClusterAggregator(Fields fieldDeclaration) {
		// expects 1 argument, fail otherwise
		super(1, fieldDeclaration);
	}

	@Override
	public void start(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		// TODO Auto-generated method stub
		aggregatorCall.setContext(new Context());

	}

	@Override
	public void aggregate(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
		// create a Tuple to hold our result values
		Context context = aggregatorCall.getContext();
		Tuple result = new Tuple();

		StringBuilder resultString = new StringBuilder();
		for (int i = 0; i < context.userNames.size(); i++) {
			if (i == context.userNames.size() - 1) {
				resultString.append(context.userNames.get(i));
			} else {
				resultString.append(context.userNames.get(i) + ",");
			}

		}

		// remove the last comma

		Userset tmpUserSet = new Userset(resultString.toString());

		ClusterModel clusterModel = ClusterModelFactory
				.readFromCsvResource(BdaConstants.CLUSTER_MODEL);
		UserSetClusterModel model = new UserSetClusterModel(clusterModel);

		result.add(model.findClosestCluster(tmpUserSet));

		// return the result Tuple
		aggregatorCall.getOutputCollector().add(result);

	}

}
