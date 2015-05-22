package de.bigdata.solution.aggregators;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import de.bigdata.solution.UserSet;
import de.bigdata.solution.UserSetClusterModel;
import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModel;
import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModelFactory;
import de.fraunhofer.iais.kd.livlab.bda.config.BdaConstants;

/**
 * A custom aggregator for task 5 which outputs the artist name and its cluster id
 * grouped by an artist.
 * @author
 *
 */
public class UserClusterAggregator extends BaseOperation<UserClusterAggregator.Context>
implements Aggregator<UserClusterAggregator.Context> {
	/**
	 *
	 */
	private static final long serialVersionUID = -1219305190023089328L;

	public static class Context {
		UserSet value = new UserSet();
	}

	public UserClusterAggregator() {
		// expects 1 argument, fail otherwise
		super(1, new Fields("list"));
	}

	public UserClusterAggregator(Fields fieldDeclaration) {
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

		context.value.add(arguments.getString("user_id"));
	}

	@Override
	public void complete(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		Context context = aggregatorCall.getContext();

		ClusterModel clusterModel = ClusterModelFactory.readFromCsvResource(BdaConstants.CLUSTER_MODEL);
		UserSetClusterModel UserSetClusterModel = new UserSetClusterModel(clusterModel);

		String closestClusterId = UserSetClusterModel.findClosestCluster(context.value);
		// create a Tuple to hold our result values
		Tuple result = new Tuple();
		closestClusterId = closestClusterId.replace("\"", "");
		// set the sum
		result.add(closestClusterId);

		// return the result Tuple
		aggregatorCall.getOutputCollector().add(result);
	}
}