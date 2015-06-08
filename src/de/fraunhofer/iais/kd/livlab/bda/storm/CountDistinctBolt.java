package de.fraunhofer.iais.kd.livlab.bda.storm;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.fraunhofer.iais.kd.livlab.bda.assignment2.solution.CountDistinctClusterModel;
import de.fraunhofer.iais.kd.livlab.bda.assignment2.solution.CountDistinctContainer;
import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModel;
import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModelFactory;
import de.fraunhofer.iais.kd.livlab.bda.config.BdaConstants;
import de.fraunhofer.iais.kd.livlab.bda.countdistinct.detector.CountDistinctDetecor;

/**
 *
 *
 */
public class CountDistinctBolt extends BaseRichBolt {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	OutputCollector collector;
	private CountDistinctDetecor detector;
	String currentArtistName = "";
	CountDistinctContainer currentContainer;
	static HashMap<String, CountDistinctContainer> artistUsers = new HashMap<String, CountDistinctContainer>();

	ClusterModel clusterModel = ClusterModelFactory
			.readFromCsvResource(BdaConstants.CLUSTER_MODEL);

	CountDistinctClusterModel distinctClusterModel = new CountDistinctClusterModel(
			clusterModel);

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		detector = new CountDistinctDetecor();

	}

	@Override
	public void execute(Tuple tuple) {

		String userid = tuple.getStringByField("userid");
		String artid = tuple.getStringByField("artid");
		String artname = tuple.getStringByField("artname");

		CountDistinctContainer container = artistUsers.get(artname);
		if (container == null) {
			// new artist
			container = new CountDistinctContainer(artname);
			// artistUsers.put(artname, container);
		}
		container.addUser(userid);
		artistUsers.put(artname, container);

		if (container.isIncreased()) {
			System.out.println("ArtName:" + artname + " nofuser:"
					+ container.getCount() + " closest_cluster"
					+ distinctClusterModel.getClosest(container.getSketch()));

			// container.setIncreased(false);

			// printing the keys
			String resultStr = "";
			for (String key : artistUsers.keySet()) {

				CountDistinctContainer tmpContainer = artistUsers.get(key);
				resultStr = resultStr + key + ", " + tmpContainer.getCount()
						+ "      //";

			}
			// System.out.println(resultStr);

		}

		String[] result = detector.process(userid, artid, artname);

		if (result != null) {
			// System.out.println("Userid:" + result[0] + " artname:" +
			// result[1]);
		}

		// collector.emit(tuple(word, count));
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("artname", "clusterid"));

	}

}
