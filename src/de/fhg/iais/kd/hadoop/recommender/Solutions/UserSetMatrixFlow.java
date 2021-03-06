package de.fhg.iais.kd.hadoop.recommender.Solutions;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Aggregator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import de.fhg.iais.kd.hadoop.recommender.functions.ProjectToFields;

public class UserSetMatrixFlow {

	private static final String DELIMITER = ",";

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Flow getUserSetMatrixFlow(String infile, String outfile) {

		String inputPath = infile;
		String outputPath = outfile;

		// define source and sink Taps.
		Fields sourceFields = new Fields("uid", "datetime", "artist_mbid",
				"artist_name", "track_mbid", "track_name");
		Scheme sourceScheme = new TextDelimited(sourceFields);
		Tap source = new Hfs(sourceScheme, inputPath);

		Scheme outputSehema = new TextDelimited(false, DELIMITER);

		Tap userSetTap = new Hfs(outputSehema, outputPath + "/UserSetMatrix",
				SinkMode.REPLACE);

		Pipe pipe = new Pipe("UserSetMatrixPipe");

		Fields targetFields = new Fields("artist_name", "uid");
		ProjectToFields projector = new ProjectToFields(targetFields);
		// go for each record
		pipe = new Each(pipe, targetFields, projector);
		// group the Tuple stream by the "artist_name" value
		pipe = new GroupBy(pipe, new Fields("artist_name"));
		// aggregate on user_id
		Aggregator userAggregator = new UserSetMatrixAggregator(new Fields(
				"user_id"));
		pipe = new Every(pipe, userAggregator);

		// initialize app properties, tell Hadoop which jar file to use
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, UserSetMatrixFlow.class);

		FlowConnector flowConnector = new HadoopFlowConnector(properties);

		Map<String, Tap> endPoints = new HashMap<>();
		endPoints.put("UserSetMatrixPipe", userSetTap);

		Flow flow = flowConnector.connect("UserSetMatrixModel", source,
				endPoints, pipe);
		return flow;

	}

}
