package de.bigdata.solution;

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
import de.bigdata.solution.aggregators.MatrixListAggregator;
import de.fhg.iais.kd.hadoop.recommender.functions.ProjectToFields;

/**
 * Task 3:
 * The workflow should compute the userset for each artist in a matrix output
 * format.
 * Input used: last.fm
 * Output: artname, 0,1,0,0,1,1,0,0,
 * one line of 1000 columns per artist name, where column i represents
 *  whether user i listened to this artist (1) or not (0).
 *
 * @author
 *
 */
public class UserSetMatrix {


	private static final String DELIMITER = ", ";

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Flow getUserSetMatrix(String infile, String outfile) {

		String inputPath = infile;
		String outputPath = outfile;

		// define source and sink Taps.

		Fields sourceFields = new Fields("user_id", "datetime", "artist_mbid", "artist_name", "track_mbid", "track_name");
		Scheme sourceScheme = new TextDelimited(sourceFields);
		Tap source = new Hfs(sourceScheme, inputPath);

		Scheme outputSehema = new TextDelimited(false, DELIMITER);

		Tap matrix = new Hfs(outputSehema, outputPath + "/matrix",
				SinkMode.REPLACE);

		Pipe pipe = new Pipe("listenEvts");

		Fields targetFields = new Fields("artist_name", "user_id");
		ProjectToFields projector = new ProjectToFields(targetFields);
		pipe = new Each(pipe, targetFields, projector);

		// group the Tuple stream by the "artist name" value
		pipe = new GroupBy( pipe, new Fields( "artist_name" ) );

		Aggregator usersListPerArtist = new MatrixListAggregator(new Fields("user_id"));
		pipe = new Every(pipe, usersListPerArtist);


		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties,
				UserSetMatrix.class);

		FlowConnector flowConnector = new HadoopFlowConnector(properties);
		// FlowConnector flowConnector = new HadoopFlowConnector();

		Map<String, Tap> endPoints = new HashMap<>();
		endPoints.put("listenEvts", matrix);

		Flow flow = flowConnector.connect("utilityMatrix", source, endPoints,
				pipe);
		return flow;
	}

}
