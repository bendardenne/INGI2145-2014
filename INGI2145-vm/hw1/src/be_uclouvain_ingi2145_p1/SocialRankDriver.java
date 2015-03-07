package be_uclouvain_ingi2145_p1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.varia.LevelRangeFilter;

public class SocialRankDriver extends Configured implements Tool
{
	/**
	 * Singleton instance.
	 */
	public static SocialRankDriver GET;

	/**
	 * Number of iterations to perform in between diff checks.
	 */
	public static final double DIFF_INTERVAL = 20;

	/**
	 * Author's name.
	 */
	public static final String NAME = "Benoît Dardenne";

	// ---------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception
	{
		// configure log4j to output to a file
		Logger logger = LogManager.getRootLogger();
		logger.addAppender(new FileAppender(new SimpleLayout(), "p1.log"));

		// configure log4j to output to the console
		Appender consoleAppender = new ConsoleAppender(new SimpleLayout());
		LevelRangeFilter filter = new LevelRangeFilter();
		// switch to another level for more detail (own (INGI2145) messages use FATAL)
		filter.setLevelMin(Level.ERROR);
		consoleAppender.addFilter(filter);
		// (un)comment to (un)mute console output
		logger.addAppender(consoleAppender);

		// switch to Level.DEBUG or Level.TRACE for more detail
		logger.setLevel(Level.INFO);

		GET = new SocialRankDriver();
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, GET, args);
		System.exit(res);
	}

	// ---------------------------------------------------------------------------------------------

	@Override
	public int run(String[] args) throws Exception
	{
		System.out.println(NAME);

		if (args.length == 0) {
			args = new String[]{ "command missing" };
		}

		switch (args[0]) {
		case "init":
			init(args[1], args[2], Integer.parseInt(args[3]));
			break;
		case "iter":
			iter(args[1], args[2], Integer.parseInt(args[3]));
			break;
		case "diff":
			diff(args[1], args[2], args[3], args[4], Integer.parseInt(args[5]));
			break;
		case "finish":
			finish(args[1], args[2], args[3], Integer.parseInt(args[4]));
			break;
		case "composite":
			composite(args[1], args[2], args[3], args[4], args[5],
					args[6], Double.parseDouble(args[7]), Integer.parseInt(args[8]));
			break;
		default:
			System.out.println("Unknown command: " + args[0]);
			break;
		}

		return 0;
	}

	// ---------------------------------------------------------------------------------------------

	void init(String inputDir, String outputDir, int nReducers) throws Exception
	{
		Logger.getRootLogger().fatal("[INGI2145] init");
		Utils.deleteDir(outputDir);

		Job job = Utils.configureJob(inputDir, outputDir, 
				InitJob.Map.class, null, InitJob.Reduce.class, 
				LongWritable.class, LongWritable.class, 
				LongWritable.class, Text.class, nReducers);	FileOutputFormat.setOutputPath(job, new Path(outputDir));

		job.waitForCompletion(true);
	}

	// ---------------------------------------------------------------------------------------------

	void iter(String inputDir, String outputDir, int nReducers) throws Exception
	{
		Logger.getRootLogger().fatal("[INGI2145] iter: " + inputDir + " (to) " + outputDir);

		Utils.deleteDir(outputDir);

		Job job = Utils.configureJob(inputDir, outputDir, 
				IterJob.Map.class, null, IterJob.Reduce.class, 
				LongWritable.class, Text.class, 
				LongWritable.class, Text.class, nReducers);
		job.waitForCompletion(true);
	}

	// ---------------------------------------------------------------------------------------------

	double diff(String inputDir1, String inputDir2, String tmpDir, String outputDir, int nReducers)
			throws Exception
	{
		Logger.getRootLogger().fatal("[INGI2145] diff");

		Utils.deleteDir(outputDir);
		Utils.deleteDir(tmpDir);

        Job job = Utils.configureJob(inputDir1, tmpDir, 
				DiffJob.Map.class, null, DiffJob.Reduce.class, 
				LongWritable.class, DoubleWritable.class, 
				DoubleWritable.class, NullWritable.class, 1);
        		
        		Job.getInstance(SocialRankDriver.GET.getConf());
        
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(NullWritable.class);        
        
        FileInputFormat.addInputPath(job, new Path(inputDir2));
        job.waitForCompletion(true);
        
        
        job = Utils.configureJob(tmpDir, outputDir, 
				DiffJob.SortMap.class, null, DiffJob.SortReduce.class, 
				Text.class, DoubleWritable.class, 
				DoubleWritable.class, NullWritable.class, 1);
        job.waitForCompletion(true);
        
		double diffResult = Utils.readDiffResult(outputDir);
		Logger.getRootLogger().fatal("[INGI2145] diff was " + diffResult);
		return diffResult;
	}

	// ---------------------------------------------------------------------------------------------

	void finish(String inputDir, String tmpDir, String outputDir, int nReducers) throws Exception
	{
		Logger.getRootLogger().fatal("[INGI2145] finish from:" + inputDir);

		Utils.deleteDir(outputDir);
		Utils.deleteDir(tmpDir);

		Job job = Utils.configureJob(inputDir, outputDir, 
				FinishJob.Map.class, null, FinishJob.Reduce.class, 
				DoubleWritable.class, Text.class, 
				DoubleWritable.class, Text.class, 1);
		
		job.waitForCompletion(true);
	}

	// ---------------------------------------------------------------------------------------------

	void composite(String inputDir, String outputDir, String intermDir1, String intermDir2,
			String diffDir, String tmpDiffDir, double delta, int nReducers) throws Exception
	{
		init(inputDir, intermDir1, nReducers);

		String a = intermDir1;
		String b = intermDir2;
		String tmp;
		
		do{
			for(int i = 0; i < DIFF_INTERVAL; i++)
			{
				iter(a, b, nReducers);
				tmp = a;
				a = b;
				b = tmp;
			}

			diff(a, b, tmpDiffDir, diffDir, nReducers);
		} while(Utils.readDiffResult(diffDir) > delta);
		
		finish(b, tmpDiffDir, outputDir, nReducers);
	}
}
