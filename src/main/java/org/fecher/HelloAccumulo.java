package org.fecher;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class HelloAccumulo
{
	private final static Logger LOGGER = Logger.getLogger(HelloAccumulo.class);
	private static final int DEFAULT_NUM_THREADS = 16;
	private static final long DEFAULT_TIMEOUT_MILLIS = 1000L; // 1 second
	private static final long DEFAULT_BYTE_BUFFER_SIZE = 1048576L; // 1 MB

	public static void main(
			final String[] args ) {

		final Options accumuloOptions = new Options();
		AccumuloCommandLineOptions.applyOptions(accumuloOptions);
		if (args.length < 1) {
			final HelpFormatter help = new HelpFormatter();
			help.printHelp(
					"<options>",
					"\nOtions:",
					accumuloOptions,
					"\nOptions are similar to geowave-ingest.");
			System.exit(-1);
		}
		final Parser parser = new BasicParser();
		CommandLine optionCommandLine;
		try {
			optionCommandLine = parser.parse(
					accumuloOptions,
					args);
			final AccumuloCommandLineOptions accumuloOption = AccumuloCommandLineOptions.parseOptions(optionCommandLine);
			System.out.println("Attempting to get zookeeper instance");

			final Instance inst = new ZooKeeperInstance(
					accumuloOption.getInstanceId(),
					accumuloOption.getZookeepers());
			System.out.println("Attempting to get accumulo connector");
			final Connector connector = inst.getConnector(
					accumuloOption.getUser(),
					accumuloOption.getPassword());
			System.out.println("Got successful connection to accumulo ");
			if (accumuloOption.isClearNamespace()) {
				System.out.println("Attempting to delete table '" + accumuloOption.getNamespace() + "'");
				if (connector.tableOperations().exists(
						accumuloOption.getNamespace())) {
					connector.tableOperations().delete(
							accumuloOption.getNamespace());
					System.out.println("Deleted table successfully");
				}
				else {
					System.out.println("Table doesn't exist");
				}
			}
			else {
				if (connector.tableOperations().exists(
						accumuloOption.getNamespace())) {
					System.out.println("Table '" + accumuloOption.getNamespace() + "' exists");
				}
				else {
					System.out.println("Table '" + accumuloOption.getNamespace() + "' doesn't exist, creating table");
					connector.tableOperations().create(
							accumuloOption.getNamespace());
					System.out.println("Table created successfully");
				}
				System.out.println("Creating BatchWriter");
				final BatchWriter writer = connector.createBatchWriter(
						accumuloOption.getNamespace(),
						new BatchWriterConfig().setMaxWriteThreads(
								DEFAULT_NUM_THREADS).setMaxMemory(
								DEFAULT_BYTE_BUFFER_SIZE).setTimeout(
								DEFAULT_TIMEOUT_MILLIS,
								TimeUnit.MILLISECONDS));
				System.out.println("BatchWriter created");
				final Mutation testMutation = new Mutation(
						new Text(
								"Test Row"));
				testMutation.put(
						new Text(
								"Test Column Family"),
						new Text(
								"Test Column Qualifier"),
						new Value(
								"Test Value".getBytes(Charset.forName("UTF-8"))));

				System.out.println("adding test mutation");
				writer.addMutation(testMutation);
				System.out.println("flushing batch writer");
				writer.flush();
				System.out.println("closing batch writer");
				writer.close();
			}
		}
		catch (ParseException | AccumuloException | AccumuloSecurityException | TableNotFoundException | TableExistsException e) {
			LOGGER.fatal(
					"Exception while parsing or running hello accumulo",
					e);
		}
	}
}
