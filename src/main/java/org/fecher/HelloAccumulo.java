package org.fecher;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

public class HelloAccumulo {
	private final static Logger LOGGER = Logger.getLogger(HelloAccumulo.class);
	public static void main(String[] args) {

		final Options operations = new Options();
		applyOptions(operations);
		if (args.length < 1) {
			final HelpFormatter help = new HelpFormatter();
			help.printHelp(
					"<options>",
					"\nOtions:",
					operations,
					"\nOptions are similar to geowave-ingest.");
			System.exit(-1);
		}
		final String[] optionsArgs = new String[args.length - 1];
		System.arraycopy(
				args,
				1,
				optionsArgs,
				0,
				optionsArgs.length);
		final String[] operationsArgs = new String[] {
			args[0]
		};
		final Parser parser = new BasicParser();
		CommandLine operationCommandLine;
		try {
			operationCommandLine = parser.parse(
					operations,
					operationsArgs);
			final OperationCommandLineOptions operationOption = OperationCommandLineOptions.parseOptions(operationCommandLine);
			operationOption.getOperation().getDriver().run(
					optionsArgs);
		}
		catch (final ParseException e) {
			LOGGER.fatal(
					"Unable to parse operation",
					e);
		}
	}
	public static AccumuloCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		boolean success = true;
		final String zookeepers = commandLine.getOptionValue("z");
		final String instanceId = commandLine.getOptionValue("i");
		final String user = commandLine.getOptionValue("u");
		final String password = commandLine.getOptionValue("p");
		boolean clearNamespace = false;
		if (commandLine.hasOption("c")) {
			clearNamespace = true;
		}
		String visibility = null;
		if (commandLine.hasOption("v")) {
			visibility = commandLine.getOptionValue("v");
		}
		final String namespace = commandLine.getOptionValue(
				"n",
				"");
		if (zookeepers == null) {
			success = false;
			LOGGER.fatal("Zookeeper URL not set");
		}
		if (instanceId == null) {
			success = false;
			LOGGER.fatal("Accumulo instance ID not set");
		}
		if (user == null) {
			success = false;
			LOGGER.fatal("Accumulo user ID not set");
		}
		if (password == null) {
			success = false;
			LOGGER.fatal("Accumulo password not set");
		}
		if (!success) {
			throw new ParseException(
					"Required option is missing");
		}
		try {
			return new AccumuloCommandLineOptions(
					zookeepers,
					instanceId,
					user,
					password,
					namespace,
					visibility,
					clearNamespace,
					type);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.fatal(
					"Unable to connect to Accumulo with the specified options",
					e);
		}
		return null;
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option zookeeperUrl = new Option(
				"z",
				"zookeepers",
				true,
				"A comma-separated list of zookeeper servers that an Accumulo instance is using");
		allOptions.addOption(zookeeperUrl);
		final Option instanceId = new Option(
				"i",
				"instance-id",
				true,
				"The Accumulo instance ID");
		allOptions.addOption(instanceId);
		final Option user = new Option(
				"u",
				"user",
				true,
				"A valid Accumulo user ID");
		allOptions.addOption(user);
		final Option password = new Option(
				"p",
				"password",
				true,
				"The password for the user");
		allOptions.addOption(password);
		final Option visibility = new Option(
				"v",
				"visibility",
				true,
				"The visiblity of the data ingested (optional; default is 'public')");
		allOptions.addOption(visibility);

		final Option namespace = new Option(
				"n",
				"namespace",
				true,
				"The table namespace (optional; default is no namespace)");
		allOptions.addOption(namespace);
	}
}
