package org.fecher;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class AccumuloCommandLineOptions
{

	private final static Logger LOGGER = Logger.getLogger(AccumuloCommandLineOptions.class);

	private final String zookeepers;
	private final String instanceId;
	private final String user;
	private final String password;
	private final String namespace;
	private final boolean clearNamespace;

	public AccumuloCommandLineOptions(
			final String zookeepers,
			final String instanceId,
			final String user,
			final String password,
			final String namespace,
			final boolean clearNamespace ) {
		this.zookeepers = zookeepers;
		this.instanceId = instanceId;
		this.user = user;
		this.password = password;
		this.namespace = namespace;
		this.clearNamespace = clearNamespace;
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

		final String namespace = commandLine.getOptionValue(
				"n",
				"test");
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
		return new AccumuloCommandLineOptions(
				zookeepers,
				instanceId,
				user,
				password,
				namespace,
				clearNamespace);
	}

	public String getZookeepers() {
		return zookeepers;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}

	public String getNamespace() {
		return namespace;
	}

	public boolean isClearNamespace() {
		return clearNamespace;
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
		final Option namespace = new Option(
				"n",
				"name",
				true,
				"The table name (optional; default is 'test')");
		allOptions.addOption(namespace);
	}
}
