package com.awesome.pro.context.transformer;

import gnu.trove.map.hash.THashMap;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

public class ContextTransformerMain {

	/**
	 * Root logger instance.
	 */
	private static final Logger LOGGER = Logger.getLogger(
			ContextTransformerMain.class);

	/**
	 * Map of transformer reference name to instance.
	 */
	private static final Map<String, ContextTransformer> INSTANCES =
			new THashMap<>();

	/**
	 * Starts the transformation process. 
	 * @param name Reference name for the transformation process.
	 * @param configFile Configuration file to be read.
	 */
	public static void start(final String name, final String configFile) {
		if (!INSTANCES.containsKey(name)) {
			synchronized (ContextTransformerMain.class) {
				if (!INSTANCES.containsKey(name)) {
					INSTANCES.put(name, new ContextTransformer(name, configFile));
					LOGGER.info("Started a new context transfomer: " + name);
				}
			}
		}
	}

	/**
	 * Waits till the entire transformation process is completed.
	 */
	public static void waitForCompletion() {
		final Iterator<Entry<String, ContextTransformer>> iter =
				INSTANCES.entrySet().iterator();
		while (iter.hasNext()) {
			final Entry<String, ContextTransformer> entry = iter.next();
			LOGGER.info("Waiting for completion of transformer: " + entry.getKey());
			entry.getValue().waitForCompletion();
		}
	}

	/**
	 * @param name Name of the context transformer instance.
	 * @return Instance of the context transformer.
	 */
	public static final ContextTransformer getContextTransformerInstance(final String name) {
		if (INSTANCES.containsKey(name)) {
			return INSTANCES.get(name);
		}
		else {
			LOGGER.warn("Context transformer instance not found: " + name);
			return null;
		}
	}

}
