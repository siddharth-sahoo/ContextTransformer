package com.awesome.pro.context.transformer;

import java.util.Map;

import org.apache.log4j.Logger;

import com.awesome.pro.context.ContextBuilder;
import com.awesome.pro.context.ContextFactory;
import com.awesome.pro.context.transformer.references.ContextTransformerConfigReferences;
import com.awesome.pro.context.transformer.references.ContextTransformerMongoReferences;
import com.awesome.pro.executor.IThreadPool;
import com.awesome.pro.executor.ThreadPool;
import com.awesome.pro.utilities.PropertyFileUtility;
import com.awesome.pro.utilities.db.mongo.MongoConnection;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;

/**
 * Transforms context data according to configurations.
 * @author siddharth.s
 */
public class ContextTransformer {

	/**
	 * Root logger instance.
	 */
	private static final Logger LOGGER = Logger.getLogger(
			ContextTransformer.class);

	/**
	 * Name of the context transformer instance.
	 */
	private final String instanceName;

	/**
	 * Thread pool executor instance.
	 */
	private final IThreadPool threadPool;

	/**
	 * Source context instance to be transformed.
	 */
	private final ContextBuilder parentContext;

	/**
	 * Destination context for transformation.
	 */
	private final ContextBuilder targetContext;

	/**
	 * Instantiates a new transformer instance.
	 * @param configFile Configuration file name to be read.
	 * @param name Name of the context transformer instance.
	 */
	// Constructor
	ContextTransformer(final String name, final String configFile) {
		instanceName = name;
		threadPool = new ThreadPool(configFile);
		threadPool.start();

		final PropertyFileUtility config = new PropertyFileUtility(configFile);
		final DBCursor cursor = MongoConnection.getDocuments(
				config.getStringValue(
						ContextTransformerConfigReferences.PROPERTY_MONGO_DATABASE),
						config.getStringValue(
								ContextTransformerConfigReferences.PROPERTY_MONGO_COLLECTION));

		if (!cursor.hasNext()) {
			LOGGER.error("No configurations found.");
		}

		if (cursor.size() != 1) {
			LOGGER.warn("More than one entries found. "
					+ "Ignoring all except the first.");
		}

		// Retrieve references to input and output context instances.
		final BasicDBObject obj = (BasicDBObject) cursor.next();
		parentContext = ContextFactory.getContextBuilder(
				obj.getString(ContextTransformerMongoReferences
						.FIELD_PARENT_CONTEXT));
		if (parentContext != null) {
			LOGGER.info("Parent context populated: " + parentContext.getName());
		} else {
			LOGGER.error("Parent context is null.");
		}
		
		if (obj.getBoolean(ContextTransformerMongoReferences.FIELD_IN_MEMORY)) {
			LOGGER.info("Starting in memory context, recommended for count"
					+ " operations.");
			targetContext = ContextFactory.getInMemoryContextBuilder(
					obj.getString(ContextTransformerMongoReferences
							.FIELD_PARENT_CONTEXT));
		} else {
			LOGGER.info("Starting off heap context, recommended for non"
					+ " count operations.");
			targetContext = ContextFactory.getContextBuilder(
					obj.getString(ContextTransformerMongoReferences
							.FIELD_PARENT_CONTEXT));
		}

		// Iterate over name spaces in parent context.
		final BasicDBList namespaces = (BasicDBList) obj.get(
				ContextTransformerMongoReferences.FIELD_NAMESPACES);
		final int size = namespaces.size();
		LOGGER.info(size + " name spaces to be transformed for "
				+ parentContext.getName());
		LOGGER.info(parentContext.getName() + " is to be transformed into "
				+ targetContext.getName());

		for (int i = 0; i < size; i ++) {
			final BasicDBObject curr = (BasicDBObject) namespaces.get(i);
			threadPool.execute(new ContextTransformerRunnableJob(instanceName, curr));
		}
		LOGGER.info("All jobs scheduled for tranformation of "
				+ parentContext.getName());
	}

	/**
	 * Waits till the transformation process is completed.
	 */
	public final void waitForCompletion() {
		LOGGER.info("Waiting for completion of transformation of "
				+ parentContext.getName());
		threadPool.waitForCompletion();
	}

	/**
	 * @return The parent context instance or the source context.
	 */
	public final ContextBuilder getParentContext() {
		return parentContext;
	}

	/**
	 * @return The target context instance.
	 */
	public final ContextBuilder getTargetContext() {
		return targetContext;
	}

	/**
	 * @param name Name of the name space to be retrieved.
	 * @return Name space data retrieved from the parent context.
	 */
	public final Map<String, Map<String, String>> getParentContextNamespace(
			final String name) {
		return parentContext.getContextData(name);
	}

	/**
	 * @param name Name of the name space to be retrieved.
	 * @return Name space data retrieved from the target context.
	 */
	public final Map<String, Map<String, String>> getTargetContextNamespace(
			final String name) {
		return targetContext.getContextData(name);
	}

}
