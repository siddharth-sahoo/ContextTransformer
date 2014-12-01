package com.awesome.pro.context.transformer;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.awesome.pro.context.ContextBuilder;
import com.awesome.pro.context.transformer.references.ContextTransformerConfigReferences;
import com.awesome.pro.context.transformer.references.ContextTransformerMongoReferences;
import com.awesome.pro.context.transformer.utilities.Evaluators;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

/**
 * Unit runnable job which transforms a single name space of a
 * particular context.
 * @author siddharth.s
 */
public class ContextTransformerRunnableJob implements Runnable {

	/**
	 * Configurations retrieved from MongoDB.
	 */
	private final BasicDBObject config;

	/**
	 * Name of the context transformer instance name.
	 */
	private final String transformerName;

	/**
	 * Instantiates a new runnable instance for context transformation.
	 * @param mappings Configuration retrieved from MongoDB.
	 * @param name Name of the context transformer instance.
	 */
	public ContextTransformerRunnableJob(final String name, final BasicDBObject mappings) {
		transformerName = name;
		config = mappings;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// Transformer instance.
		final ContextTransformer transformer = ContextTransformerMain.
				getContextTransformerInstance(transformerName);

		// Parent context.
		final Map<String, Map<String, String>> parentNamespace = transformer
				.getParentContextNamespace(
						config.getString(
								ContextTransformerMongoReferences.FIELD_PARENT_NAMESPACE));

		// Target context.
		final ContextBuilder targetContext = transformer.getTargetContext();
		final String targetNamespace = config.getString(
				ContextTransformerMongoReferences.FIELD_TARGET_NAMEPSACE);

		// Row level meta data.
		final String condition = config.getString(
				ContextTransformerMongoReferences.FIELD_CONDITION);
		final String rowKeyExpression = config.getString(
				ContextTransformerMongoReferences.FIELD_ROW_KEY_EXPR);

		final Iterator<Entry<String, Map<String, String>>> rowKeyIterator =
				parentNamespace.entrySet().iterator();
		while (rowKeyIterator.hasNext()) {
			final Entry<String, Map<String, String>> row =
					rowKeyIterator.next();
			row.getValue().put(
					ContextTransformerConfigReferences.PIPELINE_ROW_KEY,
					row.getKey());

			if (!Evaluators.evaluateCondition(condition, row.getValue())) {
				continue;
			}

			final String rowKey = Evaluators.evaluateRowKey(rowKeyExpression,
					row.getValue());

			final BasicDBList fields = (BasicDBList) config.get(
					ContextTransformerMongoReferences.FIELD_FIELDS);
			final int size = fields.size();

			for (int i = 0; i < size; i ++) {
				final BasicDBObject transformation = (BasicDBObject) fields.get(i);
				final String value = Evaluators.evaluateTransformation(
						transformation.getString(
								ContextTransformerMongoReferences.FIELD_FIELD_OPERATION),
								transformation.getString(
										ContextTransformerMongoReferences.FIELD_FIELD_EXPRESSION),
										row.getValue());
				targetContext.addContextualData(targetNamespace, rowKey,
						transformation.getString(ContextTransformerMongoReferences.FIELD_FIELD_NAME),
						value);
			}
		}
	}

}
