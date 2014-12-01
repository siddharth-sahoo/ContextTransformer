package com.awesome.pro.context.transformer.utilities;

import java.util.Map;

import org.mvel2.MVEL;

import com.awesome.pro.context.transformer.references.ContextTransformerConfigReferences;

/**
 * Evaluates row key expression and conditions for transformation operations.
 * @author siddharth.s
 */
public class Evaluators {

	/**
	 * @param condition Condition to evaluate.
	 * @param variables Map of variable name to value.
	 * @return Evaluation result of the condition.
	 */
	public static final boolean evaluateCondition(final String condition,
			final Map<String, String> variables) {
		if (condition == null || condition.length() == 0) {
			return false;
		}

		// Pipelined operations.
		if (condition.equals(ContextTransformerConfigReferences
				.PIPELINE_CONDITION_TRUE)) {
			return true;
		}

		if (condition.equals(ContextTransformerConfigReferences
				.PIPELINE_CONDITION_FALSE)) {
			return false;
		}

		return MVEL.evalToBoolean(condition, variables);
	}

	/**
	 * @param expression Expression for the row key.
	 * @param variables Map of variable name to variable value.
	 * @return Evaluated row key value.
	 */
	public static final String evaluateRowKey(final String expression,
			final Map<String, String> variables) {
		if (expression == null || expression.length() == 0) {
			return null;
		}

		if (expression.equals(ContextTransformerConfigReferences
				.PIPELINE_ROW_KEY)) {
			return variables.get(ContextTransformerConfigReferences
					.PIPELINE_ROW_KEY);
		}

		return MVEL.evalToString(expression, variables);
	}

	/**
	 * @param operation Transformation operation to be performed.
	 * @param expression Expression to be evaluated.
	 * @param variables Values on which the transformation is to be performed.
	 * @return Transformed value.
	 */
	public static final String evaluateTransformation(final String operation,
			final String expression, final Map<String, String> variables) {
		switch (operation) {
			case ContextTransformerConfigReferences.OEPRATION_INCREMENT:
				return String.valueOf(Integer.parseInt(variables.get(expression)) + 1);

			case ContextTransformerConfigReferences.OEPRATION_DECREMENT:
				return String.valueOf(Integer.parseInt(variables.get(expression)) - 1);

			case ContextTransformerConfigReferences.OEPRATION_MVEL:
			default:
				return MVEL.evalToString(expression, variables);
		}
	}

}
