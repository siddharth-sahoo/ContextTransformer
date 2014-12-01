package com.awesome.pro.context.transformer;

/**
 * Enumeration of all supported transformations.
 * @author siddharth.s
 */
public enum TransformationOperation {

	/**
	 * Looks up the dimension ID and replaces it with the dimension value.
	 */
	DIMLOOKUP,

	/**
	 * Used in dimensional aggregation, will increment the existing count by 1.
	 */
	INC,

	/**
	 * Adds the specified value to the existing value.
	 */
	ADD,

	/**
	 * Replaces the old value with the new value.
	 */
	REPLACE,

	/**
	 * Count the occurrences uniquely.
	 */
	UNIQUE_COUNT;

	/**
	 * @param operation Operation as a string.
	 * @return Parsed operation into an enumeration type.
	 */
	public static final TransformationOperation parseOperation(final String operation) {
		switch (operation) {
			case OPERATION_DIM_LOOKUP :
				return DIMLOOKUP;
			case OPERATION_INC :
				return INC;
			case OPERATION_ADD :
				return ADD;
			case OPERATION_REPLACE :
				return REPLACE;
			case OPERATION_UNIQUE_COUNT :
				return UNIQUE_COUNT;
			default :
				throw new IllegalArgumentException("Unknown operation: " + operation);
		}
	}

	// Constants.
	private static final String OPERATION_DIM_LOOKUP = "DIMLOOKUP";
	private static final String OPERATION_INC = "INC";
	private static final String OPERATION_ADD = "ADD";
	private static final String OPERATION_REPLACE = "REPLACE";
	private static final String OPERATION_UNIQUE_COUNT = "UNIQUE_COUNT";

}
