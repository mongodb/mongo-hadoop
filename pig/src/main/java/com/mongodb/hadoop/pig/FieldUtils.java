package com.mongodb.hadoop.pig;

public class FieldUtils {
    // Escape name that starts with esc_
    private static final String ESC_PREFIX = "esc_";
    
    public static String getEscFieldName(String fieldName){
    	if (fieldName.startsWith(ESC_PREFIX)) {
    		fieldName = fieldName.replace(ESC_PREFIX, "");
    	}
    	return fieldName;
    }
}
