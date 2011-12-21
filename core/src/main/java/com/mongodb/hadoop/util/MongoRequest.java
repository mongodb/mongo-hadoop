package com.mongodb.hadoop.util;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoURI;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;

public class MongoRequest {
	private MongoURI mongoURI;
	private DBObject query;
	private DBObject fields;
	private DBObject sort;
	private int skip;
	private int limit;
	private String inputFormat;
	private String mapper;
	
	public MongoRequest (String uri, String _inputFormat, String _mapper, String _query, String _fields, String _sort, int _skip, int _limit){
		mongoURI = new MongoURI(uri);
		inputFormat = _inputFormat;
		mapper = _mapper;
		query =(DBObject) JSON.parse(_query);
		fields = (DBObject) JSON.parse(_fields);
		sort = (DBObject) JSON.parse(_sort);
		skip = _skip;
		limit = _limit;
	}

	public MongoRequest (DBObject setting){
		this((String) setting.get("uri"), 
				setting.containsField("inputFormat") ? (String) setting.get("inputFormat") : null,
				setting.containsField("mapper") ? (String) setting.get("mapper") : null,
				setting.containsField("query") ? (String) setting.get("query") : null,
				setting.containsField("fields") ? (String) setting.get("fields") : null,
				setting.containsField("sort") ? (String) setting.get("sort") : null,
				setting.containsField("skip") ? (int) Integer.valueOf((String) setting.get("skip")) : 0,
				setting.containsField("limit") ? (int) Integer.valueOf((String) setting.get("limit")) : 0);
	}

	public MongoRequest (String setting) throws JSONParseException {
		this((BasicDBObject) JSON.parse(setting));
	}
	
	public String toString(){
		String mongoRequest = 
				mongoURI.toString() + ";" + inputFormat + ";" + mapper + ";" + query + ";" + fields + ";" + sort + ";" + skip + ";" + limit;
		return mongoRequest;
	}

	public MongoURI getInputURI (){
		return mongoURI;
	}
	public String getInputFormat (){
		return inputFormat;
	}
	public String getMapper (){
		return mapper;
	}
	public DBObject getQuery (){
		return query;
	}
	public DBObject getFields (){
		return fields;
	}
	public DBObject getSort (){
		return sort;
	}
	public int getSkip (){
		return skip;
	}
	public int getLimit (){
		return limit;
	}
	
	public void setInputURI (MongoURI uri){
		mongoURI = uri;
	}
	public void setInputFormat (String _inputFormat){
		inputFormat = _inputFormat;
	}
	public void setMapper (String _mapper){
		mapper = _mapper;
	}
	public void setInputURI (String uri){
		mongoURI = new MongoURI(uri);
	}
	public void setQuery (DBObject _query){
		query = _query;
	}
	public void setFields (DBObject _fields){
		fields = _fields;
	}
	public void setSort (DBObject _sort){
		sort = _sort;
	}
	public void setSkip (int _skip){
		skip = _skip;
	}
	public void setLimit (int _limit){
		limit = _limit;
	}

}
