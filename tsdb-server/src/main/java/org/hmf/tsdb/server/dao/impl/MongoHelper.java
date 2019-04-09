package org.hmf.tsdb.server.dao.impl;

import javax.annotation.PostConstruct;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

@Component
public class MongoHelper {
	private MongoClient client = null;
	
	@Value("${mongo.connStr}")
	private String connStr;
	
	@Value("${mongo.dbName}")
	private String dbName;
	
	private MongoDatabase database = null;
	

	public String getConnStr() {
		return connStr;
	}

	public void setConnStr(String connStr) {
		this.connStr = connStr;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	@PostConstruct
	public void init() {
		this.getClient();
	}
	
	public MongoClient getClient(){
		if(client==null){
			MongoClientURI uri = new MongoClientURI(connStr); 
			client = new MongoClient(uri);
		}
		return client;
	}
	
	public MongoDatabase getDatabase(){
		if(database==null){
			MongoClient c = this.getClient();
			database = c.getDatabase(dbName);
		}
		return database;
	}
	
	public MongoCollection<Document> getCollection(String collectionName){
		return this.getDatabase().getCollection(collectionName);
	}
	
}
