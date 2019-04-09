package org.hmf.tsdb.server.dao.impl;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.hmf.tsdb.server.dao.BaseDAO;
import org.hmf.tsdb.server.entity.Entity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.Updates;

public abstract class BaseDAOImpl <T extends Entity<E>, E> implements BaseDAO<T,E> {
	private Class<T> entityClass;  
	private String collectionName;
	@Autowired
	protected MongoHelper mongo = null;
	
	
	public MongoHelper getMongo() {
		return mongo;
	}

	public void setMongo(MongoHelper mongo) {
		this.mongo = mongo;
	}

	public BaseDAOImpl(){  
		Type genType = getClass().getGenericSuperclass();  
        Type[] params = ((ParameterizedType) genType).getActualTypeArguments();  
        this.entityClass = (Class) params[0]; 
        Repository r = this.getClass().getAnnotation(Repository.class);
        if(r!=null ) {
        	this.collectionName = r.value();
        }else
        	this.collectionName = this.entityClass.getSimpleName();
        
	}
	
	protected abstract T readEntityFromDocument(Document doc);
	
	@Override
	public T create(T t)throws Exception{
		Document document = t.toDocument();
		if(document!=null){
			mongo.getCollection(collectionName).insertOne(document);
			t.setId((E)document.get("_id"));
			return t;
		}else 
			return null;
	}
	
	public void create(List<T> list)throws Exception {
		List<InsertOneModel<Document>> models = new ArrayList<InsertOneModel<Document>>();
		for(T t:list) {
			InsertOneModel<Document> one = new InsertOneModel<Document>(t.toDocument());
			models.add(one);
		}
		mongo.getCollection(collectionName).bulkWrite(models);
	}
	
	public MongoCollection<Document> getCollection() {
		return mongo.getCollection(collectionName);
	}
	
	@Override
	public int next(String seq) {
		MongoCollection<Document> coll = mongo.getCollection("seq");
		int v = 1;
		Document doc = coll.findOneAndUpdate(Filters.eq("_id",seq), Updates.inc("value", 1));
		if(doc==null) {
			doc = new Document();
			doc.append("_id", seq);
			doc.append("value", 1);
			coll.insertOne(doc);
		}else {
			v = doc.getInteger("value")+1;
		}
		return v;
	}
	
	
	@Override
	public boolean exists(E id) {
		long count = this.getCollection().count(Filters.eq("_id",id));
		return count>0;
	}
	
	@Override
	public T get(E id) {
		Document doc = mongo.getCollection(collectionName).find(Filters.eq("_id",id)).first();
		if(doc==null)
			return null;
		return this.readEntityFromDocument(doc);		
	}
	
	private BasicDBObject createUpdate(T t,String...properties){
		BasicDBObject ps = new BasicDBObject();  
		for(String pName:properties){
			ps.put(pName, t.getProperty(pName));
		}
		BasicDBObject update = new BasicDBObject(); 
		update.put("$set", ps);
		return update;
	}
	
	private BasicDBObject createUpdate(Map<String,Object> properties){
		BasicDBObject ps = new BasicDBObject();
		Set<Entry<String,Object>> set = properties.entrySet();
		for(Entry<String,Object> e :set){
			ps.put(e.getKey(),e.getValue());
		}
		BasicDBObject update = new BasicDBObject(); 
		update.put("$set", ps);
		return update;
	}
	
	public T update(T t, String... properties) {		
		mongo.getCollection(collectionName).updateOne(
				Filters.eq("_id", t.getId()), this.createUpdate(t, properties));
		return t;
	}
	
	public void update(Bson query, Map<String, Object> properties) {
		mongo.getCollection(collectionName).updateMany(query, this.createUpdate(properties));		
	}

	public T remove(T t) {
		mongo.getCollection(collectionName).deleteOne(Filters.eq("_id",t.getId()));
		return t;
	}
	
	public void remove(Object id) {
		MongoCollection<Document> coll = mongo.getCollection(collectionName);
		coll.deleteOne(Filters.eq("_id", id));
	}
	
	public List<T> listAll(){
		List<T> result = new ArrayList<T>();
		Iterator<Document> iter = mongo.getCollection(collectionName).find().iterator();		
		while(iter.hasNext()){
			Document doc = iter.next();
			T t = this.readEntityFromDocument(doc);
			result.add(t);
		}
		return result;
	}
	@Override
	public List<T> listByIds(List<E> ids){
		Bson query = Filters.in("_id", ids);
		return this.findList(query);
	}

	public List<T> findList(Bson query) {
		List<T> result = new ArrayList<T>();
		Iterator<Document> iter = mongo.getCollection(collectionName).find(query).iterator();
		
		while(iter.hasNext()){
			Document doc = iter.next();
			T t = this.readEntityFromDocument(doc);
			result.add(t);
		}
		return result;
	}
	
	public List<T> findList(Bson query,Bson projection) {
		List<T> result = new ArrayList<T>();
		Iterator<Document> iter = mongo.getCollection(collectionName).find(query).projection(projection).iterator();		
		while(iter.hasNext()){
			Document doc = iter.next();
			T t = this.readEntityFromDocument(doc);
			result.add(t);
		}
		return result;
	}
	
	
	
	@Override
	public List<T> findList(Bson query, Bson projection, Bson sort) {
		List<T> result = new ArrayList<T>();
		Iterator<Document> iter = mongo.getCollection(collectionName).find(query).projection(projection).sort(sort).iterator();		
		while(iter.hasNext()){
			Document doc = iter.next();
			T t = this.readEntityFromDocument(doc);
			result.add(t);
		}
		return result;
	}

	@Override
	public List<T> findOrderList(Bson query, Bson sort) {
		List<T> result = new ArrayList<T>();
		Iterator<Document> iter = mongo.getCollection(collectionName).find(query).sort(sort).iterator();		
		while(iter.hasNext()){
			Document doc = iter.next();
			T t = this.readEntityFromDocument(doc);
			result.add(t);
		}
		return result;
	}

	public T findOne(Bson query) {
		FindIterable<Document> finder = mongo.getCollection(collectionName).find(query).limit(1);
		Document doc = finder.first();
		if(doc==null)
			return null;
		return this.readEntityFromDocument(doc);
	}
	
	public long count() {
		return mongo.getCollection(collectionName).count();
	}
	
	
	
	@Override
	public long count(Bson query) {
		return mongo.getCollection(collectionName).count(query);
	}

	public T findOne(Bson query,Bson sort) {
		FindIterable<Document> finder = mongo.getCollection(collectionName)
				.find(query).sort(sort).limit(1);
		Document doc = finder.first();
		if(doc==null)
			return null;
		return this.readEntityFromDocument(doc);
	}
	
	public T findOne(Bson query,Bson sort,Bson projection) {
		FindIterable<Document> finder = mongo.getCollection(collectionName)
				.find(query).sort(sort).projection(projection).limit(1);
		Document doc = finder.first();
		if(doc==null)
			return null;
		return this.readEntityFromDocument(doc);
	}

	public T unique(Bson query) {
		FindIterable<Document> finder = mongo.getCollection(collectionName).find(query);
		Iterator<Document> iter = finder.iterator();
		if(iter.hasNext()){
			Document first = iter.next();
			if(iter.hasNext())
				throw new RuntimeException("more than one documents are found");
			else{
				return this.readEntityFromDocument(first);
			}
		}
		return null;
	}
	
	
	public List<T> nextPage(Bson query, Bson sort, int size, Bson startFrom) {	
		Bson q = null;
		if(query==null || startFrom==null){
			if(query!=null)
				q = query;
			else
				q = startFrom;
		}
		if(q==null){
			q = Filters.and(query,startFrom);
		}
		FindIterable<Document> finder;
		if(sort==null)
			finder = mongo.getCollection(collectionName).find(q).limit(size);
		else
			finder = mongo.getCollection(collectionName).find(q).sort(sort).limit(size);
		 
		Iterator<Document> iter = finder.iterator();
		List<T> list = new ArrayList<T>();
		while(iter.hasNext()){
			Document doc = iter.next();
			T t = this.readEntityFromDocument(doc);
			list.add(t);
		}
		return list;
	}

	public void removeAll() {
		this.getCollection().deleteMany(new BasicDBObject());
	}
	
	public void remove(Bson query) {
		this.getCollection().deleteMany(query);
	}
}
