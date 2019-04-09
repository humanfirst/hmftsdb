package org.hmf.tsdb.server.dao;


import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.hmf.tsdb.server.entity.Entity;

import com.mongodb.client.MongoCollection;

public interface BaseDAO<T extends Entity<E>,E> {
	public MongoCollection<Document> getCollection();
	public boolean exists(E id);
	public T get(E id);
	
	public T create(T t) throws Exception;
	public void create(List<T> list) throws Exception;
	
	public T update(T t,String... properties);
	public void update(Bson query,Map<String,Object> properties);	
	
	public T remove(T t);
	public void remove(Object id);
	public void remove(Bson query);
	public void removeAll();
	
	public List<T> findList(Bson query);
	public List<T> findList(Bson query,Bson projection);
	public List<T> findList(Bson query,Bson projection,Bson sort);
	public List<T> findOrderList(Bson query,Bson sort);
	public List<T> listAll();
	public List<T> listByIds(List<E> ids);
	
	public T findOne(Bson query);
	public T findOne(Bson query, Bson sort);
	public T findOne(Bson query,Bson sort,Bson projection);	
	public T unique(Bson query);
	public List<T> nextPage(Bson query,Bson sort,int size,Bson startFrom);
	public long count();
	public long count(Bson query);
	public int next(String seq);
}
