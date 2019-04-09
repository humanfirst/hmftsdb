package org.hmf.tsdb.server.entity;

import org.apache.commons.beanutils.BeanUtils;
import org.bson.Document;

public abstract class Entity<T> {
	
	public abstract Document toDocument()throws Exception;
	
	public abstract  T getId();
	
	public abstract void setId(T t);
	
	public Object getProperty(String propertyName){
		try {
			return BeanUtils.getProperty(this, propertyName);
		} catch (Exception e) {
			throw new RuntimeException("failed to get property value with name="+propertyName);
		}
	}
	
}