package org.hmf.tsdb.server.entity;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.bson.Document;

/**
 * 测点/指标
 */
public class Metric extends Entity<Integer>{
	public enum ValueType{
		intValue,
		floatValue;
	}
	/**
	 * 主键
	 */
	private Integer id;
	/**
	 * 测点名
	 */
	private String name;
	/**
	 * 测点标签
	 */
	private TreeMap<String,String> tags;
	/**
	 * 值类型
	 */
	private ValueType type;
	
	@Override
	public void setId(Integer id) {
		this.id = id;
	}
	@Override
	public Integer getId() {
		return id;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public ValueType getType() {
		return type;
	}
	public void setType(ValueType type) {
		this.type = type;
	}

	
	public String getTag(String name) {
		if(tags==null)
			return null;
		else 
			return tags.get(name);
	}
		
	public TreeMap<String, String> getTags() {
		return tags;
	}
	

	
	
	public void setTags(TreeMap<String, String> tags) {
		this.tags = tags;
	}
	public Document toDocument() {
		Document doc = new Document();
		doc.append("_id", id);
		doc.append("name", name);
		doc.append("type",type.ordinal());
		if(tags!=null && tags.size()>0) {			
			doc.append("tags", tags);
		}
		return doc;
	}
	
	private String key ;
	public String getKey() {
		if(key==null) {
			StringBuffer sb = new StringBuffer();
			sb.append(this.name);
			if(this.tags!=null && this.tags.size()>0) {
				sb.append("@");
				Iterator<Entry<String,String>> iter = tags.entrySet().iterator();
				while(iter.hasNext()) {
					Entry<String,String> tag = iter.next();
					sb.append(tag.getKey()).append(":").append(tag.getValue()).append(";");
				}
			}
			key = sb.toString();
		}
		return key;
	}
	
	@Override
	public int hashCode() {
		return this.id.hashCode();
	}
	@Override
	public boolean equals(Object obj) {
		return this.id.equals(((Metric)obj).getId());
	}
	
	@Override
	public String toString() {
		return "Metric [id=" + id + ", name=" + name + ", tags=" + tags + ", type=" + type + "]";
	}
	
}
