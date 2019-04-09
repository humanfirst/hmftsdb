package org.hmf.tsdb;

import java.util.TreeMap;

/**
 * 测点/指标
 */
public class Metric {
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
	
	public void setId(Integer id) {
		this.id = id;
	}

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
	
	
	@Override
	public String toString() {
		return "Metric [id=" + id + ", name=" + name + ", tags=" + tags + ", type=" + type + "]";
	}
	
}
