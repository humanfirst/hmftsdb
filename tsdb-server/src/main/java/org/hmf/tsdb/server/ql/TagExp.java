package org.hmf.tsdb.server.ql;

import java.util.ArrayList;
import java.util.List;

public class TagExp {
	private String name;
	private String symbol;
	private List<String> value = new ArrayList<String>();
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getSymbol() {
		return symbol;
	}
	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	
	public List<String> getValue() {
		return value;
	}
	public void setValue(List<String> value) {
		this.value = value;
	}
	public void addValue(String value) {
		this.value.add(value);
	}
	@Override
	public String toString() {
		return "TagExp [name=" + name + ", symbol=" + symbol + ", value=" + value + "]";
	}
	
	
}
