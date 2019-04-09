package org.hmf.tsdb.server.ql.func;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.hmf.tsdb.DataPointSerial;
import org.hmf.tsdb.server.ql.Exp;

/**
 * 函数表达式
 * @author hexiaodong
 *
 */
public abstract class FuncExp extends Exp {
	protected List<String> by =  new ArrayList<String>();
	
	
	public List<String> getBy() {
		return by;
	}

	public void setBy(List<String> by) {
		this.by = by;
	}
	
	public void addBy(String by) {
		this.by.add(by);
	}
	
	
	
	public List<String> getNearBy(){
		if(this.by!=null && this.by.size()>0) {
			return this.by;
		}else {
			Exp parent = this.getParent();
			if(parent instanceof FuncExp) {
				return ((FuncExp) parent).getNearBy();
			}else
				return null;
		}
	}
	
	/**
	 * 获取分组后的参数,第一个参数是在一个map中，第二个参数也在一个map中，后续运算时，key相同的参与运算。
	 * @return
	 */
	protected List<Map<String,List<DataPointSerial>>> getGroupedParameters(){
		List<Map<String,List<DataPointSerial>>> grouped = new ArrayList<Map<String,List<DataPointSerial>>>();
		List<Exp> children = this.children;
		String by = this.getNearBy().get(0);
		for(Exp exp:children) {
			List<DataPointSerial> result = exp.getResult();
			Map<String,List<DataPointSerial>> mapped = groupBy(by,result);
			grouped.add(mapped);
		}
		return grouped;
	}
	
	
	protected Map<String,List<DataPointSerial>> mergeParameters(List<Map<String,List<DataPointSerial>>> parameters) {
		Map<String,List<DataPointSerial>> result = new HashMap<String,List<DataPointSerial>>();
		for(Map<String,List<DataPointSerial>> map:parameters) {
			Set<Entry<String, List<DataPointSerial>>> set = map.entrySet();
			for(Entry<String,List<DataPointSerial>> one:set) {
				List<DataPointSerial> dps = one.getValue();
				List<DataPointSerial> merged = result.get(one.getKey());
				if(merged ==null) {
					merged = new ArrayList<DataPointSerial>();
					result.put(one.getKey(), merged);
				}
				merged.addAll(dps);
			}
		}
		return result;
	}
	
	private Map<String,List<DataPointSerial>> groupBy(String by,List<DataPointSerial> dps){
		Map<String,List<DataPointSerial>> map = new HashMap<String,List<DataPointSerial>>();
		for(DataPointSerial one:dps) {
			String tagValue = one.getTags().get(by);
			List<DataPointSerial> mapped = map.get(tagValue);
			if(mapped==null) {
				mapped = new ArrayList<DataPointSerial>();
				map.put(tagValue, mapped);
			}
			mapped.add(one);
		}
		return map;
	}
	
	/**
	 * 从每个子获取一个时序集合，比如add(p1,p2),则p1是一个时序集合，p2是另一个时序结合
	 * @return
	 */
	protected List<DataPointSerial> getParameterPerChildren(){
		List<DataPointSerial> list = new ArrayList<DataPointSerial>();
		List<Exp> children = this.children;
		int index = 0;
		for(Exp exp:children) {
			List<DataPointSerial> result = exp.getResult();
			if(result!=null ) {
				if(result.size()==0) {
					throw new RuntimeException("第"+index+"个参数没有找到时序数据");
				}else if(result.size()>1) {
					throw new RuntimeException("第"+index+"个参数返回多个时序数据");
				}else {
					list.add(result.get(0));
				}
			}else {
				throw new RuntimeException("第"+index+"个参数没有找到时序数据");
			}
			index++;
		}
		return list;
	}
	
	/**
	 * 把所有子的时序集合作为一个参数，比如avg、max、min等运算的时候
	 * @return
	 */
	protected List<DataPointSerial> getParameters(){
		List<DataPointSerial> list = new ArrayList<DataPointSerial>();
		List<Exp> children = this.children;
		for(Exp exp:children) {
			List<DataPointSerial> result = exp.getResult();
			if(result!=null && result.size()>0) {
				list.addAll(result);
			}
		}
		return list;
	}
	
	protected Number getFirstValue(List<DataPointSerial> list) {
		if(list!=null && list.size()>0) {
			Map<String,Number> col = list.get(0).getValues();	
			if(col!=null && col.size()>0) {
				return col.values().iterator().next();
			}
		}
		return null;
	}
	
	public Map<String,List<DataPointSerial>> groupBy(List<DataPointSerial> dpsList,String by){
		Map<String,List<DataPointSerial>> result = new HashMap<String,List<DataPointSerial>>();
		for(DataPointSerial dps:dpsList) {
			String tagValue = dps.getTags().get(by);
			List<DataPointSerial> list = result.get(tagValue);
			if(list==null) {
				list = new ArrayList<DataPointSerial>();
				result.put(tagValue, list);
			}
			list.add(dps);
		}
		return result;
	}
	
	public Iterator4TimePointerOnMultiDps getIterator(List<DataPointSerial> list) {
		return new Iterator4TimePointerOnMultiDps(list);
	}
	
	/**
	 * 
	 *
	 */
	class Iterator4TimePointerOnMultiDps {
		private long firstTime=Long.MIN_VALUE,lastTime=Long.MAX_VALUE;
		private List<List<Number[]>> list = null;
		private long preTime = Long.MIN_VALUE;
		private int[] indexes = null;
		public Iterator4TimePointerOnMultiDps(List<DataPointSerial> list) {
			indexes = new int[list.size()];
			this.list = new ArrayList<List<Number[]>>();
			for(DataPointSerial dps:list) {
				this.list.add(transform(dps.getValues()));
			}
			int i = 0;
			for(List<Number[]> oneSeries:this.list) {
				Long oneFirstTime = (Long)oneSeries.get(0)[0];
				if(oneFirstTime>firstTime) {
					firstTime = oneFirstTime;
				}
				
				Long oneLastTime = (Long)oneSeries.get(oneSeries.size()-1)[0];
				if(oneLastTime<lastTime) {
					lastTime = oneLastTime;
				}
				indexes[i++]=0;
			}
		}
		private List<Number[]> transform(Map<String,Number> serial){
			List<Number[]> list = null;
			
			if( serial!=null && serial.size()>0) {
				list = new ArrayList<Number[]>(); 
				Set<Entry<String, Number>> set = serial.entrySet();
				for(Entry<String,Number> e:set) {
					Number[] one = new Number[2];
					one[0] = Long.valueOf(e.getKey());
					one[1] = e.getValue();
					list.add(one);
				}
			}
			return list;
		}
		
		//先找到最接近前一个时间点的时间，然后所有序列计算在这个时间的值
		public List<Number[]> nextTimePointAndValue(){
			List<Number[]> result = new ArrayList<Number[]>();
			long time = 0;
			if(preTime == Long.MIN_VALUE) {
				time = firstTime;				
			}else if(preTime>=lastTime){
				return null;
			}else {
				time = this.nearTime(preTime);
			}
			for(int i=0;i<this.list.size();i++) {
				List<Number[]> series = this.list.get(i);
				Number[] r = this.moveIndexToCurrent(series, indexes[i], time);
				if(r==null)
					return null;
				Number[] p = new Number[2];
				indexes[i] =(Integer) r[0]+1;
				p[0] = r[1];
				p[1] = r[2];
				result.add(p);
			}
			
			preTime = time;
			return result;
		}
		//从所有序列中，找到最接近time的时间值
		private Long nearTime(long time) {
			long min = Long.MAX_VALUE;
			for(int i=0;i<list.size();i++) {
				List<Number[]> series = list.get(i);
				if(indexes[i]>=series.size())
					return null;
				Number[] point = series.get(indexes[i]);
				Long cTime =(Long) point[0];
				if(cTime<min) {
					min = cTime;
				}
			}
			return min;
		}
		
		private Number[] moveIndexToCurrent(List<Number[]> series,int index ,long time) {
			Number[] indexTimeAndValue = new Number[3];
			for(;;) {
				if(index>=series.size()) {
					return null;
				}
				Number[] p = series.get(index);
				Long cTime = (Long)p[0];
				if(cTime>lastTime)
					return null;
				if(cTime>time) {
					indexTimeAndValue[0] = index-1;
					Number[] prePoint = series.get(index-1);
					Number cValue = calcInterValue((Long)prePoint[0],time,cTime,prePoint[1],p[1]);
					indexTimeAndValue[1] = time;
					indexTimeAndValue[2] = cValue;
					return indexTimeAndValue;
				}else if(cTime==time) {
					indexTimeAndValue[0] = index;
					indexTimeAndValue[1] = cTime;
					indexTimeAndValue[2] = p[1];
					return indexTimeAndValue;
				}else if(cTime<time) {
					index++;
				}
			}
		}
		private Number calcInterValue(long preTime,long interTime,long nowTime,Number preValue,Number nowValue) {
			float nowFloat = (Float)nowValue;
			float preFloat = (Float)preValue;
			return preFloat + (interTime-preTime)*(nowFloat-preFloat)/(nowTime-preTime);
		}
	}
	
	

	public static FuncExp build(String func) {
		FuncExp exp = null;
		switch(func){
		case "join":
			exp = new JoinFuncExp();
			break;
		case "avg":
			exp = new AvgFuncExp();
			break;
		case "max":
			exp = new MaxFuncExp();
			break;
		case "min":
			exp = new MinFuncExp();
			break;
		case "add":
			exp = new AddFuncExp();
			break;
		case "sum":
			exp = new SumFuncExp();
			break;
		case "sub":
			exp = new SubFuncExp();
			break;
		case "mul":
			exp = new MulFuncExp();
			break;
		case "div":
			exp = new DivFuncExp();
			break;
		}
		return exp;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName()+"[from=" + from + ", to=" + to + ", children=" + children + ", by=" + by
				+ ", ds="+ ds +"]";
	}

	
	
}
