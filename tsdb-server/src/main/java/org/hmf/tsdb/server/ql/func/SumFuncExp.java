package org.hmf.tsdb.server.ql.func;

import java.util.ArrayList;
import java.util.List;

import org.hmf.tsdb.DataPointSerial;
import org.hmf.tsdb.server.ql.Exp;
import org.hmf.tsdb.server.service.DataPointService;

/**
 * 一个或多个exp产生的多个 时序数据集合，按时间点 求和形成新的  时序数据集合,如果存在groupBy，则先分组，再求和
 */
public class SumFuncExp extends FuncOnCollection{

	@Override
	public List<DataPointSerial> execSelf(DataPointService service) {
		return super.calcOnChildrenResult();
	}
	
	@Override
	protected List<DataPointSerial> calcBySelfAlgorithm(List<DataPointSerial> list) {
		Exp root = this.findRoot();
		DataPointSerial dps = new DataPointSerial();
		dps.setMetric("sum");
		dps.setStart(root.getFrom().getTime());
		if(root.getTo()==null) {
			dps.setEnd(super.getLastTime());
		}else
			dps.setEnd(root.getTo().getTime());
		
		Number v = getFirstValue(list);
		if(v==null) {
			//do nothing
		}if(v instanceof Integer) {
			this.sumOnIntegerSeries(list, dps);
		}else if(v instanceof Float) {
			this.sumOnFloatSeries(list, dps);
		}else 
			throw new RuntimeException("不可处理的值类型:"+v.getClass().getSimpleName());
		
		List<DataPointSerial> result = new ArrayList<DataPointSerial>();
		result.add(dps);
		return result;		
	}
	
	
	
	private DataPointSerial sumOnFloatSeries(List<DataPointSerial> list,DataPointSerial dps){
		Iterator4TimePointerOnMultiDps iter = super.getIterator(list);
		List<Number[]> points = null;
		while((points=iter.nextTimePointAndValue())!=null) {
			long time =(Long) points.get(0)[0];
			Number value = this.sumFloat(points);
			dps.addDataPoint(time, value);
		}
		return dps;
	}
	
	private DataPointSerial sumOnIntegerSeries(List<DataPointSerial> list,DataPointSerial dps){
		Iterator4TimePointerOnMultiDps iter = super.getIterator(list);
		List<Number[]> points = null;
		while((points=iter.nextTimePointAndValue())!=null) {
			long time =(Long) points.get(0)[0];
			Number value = this.sumInteger(points);
			dps.addDataPoint(time, value);
		}
		return dps;
	}
	
	private Float sumFloat(List<Number[]> points) {
		float r = 0.0f;
		for(Number[] p:points) {
			r += (Float) p[1];
		}
		return r;
	}
	private Integer sumInteger(List<Number[]> points) {
		Integer r = 0;
		for(Number[] p:points) {
			r += (Integer) p[1];
		}
		return r;
	}

	
}
