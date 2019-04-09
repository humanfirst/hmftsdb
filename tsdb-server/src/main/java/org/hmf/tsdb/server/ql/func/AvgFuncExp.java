package org.hmf.tsdb.server.ql.func;

import java.util.ArrayList;
import java.util.List;

import org.hmf.tsdb.DataPointSerial;
import org.hmf.tsdb.server.ql.Exp;
import org.hmf.tsdb.server.service.DataPointService;

public class AvgFuncExp extends FuncOnCollection {

	@Override
	public List<DataPointSerial> execSelf(DataPointService service) {
		return super.calcOnChildrenResult();
	}
	
	@Override
	protected List<DataPointSerial> calcBySelfAlgorithm(List<DataPointSerial> list) {
		Exp root = this.findRoot();
		DataPointSerial dps = new DataPointSerial();
		dps.setMetric("avg");
		dps.setStart(root.getFrom().getTime());
		if(root.getTo()==null) {
			dps.setEnd(super.getLastTime());
		}else
			dps.setEnd(root.getTo().getTime());
		Number v = getFirstValue(list);
		if(v==null) {
			//do nothing
		}if(v instanceof Integer) {
			this.avgOnIntegerSeries(list, dps);
		}else if(v instanceof Float) {
			this.avgOnFloatSeries(list, dps);
		}else 
			throw new RuntimeException("不可处理的值类型:"+v.getClass().getSimpleName());
		
		List<DataPointSerial> result = new ArrayList<DataPointSerial>();
		result.add(dps);
		return result;		
	}
	
	private DataPointSerial avgOnFloatSeries(List<DataPointSerial> list,DataPointSerial dps){
		Iterator4TimePointerOnMultiDps iter = super.getIterator(list);
		List<Number[]> points = null;
		while((points=iter.nextTimePointAndValue())!=null) {
			long time =(Long) points.get(0)[0];
			Number value = this.avgFloat(points);
			dps.addDataPoint(time, value);
		}
		return dps;
	}
	
	private DataPointSerial avgOnIntegerSeries(List<DataPointSerial> list,DataPointSerial dps){
		Iterator4TimePointerOnMultiDps iter = super.getIterator(list);
		List<Number[]> points = null;
		while((points=iter.nextTimePointAndValue())!=null) {
			long time =(Long) points.get(0)[0];
			Number value = this.avgInteger(points);
			dps.addDataPoint(time, value);
		}
		return dps;
	}
	
	private Float avgFloat(List<Number[]> points) {
		float r = 0.0f;
		for(Number[] p:points) {
			r += (Float) p[1];
		}
		return r/points.size();
	}
	
	private Float avgInteger(List<Number[]> points) {
		Integer r = 0;
		for(Number[] p:points) {
			r += (Integer) p[1];
		}
		return (1.0f*r)/points.size();
	}
}
