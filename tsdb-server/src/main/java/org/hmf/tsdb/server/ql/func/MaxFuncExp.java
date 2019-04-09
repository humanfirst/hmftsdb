package org.hmf.tsdb.server.ql.func;

import java.util.ArrayList;
import java.util.List;

import org.hmf.tsdb.DataPointSerial;
import org.hmf.tsdb.server.ql.Exp;
import org.hmf.tsdb.server.service.DataPointService;

public class MaxFuncExp extends FuncOnCollection{

	@Override
	public List<DataPointSerial> execSelf(DataPointService service) {
		return super.calcOnChildrenResult();
	}
	
	@Override
	protected List<DataPointSerial> calcBySelfAlgorithm(List<DataPointSerial> list) {
		Exp root = this.findRoot();
		DataPointSerial dps = new DataPointSerial();
		dps.setMetric("max");
		dps.setStart(root.getFrom().getTime());
		if(root.getTo()==null) {
			dps.setEnd(super.getLastTime());
		}else
			dps.setEnd(root.getTo().getTime());
		Number v = getFirstValue(list);
		if(v==null) {
			throw new RuntimeException("存在空值!");
		}if(v instanceof Integer) {
			this.maxOnIntegerSeries(list, dps);
		}else if(v instanceof Float) {
			this.maxOnFloatSeries(list, dps);
		}else 
			throw new RuntimeException("不可处理的值类型:"+v.getClass().getSimpleName());
		
		List<DataPointSerial> result = new ArrayList<DataPointSerial>();
		result.add(dps);
		return result;		
	}
	
	
	
	private DataPointSerial maxOnFloatSeries(List<DataPointSerial> list,DataPointSerial dps){
		Iterator4TimePointerOnMultiDps iter = super.getIterator(list);
		List<Number[]> points = null;
		while((points=iter.nextTimePointAndValue())!=null) {
			long time =(Long) points.get(0)[0];
			Number value = this.maxFloat(points);
			dps.addDataPoint(time, value);
		}
		return dps;
	}
	
	private DataPointSerial maxOnIntegerSeries(List<DataPointSerial> list,DataPointSerial dps){
		Iterator4TimePointerOnMultiDps iter = super.getIterator(list);
		List<Number[]> points = null;
		while((points=iter.nextTimePointAndValue())!=null) {
			long time =(Long) points.get(0)[0];
			Number value = this.maxInteger(points);
			dps.addDataPoint(time, value);
		}
		return dps;
	}
	
	private Float maxFloat(List<Number[]> points) {
		float max = 0.0f;
		for(Number[] p:points) {
			Float v = (Float) p[1];
			if( v>max)
				max = v;
		}
		return max;
	}
	private Integer maxInteger(List<Number[]> points) {
		Integer max = 0;
		for(Number[] p:points) {
			Integer v = (Integer)p[1];
			if(v>max)
				max = v;
		}
		return max;
	}

	
}
