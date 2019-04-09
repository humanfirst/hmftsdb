package org.hmf.tsdb.server.ql.func;

import java.util.ArrayList;
import java.util.List;

import org.hmf.tsdb.DataPointSerial;
import org.hmf.tsdb.server.ql.Exp;
import org.hmf.tsdb.server.service.DataPointService;

public class SubFuncExp extends FuncOnDual{

	@Override
	public List<DataPointSerial> execSelf(DataPointService service) throws Exception{
		return super.calcOnDualParameters();
	}

	private Float subFloat(Number n1,Number n2) {
		return (Float)n1-(Float)n2;
	}
	private Integer subInteger(Number n1,Number n2) {
		return (Integer)n1-(Integer)n2;
	}

	@Override
	protected List<DataPointSerial> calcBySelfAlgorithm(List<DataPointSerial> list) {
		Exp root = this.findRoot();
		DataPointSerial dps = new DataPointSerial();
		dps.setMetric("sub");
		dps.setStart(root.getFrom().getTime());
		if(root.getTo()==null) {
			dps.setEnd(super.getLastTime());
		}else
			dps.setEnd(root.getTo().getTime());
		Number v = super.getFirstValue(list);
		if(v instanceof Integer) {
			this.subIntegerSeries(list,dps);
		}else if(v instanceof Float) {
			this.subFloatSeries(list,dps);
		}else {
			throw new RuntimeException("不可处理的数值类型:"+v.getClass().getSimpleName());
		}
		List<DataPointSerial> result = new ArrayList<DataPointSerial>();
		result.add(dps);
		return result;
	}
	
	private DataPointSerial subIntegerSeries(List<DataPointSerial> list,DataPointSerial dps) {
		Iterator4TimePointerOnMultiDps iter = super.getIterator(list);
		List<Number[]> points = null;
		while((points=iter.nextTimePointAndValue())!=null) {
			long time =(Long) points.get(0)[0];
			Number value = this.subInteger(points.get(0)[1], points.get(1)[1]);
			dps.addDataPoint(time, value);
		}
		return dps;
	}
	
	private DataPointSerial subFloatSeries(List<DataPointSerial> list,DataPointSerial dps) {
		Iterator4TimePointerOnMultiDps iter = super.getIterator(list);
		List<Number[]> points = null;
		while((points=iter.nextTimePointAndValue())!=null) {
			long time =(Long) points.get(0)[0];
			Number value = this.subFloat(points.get(0)[1], points.get(1)[1]);
			dps.addDataPoint(time, value);
		}
		return dps;
	}
}
