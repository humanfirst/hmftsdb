package org.hmf.tsdb.server.ql.func;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.hmf.tsdb.DataPointSerial;

public abstract class FuncOnCollection extends FuncExp {

	/**
	 * 基于集合（把所有子运算的结果合并到一个集合中，可能有groupBy）做计算
	 */
	protected List<DataPointSerial> calcOnChildrenResult() {
		List<String> by = this.getNearBy();
		if(by!=null && by.size()>0) {
			List<Map<String,List<DataPointSerial>>> parameters = this.getGroupedParameters();
			Map<String,List<DataPointSerial>> map = mergeParameters(parameters);
			Set<Entry<String, List<DataPointSerial>>> set = map.entrySet();
			List<DataPointSerial> result = new ArrayList<DataPointSerial>();
			for(Entry<String,List<DataPointSerial>> entry:set) {
				String key = entry.getKey();
				List<DataPointSerial> list = entry.getValue();
				List<DataPointSerial> oneResult = this.calcBySelfAlgorithm(list);
				if(result!=null) {
					DataPointSerial one = oneResult.get(0);
					one.setMetric(key);
					result.add(one);
				}
			}
			return result;
		}else {
			List<DataPointSerial> parameters = this.getParameters();
			return this.calcBySelfAlgorithm(parameters);
		}
	}
	
	protected abstract List<DataPointSerial> calcBySelfAlgorithm(List<DataPointSerial> list);

}
