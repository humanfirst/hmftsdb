package org.hmf.tsdb.server.ql.func;

import java.util.ArrayList;
import java.util.List;

import org.hmf.tsdb.DataPointSerial;

/**
 * 二元计算函数
 */
public abstract class FuncOnDual extends FuncExp{
	/**
	 * 基于二元参数做计算
	 * @return
	 */
	protected List<DataPointSerial> calcOnDualParameters()throws Exception {
		List<DataPointSerial> seriesList = new ArrayList<DataPointSerial>();
		
		if(this.children==null || this.children.size()!=2) {
			throw new Exception("二元运算必须有且仅有2个参数!");
		}else {
			seriesList = super.getParameterPerChildren();
		}
		
		return this.calcBySelfAlgorithm(seriesList);
	}
	
	protected abstract List<DataPointSerial> calcBySelfAlgorithm(List<DataPointSerial> list);

}
