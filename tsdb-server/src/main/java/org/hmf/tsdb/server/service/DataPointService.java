package org.hmf.tsdb.server.service;

import java.util.List;

import org.hmf.tsdb.DataPoint;
import org.hmf.tsdb.DataPointSerial;
import org.hmf.tsdb.server.ql.Exp;
import org.hmf.tsdb.server.ql.SelectorExp;

public interface DataPointService {
	public void acceptDataPointList(List<DataPoint> dps);

	public List<DataPointSerial> queryBySelector(SelectorExp selector) throws Exception;

	public List<DataPointSerial> queryByExp(Exp exp) throws Exception;

	public List<DataPointSerial> queryByExp(String exp) throws Exception;

	public void acceptDataPoint(DataPoint dp);
}
