package org.hmf.tsdb.server.dao.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.annotation.PostConstruct;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.hmf.tsdb.server.dao.SnapshotDAO;
import org.hmf.tsdb.server.entity.Snapshot;
import org.hmf.tsdb.server.entity.Snapshot.Type;
import org.springframework.stereotype.Repository;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

/**
 *
 */
@Repository(value="snapshot")
public class SnapshotDAOImpl extends BaseDAOImpl<Snapshot,ObjectId> implements SnapshotDAO {
	@PostConstruct
	public void init() {
		long count = super.getCollection().count();
		if(count==0L) {
			super.getCollection().dropIndexes();
			BasicDBObject indexes = new BasicDBObject("groupIdx",1);
			indexes.append("_id", 1);
			super.getCollection().createIndex(indexes);
			
			indexes = new BasicDBObject("parent",1);
			indexes.append("index", 1);
			super.getCollection().createIndex(indexes);
		}
	}

	@Override
	public void deleteTimeoutSnapshots(int groupIdx, ObjectId lastFullSnapshotId) {
		super.remove(Filters.and(Filters.eq("groupIdx",groupIdx),Filters.lt("_id", lastFullSnapshotId)));		
		
	}
	
	@Override
	public List<ObjectId> findIdsBefore(int groupIdx, ObjectId fullSnapshotId,int batchSize) {
		List<ObjectId> result = new ArrayList<ObjectId>();
		Bson filter = Filters.and(Filters.eq("groupIdx",groupIdx),Filters.lt("_id", fullSnapshotId));
		BasicDBObject projection = new BasicDBObject("_id",1);
	
		MongoCursor<Document> cursor = super.getCollection().find(filter).projection(projection)
				.sort(Sorts.ascending("_id")).limit(batchSize).iterator();
		while(cursor.hasNext()) {
			Document doc = cursor.next();
			result.add(doc.getObjectId("_id"));
		}
		return result;
	}
	@Override
	public void delete(List<ObjectId> ids) {
		Bson filter = Filters.in("_id", ids);
		super.remove(filter);
	}

	@Override
	public List<Snapshot> findLastFullSnapshot(int groupIdx) {
		Bson filters = Filters.and(Filters.eq("groupIdx",groupIdx),Filters.eq("type",Snapshot.Type.full.toString()));
		Snapshot snapshot= super.findOne(filters, Sorts.descending("_id"));
		if(snapshot!=null) {
			List<Snapshot> ss = new ArrayList<Snapshot>();
			ss.add(snapshot);
			List<Snapshot> parts = super.findOrderList(Filters.eq("parent",snapshot.getId()), Sorts.ascending("_id"));
			ss.addAll(parts);
			return ss;
		}else
			return null;
	}

	@Override
	public List<Snapshot> findAdditionSnapshots(int groupIdx, ObjectId fullSnapshotId, int batchSize) {		
		Bson filters =  Filters.and(Filters.eq("groupIdx",groupIdx),Filters.gt("_id", fullSnapshotId));       
		List<Snapshot> list = super.nextPage(filters, Sorts.ascending("_id"), batchSize, null);
		return list;
	}

	@Override
	public List<Snapshot> nextPageSnapshots(int groupIdx, ObjectId after, int size) {
		Bson startFrom = Filters.gt("_id", after);
		List<Snapshot> list = super.nextPage(Filters.eq("groupIdx",groupIdx), Sorts.ascending("_id"), size, startFrom);
		return list;
	}

	@Override
	public boolean existFullSnapshot(int groupIdx) {
		Bson filters = Filters.and(Filters.eq("groupIdx",groupIdx),Filters.eq("type",Type.full.toString()));
		long count =  super.count(filters);		
		return count>0;
	}

	@Override
	public List<Integer> findAllGroupIdx() {
		MongoCursor<Integer> iter = this.getCollection().distinct("groupIdx", Integer.class).iterator();
		List<Integer> result = new ArrayList<Integer>();
		while(iter.hasNext()) {
			Integer s = iter.next();
			result.add(s);
		}
		return result;
	}

	@Override
	public void clearSnapshotsOfShards(List<String> offlineNodeIds) {
		this.getCollection().deleteMany(Filters.in("nodeId", offlineNodeIds));
	}

	@Override
	protected Snapshot readEntityFromDocument(Document doc) {
		ObjectId id = doc.getObjectId("_id");
		Date time = doc.getDate("time");
		int groupIdx = doc.getInteger("groupIdx");
		String typeName = doc.getString("type");
		if(typeName==null) {
			throw new RuntimeException("快照类型不能为空");
		}
		Type type = Type.valueOf(doc.getString("type"));
		Binary data = (Binary) doc.get("data");
		int index  = doc.getInteger("index", 0);
		ObjectId parent = doc.getObjectId("parent");
		Snapshot ss = new Snapshot();
		ss.setId(id);
		ss.setTime(time);
		ss.setType(type);
		ss.setData(data.getData());
		ss.setIndex(index);
		ss.setParent(parent);
		ss.setGroupIdx(groupIdx);
		return ss;
	}

	
	
}
