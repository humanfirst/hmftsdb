package org.hmf.tsdb.server.dao;

import java.util.List;

import org.bson.types.ObjectId;
import org.hmf.tsdb.server.entity.Snapshot;

public interface SnapshotDAO extends BaseDAO<Snapshot,ObjectId> {
	/**
	 * 每次新建全量快照后，删除本组所有之前的快照
	 * @param shard
	 * @param newSnapshotId 新建的全量快照Id
	 */
	public void deleteTimeoutSnapshots(int groupIdx,ObjectId newSnapshotId) ;
	
	/**
	 * 找到某组最近的全量快照
	 * @param selfId 
	 * @return
	 */
	public List<Snapshot> findLastFullSnapshot(int groupIdx);
	
	
	/**
	 * 查找某组的增量快照
	 * @param groupIdx
	 * @param fullSnapshotId
	 * @param batchSize
	 * @return
	 */
	public List<Snapshot> findAdditionSnapshots(int groupIdx, ObjectId fullSnapshotId, int batchSize);
	
	/**
	 * 查找某组下一页增量快照
	 * @param nodeId
	 * @param after
	 * @param size
	 * @return
	 */
	public List<Snapshot> nextPageSnapshots(int groupIdx, ObjectId after, int size);
	
	/**
	 * 某组是否存在增量快照
	 * @param groupIdx
	 * @return
	 */
	public boolean existFullSnapshot(int groupIdx);

	/**
	 * 找到所有的组
	 * @return
	 */
	public List<Integer> findAllGroupIdx();

	/**
	 * 删除不需要的组快照。假如缩减节点，可能减少组，此时需要删除相应的快照
	 * @param offlineNodeIds
	 */
	public void clearSnapshotsOfShards(List<String> offlineNodeIds);

	/**
	 * 获取一批已经过期的快照的id
	 * @param groupIdx
	 * @param fullSnapshotId
	 * @param batchSize
	 * @return
	 */
	public List<ObjectId> findIdsBefore(int groupIdx, ObjectId fullSnapshotId,int batchSize);

	public void delete(List<ObjectId> ids);

	

	
}
