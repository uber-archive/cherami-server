package replicator

import (
	"sync/atomic"

	"github.com/uber/cherami-server/common"
	"github.com/uber/cherami-thrift/.generated/go/admin"
	"github.com/uber/tchannel-go/thrift"
)

// DumpConnectionStatus implements the admin API
func (r *Replicator) DumpConnectionStatus(ctx thrift.Context) (*admin.ReplicatorConnectionStatus, error) {
	connStatus := admin.NewReplicatorConnectionStatus()

	r.remoteReplicatorConnMutex.Lock()
	defer r.remoteReplicatorConnMutex.Unlock()
	for extent, conn := range r.remoteReplicatorConn {
		connStatus.RemoteReplicatorConn = append(connStatus.RemoteReplicatorConn, &admin.ReplicatorConnection{
			ExtentUUID:            common.StringPtr(extent),
			StartTime:             common.Int64Ptr(conn.startTime),
			TotalMsgReplicated:    common.Int32Ptr(atomic.LoadInt32(&conn.totalMsgReplicated)),
			LastMsgReplicatedTime: common.Int64Ptr(atomic.LoadInt64(&conn.lastMsgReplicatedTime)),
		})
	}

	r.storehostConnMutex.Lock()
	defer r.storehostConnMutex.Unlock()
	for extent, conn := range r.storehostConn {
		connStatus.StorehostConn = append(connStatus.StorehostConn, &admin.ReplicatorConnection{
			ExtentUUID:            common.StringPtr(extent),
			StartTime:             common.Int64Ptr(conn.startTime),
			TotalMsgReplicated:    common.Int32Ptr(atomic.LoadInt32(&conn.totalMsgReplicated)),
			LastMsgReplicatedTime: common.Int64Ptr(atomic.LoadInt64(&conn.lastMsgReplicatedTime)),
		})
	}

	return connStatus, nil
}
