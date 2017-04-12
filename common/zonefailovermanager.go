package common

// ZoneFailoverManager is a daemon that can be used to manage the zone failover behavior.
type (
	ZoneFailoverManager interface {
		Daemon
	}

	dummyZoneFailoverManager struct {
	}
)

// NewDummyZoneFailoverManager creates a dummy zone failover manager
func NewDummyZoneFailoverManager() ZoneFailoverManager {
	return &dummyZoneFailoverManager{}
}

func (d *dummyZoneFailoverManager) Start() {
	return
}

func (d *dummyZoneFailoverManager) Stop() {
	return
}
