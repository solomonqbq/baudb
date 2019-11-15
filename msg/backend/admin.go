//go:generate msgp -io=false -tests=false

package backend

type AdminCmdInfo struct {
}

type AdminCmdJoinCluster struct {
	Addr string
}
