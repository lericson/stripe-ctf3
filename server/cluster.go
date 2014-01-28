package server

import (
	"errors"
	"fmt"
	"stripe-ctf.com/sqlcluster/log"
)

type ServerAddress struct {
	Name             string `json:"name"`
	ConnectionString string `json:"address"`
}

func (sa ServerAddress) String() string {
	return fmt.Sprintf("Server<%s>", sa.Name)
}

type Cluster struct {
	self    ServerAddress
	leader  ServerAddress
	members []ServerAddress
}

func NewCluster(name, connectionString string) *Cluster {
	c := &Cluster{}
	c.self = ServerAddress{
		Name:             name,
		ConnectionString: connectionString,
	}
	return c
}

func (c *Cluster) Init() {
	log.Printf("Initializing cluster and promoting self to leader")
	c.leader = c.self
	c.members = make([]ServerAddress, 0)
}

func (c *Cluster) Join(leader ServerAddress, members []ServerAddress) {
	log.Printf("Joining existing cluster: leader %v, members %v", leader, members)
	c.leader = leader
	c.members = members
}

func (c *Cluster) AddMember(identity ServerAddress) error {
	state := c.State()
	if state != "leader" {
		return errors.New("Can only join to a leader, but you're talking to a " + state)
	}

	log.Printf("Adding new cluster member %v", identity)
	c.members = append(c.members, identity)

	return nil
}

func (c *Cluster) State() string {
	switch true {
	case c.leader.Name == c.self.Name:
		return "leader"
	case c.leader.Name == "":
		return "candidate"
	default:
		return "follower"
	}
}

func (c *Cluster) LeaderDead() {
	log.Printf("The leader is dead. Long live the leader!")
	c.leader.Name = ""
	c.leader.ConnectionString = ""
}
