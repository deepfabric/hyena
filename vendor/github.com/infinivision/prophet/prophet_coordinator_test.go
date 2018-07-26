package prophet

import (
	"testing"
)

type testScheduler struct{}

func (s *testScheduler) Name() string                  { return "test-scheduler" }
func (s *testScheduler) ResourceKind() ResourceKind    { return LeaderKind }
func (s *testScheduler) ResourceLimit() uint64         { return 0 }
func (s *testScheduler) Prepare(rt *Runtime) error     { return nil }
func (s *testScheduler) Cleanup(rt *Runtime)           {}
func (s *testScheduler) Schedule(rt *Runtime) Operator { return nil }

type testOperator struct {
	id   uint64
	kind ResourceKind
}

func newTestOperator(id uint64, kind ResourceKind) Operator {
	return &testOperator{
		id:   id,
		kind: kind,
	}
}

func (op *testOperator) ResourceID() uint64                                       { return op.id }
func (op *testOperator) ResourceKind() ResourceKind                               { return op.kind }
func (op *testOperator) Do(target *ResourceRuntime) (*resourceHeartbeatRsp, bool) { return nil, false }

func TestAddScheduler(t *testing.T) {
	cfg := &Cfg{}
	cfg.adujst()

	runner := NewRunner()
	sch := &testScheduler{}
	c := newCoordinator(cfg, runner, newRuntime(nil))
	c.addScheduler(sch)

	if _, ok := c.schedulers[sch.Name()]; !ok {
		t.Error("add scheduler failed")
	}
}

func TestAddAndGetOperator(t *testing.T) {
	cfg := &Cfg{}
	cfg.adujst()

	runner := NewRunner()
	c := newCoordinator(cfg, runner, newRuntime(nil))
	if !c.addOperator(newTestOperator(1, LeaderKind)) {
		t.Errorf("add operator failed")
	}

	if nil == c.getOperator(1) {
		t.Errorf("get operator failed")
	}
}

func TestRemoveOperator(t *testing.T) {
	cfg := &Cfg{}
	cfg.adujst()

	runner := NewRunner()
	c := newCoordinator(cfg, runner, newRuntime(nil))
	c.addOperator(newTestOperator(1, LeaderKind))
	c.removeOperator(c.getOperator(1))
	if nil != c.getOperator(1) {
		t.Errorf("remove operator failed")
	}
}
