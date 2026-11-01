package mode

import "encoding/json"

// Snapshot 返回模式记录（账本检查点）。
func (m *Machine) Snapshot() (json.RawMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return json.Marshal(record{Mode: m.mode, Started: m.started, Expired: m.expired})
}

// Restore 以检查点替换模式记录。
func (m *Machine) Restore(data json.RawMessage) error {
	var r record
	if err := json.Unmarshal(data, &r); err != nil {
		return err
	}
	m.mu.Lock()
	m.mode, m.started, m.expired = r.Mode, r.Started, r.Expired
	m.mu.Unlock()
	return nil
}
