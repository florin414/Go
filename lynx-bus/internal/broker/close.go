package broker

import "time"

// Close releases any resources held by the Producer. For this minimal
// implementation we simply clear the reachable list so future calls to
// IsConnected/Reachable behave as closed. Close is safe to call multiple
// times and on a nil receiver.
func (p *Producer) Close() error {
	if p == nil {
		return nil
	}
	p.reachableMu.Lock()
	p.reachable = nil
	p.lastChecked = time.Time{}
	p.reachableMu.Unlock()
	return nil
}
