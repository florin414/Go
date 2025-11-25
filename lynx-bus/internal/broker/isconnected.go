package broker

// IsConnected reports whether at least one broker was reachable.
func (p *Producer) IsConnected() bool {
	if p == nil {
		return false
	}
	p.reachableMu.RLock()
	defer p.reachableMu.RUnlock()
	return len(p.reachable) > 0
}
