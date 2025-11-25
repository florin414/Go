package broker

// Reachable returns the list of brokers that were reachable during the last check.
func (p *Producer) Reachable() []string {
	if p == nil {
		return nil
	}
	p.reachableMu.RLock()
	defer p.reachableMu.RUnlock()
	out := make([]string, len(p.reachable))
	copy(out, p.reachable)
	return out
}
