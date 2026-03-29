package broker

// Message holds the minimal fields needed for exchange routing decisions.
type Message struct {
	ExchangeName string
	RoutingKey   string
	Mandatory    bool
	Immediate    bool
	Headers      map[string]interface{}
	Body         []byte
}
