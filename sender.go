package kasper

type Sender interface {
	Send(OutgoingMessage)
}