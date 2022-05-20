package stream

type SimpleSender interface {
	Send(key string, mtype string, data []byte, meta ...map[string]string) error
}
