package recorder

import (
	"errors"
	"sync"
)

var (
	msg   = make(chan string, 1)
	mutex sync.Mutex
)

// FetchURL will fetch list target database url from ops database
func FetchURL() (string, error) {

	uri, ok := <-msg
	if !ok || uri == "error" {
		return "", errors.New("message is nil")
	}
	return uri, nil
}

func pubMsg(uri *string) error {
	mutex.Lock()
	defer mutex.Unlock()

	if uri == nil {
		msg <- "error"
		return errors.New("message is nil")
	}
	msg <- *uri
	return nil
}
