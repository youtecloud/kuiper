package plugins

import (
	"errors"
	"sync"

	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/contexts"
)

var (
	registeredSinks     = make(map[string]api.Sink)
	registeredFunctions = make(map[string]api.Function)
	registeredSources   = make(map[string]api.Source)

	regLock sync.RWMutex
)

func Register(name string, nf interface{}) error {
	switch t := nf.(type) {
	case api.Function:
		return registerFunction(name, t)
	case func() api.Function:
		return registerFunction(name, t())
	case api.Sink:
		return registerSink(name, t)
	case func() api.Sink:
		return registerSink(name, t())
	case api.Source:
		return registerSource(name, t)
	case func() api.Source:
		return registerSource(name, t())
	default:
		return errors.New("Unsupport plugin type")
	}
}

func registerFunction(name string, plug api.Function) error {
	if _, ok := registeredFunctions[name]; ok {
		return errors.New("function name already exist")
	}

	regLock.Lock()
	registeredFunctions[name] = plug
	regLock.Unlock()

	logger := contexts.Background().GetLogger()
	logger.Infof("registered plugin: %s", name)

	return nil
}

func registerSink(name string, plug api.Sink) error {
	if _, ok := registeredSinks[name]; ok {
		return errors.New("sink name already exist")
	}

	regLock.Lock()
	registeredSinks[name] = plug
	regLock.Unlock()

	logger := contexts.Background().GetLogger()
	logger.Infof("registered plugin: %s", name)

	return nil
}

func registerSource(name string, plug api.Source) error {
	if _, ok := registeredSources[name]; ok {
		return errors.New("source name already exist")
	}

	regLock.Lock()
	registeredSources[name] = plug
	regLock.Unlock()

	logger := contexts.Background().GetLogger()
	logger.Infof("registered plugin: %s", name)

	return nil
}
