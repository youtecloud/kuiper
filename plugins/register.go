package plugins

import (
	"errors"
	"fmt"
	"sync"

	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/contexts"
)

var (
	registeredSinks     = make(map[string]interface{})
	registeredFunctions = make(map[string]interface{})
	registeredSources   = make(map[string]interface{})

	regLock sync.RWMutex
)

func Register(name string, nf interface{}) error {
	logger := contexts.Background().GetLogger()

	switch t := nf.(type) {
	case api.Function:
		return registerFunction(name, t)
	case func() api.Function:
		return registerFunction(name, t)
	case api.Sink:
		return registerSink(name, t)
	case func() api.Sink:
		return registerSink(name, t)
	case api.Source:
		return registerSource(name, t)
	case func() api.Source:
		return registerSource(name, t)
	default:
		logger.Infof("unsupport plugin type: %s", name)
		return errors.New("unsupport plugin type")
	}
}

func registerFunction(name string, plug interface{}) error {
	if _, ok := registeredFunctions[name]; ok {
		return fmt.Errorf("function name already exist: %s", name)
	}

	regLock.Lock()
	registeredFunctions[name] = plug
	regLock.Unlock()

	logger := contexts.Background().GetLogger()
	logger.Infof("function plugin registered: %s", name)

	return nil
}

func registerSink(name string, plug interface{}) error {
	if _, ok := registeredSinks[name]; ok {
		return fmt.Errorf("sink name already exist: %s", name)
	}

	regLock.Lock()
	registeredSinks[name] = plug
	regLock.Unlock()

	logger := contexts.Background().GetLogger()
	logger.Infof("sink plugin registered: %s", name)

	return nil
}

func registerSource(name string, plug interface{}) error {
	if _, ok := registeredSources[name]; ok {
		return fmt.Errorf("source name already exist: %s", name)
	}

	regLock.Lock()
	registeredSources[name] = plug
	regLock.Unlock()

	logger := contexts.Background().GetLogger()
	logger.Infof("source plugin registered: %s", name)

	return nil
}

func getSourceFromNative(name string) (api.Source, bool) {
	p, ok := registeredSources[name]
	if !ok {
		return nil, false
	}

	switch t := p.(type) {
	case api.Source:
		return t, true
	case func() api.Source:
		return t(), true
	default:
		return nil, false
	}
}

func getSinkFromNative(name string) (api.Sink, bool) {
	p, ok := registeredSinks[name]
	if !ok {
		return nil, false
	}

	switch t := p.(type) {
	case api.Sink:
		return t, true
	case func() api.Sink:
		return t(), true
	default:
		return nil, false
	}
}

func getFunctionsFromNative(name string) (api.Function, bool) {
	p, ok := registeredFunctions[name]
	if !ok {
		return nil, false
	}

	switch t := p.(type) {
	case api.Function:
		return t, true
	case func() api.Function:
		return t(), true
	default:
		return nil, false
	}
}
