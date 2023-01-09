package instance

import (
	"os"
	"os/signal"
	"syscall"

	adapter_service "github.com/BrobridgeOrg/gravity-adapter-jetstream/pkg/adapter/service"
	gravity_adapter "github.com/BrobridgeOrg/gravity-sdk/adapter"
	log "github.com/sirupsen/logrus"
)

type AppInstance struct {
	done             chan os.Signal
	adapter          *adapter_service.Adapter
	adapterConnector *gravity_adapter.AdapterConnector
}

func NewAppInstance() *AppInstance {

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGTERM)

	a := &AppInstance{
		done: sig,
	}

	a.adapter = adapter_service.NewAdapter(a)

	return a
}

func (a *AppInstance) Init() error {

	log.Info("Starting application")

	// Initializing adapter connector
	err := a.initAdapterConnector()
	if err != nil {
		return err
	}

	err = a.adapter.Init()
	if err != nil {
		return err
	}

	return nil
}

func (a *AppInstance) Uninit() {
}

func (a *AppInstance) Run() error {

	<-a.done
	//a.adapter.Stop()
	//time.Sleep(5 * time.Second)
	log.Error("Bye!")

	return nil
}
