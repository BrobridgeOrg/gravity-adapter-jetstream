package adapter

import (
	"fmt"
	"os"
	"strings"

	"github.com/BrobridgeOrg/broton"
	"github.com/BrobridgeOrg/gravity-adapter-jetstream/pkg/app"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Adapter struct {
	app      app.App
	sm       *SourceManager
	clientID string
	storeMgr *broton.Broton
}

func NewAdapter(a app.App) *Adapter {
	adapter := &Adapter{
		app: a,
	}

	adapter.sm = NewSourceManager(adapter)

	return adapter
}

func (adapter *Adapter) Init() error {

	// Using hostname (pod name) by default
	host, err := os.Hostname()
	if err != nil {
		log.Error(err)
		return err
	}

	host = strings.ReplaceAll(host, ".", "_")

	adapter.clientID = fmt.Sprintf("gravity_adapter_jetstream-%s", host)

	// Initializing store manager
	viper.SetDefault("store.enabled", false)
	enabled := viper.GetBool("store.enabled")
	if enabled {
		viper.SetDefault("store.path", "./store")
		options := broton.NewOptions()
		options.DatabasePath = viper.GetString("store.path")

		log.WithFields(log.Fields{
			"path": options.DatabasePath,
		}).Info("Initializing store")
		broton, err := broton.NewBroton(options)
		if err != nil {
			return err
		}

		adapter.storeMgr = broton
	}

	err = adapter.sm.Initialize()
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

func (adapter *Adapter) Stop() {
	adapter.sm.Stop()
}
