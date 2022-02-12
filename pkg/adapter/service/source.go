package adapter

import (
	//"context"
	//"encoding/json"
	"fmt"
	"sync"
	"time"
	"unsafe"

	eventbus "github.com/BrobridgeOrg/gravity-adapter-jetstream/pkg/eventbus/service"
	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

var defaultInfo = SourceInfo{
	DurableName:         "DefaultGravity",
	PingInterval:        10,
	MaxPingsOutstanding: 3,
	MaxReconnects:       -1,
}

type Packet struct {
	EventName string
	Payload   []byte
}

type Source struct {
	adapter             *Adapter
	eventBus            *eventbus.EventBus
	name                string
	host                string
	port                int
	clientID            string
	durableName         string
	channel             string
	pingInterval        int64
	maxPingsOutstanding int
	maxReconnects       int
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return &Packet{}
	},
}

func StrToBytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func NewSource(adapter *Adapter, name string, sourceInfo *SourceInfo) *Source {

	// required channel
	if len(sourceInfo.Channel) == 0 {
		log.WithFields(log.Fields{
			"source": name,
		}).Error("Required channel")

		return nil
	}

	info := sourceInfo

	// default settings
	//if defaultInfo.DurableName != info.DurableName {
	if info.DurableName == "" {
		info.DurableName = defaultInfo.DurableName
	}

	//if defaultInfo.PingInterval != info.PingInterval {
	if info.PingInterval < 1 {
		info.PingInterval = defaultInfo.PingInterval
	}

	//if defaultInfo.MaxPingsOutstanding != info.MaxPingsOutstanding {
	if info.MaxPingsOutstanding < 1 {
		info.MaxPingsOutstanding = defaultInfo.MaxPingsOutstanding
	}

	//if defaultInfo.MaxReconnects != info.MaxReconnects {
	if info.MaxReconnects <= 0 {
		info.MaxReconnects = defaultInfo.MaxReconnects
	}

	return &Source{
		adapter:             adapter,
		name:                name,
		host:                info.Host,
		port:                info.Port,
		durableName:         info.DurableName,
		channel:             info.Channel,
		pingInterval:        info.PingInterval,
		maxPingsOutstanding: info.MaxPingsOutstanding,
		maxReconnects:       info.MaxReconnects,
	}
}

func (source *Source) InitSubscription() error {

	// Subscribe to channel
	jetStreamConn := source.eventBus.GetJetStreamConnection()
	if len(source.durableName) == 0 {

		// Subscribe without durable name
		_, err := jetStreamConn.Subscribe(source.channel, source.HandleMessage, nats.ManualAck())
		if err != nil {
			return err
		}

		return nil
	}

	// Subscribe with durable name
	_, err := jetStreamConn.Subscribe(source.channel, source.HandleMessage, nats.Durable(source.durableName), nats.ManualAck())
	if err != nil {
		log.Error(source.durableName)
		return err
	}

	return nil
}

func (source *Source) Init() error {

	address := fmt.Sprintf("%s:%d", source.host, source.port)

	if source.durableName == "" {
		source.clientID = source.adapter.clientID + "-" + source.name
	} else {
		source.clientID = source.durableName + "-" + source.name
	}

	log.WithFields(log.Fields{
		"source":      source.name,
		"address":     address,
		"client_name": source.clientID,
		"durableName": source.durableName,
		"channel":     source.channel,
	}).Info("Initializing source connector")

	options := eventbus.Options{
		ClientName:          source.clientID,
		PingInterval:        time.Duration(source.pingInterval),
		MaxPingsOutstanding: source.maxPingsOutstanding,
		MaxReconnects:       source.maxReconnects,
	}

	source.eventBus = eventbus.NewEventBus(
		address,
		eventbus.EventBusHandler{
			Reconnect: func(natsConn *nats.Conn) {
				err := source.InitSubscription()
				if err != nil {
					log.Error(err)
					return
				}

				log.Warn("re-connected to event server")
			},
			Disconnect: func(natsConn *nats.Conn) {
				log.Error("event server was disconnected")
			},
		},
		options,
	)

	err := source.eventBus.Connect()
	if err != nil {
		return err
	}

	return source.InitSubscription()
}

func (source *Source) HandleMessage(m *nats.Msg) {

	eventName := jsoniter.Get(m.Data, "event").ToString()
	payload := jsoniter.Get(m.Data, "payload").ToString()

	//filter not gravity format
	if eventName == "" || payload == "" {
		log.Error("Not gravity's format.")
		m.Ack()
		return
	}

	// Preparing request
	request := requestPool.Get().(*Packet)
	request.EventName = eventName
	request.Payload = StrToBytes(payload)

	for {
		connector := source.adapter.app.GetAdapterConnector()
		err := connector.Publish(request.EventName, request.Payload, nil)
		if err != nil {
			log.Error(err)
			time.Sleep(time.Second)
			continue
		}
		break
	}
	m.Ack()
	requestPool.Put(request)
}
