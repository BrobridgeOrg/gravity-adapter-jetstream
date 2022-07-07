package adapter

import (
	//"context"
	//"encoding/json"
	//"sync/atomic"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/BrobridgeOrg/broton"
	eventbus "github.com/BrobridgeOrg/gravity-adapter-jetstream/pkg/eventbus/service"
	adapter_sdk "github.com/BrobridgeOrg/gravity-sdk/adapter"

	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var counter uint64

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
	store               *broton.Store
	subscription        *nats.Subscription
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
		store:               nil,
	}
}

func (source *Source) InitSubscription() error {

	// Subscribe to channel
	jetStreamConn := source.eventBus.GetJetStreamConnection()
	if len(source.durableName) == 0 {

		// Subscribe without durable name
		subscription, err := jetStreamConn.Subscribe(source.channel, source.HandleMessage, nats.ManualAck())
		if err != nil {
			return err
		}

		source.subscription = subscription

		return nil
	}

	// Subscribe with durable name
	subscription, err := jetStreamConn.Subscribe(source.channel, source.HandleMessage, nats.Durable(source.durableName), nats.ManualAck(), nats.MaxAckPending(1)) //, nats.MaxDeliver(1))
	if err != nil {
		log.Error(source.durableName)
		return err
	}

	source.subscription = subscription

	return nil
}

func (source *Source) Stop() error {

	log.Info("Drain subscriber")
	return source.subscription.Drain()
	//return source.subscription.Unsubscribe()
}

func (source *Source) Init() error {

	address := fmt.Sprintf("%s:%d", source.host, source.port)

	if source.durableName == "" {
		source.clientID = source.adapter.clientID + "-" + source.name
	} else {
		source.clientID = source.durableName + "-" + source.name
	}

	if viper.GetBool("store.enabled") && viper.GetBool("adapter.batchMode") {
		// Initializing store
		log.WithFields(log.Fields{
			"store": "adapter-" + source.name,
		}).Info("Initializing store for adapter")

		store, err := source.adapter.storeMgr.GetStore("adapter-" + source.name)
		if err != nil {
			return err
		}

		// register columns
		columns := []string{"status"}
		err = store.RegisterColumns(columns)
		if err != nil {
			log.Error(err)
			return err
		}
		source.store = store
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

	connector := source.adapter.app.GetAdapterConnector()
	// process batch data
	if viper.GetBool("adapter.batchMode") {
		// Get The data processed record

		var lsn int64 = 0
		meta, _ := m.Metadata()
		lsnKey := fmt.Sprintf("%s", "LSN")
		lastlsn, err := source.store.GetInt64("status", []byte(lsnKey))
		if err == nil && meta.NumDelivered > 1 {
			lsn = lastlsn
		}

		packets := jsoniter.Get(m.Data, "payloads").GetInterface()
		var packetsArr []interface{}
		if arr, ok := packets.([]interface{}); ok {
			packetsArr = arr
		} else {
			log.Warn("Not batch packets.")
			m.Ack()
			return
		}
		//packetsArr := packets.([]interface{})

		requests := make([]*adapter_sdk.Request, 0)
		for _, packet := range packetsArr[int(lsn):] {
			packetAny := jsoniter.Wrap(packet)
			eventName := packetAny.Get("event").ToString()
			payload := packetAny.Get("payload").ToString()

			//filter not gravity format
			if eventName == "" || payload == "" {
				log.Error("Not gravity's format.")
				m.Ack()
				return
			}

			// Preparing request
			var request adapter_sdk.Request
			request.EventName = eventName
			request.Payload = StrToBytes(payload)

			requests = append(requests, &request)
		}

		// Publish by batch
		for {
			_, count, err := connector.BatchPublish(requests)
			if err == nil {
				log.Info("Success: ", len(requests))
				m.Ack()

				// Reset lsn
				err = source.store.PutInt64("status", []byte(lsnKey), int64(0))
				if err != nil {
					log.Error("Failed to reset LSN")
					log.Error(err)
				}
				break
			} else if count == 0 && err != nil {
				meta, _ := m.Metadata()
				log.Error(err, " stream seq: ", meta.Sequence.Stream)
				log.Error(string(m.Data))
				log.Error("Retry.")
				time.Sleep(time.Second)
				continue
			} else if count > 0 && err != nil {
				log.Error("Count: ", count)
				log.Error(err)
				// The data processed must be recorded
				err := source.store.PutInt64("status", []byte(lsnKey), int64(int(count)+int(lsn)))
				if err != nil {
					log.Error("Failed to update LSN")
					log.Error(err)
					time.Sleep(time.Second)
					return
				}

			}
		}

	} else {

		/*
			id := atomic.AddUint64((*uint64)(&counter), 1)

			if id%100 == 0 {
				log.Info(id)
			}
		*/

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
}
