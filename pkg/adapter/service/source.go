package adapter

import (
	//"context"
	//"encoding/json"
	//"sync/atomic"
	"fmt"
	"sync"
	"time"
	"unsafe"

	//"github.com/BrobridgeOrg/broton"
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
	//stopping            bool
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return &Packet{}
	},
}

var sdkRequestPool = sync.Pool{
	New: func() interface{} {
		return &adapter_sdk.Request{}
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
		//stopping:            false,
	}
}

func (source *Source) InitSubscription() error {

	// Subscribe to channel
	jetStreamConn := source.eventBus.GetJetStreamConnection()
	if len(source.durableName) == 0 {

		// Subscribe without durable name
		subscription, err := jetStreamConn.Subscribe(source.channel, source.HandleMessage, nats.MaxAckPending(1), nats.ManualAck())
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

	/*
		log.Info("Stop subscriber")
		//return source.subscription.Drain()
		source.stopping = true

			//stop source
			sourceConn := source.eventBus.GetConnection()
			sourceConn.Flush()

			//stop target
			targetConn := source.adapter.app.GetAdapterConnector()
			<-targetConn.PublishComplete()
	*/
	return nil

}

func (source *Source) Init() error {

	address := fmt.Sprintf("%s:%d", source.host, source.port)

	if source.durableName == "" {
		source.clientID = source.adapter.clientID + "-" + source.name
	} else {
		source.clientID = source.durableName + "-" + source.name
	}

	/*
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
	*/

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

	/*
		if source.stopping {
			log.Warn("stopping ...")
			time.Sleep(time.Second)
			return
		}
	*/

	connector := source.adapter.app.GetAdapterConnector()
	// process batch data
	if viper.GetBool("adapter.batchMode") {
		// Get The data processed record

		/*
			var lsn int64 = 0
			meta, _ := m.Metadata()
			lsnKey := fmt.Sprintf("%s-%s", source.name, "LSN")
			lastlsn, err := source.store.GetInt64("status", []byte(lsnKey))
			if err == nil && meta.NumDelivered > 1 {
				log.Infof("Using %s lsn %v", lsnKey, lastlsn)
				lsn = lastlsn
			}
		*/

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
		//for _, packet := range packetsArr[int(lsn):] {
		for _, packet := range packetsArr {
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
			//var request adapter_sdk.Request
			request := sdkRequestPool.Get().(*adapter_sdk.Request)
			request.EventName = eventName
			request.Payload = StrToBytes(payload)

			requests = append(requests, request)
		}

		// Publish by batch
		for i, request := range requests {
			for {
				sourceMeta, _ := m.Metadata()
				meta := make(map[string]interface{})
				meta["Msg-Id"] = fmt.Sprintf("%s-%s-%s-%d", sourceMeta.Stream, sourceMeta.Consumer, sourceMeta.Sequence.Stream, i)
				err := connector.Publish(request.EventName, request.Payload, meta)
				if err != nil {
					log.Error(err, " stream seq: ", sourceMeta.Sequence.Stream, " retry ...")
					time.Sleep(time.Second)
					continue
				}

				/*
					// Update lsn
					for {
						lsn = lsn + int64(1)
						err := source.store.PutInt64("status", []byte(lsnKey), lsn)
						if err != nil {
							log.Error("Failed to update LSN")
							log.Error(err)
							time.Sleep(time.Second)
							continue
						}

						break
					}
				*/

				sdkRequestPool.Put(request)

				break
			}
		}
		<-connector.PublishComplete()
		log.Info("Success: ", len(requests))

		/*
			// Reset lsn
			for {
				err = source.store.PutInt64("status", []byte(lsnKey), int64(0))
				if err != nil {
					log.Error("Failed to reset LSN")
					log.Error(err)
					time.Sleep(time.Second)
					continue
				}
				m.Ack()
				break
			}

		*/
		m.Ack()

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
			sourceMeta, _ := m.Metadata()
			meta := make(map[string]interface{})
			meta["Msg-Id"] = fmt.Sprintf("%s-%s-%s", sourceMeta.Stream, sourceMeta.Consumer, sourceMeta.Sequence.Stream)
			err := connector.Publish(request.EventName, request.Payload, meta)
			if err != nil {
				log.Error(err)
				time.Sleep(time.Second)
				continue
			}
			break
		}
		<-connector.PublishComplete()
		m.Ack()
		requestPool.Put(request)
	}
}
