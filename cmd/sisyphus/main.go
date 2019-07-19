// Copyright 2019 CanonicalLtd

package main

import (
	"context"
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/juju/zaputil"
	"github.com/juju/zaputil/zapctx"
	"go.uber.org/zap"
	"gopkg.in/macaroon-bakery.v1/httpbakery"
	yaml "gopkg.in/yaml.v1"

	"github.com/cloud-green/sisyphus/config"
	"github.com/cloud-green/sisyphus/simulation"
	"github.com/cloud-green/sisyphus/simulation/call"
)

func main() {
	zapctx.LogLevel.SetLevel(LogLevel())
	ctx := context.Background()

	flag.Parse()

	data, err := ioutil.ReadFile(Config())
	if err != nil {
		zapctx.Error(ctx, "failed to read the configuration file [%q]", zap.String("config", Config()), zaputil.Error(err))
		return
	}

	var simConfig config.Config
	err = yaml.Unmarshal(data, &simConfig)
	if err != nil {
		zapctx.Error(ctx, "failed to unmarshal configuration file", zaputil.Error(err))
		return
	}

	var callBackend simulation.CallBackend
	switch simConfig.Backend {
	case "nop":
		callBackend = call.NewNOPCallBackend()
	case "http":
		callBackend = call.NewHTTPCallBackend(httpbakery.NewClient())
	case "kafka":
		version, err := KafkaVersion()
		if err != nil {
			zapctx.Error(ctx, "failed to parse kafka version", zaputil.Error(err))
			return
		}
		config := sarama.NewConfig()
		config.ClientID = KafkaClientID()
		config.Producer.Return.Successes = true
		config.Producer.Partitioner = sarama.NewHashPartitioner
		config.Version = version

		TLSConfig, err := KafkaTLS()
		if err != nil {
			zapctx.Error(ctx, "failed to parse kafka TLS config", zaputil.Error(err))
			return
		}
		if TLSConfig != nil {
			cfg, err := TLSConfig.Config()
			if err != nil {
				zapctx.Error(ctx, "failed to parse kafka tls config", zaputil.Error(err))
				return
			}
			config.Net.TLS.Config = cfg
			config.Net.TLS.Enable = true
		}

		if err := config.Validate(); err != nil {
			zapctx.Error(ctx, "failed to validate kafka configuration", zaputil.Error(err))
			return
		}

		client, err := sarama.NewClient(KafkaBrokerURLs(), config)
		if err != nil {
			zapctx.Error(ctx, "failed to create kafka client", zaputil.Error(err))
			return
		}

		producer, err := sarama.NewSyncProducerFromClient(client)
		if err != nil {
			zapctx.Error(ctx, "failed to create a new kafka producer", zaputil.Error(err))
			return
		}

		callBackend = call.NewKafkaCallBackend(producer)
	default:
		zapctx.Error(ctx, "unknown call backend", zap.String("backend", string(simConfig.Backend)))
		return
	}

	numberOfWorkers, err := Workers()
	if err != nil {
		zapctx.Error(ctx, "failed to parse the number of workers", zaputil.Error(err))
		return
	}

	s := simulation.New(simConfig, callBackend, numberOfWorkers)

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	s.Close()
	os.Exit(1)
}
