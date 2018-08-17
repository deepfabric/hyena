package util

import (
	"reflect"

	"github.com/Shopify/sarama"
	"github.com/bouk/monkey"
	cluster "github.com/bsm/sarama-cluster"
)

// PatchSaramaOffset add spec offset patch for sarama
func PatchSaramaOffset(config *cluster.Config, committedOffset int64) error {
	if err := config.Validate(); err != nil {
		return err
	}
	if err := config.Config.Validate(); err != nil {
		return err
	}

	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	if committedOffset >= 0 {
		config.Consumer.Offsets.Initial = committedOffset + 1
		monkey.PatchInstanceMethod(reflect.TypeOf(config), "Validate", func(c *cluster.Config) error {
			return nil
		})
		monkey.PatchInstanceMethod(reflect.TypeOf(&config.Config), "Validate", func(c *sarama.Config) error {
			return nil
		})
	}

	return nil
}
