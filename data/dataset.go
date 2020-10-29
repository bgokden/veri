package data

import (
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
)

type Dataset struct {
	DataList *cache.Cache
	Path     string
}

func NewDataset(path string) *Dataset {
	dts := &Dataset{
		Path: path,
	}
	dts.DataList = cache.New(5*time.Minute, 10*time.Minute)
	return dts
}

func (dts *Dataset) Get(name string) (*Data, error) {
	item, ok := dts.DataList.Get(name)
	if !ok {
		return nil, errors.Errorf("Data %v does not exist", name)
	}
	if data, ok := item.(*Data); ok {
		return data, nil
	}
	return nil, errors.Errorf("Data %v is currupt", name)
}

func (dts *Dataset) GetOrCreateIfNotExists(config *DataConfig) (*Data, error) {
	err := dts.CreateIfNotExists(config)
	if err == nil {
		return nil, err
	}
	return dts.Get(config.Name)
}

func (dts *Dataset) CreateIfNotExists(config *DataConfig) error {
	preData := NewPreData(config, dts.Path)
	err := dts.DataList.Add(config.Name, preData, cache.NoExpiration)
	if err == nil {
		preData.InitData()
	}
	return err
}

func (dts *Dataset) Delete(name string) error {
	item, ok := dts.DataList.Get(name)
	if !ok {
		return errors.Errorf("Data %v does not exist", name)
	}
	if data, ok := item.(*Data); ok {
		data.Close()
	}
	dts.DataList.Delete(name)
	return nil
}

func (dts *Dataset) List() []string {
	sourceList := dts.DataList.Items()
	keys := make([]string, 0, len(sourceList))
	for k := range sourceList {
		keys = append(keys, k)
	}
	return keys
}

func (dts *Dataset) DataConfigList() []DataConfig {
	sourceList := dts.DataList.Items()
	configs := make([]DataConfig, 0, len(sourceList))
	for k := range sourceList {
		data, _ := dts.Get(k)
		config := data.GetConfig()
		configs = append(configs, *config)
	}
	return configs
}
