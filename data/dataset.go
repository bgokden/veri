package data

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/bgokden/go-cache"
	"github.com/pkg/errors"

	pb "github.com/bgokden/veri/veriservice"
)

type Dataset struct {
	DataList *cache.Cache
	Path     string
	DataPath string
}

func closeData(key string, value interface{}) {
	if data, ok := value.(*Data); ok {
		data.Close()
	}
}

func NewDataset(datasetPath string) *Dataset {
	dts := &Dataset{
		Path: datasetPath,
	}
	dts.DataList = cache.New(24*time.Hour, 1*time.Minute)
	dts.DataList.OnEvicted(closeData)
	dts.DataPath = path.Join(dts.Path, "data")
	os.MkdirAll(dts.DataPath, os.ModePerm)
	err := dts.LoadIndex()
	if err != nil {
		log.Printf("Loading error: %v\n", err)
	}
	return dts
}

func GetDefaultConfig(name string) *pb.DataConfig {
	return &pb.DataConfig{
		Name:                       name,
		Version:                    0,
		TargetN:                    1000,
		TargetUtilization:          0.4,
		NoTarget:                   false,
		ReplicationOnInsert:        1,
		EnforceReplicationOnInsert: true,
		Retention:                  0,
	}
}

func GetRetention(retention uint64) time.Duration {
	if retention == 0 {
		return time.Duration(14*24) * time.Hour
	}
	return time.Duration(retention) * time.Second
}

func (dts *Dataset) Get(name string) (*Data, error) {
	item, ok := dts.DataList.Get(name)
	if !ok {
		return dts.GetOrCreateIfNotExists(GetDefaultConfig(name))
	}
	if data, ok := item.(*Data); ok {
		dts.DataList.IncrementExpiration(name, GetRetention(data.GetConfig().Retention))
		return data, nil
	}
	return nil, errors.Errorf("Data %v is currupt", name)
}

func (dts *Dataset) GetNoOp(name string) (*Data, error) {
	item, ok := dts.DataList.Get(name)
	if !ok {
		return dts.GetOrCreateIfNotExists(GetDefaultConfig(name))
	}
	if data, ok := item.(*Data); ok {
		return data, nil
	}
	return nil, errors.Errorf("Data %v is currupt", name)
}

func (dts *Dataset) GetNoCreate(name string) (*Data, error) {
	item, ok := dts.DataList.Get(name)
	if !ok {
		return nil, errors.Errorf("Data %v does not exist. Retry recomended", name)
	}
	if data, ok := item.(*Data); ok {
		return data, nil
	}
	return nil, errors.Errorf("Data %v is currupt", name)
}

func (dts *Dataset) GetOrCreateIfNotExists(config *pb.DataConfig) (*Data, error) {
	err := dts.CreateIfNotExists(config)
	if err != nil {
		return nil, err
	}
	return dts.Get(config.Name)
}

func (dts *Dataset) CreateIfNotExists(config *pb.DataConfig) error {
	preData := NewPreData(config, dts.DataPath)
	retention := GetRetention(config.Retention)
	// log.Printf("Data %v Retention: %v Version: %v\n", config.Name, retention, config.Version)
	err := dts.DataList.Add(config.Name, preData, retention)
	if err == nil {
		go dts.SaveIndex()
		return preData.InitData()
	}
	// log.Printf("Data %v Error: %v\n", config.Name, err.Error())
	if err.Error() == fmt.Sprintf("Item %s already exists", config.Name) {
		data, errGetNoOp := dts.GetNoOp(config.Name)
		if errGetNoOp == nil {
			if config.Version > data.Config.Version {
				log.Printf("Update Data %v Config.Version: %v\n", config.Name, config.Version)
				data.Config = config
			}
		} else {
			log.Printf("Error errGetNoOp: %v\n", errGetNoOp.Error())
		}
		return nil
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
	go dts.SaveIndex()
	return nil
}

func (dts *Dataset) List() []string {
	sourceList := dts.DataList.Items()
	keys := make([]string, 0, len(sourceList))
	for k := range sourceList {
		keys = append(keys, k)
	}
	go dts.SaveIndex()
	return keys
}

func (dts *Dataset) DataConfigList() []*pb.DataConfig {
	sourceList := dts.DataList.Items()
	configs := make([]*pb.DataConfig, 0, len(sourceList))
	for k := range sourceList {
		data, _ := dts.Get(k)
		config := data.GetConfig()
		configs = append(configs, config)
	}
	return configs
}

func (dts *Dataset) LoadIndex() error {
	indexPath := path.Join(dts.Path, "index.save")
	file, err := os.OpenFile(indexPath, os.O_APPEND|os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		var config pb.DataConfig
		if err := json.Unmarshal([]byte(line), &config); err == nil {
			err2 := dts.CreateIfNotExists(&config)
			if err2 != nil {
				log.Printf("Err creatding data: %v\n", err2)
			}
		}
	}
	file.Close()
	return nil
}

func (dts *Dataset) ReloadRuns() error {
	itemList := dts.DataList.Items()
	removeList := make([]string, 0, len(itemList))
	for key, itemInterface := range itemList {
		if item, ok := itemInterface.Object.(*Data); ok {
			if item.CheckIfActive() {
				go item.Run()
			} else {
				item.Close()
				removeList = append(removeList, key)
			}
		}
	}
	for _, key := range removeList {
		dts.DataList.Delete(key)
	}
	return nil
}

func (dts *Dataset) SaveIndex() error {
	indexPath := path.Join(dts.Path, "index.save")
	sourceList := dts.DataList.Items()
	file, err := os.OpenFile(indexPath, os.O_TRUNC|os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	datawriter := bufio.NewWriter(file)
	for k := range sourceList {
		data, _ := dts.Get(k)
		config := data.GetConfig()
		jsonData, _ := json.Marshal(config)
		_, _ = datawriter.WriteString(string(jsonData) + "\n")

	}
	datawriter.Flush()
	file.Close()
	return nil
}

func (dts *Dataset) Close() error {
	dts.SaveIndex()
	datalist := dts.DataList.Items()
	for k := range datalist {
		data, err := dts.Get(k)
		if err == nil {
			data.Close()
		}
	}
	return nil
}
