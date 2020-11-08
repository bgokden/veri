package data

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"

	pb "github.com/bgokden/veri/veriservice"
)

type Dataset struct {
	DataList *cache.Cache
	Path     string
	DataPath string
}

func NewDataset(datasetPath string) *Dataset {
	dts := &Dataset{
		Path: datasetPath,
	}
	dts.DataList = cache.New(5*time.Minute, 10*time.Minute)
	dts.DataPath = path.Join(dts.Path, "data")
	os.MkdirAll(dts.DataPath, os.ModePerm)
	err := dts.LoadIndex()
	if err != nil {
		fmt.Printf("err %v\n", err)
	}
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

func (dts *Dataset) GetOrCreateIfNotExists(config *pb.DataConfig) (*Data, error) {
	err := dts.CreateIfNotExists(config)
	if err != nil {
		return nil, err
	}
	return dts.Get(config.Name)
}

func (dts *Dataset) CreateIfNotExists(config *pb.DataConfig) error {
	preData := NewPreData(config, dts.DataPath)
	err := dts.DataList.Add(config.Name, preData, cache.NoExpiration)
	if err == nil {
		return preData.InitData()
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
		var jsonData []byte
		jsonData, err := json.Marshal(config)
		if err != nil {
			log.Println(err)
		}

		_, _ = datawriter.WriteString(string(jsonData) + "\n")

	}
	datawriter.Flush()
	file.Close()
	return nil
}

func (dts *Dataset) Close() error {
	dts.SaveIndex()
	sourceList := dts.DataList.Items()
	for k := range sourceList {
		data, _ := dts.Get(k)
		data.Close()
	}
	return nil
}
