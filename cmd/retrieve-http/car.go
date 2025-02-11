package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/service-sdk/go-sdk-qn/v2/operation"
)

type CarInfo struct {
	DataCid   string `json:"dataCid"`
	PieceCid  string `json:"pieceCid"`
	PieceSize int64  `json:"pieceSize"`
	CarSize   int64  `json:"carSize"`
	FileName  string `json:"fileName"`
}

var (
	carInfoMap sync.Map
)

func loadCarInfo(dirPath string) error {
	carInfoMap.Range(func(key, value interface{}) bool {
		carInfoMap.Delete(key)
		return true
	})

	files, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.IsDir() {
			filePath := filepath.Join(dirPath, file.Name())
			log.Infof("load car info from %s", filePath)

			f, err := os.Open(filePath)
			if err != nil {
				return err
			}
			defer f.Close()

			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				var carInfo CarInfo
				if err := json.Unmarshal(scanner.Bytes(), &carInfo); err != nil {
					return err
				}
				carInfoMap.Store(carInfo.DataCid, carInfo)
			}

			if err := scanner.Err(); err != nil {
				return err
			}
		}
	}
	count := 0
	carInfoMap.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	log.Infof("load car info from %s, total %d", dirPath, count)
	return nil
}

func getRandomDataCid() (string, error) {
	var keys []string

	carInfoMap.Range(func(key, value interface{}) bool {
		dataCid := key.(string)
		keys = append(keys, dataCid)
		return true
	})

	if len(keys) == 0 {
		return "", fmt.Errorf("carInfoMap is empty")
	}

	randomIndex := rand.IntN(len(keys))

	return keys[randomIndex], nil
}

func getCarInfo(dataCid string) (CarInfo, error) {
	if value, ok := carInfoMap.Load(dataCid); ok {
		return value.(CarInfo), nil
	}

	randomCid, err := getRandomDataCid()
	if err != nil {
		return CarInfo{}, err
	}

	if value, ok := carInfoMap.Load(randomCid); ok {
		return value.(CarInfo), nil
	}

	return CarInfo{}, fmt.Errorf("cannot get car info")
}

func car(w http.ResponseWriter, r *http.Request) {
	dataCid := strings.TrimPrefix(r.URL.Path, "/ipfs/")
	if _, err := cid.Parse(dataCid); err != nil {
		log.Errorw("invalid cid", "cid", dataCid, "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	carInfo, err := getCarInfo(dataCid)
	if err != nil {
		log.Errorw("cannot get car info", "cid", dataCid, "error", err)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	log.Debugw("car handler", "dataCid", dataCid, "carInfo.DataCid", carInfo.DataCid, "fileName", carInfo.FileName)

	downloader := operation.NewDownloaderV2()
	resp, err := downloader.DownloadRaw(carInfo.FileName, nil)
	if err != nil {
		log.Errorw("cannot download car", "cid", carInfo.DataCid, "fileName", carInfo.FileName, "error", err)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	io.Copy(w, resp.Body)
}
