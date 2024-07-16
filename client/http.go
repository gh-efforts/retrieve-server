package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func GetBlock(addr string, root string) (*RootBlock, error) {
	url := fmt.Sprintf("http://%s/block/%s", addr, root)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		r, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("status: %s msg: %s", resp.Status, string(r))
	}

	var rb RootBlock
	err = json.NewDecoder(resp.Body).Decode(&rb)
	if err != nil {
		return nil, err
	}

	return &rb, nil
}

func GetSize(addr string, root string) (*RootSize, error) {
	url := fmt.Sprintf("http://%s/size/%s", addr, root)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		r, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("status: %s msg: %s", resp.Status, string(r))
	}

	var rz RootSize
	err = json.NewDecoder(resp.Body).Decode(&rz)
	if err != nil {
		return nil, err
	}

	return &rz, nil
}

func GetHas(addr string, root string) bool {
	rz, err := GetSize(addr, root)
	if err != nil {
		log.Error(err)
		return false
	}

	if root == rz.Root {
		return true
	}

	return false
}

func PostRootBlock(addr string, root string, block []byte) error {
	rb := RootBlock{
		Root:  root,
		Block: block,
	}

	body, err := json.Marshal(&rb)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://%s/block", addr)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		r, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("status: %s msg: %s", resp.Status, string(r))
	}

	return nil
}
