package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("client")

var ErrNotFound = errors.New("block not found")

type RootBlock struct {
	Root  string `json:"root"`
	Block []byte `json:"block"`
}

type RootSize struct {
	Root string `json:"root"`
	Size int    `json:"size"`
}

type Client struct {
	addr string
}

func New(addr string) *Client {
	return &Client{
		addr: addr,
	}
}

func (c *Client) BlockstoreGet(ctx context.Context, cid cid.Cid) ([]byte, error) {
	rb, err := GetBlock(c.addr, cid.String())
	if err != nil {
		log.Error(err)
		return nil, ErrNotFound
	}

	return rb.Block, nil
}

func (c *Client) BlockstoreGetSize(ctx context.Context, cid cid.Cid) (int, error) {
	rz, err := GetSize(c.addr, cid.String())
	if err != nil {
		log.Error(err)
		return 0, ErrNotFound
	}

	return rz.Size, nil
}

func (c *Client) BlockstoreHas(ctx context.Context, cid cid.Cid) (bool, error) {
	return GetHas(c.addr, cid.String()), nil
}

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
