package zk

import (
	"fmt"
	"time"

	"github.com/xordataexchange/crypt/backend"

	gozk "github.com/samuel/go-zookeeper/zk"
)

type Client struct {
	client    *gozk.Conn
	waitIndex uint64
}

func New(machines []string) (*Client, error) {
	c, _, err := gozk.Connect(machines, time.Second)
	return &Client{c, 0}, err
}

func (c *Client) Get(key string) ([]byte, error) {
	resp, _, err := c.client.Get(key)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// func addKVPairs(node *gozk.Node, list backend.KVPairs) backend.KVPairs {
// 	if node.Dir {
// 		for _, n := range node.Nodes {
// 			list = addKVPairs(n, list)
// 		}
// 		return list
// 	}
// 	return append(list, &backend.KVPair{Key: node.Key, Value: []byte(node.Value)})
// }

func (c *Client) List(key string) (backend.KVPairs, error) {
	resp, _, err := c.client.Children(key)
	fmt.Println(resp)
	if err != nil {
		return nil, err
	}
	// if !resp.Node.Dir {
	// 	return nil, errors.New("key is not a directory")
	// }
	// list := addKVPairs(resp.Node, nil)
	return nil, nil
}

func (c *Client) Set(key string, value []byte) error {
	_, err := c.client.Set(key, value, 0)
	fmt.Println(err)
	return err
}

func (c *Client) Watch(key string, stop chan bool) <-chan *backend.Response {
	respChan := make(chan *backend.Response, 0)
	go func() {
		for {

			resp, _, ch, err := c.client.GetW(key)
			if err != nil {
				respChan <- &backend.Response{nil, err}
				time.Sleep(time.Second * 5)
				continue
			}

			respChan <- &backend.Response{resp, nil}

			<-ch
		}
	}()
	return respChan
}
