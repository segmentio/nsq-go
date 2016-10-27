package nsq

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/pkg/errors"
)

type Client struct {
	http.Client
	Address   string
	Scheme    string
	UserAgent string
}

func (c *Client) Ping() error {
	return c.call("GET", "/ping", nil)
}

func (c *Client) Publish(topic string, message []byte) (err error) {
	_, err = c.do("POST", "/pub", url.Values{
		"topic": []string{topic},
	}, message)
	return
}

func (c *Client) MutliPublish(topic string, messages ...[]byte) (err error) {
	_, err = c.do("POST", "/mpub", url.Values{
		"topic": []string{topic},
	}, bytes.Join(messages, []byte("\n")))
	return
}

func (c *Client) CreateTopic(topic string) error {
	return c.call("POST", "/topic/create", url.Values{
		"topic": []string{topic},
	})
}

func (c *Client) DeleteTopic(topic string) error {
	return c.call("POST", "/topic/delete", url.Values{
		"topic": []string{topic},
	})
}

func (c *Client) EmptyTopic(topic string) error {
	return c.call("POST", "/topic/empty", url.Values{
		"topic": []string{topic},
	})
}

func (c *Client) PauseTopic(topic string) error {
	return c.call("POST", "/topic/pause", url.Values{
		"topic": []string{topic},
	})
}

func (c *Client) UnpauseTopic(topic string) error {
	return c.call("POST", "/topic/unpause", url.Values{
		"topic": []string{topic},
	})
}

func (c *Client) CreateChannel(topic string, channel string) error {
	return c.call("POST", "/channel/create", url.Values{
		"topic":   []string{topic},
		"channel": []string{channel},
	})
}

func (c *Client) DeleteChannel(topic string, channel string) error {
	return c.call("POST", "/channel/delete", url.Values{
		"topic":   []string{topic},
		"channel": []string{channel},
	})
}

func (c *Client) EmptyChannel(topic string, channel string) error {
	return c.call("POST", "/channel/empty", url.Values{
		"topic":   []string{topic},
		"channel": []string{channel},
	})
}

func (c *Client) PauseChannel(topic string, channel string) error {
	return c.call("POST", "/channel/pause", url.Values{
		"topic":   []string{topic},
		"channel": []string{channel},
	})
}

func (c *Client) UnpauseChannel(topic string, channel string) error {
	return c.call("POST", "/channel/unpause", url.Values{
		"topic":   []string{topic},
		"channel": []string{channel},
	})
}

func (c *Client) call(method string, path string, query url.Values) (err error) {
	_, err = c.do(method, path, query, nil)
	return
}

func (c *Client) do(method string, path string, query url.Values, data []byte) (ret []byte, err error) {
	var res *http.Response
	var host = c.Address
	var scheme = c.Scheme
	var userAgent = c.UserAgent
	var body io.ReadCloser

	if len(host) == 0 {
		host = "localhost:4151"
	}

	if len(scheme) == 0 {
		scheme = "http"
	}

	if len(userAgent) == 0 {
		userAgent = DefaultUserAgent
	}

	if len(data) != 0 {
		body = ioutil.NopCloser(bytes.NewReader(data))
	}

	if res, err = c.Do(&http.Request{
		Method: method,
		URL: &url.URL{
			Scheme:   scheme,
			Host:     host,
			Path:     path,
			RawQuery: query.Encode(),
		},
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header: http.Header{
			"Content-Type": []string{"application/octet-stream"},
			"User-Agent":   []string{userAgent},
		},
		Body:          body,
		Host:          host,
		ContentLength: int64(len(data)),
	}); err != nil {
		err = errors.Wrapf(err, "%s %s://%s?%s", method, scheme, host, query.Encode())
		return
	}

	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		err = errors.Errorf("%s %s://%s?%s: %d %s", method, scheme, host, query.Encode(), res.StatusCode, res.Status)
		return
	}

	if ret, err = ioutil.ReadAll(res.Body); err != nil {
		err = errors.Wrapf(err, "%s %s://%s?%s", method, scheme, host, query.Encode())
		return
	}

	return
}
