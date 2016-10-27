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

func (c *Client) CreateTopic(topic string) (err error) {
	var ret io.ReadCloser

	if ret, err = c.do("POST", "/topic/create", url.Values{"topic": []string{topic}}, nil); err != nil {
		return
	}

	ret.Close()
	return
}

func (c *Client) do(method string, path string, query url.Values, data []byte) (ret io.ReadCloser, err error) {
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
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{"User-Agent": []string{userAgent}},
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

	ret = res.Body
	return
}
