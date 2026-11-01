package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	hclient "github.com/cloudwego/hertz/pkg/app/client"
	"github.com/cloudwego/hertz/pkg/protocol"
)

// roundTripper sends each request through a Hertz client.
type roundTripper struct {
	c *hclient.Client
}

// Compile-time check that roundTripper implements http.RoundTripper.
var _ http.RoundTripper = (*roundTripper)(nil)

// RoundTripper returns an http.RoundTripper that sends each request through c,
// so a vendor SDK that takes an http.RoundTripper gets the same retry,
// timeouts, and telemetry as a call to c.Do. Use it as
// &http.Client{Transport: RoundTripper(c)}.
//
// c should come from New. A Hertz client that New did not build has no
// policy: no retry, no per-attempt deadline, and no telemetry.
//
// Do not add retry on top of the returned RoundTripper. The client already
// retries, and another layer would multiply the attempts and send
// non-idempotent requests again.
//
// Request and response bodies are buffered, not streamed. The request body is
// read in full before the call, so it can be sent again on a retry, and the
// response body is copied out before RoundTrip returns. Errors are returned
// unchanged, so errors.Is(err, context.DeadlineExceeded) still matches a
// timeout through the *url.Error that net/http wraps around it.
func RoundTripper(c *hclient.Client) http.RoundTripper {
	return &roundTripper{c: c}
}

// RoundTrip sends req through the Hertz client and returns the response. The
// request body is always closed, on success and on error, as the
// http.RoundTripper contract requires. Response headers keep every value, and
// Content-Encoding is left as the vendor sent it.
func (rt *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := readBody(req)
	if err != nil {
		return nil, err
	}
	if req.URL == nil {
		return nil, errors.New("rest/client: request URL is nil")
	}

	hreq := protocol.AcquireRequest()
	hresp := protocol.AcquireResponse()
	defer protocol.ReleaseRequest(hreq)
	defer protocol.ReleaseResponse(hresp)

	hreq.SetRequestURI(req.URL.String())
	hreq.SetMethod(req.Method)
	// Hertz adds a form Content-Type to a POST without one. net/http sends
	// none, so the vendor must see only the headers the caller set.
	hreq.Header.SetNoDefaultContentType(true)
	copyHeader(hreq, req.Header)
	// Header.SetHost sets only the Host header. The dial target is the host of
	// the absolute request URI, which SetRequestURI has already parsed.
	if req.Host != "" {
		hreq.Header.SetHost(req.Host)
	}
	if len(body) > 0 {
		hreq.SetBody(body)
	}

	if err := rt.c.Do(req.Context(), hreq, hresp); err != nil {
		return nil, err
	}
	return httpResponse(req, hresp), nil
}

// readBody reads all of req.Body and closes it, whether or not the read
// succeeds. A nil body gives no bytes.
func readBody(req *http.Request) ([]byte, error) {
	if req.Body == nil {
		return nil, nil
	}
	defer func() { _ = req.Body.Close() }()
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, fmt.Errorf("rest/client: read request body: %w", err)
	}
	return body, nil
}

// copyHeader adds every value of h to hreq. It leaves out the headers that
// Hertz sets itself, as net/http does. Host comes from req.Host. Content-Length
// is computed from the buffered body, and Transfer-Encoding is managed by Hertz,
// so a copied value could only be wrong. Trailer is left out as well.
//
// The trace context headers are left out too. A global propagator may have
// injected them into the caller's request, and the client must not send them
// to a vendor, as it sends no trace context on Do either.
func copyHeader(hreq *protocol.Request, h http.Header) {
	for k, vs := range h {
		switch http.CanonicalHeaderKey(k) {
		case "Host", "Content-Length", "Transfer-Encoding", "Trailer",
			"Traceparent", "Tracestate", "Baggage":
			continue
		}
		for _, v := range vs {
			hreq.Header.Add(k, v)
		}
	}
}

// httpResponse builds the net/http response for hresp. It copies the body and
// the header values out, because the caller releases hresp when RoundTrip
// returns. For a HEAD request the body is http.NoBody, and ContentLength is the
// Content-Length the vendor sent, as net/http reports it, or -1 when there is
// none.
func httpResponse(req *http.Request, hresp *protocol.Response) *http.Response {
	code := hresp.StatusCode()
	status := strconv.Itoa(code)
	if text := http.StatusText(code); text != "" {
		status += " " + text
	}
	header := make(http.Header)
	hresp.Header.VisitAll(func(k, v []byte) {
		header.Add(string(k), string(v))
	})
	body := bytes.Clone(hresp.Body())
	resp := &http.Response{
		Status:        status,
		StatusCode:    code,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
	if req.Method == http.MethodHead {
		// A HEAD response has no body, but its Content-Length is the length a GET
		// would have had, so it is kept. Hertz reports a missing length as a
		// negative number, which net/http writes as -1.
		resp.Body = http.NoBody
		resp.ContentLength = int64(max(hresp.Header.ContentLength(), -1))
	}
	return resp
}
