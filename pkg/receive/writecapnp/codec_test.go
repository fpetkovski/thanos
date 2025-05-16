package writecapnp_test

import (
	"bytes"
	"fmt"
	"testing"

	"capnproto.org/go/capnp/v3"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestCodec(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	codec, err := writecapnp.NewZSTDCodec(nopCloser{buffer})
	require.NoError(t, err)

	arena := capnp.SingleSegment(nil)
	defer arena.Release()
	msg, err := makeMessage(arena)
	require.NoError(t, err)

	for range 100 {
		require.NoError(t, codec.Encode(msg))
	}

	decoder, err := writecapnp.NewZSTDCodec(nopCloser{bytes.NewBuffer(buffer.Bytes())})
	require.NoError(t, err)
	for range 100 {
		recv, err := decoder.Decode()
		require.NoError(t, err)
		root, err := writecapnp.ReadRootWriteRequest(recv)
		require.NoError(t, err)
		req, err := writecapnp.NewRequest(root)
		require.NoError(t, err)
		series := writecapnp.Series{}
		require.True(t, req.Next())
		require.NoError(t, req.At(&series))

		require.Equal(t, "test-name", series.Labels[0].Name)
		require.Equal(t, "test-val", series.Labels[0].Value)

		require.False(t, req.Next())
	}
}

func makeMessage(arena *capnp.SingleSegmentArena) (*capnp.Message, error) {
	msg, seg, err := capnp.NewMessage(arena)
	if err != nil {
		return nil, err
	}
	wr, err := writecapnp.NewRootWriteRequest(seg)
	if err != nil {
		return nil, err
	}
	if err := writecapnp.BuildInto(wr, "test", []prompb.TimeSeries{{
		Labels:  []labelpb.ZLabel{{Name: "test-name", Value: "test-val"}},
		Samples: []prompb.Sample{{Value: 1, Timestamp: 2}},
	}}); err != nil {
		return nil, err
	}
	return msg, nil
}

type nopCloser struct {
	*bytes.Buffer
}

func (c nopCloser) Read(b []byte) (int, error) {
	n, err := c.Buffer.Read(b)
	fmt.Println(err)
	return n, err
}

func (c nopCloser) Close() error {
	fmt.Println(c.Buffer.String())
	return nil
}
