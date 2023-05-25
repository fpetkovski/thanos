// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"os"
	"testing"

	"github.com/pkg/errors"

	"github.com/efficientgo/core/testutil"
)

func TestValidateConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  []byte
		err  error
	}{
		{
			name: "<nil> config",
			cfg:  nil,
			err:  errEmptyConfigurationFile,
		},
		{
			name: "empty config",
			cfg:  []byte(`[]`),
			err:  errEmptyConfigurationFile,
		},
		{
			name: "unparsable config",
			cfg:  []byte(`{x}`),
			err:  errParseConfigurationFile,
		},
		{
			name: "valid config with endpoints as strings",
			cfg: []byte(`
[{
  "endpoints": ["node-1"]
}]`),
			err: nil, // means it's valid.
		},
		{
			name: "valid config with endpoints as objects",
			cfg: []byte(`
[{
  "endpoints": [{"address": "node-1", "az": "eu-central-1a"}]
}]`),
			err: nil, // means it's valid.
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmpfile, err := os.CreateTemp("", "configwatcher_test.*.json")
			testutil.Ok(t, err)

			defer func() {
				testutil.Ok(t, os.Remove(tmpfile.Name()))
			}()

			_, err = tmpfile.Write(tc.cfg)
			testutil.Ok(t, err)

			err = tmpfile.Close()
			testutil.Ok(t, err)

			cw, err := NewConfigWatcher(nil, nil, tmpfile.Name(), 1)
			testutil.Ok(t, err)
			defer cw.Stop()

			if err := cw.ValidateConfig(); err != nil && !errors.Is(err, tc.err) {
				t.Errorf("case %q: got unexpected error: %v", tc.name, err)
			}
		})
	}
}
