package main

import "testing"

func TestEffectiveListen(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "127.0.0.1:8877"},
		{[]string{"--listen", "127.0.0.1:9001"}, "127.0.0.1:9001"},
		{[]string{"-listen=127.0.0.1:9002", "--listen=127.0.0.1:9003"}, "127.0.0.1:9003"},
		{[]string{"--listen", "127.0.0.1:9004", "-listen", "127.0.0.1:9005"}, "127.0.0.1:9005"},
		{[]string{"-data", "-listen", "--listen=127.0.0.1:9006"}, "127.0.0.1:9006"},
		{[]string{"--", "--listen=127.0.0.1:9007"}, "127.0.0.1:8877"},
		{[]string{"positional", "-listen=127.0.0.1:9008"}, "127.0.0.1:8877"},
	} {
		if got := effectiveListen(tc.args); got != tc.want {
			t.Errorf("%v: got %q want %q", tc.args, got, tc.want)
		}
	}
}
