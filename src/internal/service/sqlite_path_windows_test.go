//go:build windows

package service

import (
	"net/url"
	"testing"
)

func TestWindowsSQLiteFileURI(t *testing.T) {
	for _, tc := range []struct{ path, want string }{
		{`C:\Users\user\Agent Chat #1\agentchat.db`, "file:///C:/Users/user/Agent%20Chat%20%231/agentchat.db"},
		{`\\server\share\agentchat.db`, "file:////server/share/agentchat.db"},
	} {
		u := url.URL{Scheme: "file", Path: sqliteURIPath(tc.path)}
		if got := u.String(); got != tc.want {
			t.Errorf("%q: got %q, want %q", tc.path, got, tc.want)
		}
	}
}
