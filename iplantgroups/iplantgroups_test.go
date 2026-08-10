package iplantgroups

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseID(t *testing.T) {
	tests := []struct {
		name     string
		username string
		want     string
	}{
		{name: "strips the DE domain suffix", username: "someone@iplantcollaborative.org", want: "someone"},
		{name: "no suffix is unchanged", username: "someone", want: "someone"},
		{name: "splits on the last @", username: "some.one@example.org@iplantcollaborative.org", want: "some.one@example.org"},
		{name: "empty stays empty", username: "", want: ""},
		{name: "a bare suffix yields an empty id", username: "@iplantcollaborative.org", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ParseID(tt.username))
		})
	}
}

func TestGetSubject(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		wantErr    bool
		wantErrMsg string
		wantEmail  string
	}{
		{
			name:      "returns the subject",
			status:    http.StatusOK,
			body:      `{"id":"someone","email":"someone@example.org","first_name":"Some","last_name":"One"}`,
			wantEmail: "someone@example.org",
		},
		{
			name:      "a subject with no email is not an error",
			status:    http.StatusOK,
			body:      `{"id":"someone"}`,
			wantEmail: "",
		},
		{
			// The standalone timelord treated a non-200 as success because it
			// wrapped a nil error, which produced notifications addressed to an
			// empty email. A non-2xx must be an error.
			name:       "a not-found response is an error",
			status:     http.StatusNotFound,
			body:       `{"error":"no such subject"}`,
			wantErr:    true,
			wantErrMsg: "404",
		},
		{
			name:       "a server error is an error",
			status:     http.StatusInternalServerError,
			body:       `boom`,
			wantErr:    true,
			wantErrMsg: "500",
		},
		{
			name:       "an unparseable body is an error",
			status:     http.StatusOK,
			body:       `not json`,
			wantErr:    true,
			wantErrMsg: "decoding",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath, gotUser string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				gotUser = r.URL.Query().Get("user")
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()

			client, err := New(server.URL, "de_grouper")
			require.NoError(t, err)

			subject, err := client.GetSubject(context.Background(), "someone@iplantcollaborative.org")

			assert.Equal(t, "/subjects/someone", gotPath, "the domain suffix is stripped from the path")
			assert.Equal(t, "de_grouper", gotUser, "iplant-groups requires the acting user on every request")

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				assert.Nil(t, subject)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, subject)
			assert.Equal(t, tt.wantEmail, subject.Email)
		})
	}
}

func TestGetSubjectPreservesBasePath(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{"id":"someone"}`))
	}))
	defer server.Close()

	client, err := New(server.URL+"/groups", "de_grouper")
	require.NoError(t, err)

	_, err = client.GetSubject(context.Background(), "someone")
	require.NoError(t, err)

	assert.Equal(t, "/groups/subjects/someone", gotPath,
		"a base URL with a path prefix must keep it")
}

func TestNewRejectsAnUnparseableBaseURL(t *testing.T) {
	_, err := New("http://[::1]:namedport", "de_grouper")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing iplant-groups base URL")
}
