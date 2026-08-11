// Package iplantgroups provides a client for the DE's iplant-groups service,
// used here to resolve a username to the profile fields (notably the email
// address) needed when notifying a user about their analysis.
package iplantgroups

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cyverse-de/app-exposer/common"
	"github.com/sirupsen/logrus"
)

var log = common.Log.WithFields(logrus.Fields{"package": "iplantgroups"})

// DefaultTimeout bounds a single iplant-groups lookup.
const DefaultTimeout = 30 * time.Second

// Subject is the subset of an iplant-groups subject record that the DE needs.
// iplant-groups calls users "subjects"; ID is the bare username.
type Subject struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	FirstName   string `json:"first_name"`
	LastName    string `json:"last_name"`
	Email       string `json:"email"`
	Institution string `json:"institution"`
	SourceID    string `json:"source_id"`
}

// Client looks up subjects in the iplant-groups service.
type Client struct {
	baseURL    *url.URL
	actAsUser  string
	httpClient *http.Client
}

// New returns a Client for the iplant-groups service at baseURL. actAsUser is
// the privileged Grouper account the lookups are performed as; iplant-groups
// requires it on every request.
func New(baseURL, actAsUser string) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parsing iplant-groups base URL %q: %w", baseURL, err)
	}
	return &Client{
		baseURL:    u,
		actAsUser:  actAsUser,
		httpClient: &http.Client{Timeout: DefaultTimeout},
	}, nil
}

// GetSubject returns the subject record for the given username. The username
// may carry the DE's domain suffix; only the portion to the left of the last
// "@" is meaningful to iplant-groups.
func (c *Client) GetSubject(ctx context.Context, username string) (*Subject, error) {
	id := ParseID(username)

	reqURL := c.baseURL.JoinPath("subjects", id)
	q := reqURL.Query()
	q.Set("user", c.actAsUser)
	reqURL.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("building subject lookup request for %s: %w", id, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("looking up subject %s: %w", id, err)
	}
	defer common.CloseBody(resp)

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("subject lookup for %s returned %s", id, resp.Status)
	}

	var subject Subject
	if err := json.NewDecoder(resp.Body).Decode(&subject); err != nil {
		return nil, fmt.Errorf("decoding subject lookup response for %s: %w", id, err)
	}

	if subject.Email == "" {
		log.Warnf("iplant-groups returned no email address for %s; notifications for this user cannot be emailed", id)
	}

	return &subject, nil
}

// ParseID reduces a DE username to the bare identifier iplant-groups expects:
// everything to the left of the last "@". Usernames without a suffix are
// returned unchanged.
func ParseID(username string) string {
	idx := strings.LastIndex(username, "@")
	if idx < 0 {
		return username
	}
	return username[:idx]
}
