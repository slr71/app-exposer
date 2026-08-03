package operator

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// testCAPEM returns a self-signed certificate in PEM form, so the validation
// test exercises the real parse rather than a stand-in string.
func testCAPEM(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func TestValidateCABundle(t *testing.T) {
	const ns = "vice-apps"
	caPEM := testCAPEM(t)

	for _, tt := range []struct {
		name       string
		existing   *apiv1.ConfigMap
		configMap  string
		key        string
		wantErrMsg string // empty means the bundle must validate
	}{
		{
			name:      "valid with default key",
			existing:  &apiv1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "local-ca", Namespace: ns}, Data: map[string]string{"ca.crt": caPEM}},
			configMap: "local-ca",
		},
		{
			name:      "valid with explicit key",
			existing:  &apiv1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "local-ca", Namespace: ns}, Data: map[string]string{"bundle.pem": caPEM}},
			configMap: "local-ca",
			key:       "bundle.pem",
		},
		{
			name:      "valid from binary data",
			existing:  &apiv1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "local-ca", Namespace: ns}, BinaryData: map[string][]byte{"ca.crt": []byte(caPEM)}},
			configMap: "local-ca",
		},
		{
			name:       "missing configmap",
			configMap:  "local-ca",
			wantErrMsg: "does not exist",
		},
		{
			name:       "wrong namespace",
			existing:   &apiv1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "local-ca", Namespace: "somewhere-else"}, Data: map[string]string{"ca.crt": caPEM}},
			configMap:  "local-ca",
			wantErrMsg: "does not exist",
		},
		{
			name:       "mistyped key",
			existing:   &apiv1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "local-ca", Namespace: ns}, Data: map[string]string{"ca.crt": caPEM}},
			configMap:  "local-ca",
			key:        "ca.pem",
			wantErrMsg: `has no key "ca.pem"`,
		},
		{
			name:       "key holds no certificates",
			existing:   &apiv1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "local-ca", Namespace: ns}, Data: map[string]string{"ca.crt": "not a certificate"}},
			configMap:  "local-ca",
			wantErrMsg: "no PEM-encoded certificates",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var clientset *fake.Clientset
			if tt.existing != nil {
				clientset = fake.NewSimpleClientset(tt.existing)
			} else {
				clientset = fake.NewSimpleClientset()
			}

			err := ValidateCABundle(context.Background(), clientset, ns, tt.configMap, tt.key)
			if tt.wantErrMsg == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErrMsg)
		})
	}
}

// TestValidateCABundleListsAvailableKeys keeps the mistyped-key error useful:
// it must name what the ConfigMap actually holds, from both data sections.
func TestValidateCABundleListsAvailableKeys(t *testing.T) {
	const ns = "vice-apps"
	clientset := fake.NewSimpleClientset(&apiv1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "local-ca", Namespace: ns},
		Data:       map[string]string{"root.pem": "", "intermediate.pem": ""},
		BinaryData: map[string][]byte{"extra.der": nil},
	})

	err := ValidateCABundle(context.Background(), clientset, ns, "local-ca", "ca.crt")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "it holds: intermediate.pem, root.pem, extra.der")
}
