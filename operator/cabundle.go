package operator

import (
	"context"
	"crypto/x509"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/cyverse-de/app-exposer/vicebuild"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ValidateCABundle confirms the ConfigMap that analysis pods mount for
// vice-proxy's trust store exists in namespace and holds a parseable PEM
// bundle under key. The operator never reads this ConfigMap itself — the
// kubelet mounts it — so without a startup check a missing ConfigMap or a
// mistyped key first surfaces as users' analyses wedged in ContainerCreating.
// An empty key resolves to the conventional ca.crt.
func ValidateCABundle(ctx context.Context, clientset kubernetes.Interface, namespace, configMapName, key string) error {
	key = vicebuild.CABundleKeyOrDefault(key)

	cm, err := clientset.CoreV1().ConfigMaps(namespace).Get(ctx, configMapName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("ConfigMap %s/%s does not exist; --ca-bundle-configmap must name a ConfigMap in the analysis namespace", namespace, configMapName)
	}
	if err != nil {
		return fmt.Errorf("reading ConfigMap %s/%s: %w", namespace, configMapName, err)
	}

	pem, ok := cm.Data[key]
	if !ok {
		if raw, binOK := cm.BinaryData[key]; binOK {
			pem = string(raw)
		} else {
			present := slices.Sorted(maps.Keys(cm.Data))
			present = append(present, slices.Sorted(maps.Keys(cm.BinaryData))...)
			return fmt.Errorf("ConfigMap %s/%s has no key %q (--ca-bundle-key); it holds: %s",
				namespace, configMapName, key, strings.Join(present, ", "))
		}
	}

	if !x509.NewCertPool().AppendCertsFromPEM([]byte(pem)) {
		return fmt.Errorf("key %q in ConfigMap %s/%s holds no PEM-encoded certificates", key, namespace, configMapName)
	}

	return nil
}
