package incluster

import (
	"context"

	"github.com/cyverse-de/model/v8"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func (i *Incluster) createPodDisruptionBudget(ctx context.Context, analysis *model.Analysis) (*policyv1.PodDisruptionBudget, error) {
	labels, err := i.labelsFromJob(ctx, analysis)
	if err != nil {
		return nil, err
	}

	pdb := policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:   analysis.InvocationID,
			Labels: labels,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"external-id": analysis.InvocationID,
				},
			},
			MaxUnavailable: &intstr.IntOrString{
				Type:   intstr.Int,
				IntVal: 0,
			},
		},
	}

	return &pdb, nil
}
