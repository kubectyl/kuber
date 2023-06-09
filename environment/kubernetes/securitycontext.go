package kubernetes

import (
	"emperror.dev/errors"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/pointer"

	"github.com/imdario/mergo"
)

func (e *Environment) ConfigurePodSpecForRestrictedStandard(podSpec *corev1.PodSpec) error {
	podSecurityContext := corev1.PodSpec{
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot: pointer.Bool(true),
			SeccompProfile: &corev1.SeccompProfile{
				Type: corev1.SeccompProfileTypeRuntimeDefault,
			},
		},
	}

	containerSecurityContext := corev1.Container{
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: pointer.Bool(false),
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"},
			},
		},
	}

	for i, container := range podSpec.Containers {
		if err := mergo.Merge(&container, containerSecurityContext, mergo.WithOverride); err != nil {
			return err
		}
		podSpec.Containers[i] = container
	}

	for i, initContainer := range podSpec.InitContainers {
		if err := mergo.Merge(&initContainer, containerSecurityContext, mergo.WithOverride); err != nil {
			return err
		}
		podSpec.InitContainers[i] = initContainer
	}

	if err := mergo.Merge(podSpec, podSecurityContext, mergo.WithOverride); err != nil {
		return errors.Wrap(err, "failed to merge pod security context")
	}

	return nil
}
