package system

import (
	"context"
	"fmt"
	"runtime"

	"github.com/acobaugh/osrelease"
	"github.com/docker/docker/pkg/parsers/kernel"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/kubernetes"
)

type Information struct {
	Version    string         `json:"version"`
	Kubernetes KubernetesInfo `json:"kubernetes"`
	System     System         `json:"system"`
}

type KubernetesInfo struct {
	Nodes     []NodeInfo        `json:"nodes"`
	PodStatus map[string]string `json:"pod_status"`
	Version   version.Info      `json:"version"`
}

type NodeInfo struct {
	Name    string `json:"name"`
	PodsNum int64  `json:"pods_num"`
}

type PodInfo struct {
	Name   string          `json:"name"`
	Status corev1.PodPhase `json:"status"`
}

type System struct {
	Architecture  string `json:"architecture"`
	CPUThreads    int    `json:"cpu_threads"`
	MemoryBytes   uint64 `json:"memory_bytes"`
	KernelVersion string `json:"kernel_version"`
	OS            string `json:"os"`
	OSType        string `json:"os_type"`
}

func GetSystemInformation(clientset *kubernetes.Clientset, namespace string) (*Information, error) {
	k, err := kernel.GetKernelVersion()
	if err != nil {
		return nil, err
	}

	info, err := getKubernetesInfo(context.Background(), clientset, namespace)
	if err != nil {
		return nil, err
	}

	release, err := osrelease.Read()
	if err != nil {
		return nil, err
	}

	var os string
	if release["PRETTY_NAME"] != "" {
		os = release["PRETTY_NAME"]
	} else if release["NAME"] != "" {
		os = release["NAME"]
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	systemMemory := memStats.Sys

	return &Information{
		Version: Version,
		Kubernetes: KubernetesInfo{
			Nodes:     info.Nodes,
			PodStatus: info.PodStatus,
			Version:   info.Version,
		},
		System: System{
			Architecture:  runtime.GOARCH,
			CPUThreads:    runtime.NumCPU(),
			MemoryBytes:   systemMemory,
			KernelVersion: k.String(),
			OS:            os,
			OSType:        runtime.GOOS,
		},
	}, nil
}

func getKubernetesInfo(ctx context.Context, clientset *kubernetes.Clientset, namespace string) (*KubernetesInfo, error) {
	// TODO: Change method to get namespace string
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("error getting pods: %v", err)
	}

	info, err := clientset.Discovery().ServerVersion()
	if err != nil {
		panic(err.Error())
	}

	kubeInfo := &KubernetesInfo{
		PodStatus: getPodStatuses(pods),
		Version: version.Info{
			Major:        info.Major,
			Minor:        info.Minor,
			GitVersion:   info.GitVersion,
			GitCommit:    info.GitCommit,
			GitTreeState: info.GitTreeState,
			BuildDate:    info.BuildDate,
			GoVersion:    info.GoVersion,
			Compiler:     info.Compiler,
			Platform:     info.Platform,
		},
	}

	return kubeInfo, nil
}

func getPodStatuses(pods *corev1.PodList) map[string]string {
	podStatuses := make(map[string]string)
	for _, pod := range pods.Items {
		podStatuses[pod.Name] = string(pod.Status.Phase)
	}
	return podStatuses
}
