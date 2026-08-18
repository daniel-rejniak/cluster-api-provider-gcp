/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package controllers implements controller types.
package controllers

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/pkg/errors"
	compute "google.golang.org/api/compute/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	infrav1 "sigs.k8s.io/cluster-api-provider-gcp/api/v1beta1"
	"sigs.k8s.io/cluster-api-provider-gcp/cloud/scope"
	"sigs.k8s.io/cluster-api-provider-gcp/util/reconciler"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/annotations"
	"sigs.k8s.io/cluster-api/util/predicates"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	machineTemplateCapacityRequeueAfter = 30 * time.Second
	gcpMachineTemplateKind              = "GCPMachineTemplate"
)

// GCPMachineTemplateReconciler reconciles GCPMachineTemplate objects.
//
// This controller populates template capacity to enable Cluster Autoscaler to
// scale a MachineDeployment from zero replicas.
type GCPMachineTemplateReconciler struct {
	client.Client
	ReconcileTimeout time.Duration
	WatchFilterValue string
}

// +kubebuilder:rbac:groups="",resources=events,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters;machinedeployments;machinesets,verbs=get;list;watch
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=gcpclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=gcpmachinetemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=gcpmachinetemplates/status,verbs=get;update;patch

// SetupWithManager sets up the controller with the Manager.
func (r *GCPMachineTemplateReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager, options controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(options).
		For(&infrav1.GCPMachineTemplate{}).
		WithEventFilter(predicates.ResourceNotPausedAndHasFilterLabel(mgr.GetScheme(), ctrl.LoggerFrom(ctx), r.WatchFilterValue)).
		Watches(
			&clusterv1.MachineDeployment{},
			handler.EnqueueRequestsFromMapFunc(machineDeploymentToGCPMachineTemplate),
		).
		Watches(
			&clusterv1.MachineSet{},
			handler.EnqueueRequestsFromMapFunc(machineSetToGCPMachineTemplate),
		).
		Complete(r)
}

func machineDeploymentToGCPMachineTemplate(_ context.Context, object client.Object) []ctrl.Request {
	md, ok := object.(*clusterv1.MachineDeployment)
	if !ok || md.Spec.Template.Spec.InfrastructureRef.Kind != gcpMachineTemplateKind {
		return nil
	}
	return []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: md.Namespace, Name: md.Spec.Template.Spec.InfrastructureRef.Name}}}
}

func machineSetToGCPMachineTemplate(_ context.Context, object client.Object) []ctrl.Request {
	ms, ok := object.(*clusterv1.MachineSet)
	if !ok || ms.Spec.Template.Spec.InfrastructureRef.Kind != gcpMachineTemplateKind {
		return nil
	}
	return []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: ms.Namespace, Name: ms.Spec.Template.Spec.InfrastructureRef.Name}}}
}

// Reconcile populates capacity information for GCPMachineTemplate.
func (r *GCPMachineTemplateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, reterr error) {
	ctx, cancel := context.WithTimeout(ctx, reconciler.DefaultedLoopTimeout(r.ReconcileTimeout))
	defer cancel()

	logger := log.FromContext(ctx)
	template := &infrav1.GCPMachineTemplate{}
	if err := r.Get(ctx, req.NamespacedName, template); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !template.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	instanceType := template.Spec.Template.Spec.InstanceType
	if instanceType == "" || len(template.Status.Capacity) > 0 {
		return ctrl.Result{}, nil
	}

	cluster, err := util.GetOwnerCluster(ctx, r.Client, template.ObjectMeta)
	if err != nil {
		return ctrl.Result{}, err
	}
	if cluster == nil {
		logger.Info("GCPMachineTemplate does not yet have an owner Cluster")
		return ctrl.Result{}, nil
	}

	if annotations.IsPaused(cluster, template) {
		logger.Info("GCPMachineTemplate or owner Cluster is paused")
		return ctrl.Result{}, nil
	}

	if cluster.Spec.InfrastructureRef.Kind != "GCPCluster" || cluster.Spec.InfrastructureRef.Name == "" {
		return ctrl.Result{}, nil
	}

	gcpCluster := &infrav1.GCPCluster{}
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Spec.InfrastructureRef.Name,
	}, gcpCluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: machineTemplateCapacityRequeueAfter}, nil
		}
		return ctrl.Result{}, errors.Wrap(err, "failed to get GCPCluster")
	}

	clusterScope, err := scope.NewClusterScope(ctx, scope.ClusterScopeParams{
		Client:     r.Client,
		Cluster:    cluster,
		GCPCluster: gcpCluster,
	})
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to create cluster scope")
	}
	defer func() {
		if err := clusterScope.Close(ctx); err != nil && reterr == nil {
			reterr = err
		}
	}()

	failureDomains := clusterScope.FailureDomains()
	if len(failureDomains) == 0 {
		logger.Info("GCPCluster has no discovered failure domains yet")
		return ctrl.Result{RequeueAfter: machineTemplateCapacityRequeueAfter}, nil
	}
	sort.Strings(failureDomains)
	zone := failureDomains[0]

	capacity, err := getMachineTypeCapacity(ctx, clusterScope.Compute, clusterScope.Project(), zone, instanceType)
	if err != nil {
		return ctrl.Result{}, err
	}

	original := template.DeepCopy()
	template.Status.Capacity = capacity
	if err := r.Status().Patch(ctx, template, client.MergeFrom(original)); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to patch GCPMachineTemplate status")
	}

	logger.Info("Populated GCPMachineTemplate capacity", "instanceType", instanceType, "zone", zone, "capacity", template.Status.Capacity)
	return ctrl.Result{}, nil
}

func getMachineTypeCapacity(ctx context.Context, computeService *compute.Service, project, zone, instanceType string) (corev1.ResourceList, error) {
	machineType, err := computeService.MachineTypes.Get(project, zone, instanceType).Context(ctx).Do()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get machine type %q in zone %q", instanceType, zone)
	}
	capacity, err := machineTypeCapacity(machineType)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid capacity for machine type %q", instanceType)
	}
	return capacity, nil
}

func machineTypeCapacity(machineType *compute.MachineType) (corev1.ResourceList, error) {
	if machineType.GuestCpus <= 0 || machineType.MemoryMb <= 0 {
		return nil, fmt.Errorf("cpu=%d memoryMb=%d", machineType.GuestCpus, machineType.MemoryMb)
	}

	return corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewQuantity(machineType.GuestCpus, resource.DecimalSI),
		corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dMi", machineType.MemoryMb)),
	}, nil
}
