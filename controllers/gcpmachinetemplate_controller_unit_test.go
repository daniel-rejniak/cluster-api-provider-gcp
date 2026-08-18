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

package controllers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/onsi/gomega"
	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
)

func TestMachineDeploymentAndMachineSetToGCPMachineTemplate(t *testing.T) {
	g := gomega.NewWithT(t)

	md := &clusterv1.MachineDeployment{ObjectMeta: metav1.ObjectMeta{Name: "workers", Namespace: "default"}}
	md.Spec.Template.Spec.InfrastructureRef.Kind = "GCPMachineTemplate"
	md.Spec.Template.Spec.InfrastructureRef.Name = "worker-template"
	ms := &clusterv1.MachineSet{ObjectMeta: metav1.ObjectMeta{Name: "workers-set", Namespace: "default"}}
	ms.Spec.Template.Spec.InfrastructureRef.Kind = "GCPMachineTemplate"
	ms.Spec.Template.Spec.InfrastructureRef.Name = "worker-template"
	expected := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "worker-template"}}

	g.Expect(machineDeploymentToGCPMachineTemplate(context.Background(), md)).To(gomega.ConsistOf(expected))
	g.Expect(machineSetToGCPMachineTemplate(context.Background(), ms)).To(gomega.ConsistOf(expected))
	g.Expect(machineDeploymentToGCPMachineTemplate(context.Background(), &clusterv1.MachineDeployment{})).To(gomega.BeEmpty())
}

func TestGetMachineTypeCapacity(t *testing.T) {
	g := gomega.NewWithT(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.Expect(r.URL.Path).To(gomega.Equal("/projects/test-project/zones/us-central1-a/machineTypes/n2-standard-4"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"guestCpus":4,"memoryMb":16384}`))
	}))
	defer server.Close()

	service, err := compute.NewService(context.Background(), option.WithEndpoint(server.URL), option.WithoutAuthentication())
	g.Expect(err).NotTo(gomega.HaveOccurred())
	capacity, err := getMachineTypeCapacity(context.Background(), service, "test-project", "us-central1-a", "n2-standard-4")
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(capacity.Cpu().String()).To(gomega.Equal("4"))
	g.Expect(capacity.Memory().String()).To(gomega.Equal("16Gi"))
}

func TestMachineTypeCapacity(t *testing.T) {
	g := gomega.NewWithT(t)

	capacity, err := machineTypeCapacity(&compute.MachineType{GuestCpus: 4, MemoryMb: 16384})

	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(capacity.Cpu().String()).To(gomega.Equal("4"))
	g.Expect(capacity.Memory().String()).To(gomega.Equal("16Gi"))
}

func TestMachineTypeCapacityRejectsInvalidValues(t *testing.T) {
	g := gomega.NewWithT(t)

	_, err := machineTypeCapacity(&compute.MachineType{GuestCpus: 0, MemoryMb: 16384})

	g.Expect(err).To(gomega.HaveOccurred())
}
