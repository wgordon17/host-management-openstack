/*
Copyright 2026.

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

package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	v1alpha1 "github.com/osac-project/bare-metal-fulfillment-operator/api/v1alpha1"
	"github.com/osac-project/host-management-openstack/internal/management"
	opv1alpha1 "github.com/osac-project/osac-operator/api/v1alpha1"
	"github.com/osac-project/osac-operator/pkg/provisioning"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	testNodeID    = "node-1"
	testNamespace = "default"
	testNoopTmpl  = "noop"
)

type mockManagementClient struct {
	getPowerStateFunc func(ctx context.Context, hostID string) (*management.PowerStatus, error)
	setPowerStateFunc func(ctx context.Context, hostID string, target management.PowerState) error
}

func (m *mockManagementClient) GetPowerState(ctx context.Context, hostID string) (*management.PowerStatus, error) {
	if m.getPowerStateFunc != nil {
		return m.getPowerStateFunc(ctx, hostID)
	}
	return &management.PowerStatus{State: management.PowerOff}, nil
}

func (m *mockManagementClient) SetPowerState(ctx context.Context, hostID string, target management.PowerState) error {
	if m.setPowerStateFunc != nil {
		return m.setPowerStateFunc(ctx, hostID, target)
	}
	return nil
}

func boolPtr(b bool) *bool {
	return &b
}

var _ = Describe("HostLeaseReconciler", func() {
	var (
		reconciler *HostLeaseReconciler
		mockMgmt   *mockManagementClient
		testScheme *runtime.Scheme
		log        = zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true))
	)

	BeforeEach(func() {
		logf.SetLogger(log)
		testScheme = runtime.NewScheme()
		Expect(v1alpha1.AddToScheme(testScheme)).To(Succeed())

		mockMgmt = &mockManagementClient{}
		reconciler = &HostLeaseReconciler{
			Scheme:           testScheme,
			ManagementClient: mockMgmt,
			RecheckInterval:  10 * time.Second,
		}
	})

	Describe("NewHostLeaseReconciler", func() {
		It("should use the provided recheck interval when positive", func() {
			r := NewHostLeaseReconciler(nil, testScheme, mockMgmt, nil, 30*time.Second)
			Expect(r.RecheckInterval).To(Equal(30 * time.Second))
		})

		It("should use the default recheck interval when zero", func() {
			r := NewHostLeaseReconciler(nil, testScheme, mockMgmt, nil, 0)
			Expect(r.RecheckInterval).To(Equal(DefaultRecheckInterval))
		})

		It("should store the provisioning provider", func() {
			mockProvider := &provisioning.AAPProvider{}
			r := NewHostLeaseReconciler(nil, testScheme, mockMgmt, mockProvider, 0)
			Expect(r.ProvisioningProvider).To(Equal(mockProvider))
		})
	})

	Describe("validateOpenStackHost", func() {
		It("should return false when ExternalHostID is empty", func() {
			hostLease := &v1alpha1.HostLease{
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: "",
					HostClass:      hostClass,
				},
			}
			Expect(reconciler.validateOpenStackHost(hostLease, log)).To(BeFalse())
		})

		It("should return false when HostClass does not match", func() {
			hostLease := &v1alpha1.HostLease{
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: testNodeID,
					HostClass:      "other",
				},
			}
			Expect(reconciler.validateOpenStackHost(hostLease, log)).To(BeFalse())
		})

		It("should return true when ExternalHostID and HostClass are valid", func() {
			hostLease := &v1alpha1.HostLease{
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: testNodeID,
					HostClass:      hostClass,
				},
			}
			Expect(reconciler.validateOpenStackHost(hostLease, log)).To(BeTrue())
		})
	})

	Describe("reconcilePower", func() {
		var (
			ctx       context.Context
			hostLease *v1alpha1.HostLease
		)

		BeforeEach(func() {
			ctx = context.Background()
			hostLease = &v1alpha1.HostLease{
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: testNodeID,
					HostClass:      hostClass,
				},
			}
		})

		It("should power on when desired on and currently off", func() {
			hostLease.Spec.PoweredOn = boolPtr(true)
			powerStatus := &management.PowerStatus{State: management.PowerOff}

			var calledTarget management.PowerState
			mockMgmt.setPowerStateFunc = func(_ context.Context, _ string, target management.PowerState) error {
				calledTarget = target
				return nil
			}

			err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
			Expect(err).NotTo(HaveOccurred())
			Expect(calledTarget).To(Equal(management.PowerOn))
		})

		It("should power off when desired off and currently on", func() {
			hostLease.Spec.PoweredOn = boolPtr(false)
			powerStatus := &management.PowerStatus{State: management.PowerOn}

			var calledTarget management.PowerState
			mockMgmt.setPowerStateFunc = func(_ context.Context, _ string, target management.PowerState) error {
				calledTarget = target
				return nil
			}

			err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
			Expect(err).NotTo(HaveOccurred())
			Expect(calledTarget).To(Equal(management.PowerOff))
		})

		It("should not call SetPowerState when power state already matches (on)", func() {
			hostLease.Spec.PoweredOn = boolPtr(true)
			powerStatus := &management.PowerStatus{State: management.PowerOn}

			called := false
			mockMgmt.setPowerStateFunc = func(_ context.Context, _ string, _ management.PowerState) error {
				called = true
				return nil
			}

			err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
			Expect(err).NotTo(HaveOccurred())
			Expect(called).To(BeFalse())
		})

		It("should not call SetPowerState when power state already matches (off)", func() {
			hostLease.Spec.PoweredOn = boolPtr(false)
			powerStatus := &management.PowerStatus{State: management.PowerOff}

			called := false
			mockMgmt.setPowerStateFunc = func(_ context.Context, _ string, _ management.PowerState) error {
				called = true
				return nil
			}

			err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
			Expect(err).NotTo(HaveOccurred())
			Expect(called).To(BeFalse())
		})

		It("should not be called when PoweredOn is nil (guarded by Reconcile)", func() {
			hostLease.Spec.PoweredOn = boolPtr(true)
			powerStatus := &management.PowerStatus{State: management.PowerOn}

			err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should skip SetPowerState when node is transitioning", func() {
			hostLease.Spec.PoweredOn = boolPtr(true)
			powerStatus := &management.PowerStatus{State: management.PowerOff, IsTransitioning: true}

			called := false
			mockMgmt.setPowerStateFunc = func(_ context.Context, _ string, _ management.PowerState) error {
				called = true
				return nil
			}

			err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
			Expect(err).NotTo(HaveOccurred())
			Expect(called).To(BeFalse())
		})

		It("should not return error when SetPowerState returns ErrTransitioning", func() {
			hostLease.Spec.PoweredOn = boolPtr(true)
			powerStatus := &management.PowerStatus{State: management.PowerOff}

			mockMgmt.setPowerStateFunc = func(_ context.Context, _ string, _ management.PowerState) error {
				return fmt.Errorf("node %s: %w", testNodeID, management.ErrTransitioning)
			}

			err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should return error when SetPowerState fails on power on", func() {
			hostLease.Spec.PoweredOn = boolPtr(true)
			powerStatus := &management.PowerStatus{State: management.PowerOff}

			mockMgmt.setPowerStateFunc = func(_ context.Context, _ string, _ management.PowerState) error {
				return errors.New("ironic API error")
			}

			err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("ironic API error"))
		})

		It("should return error when SetPowerState fails on power off", func() {
			hostLease.Spec.PoweredOn = boolPtr(false)
			powerStatus := &management.PowerStatus{State: management.PowerOn}

			mockMgmt.setPowerStateFunc = func(_ context.Context, _ string, _ management.PowerState) error {
				return errors.New("ironic API error")
			}

			err := reconciler.reconcilePower(ctx, hostLease, powerStatus, log)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("ironic API error"))
		})
	})

	Describe("reconcileDelete", func() {
		It("should unset host class and remove finalizer on delete", func() {
			now := metav1.Now()
			hostLease := &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "hostlease-delete",
					Namespace:         testNamespace,
					Finalizers:        []string{hostLeaseFinalizer},
					DeletionTimestamp: &now,
				},
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: "node-delete",
					HostClass:      hostClass,
				},
			}
			reconciler.Client = fake.NewClientBuilder().
				WithScheme(testScheme).
				WithStatusSubresource(hostLease).
				WithObjects(hostLease).
				Build()

			getPowerStateCalls := 0
			setPowerStateCalls := 0
			mockMgmt.getPowerStateFunc = func(_ context.Context, _ string) (*management.PowerStatus, error) {
				getPowerStateCalls++
				return &management.PowerStatus{State: management.PowerOff}, nil
			}
			mockMgmt.setPowerStateFunc = func(_ context.Context, _ string, _ management.PowerState) error {
				setPowerStateCalls++
				return nil
			}

			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      hostLease.Name,
					Namespace: hostLease.Namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
			Expect(getPowerStateCalls).To(Equal(0))
			Expect(setPowerStateCalls).To(Equal(0))

			updatedHostLease := &v1alpha1.HostLease{}
			err = reconciler.Get(context.Background(), types.NamespacedName{
				Name:      hostLease.Name,
				Namespace: hostLease.Namespace,
			}, updatedHostLease)
			Expect(err).To(HaveOccurred())
			Expect(client.IgnoreNotFound(err)).To(Succeed())
		})

		It("should not clean up non-openstack hostClass on delete", func() {
			now := metav1.Now()
			hostLease := &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "hostlease-delete-non-openstack",
					Namespace:         testNamespace,
					Finalizers:        []string{hostLeaseFinalizer},
					DeletionTimestamp: &now,
				},
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: "node-delete-non-openstack",
					HostClass:      "other-provider",
				},
			}
			reconciler.Client = fake.NewClientBuilder().
				WithScheme(testScheme).
				WithObjects(hostLease).
				Build()

			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      hostLease.Name,
					Namespace: hostLease.Namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
			updatedHostLease := &v1alpha1.HostLease{}
			Expect(reconciler.Get(context.Background(), types.NamespacedName{
				Name:      hostLease.Name,
				Namespace: hostLease.Namespace,
			}, updatedHostLease)).To(Succeed())
			Expect(updatedHostLease.Spec.HostClass).To(Equal("other-provider"))
			Expect(controllerutil.ContainsFinalizer(updatedHostLease, hostLeaseFinalizer)).To(BeTrue())
		})

		It("should not clean up openstack hostClass with empty externalID on delete", func() {
			now := metav1.Now()
			hostLease := &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "hostlease-delete-openstack-no-externalid",
					Namespace:         testNamespace,
					Finalizers:        []string{hostLeaseFinalizer},
					DeletionTimestamp: &now,
				},
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: "",
					HostClass:      hostClass,
				},
			}
			reconciler.Client = fake.NewClientBuilder().
				WithScheme(testScheme).
				WithObjects(hostLease).
				Build()

			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      hostLease.Name,
					Namespace: hostLease.Namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
			updatedHostLease := &v1alpha1.HostLease{}
			Expect(reconciler.Get(context.Background(), types.NamespacedName{
				Name:      hostLease.Name,
				Namespace: hostLease.Namespace,
			}, updatedHostLease)).To(Succeed())
			Expect(updatedHostLease.Spec.HostClass).To(Equal(hostClass))
			Expect(controllerutil.ContainsFinalizer(updatedHostLease, hostLeaseFinalizer)).To(BeTrue())
		})
	})

	Describe("Reconcile", func() {
		It("should add finalizer for managed host lease", func() {
			hostLease := &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hostlease-add-finalizer",
					Namespace: testNamespace,
				},
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: "node-finalizer",
					HostClass:      hostClass,
				},
			}
			reconciler.Client = fake.NewClientBuilder().
				WithScheme(testScheme).
				WithObjects(hostLease).
				Build()

			getPowerStateCalls := 0
			mockMgmt.getPowerStateFunc = func(_ context.Context, _ string) (*management.PowerStatus, error) {
				getPowerStateCalls++
				return &management.PowerStatus{State: management.PowerOff}, nil
			}

			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      hostLease.Name,
					Namespace: hostLease.Namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{Requeue: true}))
			Expect(getPowerStateCalls).To(Equal(0))

			updatedHostLease := &v1alpha1.HostLease{}
			Expect(reconciler.Get(context.Background(), types.NamespacedName{
				Name:      hostLease.Name,
				Namespace: hostLease.Namespace,
			}, updatedHostLease)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(updatedHostLease, hostLeaseFinalizer)).To(BeTrue())
		})

		It("should skip power reconcile but sync status when PoweredOn is nil", func() {
			hostLease := &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hostlease-sample",
					Namespace: testNamespace,
					Finalizers: []string{
						hostLeaseFinalizer,
					},
				},
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: testNodeID,
					HostClass:      hostClass,
					PoweredOn:      nil,
				},
			}
			reconciler.Client = fake.NewClientBuilder().
				WithScheme(testScheme).
				WithStatusSubresource(hostLease).
				WithObjects(hostLease).
				Build()

			getPowerStateCalls := 0
			setPowerStateCalls := 0
			mockMgmt.getPowerStateFunc = func(_ context.Context, _ string) (*management.PowerStatus, error) {
				getPowerStateCalls++
				return &management.PowerStatus{State: management.PowerOff}, nil
			}
			mockMgmt.setPowerStateFunc = func(_ context.Context, _ string, _ management.PowerState) error {
				setPowerStateCalls++
				return nil
			}

			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      hostLease.Name,
					Namespace: hostLease.Namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
			Expect(getPowerStateCalls).To(Equal(1))
			Expect(setPowerStateCalls).To(Equal(0))

			updatedHostLease := &v1alpha1.HostLease{}
			Expect(reconciler.Get(context.Background(), types.NamespacedName{
				Name:      hostLease.Name,
				Namespace: hostLease.Namespace,
			}, updatedHostLease)).To(Succeed())
			Expect(updatedHostLease.Status.PoweredOn).NotTo(BeNil())
			Expect(*updatedHostLease.Status.PoweredOn).To(BeFalse())
			Expect(updatedHostLease.Status.Phase).To(Equal(v1alpha1.HostLeasePhaseReady))
			condition := updatedHostLease.GetStatusCondition(v1alpha1.HostConditionPowerSynced)
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(v1alpha1.HostConditionReasonPowerOff))
		})

		It("should update status when PoweredOn is set", func() {
			hostLease := &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hostlease-managed",
					Namespace: testNamespace,
					Finalizers: []string{
						hostLeaseFinalizer,
					},
				},
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: "node-2",
					HostClass:      hostClass,
					PoweredOn:      boolPtr(false),
				},
			}
			reconciler.Client = fake.NewClientBuilder().
				WithScheme(testScheme).
				WithStatusSubresource(hostLease).
				WithObjects(hostLease).
				Build()

			mockMgmt.getPowerStateFunc = func(_ context.Context, _ string) (*management.PowerStatus, error) {
				return &management.PowerStatus{State: management.PowerOff}, nil
			}
			mockMgmt.setPowerStateFunc = func(_ context.Context, _ string, _ management.PowerState) error {
				Fail("SetPowerState should not be called when power already matches desired")
				return nil
			}

			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      hostLease.Name,
					Namespace: hostLease.Namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			updatedHostLease := &v1alpha1.HostLease{}
			Expect(reconciler.Get(context.Background(), types.NamespacedName{
				Name:      hostLease.Name,
				Namespace: hostLease.Namespace,
			}, updatedHostLease)).To(Succeed())
			Expect(updatedHostLease.Status.Phase).To(Equal(v1alpha1.HostLeasePhaseReady))
			Expect(updatedHostLease.Status.PoweredOn).NotTo(BeNil())
			Expect(*updatedHostLease.Status.PoweredOn).To(BeFalse())
			condition := updatedHostLease.GetStatusCondition(v1alpha1.HostConditionPowerSynced)
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(v1alpha1.HostConditionReasonPowerOff))
		})

		It("should requeue when power is not yet converged", func() {
			requeueInterval := 7 * time.Second
			reconciler.RecheckInterval = requeueInterval

			desiredOn := true
			hostLease := &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hostlease-requeue",
					Namespace: testNamespace,
					Finalizers: []string{
						hostLeaseFinalizer,
					},
				},
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: "node-requeue",
					HostClass:      hostClass,
					PoweredOn:      &desiredOn,
				},
			}
			reconciler.Client = fake.NewClientBuilder().
				WithScheme(testScheme).
				WithStatusSubresource(hostLease).
				WithObjects(hostLease).
				Build()

			mockMgmt.getPowerStateFunc = func(_ context.Context, _ string) (*management.PowerStatus, error) {
				return &management.PowerStatus{State: management.PowerOff}, nil
			}
			mockMgmt.setPowerStateFunc = func(_ context.Context, _ string, _ management.PowerState) error {
				return nil
			}

			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      hostLease.Name,
					Namespace: hostLease.Namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(requeueInterval))

			updatedHostLease := &v1alpha1.HostLease{}
			Expect(reconciler.Get(context.Background(), types.NamespacedName{
				Name:      hostLease.Name,
				Namespace: hostLease.Namespace,
			}, updatedHostLease)).To(Succeed())
			Expect(updatedHostLease.Status.Phase).To(Equal(v1alpha1.HostLeasePhaseProgressing))
		})
	})

	Describe("hostLeasePredicate", func() {
		var p predicate.Funcs

		BeforeEach(func() {
			p = hostLeasePredicate()
		})

		It("should allow create events", func() {
			e := event.CreateEvent{
				Object: &v1alpha1.HostLease{},
			}
			Expect(p.Create(e)).To(BeTrue())
		})

		It("should allow delete events", func() {
			e := event.DeleteEvent{
				Object: &v1alpha1.HostLease{},
			}
			Expect(p.Delete(e)).To(BeTrue())
		})

		It("should block generic events", func() {
			e := event.GenericEvent{
				Object: &v1alpha1.HostLease{},
			}
			Expect(p.Generic(e)).To(BeFalse())
		})

		It("should allow update when generation changes", func() {
			oldObj := &v1alpha1.HostLease{}
			oldObj.Generation = 1
			newObj := &v1alpha1.HostLease{}
			newObj.Generation = 2

			e := event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}
			Expect(p.Update(e)).To(BeTrue())
		})

		It("should block update when only status changes (same generation)", func() {
			oldObj := &v1alpha1.HostLease{}
			oldObj.Generation = 1
			newObj := &v1alpha1.HostLease{}
			newObj.Generation = 1
			newObj.Status.Phase = v1alpha1.HostLeasePhaseReady

			e := event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}
			Expect(p.Update(e)).To(BeFalse())
		})

		It("should allow update when deletionTimestamp is newly set", func() {
			now := metav1.Now()
			oldObj := &v1alpha1.HostLease{}
			oldObj.Generation = 1
			newObj := &v1alpha1.HostLease{}
			newObj.Generation = 1
			newObj.DeletionTimestamp = &now

			e := event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}
			Expect(p.Update(e)).To(BeTrue())
		})

		It("should block update when deletionTimestamp was already set", func() {
			now := metav1.Now()
			oldObj := &v1alpha1.HostLease{}
			oldObj.Generation = 1
			oldObj.DeletionTimestamp = &now
			newObj := &v1alpha1.HostLease{}
			newObj.Generation = 1
			newObj.DeletionTimestamp = &now

			e := event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}
			Expect(p.Update(e)).To(BeFalse())
		})
	})

	Describe("reconcileProvisioning", func() {
		It("should skip provisioning when ProvisioningProvider is nil", func() {
			hostLease := &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "hostlease-no-aap",
					Namespace:  testNamespace,
					Finalizers: []string{hostLeaseFinalizer},
				},
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: testNodeID,
					HostClass:      hostClass,
					TemplateID:     "image-provision",
				},
			}
			reconciler.Client = fake.NewClientBuilder().
				WithScheme(testScheme).
				WithStatusSubresource(hostLease).
				WithObjects(hostLease).
				Build()
			reconciler.ProvisioningProvider = nil

			mockMgmt.getPowerStateFunc = func(_ context.Context, _ string) (*management.PowerStatus, error) {
				return &management.PowerStatus{State: management.PowerOff}, nil
			}

			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      hostLease.Name,
					Namespace: hostLease.Namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
		})

		It("should skip provisioning when templateID is noop", func() {
			hostLease := &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "hostlease-noop",
					Namespace:  testNamespace,
					Finalizers: []string{hostLeaseFinalizer},
				},
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: testNodeID,
					HostClass:      hostClass,
					TemplateID:     testNoopTmpl,
				},
			}
			reconciler.ProvisioningProvider = &provisioning.AAPProvider{}
			reconciler.Client = fake.NewClientBuilder().
				WithScheme(testScheme).
				WithStatusSubresource(hostLease).
				WithObjects(hostLease).
				Build()

			mockMgmt.getPowerStateFunc = func(_ context.Context, _ string) (*management.PowerStatus, error) {
				return &management.PowerStatus{State: management.PowerOff}, nil
			}

			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      hostLease.Name,
					Namespace: hostLease.Namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
		})

		It("should skip provisioning when templateID is empty", func() {
			hostLease := &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "hostlease-empty-template",
					Namespace:  testNamespace,
					Finalizers: []string{hostLeaseFinalizer},
				},
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: testNodeID,
					HostClass:      hostClass,
					TemplateID:     "",
				},
			}
			reconciler.ProvisioningProvider = &provisioning.AAPProvider{}
			reconciler.Client = fake.NewClientBuilder().
				WithScheme(testScheme).
				WithStatusSubresource(hostLease).
				WithObjects(hostLease).
				Build()

			mockMgmt.getPowerStateFunc = func(_ context.Context, _ string) (*management.PowerStatus, error) {
				return &management.PowerStatus{State: management.PowerOff}, nil
			}

			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      hostLease.Name,
					Namespace: hostLease.Namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
		})

		It("should not re-trigger when a successful provision job exists", func() {
			hostLease := &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "hostlease-already-provisioned",
					Namespace:  testNamespace,
					Finalizers: []string{hostLeaseFinalizer},
				},
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: testNodeID,
					HostClass:      hostClass,
					TemplateID:     "image-provision",
				},
				Status: v1alpha1.HostLeaseStatus{
					Jobs: []opv1alpha1.JobStatus{
						{
							JobID:     "123",
							Type:      opv1alpha1.JobTypeProvision,
							State:     opv1alpha1.JobStateSucceeded,
							Message:   "successful",
							Timestamp: metav1.Now(),
						},
					},
				},
			}
			reconciler.ProvisioningProvider = &provisioning.AAPProvider{}
			reconciler.Client = fake.NewClientBuilder().
				WithScheme(testScheme).
				WithStatusSubresource(hostLease).
				WithObjects(hostLease).
				Build()

			mockMgmt.getPowerStateFunc = func(_ context.Context, _ string) (*management.PowerStatus, error) {
				return &management.PowerStatus{State: management.PowerOff}, nil
			}

			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      hostLease.Name,
					Namespace: hostLease.Namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
		})
	})

	Describe("syncHostLeaseStatus", func() {
		var hostLease *v1alpha1.HostLease

		BeforeEach(func() {
			hostLease = &v1alpha1.HostLease{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hostlease-sync",
					Namespace: testNamespace,
				},
				Spec: v1alpha1.HostLeaseSpec{
					ExternalHostID: testNodeID,
					HostClass:      hostClass,
				},
			}
			reconciler.Client = fake.NewClientBuilder().
				WithScheme(testScheme).
				WithStatusSubresource(hostLease).
				WithObjects(hostLease).
				Build()
		})

		It("should set PowerSynced to False on error", func() {
			reconciler.syncHostLeaseStatus(hostLease, nil, errors.New("ironic connection failed"), log)

			condition := hostLease.GetStatusCondition(v1alpha1.HostConditionPowerSynced)
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(v1alpha1.HostConditionReasonIronicAPIFailure))
			Expect(condition.Message).To(Equal("ironic connection failed"))
		})

		It("should set PowerSynced to True when node is on", func() {
			powerStatus := &management.PowerStatus{State: management.PowerOn}
			reconciler.syncHostLeaseStatus(hostLease, powerStatus, nil, log)

			Expect(hostLease.Status.PoweredOn).NotTo(BeNil())
			Expect(*hostLease.Status.PoweredOn).To(BeTrue())

			condition := hostLease.GetStatusCondition(v1alpha1.HostConditionPowerSynced)
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(v1alpha1.HostConditionReasonPowerOn))
		})

		It("should set PowerSynced to True when node is off", func() {
			powerStatus := &management.PowerStatus{State: management.PowerOff}
			reconciler.syncHostLeaseStatus(hostLease, powerStatus, nil, log)

			Expect(hostLease.Status.PoweredOn).NotTo(BeNil())
			Expect(*hostLease.Status.PoweredOn).To(BeFalse())

			condition := hostLease.GetStatusCondition(v1alpha1.HostConditionPowerSynced)
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(v1alpha1.HostConditionReasonPowerOff))
		})

		It("should set PowerSynced to False when power state does not match desired", func() {
			hostLease.Spec.PoweredOn = boolPtr(true)
			powerStatus := &management.PowerStatus{State: management.PowerOff}
			reconciler.syncHostLeaseStatus(hostLease, powerStatus, nil, log)

			Expect(hostLease.Status.PoweredOn).NotTo(BeNil())
			Expect(*hostLease.Status.PoweredOn).To(BeFalse())
			condition := hostLease.GetStatusCondition(v1alpha1.HostConditionPowerSynced)
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(v1alpha1.HostConditionReasonProgressing))
		})

		It("should set PowerSynced to False when node is transitioning", func() {
			powerStatus := &management.PowerStatus{State: management.PowerOff, IsTransitioning: true}
			reconciler.syncHostLeaseStatus(hostLease, powerStatus, nil, log)

			Expect(hostLease.Status.PoweredOn).NotTo(BeNil())
			Expect(*hostLease.Status.PoweredOn).To(BeFalse())
			condition := hostLease.GetStatusCondition(v1alpha1.HostConditionPowerSynced)
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(v1alpha1.HostConditionReasonProgressing))
			Expect(condition.Message).To(Equal("node power state is transitioning"))
		})

		It("should not modify status when powerStatus is nil and no error", func() {
			reconciler.syncHostLeaseStatus(hostLease, nil, nil, log)

			Expect(hostLease.Status.PoweredOn).To(BeNil())
			Expect(hostLease.Status.Conditions).To(BeEmpty())
		})
	})
})
