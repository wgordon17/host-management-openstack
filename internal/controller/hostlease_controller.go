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

// Package controller implements Kubernetes controllers for managing OpenStack resources.
package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/gophercloud/gophercloud/v2/openstack/baremetal/v1/nodes"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	v1alpha1 "github.com/osac-project/bare-metal-fulfillment-operator/api/v1alpha1"
	"github.com/osac-project/host-management-openstack/internal/ironic"
	opv1alpha1 "github.com/osac-project/osac-operator/api/v1alpha1"
	"github.com/osac-project/osac-operator/pkg/provisioning"
)

// HostLeaseReconciler reconciles HostLease CRs for power management via Ironic.
type HostLeaseReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	IronicClient         ironic.NodeClient
	ProvisioningProvider provisioning.ProvisioningProvider

	// RecheckInterval is the interval for polling Ironic until power state matches desired state.
	RecheckInterval time.Duration

	// ProvisionPollInterval is the interval for polling AAP job status.
	ProvisionPollInterval time.Duration
}

// NewHostLeaseReconciler creates a new HostLeaseReconciler with defaults applied.
func NewHostLeaseReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	ironicClient ironic.NodeClient,
	provider provisioning.ProvisioningProvider,
	recheckInterval time.Duration,
) *HostLeaseReconciler {
	if recheckInterval <= 0 {
		recheckInterval = DefaultRecheckInterval
	}

	return &HostLeaseReconciler{
		Client:                client,
		Scheme:                scheme,
		IronicClient:          ironicClient,
		ProvisioningProvider:  provider,
		RecheckInterval:       recheckInterval,
		ProvisionPollInterval: DefaultProvisionPollInterval,
	}
}

// Reconcile manages the lifecycle of HostLease resources by reconciling their power state with Ironic.
// +kubebuilder:rbac:groups=osac.openshift.io,resources=hostleases,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=osac.openshift.io,resources=hostleases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=osac.openshift.io,resources=hostleases/finalizers,verbs=update;patch
func (r *HostLeaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	hostLease := &v1alpha1.HostLease{}
	if err := r.Get(ctx, req.NamespacedName, hostLease); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	oldstatus := hostLease.Status.DeepCopy()

	var result ctrl.Result
	var err error
	if !hostLease.DeletionTimestamp.IsZero() {
		result, err = r.reconcileDelete(ctx, hostLease, log)
	} else {
		result, err = r.handleUpdate(ctx, hostLease, log)
	}

	if !equality.Semantic.DeepEqual(hostLease.Status, *oldstatus) {
		log.Info("Persisting HostLease status changes", "hostLease", hostLease.Name)
		if statusErr := r.Status().Update(ctx, hostLease); client.IgnoreNotFound(statusErr) != nil {
			log.Error(statusErr, "failed to update HostLease status")
			return result, statusErr
		}
	}

	return result, err
}

func (r *HostLeaseReconciler) handleUpdate(ctx context.Context, hostLease *v1alpha1.HostLease, log logr.Logger) (ctrl.Result, error) {
	if !r.validateOpenStackHost(hostLease, log) {
		return ctrl.Result{}, nil
	}

	if hostLease.Status.Phase == "" {
		hostLease.Status.Phase = v1alpha1.HostLeasePhaseProgressing
	}

	if !controllerutil.ContainsFinalizer(hostLease, hostLeaseFinalizer) {
		controllerutil.AddFinalizer(hostLease, hostLeaseFinalizer)
		if err := r.Update(ctx, hostLease); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Provisioning runs first — power reconciliation is suspended during provisioning
	if r.ProvisioningProvider != nil && hostLease.Spec.TemplateID != "" && hostLease.Spec.TemplateID != "noop" {
		result, provErr := r.reconcileProvisioning(ctx, hostLease)
		if provErr != nil {
			hostLease.Status.Phase = v1alpha1.HostLeasePhaseFailed
			return result, provErr
		}
		if !result.IsZero() {
			return result, nil
		}
	}

	node, err := r.IronicClient.GetNode(ctx, hostLease.Spec.ExternalHostID)
	if err != nil {
		log.Error(err, "failed to get Ironic node", "nodeID", hostLease.Spec.ExternalHostID)
		r.syncHostLeaseStatus(hostLease, nil, err, log)
		return ctrl.Result{}, err
	}
	log.V(1).Info("Ironic node", "nodeID", hostLease.Spec.ExternalHostID, "power_state", node.PowerState)

	if hostLease.Spec.PoweredOn != nil {
		if err := r.reconcilePower(ctx, hostLease, node, log); err != nil {
			r.syncHostLeaseStatus(hostLease, nil, err, log)
			return ctrl.Result{}, err
		}

		node, err = r.IronicClient.GetNode(ctx, hostLease.Spec.ExternalHostID)
		if err != nil {
			log.Error(err, "failed to refresh node after power reconciliation", "nodeID", hostLease.Spec.ExternalHostID)
			r.syncHostLeaseStatus(hostLease, nil, err, log)
			return ctrl.Result{}, err
		}
	}

	r.syncHostLeaseStatus(hostLease, node, nil, log)

	if hostLease.Spec.PoweredOn != nil {
		currentlyOn := node.PowerState == ironic.PowerOn.String()
		if *hostLease.Spec.PoweredOn != currentlyOn {
			return ctrl.Result{RequeueAfter: r.RecheckInterval}, nil
		}
	}

	hostLease.Status.Phase = v1alpha1.HostLeasePhaseReady
	log.Info("HostLease reconcile completed; status changes pending persistence", "hostLease", hostLease.Name)
	return ctrl.Result{}, nil
}

func (r *HostLeaseReconciler) reconcileDelete(ctx context.Context, hostLease *v1alpha1.HostLease, log logr.Logger) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(hostLease, hostLeaseFinalizer) {
		log.V(1).Info("Skipping cleanup: finalizer not present", "finalizer", hostLeaseFinalizer)
		return ctrl.Result{}, nil
	}

	log.Info("Running HostLease cleanup", "finalizer", hostLeaseFinalizer)

	if !r.validateOpenStackHost(hostLease, log) {
		log.Error(errors.New("finalizer mismatch"),
			"Skipping cleanup: HostLease not managed by this controller")
		return ctrl.Result{}, nil
	}

	hostLease.Status.Phase = v1alpha1.HostLeasePhaseDeleting

	if r.ProvisioningProvider != nil && hostLease.Spec.TemplateID != "" && hostLease.Spec.TemplateID != "noop" {
		result, done, err := r.reconcileDeprovisioning(ctx, hostLease)
		if err != nil {
			return result, err
		}
		if !done {
			return result, nil
		}
	}

	log.Info("Unsetting hostClass and removing finalizer")
	hostLease.Spec.HostClass = ""
	controllerutil.RemoveFinalizer(hostLease, hostLeaseFinalizer)
	if err := r.Update(ctx, hostLease); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("Cleanup complete, finalizer removed")

	return ctrl.Result{}, nil
}

func (r *HostLeaseReconciler) reconcileDeprovisioning(ctx context.Context, hostLease *v1alpha1.HostLease) (ctrl.Result, bool, error) {
	if hostLease.Status.Jobs == nil {
		hostLease.Status.Jobs = []opv1alpha1.JobStatus{}
	}

	latestDeprovisionJob := provisioning.FindLatestJobByType(hostLease.Status.Jobs, opv1alpha1.JobTypeDeprovision)

	if !provisioning.HasJobID(latestDeprovisionJob) {
		result, err := provisioning.TriggerDeprovisionJob(
			ctx, r.ProvisioningProvider, hostLease,
			&hostLease.Status.Jobs, provisioning.DefaultMaxJobHistory, r.ProvisionPollInterval,
		)
		if err != nil {
			hostLease.SetStatusCondition(
				v1alpha1.HostConditionDeprovisionTemplateComplete,
				metav1.ConditionFalse,
				v1alpha1.HostConditionReasonTemplateFailed,
				err.Error(),
			)
			return result, false, err
		}
		if err := r.Status().Update(ctx, hostLease); err != nil {
			return ctrl.Result{}, false, fmt.Errorf("failed to flush status after deprovision trigger: %w", err)
		}
		if !result.IsZero() {
			hostLease.SetStatusCondition(
				v1alpha1.HostConditionDeprovisionTemplateComplete,
				metav1.ConditionFalse,
				v1alpha1.HostConditionReasonProgressing,
				"Deprovision job in progress",
			)
			return result, false, nil
		}
		return ctrl.Result{}, true, nil
	}

	result, done, err := provisioning.PollDeprovisionJob(
		ctx, r.ProvisioningProvider, hostLease,
		&hostLease.Status.Jobs, latestDeprovisionJob, r.ProvisionPollInterval,
	)
	if err != nil {
		return result, false, err
	}

	if done {
		if latestDeprovisionJob.State.IsSuccessful() {
			hostLease.SetStatusCondition(
				v1alpha1.HostConditionDeprovisionTemplateComplete,
				metav1.ConditionTrue,
				"Succeeded",
				"Deprovision job completed successfully",
			)
		}
	} else {
		hostLease.SetStatusCondition(
			v1alpha1.HostConditionDeprovisionTemplateComplete,
			metav1.ConditionFalse,
			v1alpha1.HostConditionReasonProgressing,
			"Deprovision job in progress",
		)
	}

	return result, done, nil
}

func (r *HostLeaseReconciler) validateOpenStackHost(hostLease *v1alpha1.HostLease, log logr.Logger) bool {
	if hostLease.Spec.ExternalHostID == "" {
		log.V(1).Info("HostLease skipped", "reason", "spec.externalHostID not set")
		return false
	}

	if hostLease.Spec.HostClass != hostClass {
		log.V(1).Info("HostLease skipped", "reason", "hostClass mismatch", "want", hostClass, "got", hostLease.Spec.HostClass)
		return false
	}

	return true
}

func (r *HostLeaseReconciler) reconcilePower(ctx context.Context, hostLease *v1alpha1.HostLease, node *nodes.Node, log logr.Logger) error {
	currentlyOn := node.PowerState == ironic.PowerOn.String()
	desiredOn := *hostLease.Spec.PoweredOn

	// If Ironic is already processing a power state change, skip to avoid 409 Conflict.
	if r.IronicClient.IsNodePowerTransitioning(node) {
		log.V(1).Info("Node is transitioning, skipping power action",
			"nodeID", hostLease.Spec.ExternalHostID,
			"targetPowerState", node.TargetPowerState)
		return nil
	}

	var err error
	needsPowerUpdate := desiredOn != currentlyOn
	if !needsPowerUpdate {
		log.Info("Power state already matches desired", "poweredOn", desiredOn, "power_state", node.PowerState)
		return nil
	}

	targetState := ironic.PowerOff
	action := "off"
	if desiredOn {
		targetState = ironic.PowerOn
		action = "on"
	}

	log.Info("Powering "+action+" node", "nodeID", hostLease.Spec.ExternalHostID)
	if err = r.IronicClient.SetPowerState(ctx, hostLease.Spec.ExternalHostID, targetState); err != nil {
		log.Error(err, "failed to power "+action+" node", "nodeID", hostLease.Spec.ExternalHostID)
	}

	return err
}

func (r *HostLeaseReconciler) reconcileProvisioning(ctx context.Context, hostLease *v1alpha1.HostLease) (ctrl.Result, error) {
	desiredVersion, err := provisioning.ComputeDesiredConfigVersion(hostLease.Spec)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to compute desired config version: %w", err)
	}
	hostLease.Status.DesiredConfigVersion = desiredVersion

	result, err := provisioning.RunProvisioningLifecycle(ctx, r.ProvisioningProvider, hostLease,
		&provisioning.State{Jobs: &hostLease.Status.Jobs, DesiredConfigVersion: desiredVersion},
		provisioning.DefaultMaxJobHistory, r.ProvisionPollInterval,
		&provisioning.PollCallbacks{
			OnFailed: func(message string) {
				hostLease.Status.Phase = v1alpha1.HostLeasePhaseFailed
				hostLease.SetStatusCondition(
					v1alpha1.HostConditionProvisionTemplateComplete,
					metav1.ConditionFalse,
					v1alpha1.HostConditionReasonTemplateFailed,
					message,
				)
			},
			OnSuccess: func(_ provisioning.ProvisionStatus) {
				hostLease.SetStatusCondition(
					v1alpha1.HostConditionProvisionTemplateComplete,
					metav1.ConditionTrue,
					"Succeeded",
					"Provision job completed successfully",
				)
			},
		},
		func() bool {
			return provisioning.CheckAPIServerForNonTerminalProvisionJob(
				ctx, r.Client, client.ObjectKeyFromObject(hostLease), &v1alpha1.HostLease{},
			)
		},
		func() error {
			return r.Status().Update(ctx, hostLease)
		},
	)
	if err != nil {
		return result, err
	}

	// Set progressing condition while provisioning is in-flight, but don't overwrite a failure.
	provisionCond := hostLease.GetStatusCondition(v1alpha1.HostConditionProvisionTemplateComplete)
	if result.RequeueAfter > 0 && (provisionCond == nil || provisionCond.Reason != v1alpha1.HostConditionReasonTemplateFailed) {
		hostLease.SetStatusCondition(
			v1alpha1.HostConditionProvisionTemplateComplete,
			metav1.ConditionFalse,
			v1alpha1.HostConditionReasonProgressing,
			"Provisioning job in progress",
		)
	}

	return result, nil
}

// syncHostLeaseStatus syncs power-related conditions and observed power state in memory.
func (r *HostLeaseReconciler) syncHostLeaseStatus(hostLease *v1alpha1.HostLease, node *nodes.Node, reconcileErr error, log logr.Logger) {
	if reconcileErr != nil {
		hostLease.Status.Phase = v1alpha1.HostLeasePhaseFailed
		hostLease.SetStatusCondition(
			v1alpha1.HostConditionPowerSynced,
			metav1.ConditionFalse,
			v1alpha1.HostConditionReasonIronicAPIFailure,
			reconcileErr.Error(),
		)
		log.Info("HostLease status synced", "phase", hostLease.Status.Phase, "condition", v1alpha1.HostConditionPowerSynced, "conditionStatus", metav1.ConditionFalse, "reason", v1alpha1.HostConditionReasonIronicAPIFailure)
		return
	}

	if node == nil {
		return
	}

	poweredOn := node.PowerState == ironic.PowerOn.String()
	hostLease.Status.PoweredOn = &poweredOn

	if hostLease.Spec.PoweredOn != nil && *hostLease.Spec.PoweredOn != poweredOn {
		hostLease.SetStatusCondition(
			v1alpha1.HostConditionPowerSynced,
			metav1.ConditionFalse,
			v1alpha1.HostConditionReasonProgressing,
			"waiting for node power state to converge",
		)
	} else if poweredOn {
		hostLease.SetStatusCondition(v1alpha1.HostConditionPowerSynced, metav1.ConditionTrue,
			v1alpha1.HostConditionReasonPowerOn, "")
		log.Info("HostLease power status synced", "poweredOn", poweredOn, "condition", v1alpha1.HostConditionPowerSynced, "conditionStatus", metav1.ConditionTrue, "reason", v1alpha1.HostConditionReasonPowerOn)
	} else {
		hostLease.SetStatusCondition(v1alpha1.HostConditionPowerSynced, metav1.ConditionTrue,
			v1alpha1.HostConditionReasonPowerOff, "")
		log.Info("HostLease power status synced", "poweredOn", poweredOn, "condition", v1alpha1.HostConditionPowerSynced, "conditionStatus", metav1.ConditionTrue, "reason", v1alpha1.HostConditionReasonPowerOff)
	}
}

func hostLeasePredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldObj, okOld := e.ObjectOld.(*v1alpha1.HostLease)
			newObj, okNew := e.ObjectNew.(*v1alpha1.HostLease)
			if !okOld || !okNew {
				return true
			}

			// Reconcile spec changes while filtering status-only updates.
			if oldObj.GetGeneration() != newObj.GetGeneration() {
				return true
			}

			// Ensure deletion transition still triggers cleanup/finalizer reconciliation.
			if oldObj.DeletionTimestamp.IsZero() && !newObj.DeletionTimestamp.IsZero() {
				return true
			}

			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}

func (r *HostLeaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.HostLease{},
			builder.WithPredicates(hostLeasePredicate()),
		).
		Named("openstack-host").
		Complete(r)
}
