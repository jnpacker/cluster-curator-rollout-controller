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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rolloutv1alpha1 "github.com/stolostron/cluster-curator-rollout-controller/api/v1alpha1"
)

var _ = Describe("ClusterCuratorRollout Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		clustercuratorrollout := &rolloutv1alpha1.ClusterCuratorRollout{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind ClusterCuratorRollout")
			err := k8sClient.Get(ctx, typeNamespacedName, clustercuratorrollout)
			if err != nil && errors.IsNotFound(err) {
				resource := &rolloutv1alpha1.ClusterCuratorRollout{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: rolloutv1alpha1.ClusterCuratorRolloutSpec{
						OpenShiftVersion: "4.15.3",
						SelectionSpec: rolloutv1alpha1.SelectionSpec{
							Type: rolloutv1alpha1.SelectionTypeFilter,
							LabelSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"environment": "test",
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &rolloutv1alpha1.ClusterCuratorRollout{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance ClusterCuratorRollout")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &ClusterCuratorRolloutReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})

	Context("Step progression logic", func() {
		It("isStepComplete should return false when there are pending clusters", func() {
			reconciler := &ClusterCuratorRolloutReconciler{}

			// Step with pending clusters should NOT be complete
			stepWithPending := &rolloutv1alpha1.RolloutStep{
				Counts: rolloutv1alpha1.StateCounts{
					Total:      5,
					Pending:    3,
					InProgress: 0,
					Completed:  2,
				},
			}
			Expect(reconciler.isStepComplete(stepWithPending)).To(BeFalse())
		})

		It("isStepComplete should return false when there are in-progress clusters", func() {
			reconciler := &ClusterCuratorRolloutReconciler{}

			// Step with in-progress clusters should NOT be complete
			stepWithInProgress := &rolloutv1alpha1.RolloutStep{
				Counts: rolloutv1alpha1.StateCounts{
					Total:      5,
					Pending:    0,
					InProgress: 2,
					Completed:  3,
				},
			}
			Expect(reconciler.isStepComplete(stepWithInProgress)).To(BeFalse())
		})

		It("isStepComplete should return false when Total is 0", func() {
			reconciler := &ClusterCuratorRolloutReconciler{}

			// Step with zero total should NOT be complete (waiting for PlacementDecisions)
			stepWithZeroTotal := &rolloutv1alpha1.RolloutStep{
				Counts: rolloutv1alpha1.StateCounts{
					Total:      0,
					Pending:    0,
					InProgress: 0,
					Completed:  0,
				},
			}
			Expect(reconciler.isStepComplete(stepWithZeroTotal)).To(BeFalse())
		})

		It("isStepComplete should return true only when all clusters are in terminal state", func() {
			reconciler := &ClusterCuratorRolloutReconciler{}

			// Step where all clusters are complete/failed/skipped should be complete
			stepComplete := &rolloutv1alpha1.RolloutStep{
				Counts: rolloutv1alpha1.StateCounts{
					Total:      5,
					Pending:    0,
					InProgress: 0,
					Completed:  3,
					Failed:     1,
					Skipped:    1,
				},
			}
			Expect(reconciler.isStepComplete(stepComplete)).To(BeTrue())
		})

		It("step counts must be consistent for step to be considered complete", func() {
			reconciler := &ClusterCuratorRolloutReconciler{}

			// Counts must be consistent (sum of states == Total) for step to complete
			// This prevents premature completion when counts aren't properly calculated

			// Simulating an uninitialized step with only Total set (inconsistent counts)
			// The sum of states (0) != Total (5), so this should NOT be considered complete
			stepInconsistentCounts := &rolloutv1alpha1.RolloutStep{
				Counts: rolloutv1alpha1.StateCounts{
					Total:      5,
					Pending:    0, // Sum = 0, but Total = 5 - inconsistent!
					InProgress: 0,
					Completed:  0,
					Failed:     0,
					Skipped:    0,
				},
			}
			// With the fix, inconsistent counts prevent premature completion
			Expect(reconciler.isStepComplete(stepInconsistentCounts)).To(BeFalse())

			// With proper count initialization, Pending should equal Total initially
			stepProperlyInitialized := &rolloutv1alpha1.RolloutStep{
				Counts: rolloutv1alpha1.StateCounts{
					Total:      5,
					Pending:    5, // Sum = 5, Total = 5 - consistent, but still pending
					InProgress: 0,
					Completed:  0,
					Failed:     0,
					Skipped:    0,
				},
			}
			Expect(reconciler.isStepComplete(stepProperlyInitialized)).To(BeFalse())

			// Step is only complete when consistent AND no pending/in-progress
			stepActuallyComplete := &rolloutv1alpha1.RolloutStep{
				Counts: rolloutv1alpha1.StateCounts{
					Total:      5,
					Pending:    0,
					InProgress: 0,
					Completed:  3, // Sum = 5, Total = 5 - consistent
					Failed:     0,
					Skipped:    2,
				},
			}
			Expect(reconciler.isStepComplete(stepActuallyComplete)).To(BeTrue())
		})
	})

	Context("Version change detection and status reset", func() {
		It("should not reset status when version has not changed", func() {
			reconciler := &ClusterCuratorRolloutReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			rollout := &rolloutv1alpha1.ClusterCuratorRollout{
				Spec: rolloutv1alpha1.ClusterCuratorRolloutSpec{
					OpenShiftVersion: "4.16.0",
				},
				Status: rolloutv1alpha1.ClusterCuratorRolloutStatus{
					Phase:                    rolloutv1alpha1.RolloutPhaseProcessing,
					ObservedOpenShiftVersion: "4.16.0",
					Message:                  "Processing",
					RolloutPlan: []rolloutv1alpha1.RolloutStep{
						{PlacementName: "test-placement", State: rolloutv1alpha1.RolloutStepStateActive},
					},
				},
			}

			ctx := context.Background()
			wasReset := reconciler.checkAndResetForVersionChange(ctx, rollout)

			Expect(wasReset).To(BeFalse())
			Expect(rollout.Status.RolloutPlan).To(HaveLen(1))
			Expect(rollout.Status.Message).To(Equal("Processing"))
		})

		It("should reset status when version changes", func() {
			reconciler := &ClusterCuratorRolloutReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			now := metav1.Now()
			rollout := &rolloutv1alpha1.ClusterCuratorRollout{
				Spec: rolloutv1alpha1.ClusterCuratorRolloutSpec{
					OpenShiftVersion: "4.17.0", // New version
				},
				Status: rolloutv1alpha1.ClusterCuratorRolloutStatus{
					Phase:                    rolloutv1alpha1.RolloutPhaseIdle,
					ObservedOpenShiftVersion: "4.16.0", // Previous version
					Message:                  "Rollout completed successfully",
					LastReconcileTime:        &now,
					RolloutPlan: []rolloutv1alpha1.RolloutStep{
						{PlacementName: "test-placement", State: rolloutv1alpha1.RolloutStepStateCompleted},
					},
					TotalCounts: &rolloutv1alpha1.StateCounts{
						Total:     10,
						Completed: 10,
					},
					SoakStatus: &rolloutv1alpha1.SoakStatus{
						GroupPath: "test-path",
					},
				},
			}

			ctx := context.Background()
			wasReset := reconciler.checkAndResetForVersionChange(ctx, rollout)

			Expect(wasReset).To(BeTrue())
			// Verify status was reset
			Expect(rollout.Status.ObservedOpenShiftVersion).To(Equal("4.17.0"))
			Expect(rollout.Status.Phase).To(Equal(rolloutv1alpha1.RolloutPhaseProcessing))
			Expect(rollout.Status.RolloutPlan).To(BeNil())
			Expect(rollout.Status.TotalCounts).To(BeNil())
			Expect(rollout.Status.SoakStatus).To(BeNil())
			Expect(rollout.Status.Message).To(ContainSubstring("Rollout reset for new target version 4.17.0"))
			Expect(rollout.Status.Message).To(ContainSubstring("was 4.16.0"))
		})

		It("should not reset on first reconcile with empty observed version", func() {
			reconciler := &ClusterCuratorRolloutReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			rollout := &rolloutv1alpha1.ClusterCuratorRollout{
				Spec: rolloutv1alpha1.ClusterCuratorRolloutSpec{
					OpenShiftVersion: "4.16.0",
				},
				Status: rolloutv1alpha1.ClusterCuratorRolloutStatus{
					// Empty status - first reconcile or migration
					ObservedOpenShiftVersion: "",
				},
			}

			ctx := context.Background()
			wasReset := reconciler.checkAndResetForVersionChange(ctx, rollout)

			Expect(wasReset).To(BeFalse())
			// Should set the observed version without full reset
			Expect(rollout.Status.ObservedOpenShiftVersion).To(Equal("4.16.0"))
		})

		It("should handle version upgrade path correctly", func() {
			reconciler := &ClusterCuratorRolloutReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			// Simulate going from 4.15.0 -> 4.16.0 -> 4.17.0
			rollout := &rolloutv1alpha1.ClusterCuratorRollout{
				Spec: rolloutv1alpha1.ClusterCuratorRolloutSpec{
					OpenShiftVersion: "4.16.0",
				},
				Status: rolloutv1alpha1.ClusterCuratorRolloutStatus{
					ObservedOpenShiftVersion: "4.15.0",
					RolloutPlan: []rolloutv1alpha1.RolloutStep{
						{PlacementName: "step1", State: rolloutv1alpha1.RolloutStepStateCompleted},
						{PlacementName: "step2", State: rolloutv1alpha1.RolloutStepStateActive},
					},
				},
			}

			ctx := context.Background()

			// First version change: 4.15.0 -> 4.16.0
			wasReset := reconciler.checkAndResetForVersionChange(ctx, rollout)
			Expect(wasReset).To(BeTrue())
			Expect(rollout.Status.ObservedOpenShiftVersion).To(Equal("4.16.0"))
			Expect(rollout.Status.RolloutPlan).To(BeNil())

			// Now change to 4.17.0
			rollout.Spec.OpenShiftVersion = "4.17.0"
			wasReset = reconciler.checkAndResetForVersionChange(ctx, rollout)
			Expect(wasReset).To(BeTrue())
			Expect(rollout.Status.ObservedOpenShiftVersion).To(Equal("4.17.0"))
		})
	})
})
