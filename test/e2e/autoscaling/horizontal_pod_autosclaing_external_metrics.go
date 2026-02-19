/*
Copyright The Kubernetes Authors.

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

package autoscaling

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eautoscaling "k8s.io/kubernetes/test/e2e/framework/autoscaling"
	admissionapi "k8s.io/pod-security-admission/api"
)

var selectPolicyMax = v2.MaxChangePolicySelect

var _ = SIGDescribe(feature.HPA, "Horizontal pod autoscaling (external metrics)", func() {
	var (
		rc                *e2eautoscaling.ResourceConsumer
		metricsController *e2eautoscaling.ExternalMetricsController
	)

	waitBuffer := 1 * time.Minute

	f := framework.NewDefaultFramework("horizontal-pod-autoscaling-external")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	ginkgo.BeforeEach(func(ctx context.Context) {
		ginkgo.By("Setting up the external metrics server")
		metricsController = e2eautoscaling.RunExternalMetricsServer(ctx, f.ClientSet, f.Namespace.Name, "external-metrics-server", nil)
	})
	ginkgo.AfterEach(func(ctx context.Context) {
		if metricsController != nil {
			e2eautoscaling.CleanupExternalMetricsServer(ctx, f.ClientSet, f.Namespace.Name, "external-metrics-server")
		}
	})

	ginkgo.It("should scale up and down based on external metric value", func(ctx context.Context) {
		ginkgo.By("Creating the resource consumer deployment")
		initPods := 1
		rc = e2eautoscaling.NewDynamicResourceConsumer(ctx,
			hpaName, f.Namespace.Name, e2eautoscaling.KindDeployment, initPods,
			0, 0, 0,
			int64(podCPURequest), 200,
			f.ClientSet, f.ScalesGetter, e2eautoscaling.Disable, e2eautoscaling.Idle,
			nil)
		rc.WaitForReplicas(ctx, initPods, maxResourceConsumerDelay+waitBuffer)

		metricName := "queue_messages_ready"
		ginkgo.By(fmt.Sprintf("Creating an HPA based on external metric %s", metricName))
		// Disable stabilization window for faster scale-down
		stabilizationWindowZero := int32(0)
		behavior := &v2.HorizontalPodAutoscalerBehavior{
			ScaleDown: &v2.HPAScalingRules{
				StabilizationWindowSeconds: &stabilizationWindowZero,
			},
		}
		// since queue_messages_ready default is 100 this will cause the HPA to scale out till max replicas
		hpa := e2eautoscaling.CreateExternalHorizontalPodAutoscalerWithBehavior(ctx, rc, metricName, nil, v2.ValueMetricType, 50, int32(initPods), 3, behavior)

		ginkgo.By("Waiting for HPA to scale up to max replicas")
		rc.WaitForReplicas(ctx, int(hpa.Spec.MaxReplicas), maxResourceConsumerDelay+waitBuffer)

		ginkgo.By(fmt.Sprintf("Setting %s metric value to 0", metricName))
		err := metricsController.SetMetricValue(ctx, metricName, 0, nil)
		framework.ExpectNoError(err)

		ginkgo.By("Waiting for HPA to scale down to min replicas")
		rc.WaitForReplicas(ctx, int(*hpa.Spec.MinReplicas), maxResourceConsumerDelay+waitBuffer)
		ginkgo.DeferCleanup(e2eautoscaling.DeleteHorizontalPodAutoscaler, rc, hpa.Name)
	})

	ginkgo.It("should handle multiple external metrics correctly", func(ctx context.Context) {
		ginkgo.By("Creating the resource consumer deployment")
		initPods := 1
		rc = e2eautoscaling.NewDynamicResourceConsumer(ctx,
			hpaName, f.Namespace.Name, e2eautoscaling.KindDeployment, initPods,
			0, 0, 0,
			int64(podCPURequest), 200,
			f.ClientSet, f.ScalesGetter, e2eautoscaling.Disable, e2eautoscaling.Idle,
			nil)
		rc.WaitForReplicas(ctx, initPods, maxResourceConsumerDelay+waitBuffer)

		ginkgo.By("Creating an HPA with multiple external metrics")
		// Create HPA with two external metrics
		metric1Name := "requests_per_second"
		metric2Name := "queue_messages_ready"

		stabilizationWindowZero := int32(0)
		behavior := &v2.HorizontalPodAutoscalerBehavior{
			ScaleDown: &v2.HPAScalingRules{
				StabilizationWindowSeconds: &stabilizationWindowZero,
			},
		}

		// Create two metric specs
		metric1 := e2eautoscaling.CreateExternalMetricSpec(metric1Name, nil, v2.ValueMetricType, 100)
		metric2 := e2eautoscaling.CreateExternalMetricSpec(metric2Name, nil, v2.ValueMetricType, 100)

		// Create HPA with both metrics
		hpa := e2eautoscaling.CreateMultiMetricHorizontalPodAutoscalerWithBehavior(ctx, rc, []v2.MetricSpec{metric1, metric2}, int32(initPods), 5, behavior)

		// Set metric1 to scale up (high value)
		ginkgo.By(fmt.Sprintf("Setting %s to high value to trigger scale up", metric1Name))
		err := metricsController.SetMetricValue(ctx, metric1Name, 1000, nil)
		framework.ExpectNoError(err)

		// Set metric2 to lower value
		err = metricsController.SetMetricValue(ctx, metric2Name, 200, nil)
		framework.ExpectNoError(err)

		ginkgo.By("Waiting for HPA to scale up based on highest metric recommendation")
		rc.WaitForReplicas(ctx, 5, maxResourceConsumerDelay+waitBuffer)

		// Now lower both metrics
		ginkgo.By(fmt.Sprintf("Setting both metrics to low values to trigger scale down"))
		err = metricsController.SetMetricValue(ctx, metric1Name, 10, nil)
		framework.ExpectNoError(err)
		err = metricsController.SetMetricValue(ctx, metric2Name, 20, nil)
		framework.ExpectNoError(err)

		ginkgo.By("Waiting for HPA to scale down to min replicas")
		rc.WaitForReplicas(ctx, initPods, maxResourceConsumerDelay+waitBuffer)

		ginkgo.DeferCleanup(e2eautoscaling.DeleteHorizontalPodAutoscaler, rc, hpa.Name)
	})

	ginkgo.It("should respect stabilization window and not scale aggressively", func(ctx context.Context) {
		ginkgo.By("Creating the resource consumer deployment")
		initPods := 2
		rc = e2eautoscaling.NewDynamicResourceConsumer(ctx,
			hpaName, f.Namespace.Name, e2eautoscaling.KindDeployment, initPods,
			0, 0, 0,
			int64(podCPURequest), 200,
			f.ClientSet, f.ScalesGetter, e2eautoscaling.Disable, e2eautoscaling.Idle,
			nil)
		rc.WaitForReplicas(ctx, initPods, maxResourceConsumerDelay+waitBuffer)

		metricName := "fluctuating_metric"
		ginkgo.By("Creating an HPA with a long stabilization window")

		// Use a long stabilization window (120 seconds) to prevent aggressive scaling
		stabilizationWindow := int32(120)
		behavior := &v2.HorizontalPodAutoscalerBehavior{
			ScaleDown: &v2.HPAScalingRules{
				StabilizationWindowSeconds: &stabilizationWindow,
			},
			ScaleUp: &v2.HPAScalingRules{
				StabilizationWindowSeconds: &stabilizationWindow,
			},
		}

		hpa := e2eautoscaling.CreateExternalHorizontalPodAutoscalerWithBehavior(ctx, rc, metricName, nil, v2.ValueMetricType, 100, int32(initPods), 5, behavior)

		// Set initial metric value
		err := metricsController.SetMetricValue(ctx, metricName, 150, nil)
		framework.ExpectNoError(err)

		ginkgo.By("Allowing initial stabilization window time and checking replica count stabilizes")
		// Wait for initial scale up
		rc.WaitForReplicas(ctx, 3, maxResourceConsumerDelay+waitBuffer)

		ginkgo.By("Rapidly fluctuating metric values to test stabilization window")
		// Rapidly change metric values - should not cause immediate scaling
		for i := 0; i < 3; i++ {
			err := metricsController.SetMetricValue(ctx, metricName, 50, nil)
			framework.ExpectNoError(err)
			time.Sleep(10 * time.Second)

			err = metricsController.SetMetricValue(ctx, metricName, 150, nil)
			framework.ExpectNoError(err)
			time.Sleep(10 * time.Second)
		}

		ginkgo.By("Verifying HPA did not scale aggressively despite metric fluctuations")
		// After stabilization window, replica count should be reasonable, not bouncing between extremes
		// Final metric set to 50, so it should eventually scale down, but took the stabilization window
		finalMetricValue := int64(50)
		err = metricsController.SetMetricValue(ctx, metricName, finalMetricValue, nil)
		framework.ExpectNoError(err)

		// Wait beyond the stabilization window for scale down decision
		time.Sleep(time.Duration(stabilizationWindow) * time.Second)
		rc.WaitForReplicas(ctx, int(initPods), maxResourceConsumerDelay+waitBuffer)

		ginkgo.DeferCleanup(e2eautoscaling.DeleteHorizontalPodAutoscaler, rc, hpa.Name)
	})

	ginkgo.It("should enforce scaling limits from behavior policies", func(ctx context.Context) {
		ginkgo.By("Creating the resource consumer deployment")
		initPods := 1
		rc = e2eautoscaling.NewDynamicResourceConsumer(ctx,
			hpaName, f.Namespace.Name, e2eautoscaling.KindDeployment, initPods,
			0, 0, 0,
			int64(podCPURequest), 200,
			f.ClientSet, f.ScalesGetter, e2eautoscaling.Disable, e2eautoscaling.Idle,
			nil)
		rc.WaitForReplicas(ctx, initPods, maxResourceConsumerDelay+waitBuffer)

		metricName := "controlled_scaling_metric"
		ginkgo.By("Creating an HPA with specific scaling limit policies")

		// Set aggressive behavior but with limits
		// Max 2 pods added per scaling period, Max 1 pod removed per scaling period
		percentageIncrease := int32(100) // 100% increase
		podsIncrease := int32(2)         // or 2 pods, whichever is less
		periodSeconds := int32(15)       // per 15 seconds

		behavior := &v2.HorizontalPodAutoscalerBehavior{
			ScaleUp: &v2.HPAScalingRules{
				Policies: []v2.HPAScalingPolicy{
					{
						Type:          v2.PercentScalingPolicy,
						Value:         percentageIncrease,
						PeriodSeconds: periodSeconds,
					},
					{
						Type:          v2.PodsScalingPolicy,
						Value:         podsIncrease,
						PeriodSeconds: periodSeconds,
					},
				},
				SelectPolicy: &selectPolicyMax,
			},
			ScaleDown: &v2.HPAScalingRules{
				Policies: []v2.HPAScalingPolicy{
					{
						Type:          v2.PodsScalingPolicy,
						Value:         int32(1),
						PeriodSeconds: periodSeconds,
					},
				},
			},
		}

		hpa := e2eautoscaling.CreateExternalHorizontalPodAutoscalerWithBehavior(ctx, rc, metricName, nil, v2.ValueMetricType, 100, int32(initPods), 10, behavior)

		ginkgo.By("Setting metric to trigger scale up with limited policy")
		// Set metric value very high to trigger aggressive scaling attempt
		err := metricsController.SetMetricValue(ctx, metricName, 1000, nil)
		framework.ExpectNoError(err)

		ginkgo.By("Verifying HPA scales up but respects the 2-pod-per-period limit")
		// Should scale up, but slowly due to the 2 pods per 15 seconds limit
		// It should reach 3 pods (1 initial + 2 from policy) after first period
		rc.WaitForReplicas(ctx, 3, maxResourceConsumerDelay+waitBuffer)

		ginkgo.By("Waiting and verifying next scale up respects limit")
		// After another period, should scale up to 5 pods (3 + 2 more)
		time.Sleep(20 * time.Second)
		rc.WaitForReplicas(ctx, 5, maxResourceConsumerDelay+waitBuffer)

		ginkgo.By("Setting metric to scale down and verifying 1-pod-per-period limit")
		err = metricsController.SetMetricValue(ctx, metricName, 10, nil)
		framework.ExpectNoError(err)

		// Should scale down by 1 pod per period (15 seconds)
		// 5 -> 4 -> 3 -> 2 -> 1
		time.Sleep(25 * time.Second)
		rc.WaitForReplicas(ctx, 3, maxResourceConsumerDelay+waitBuffer)

		ginkgo.By("Waiting for final scale down to min replicas")
		time.Sleep(35 * time.Second)
		rc.WaitForReplicas(ctx, int(initPods), maxResourceConsumerDelay+waitBuffer)

		ginkgo.DeferCleanup(e2eautoscaling.DeleteHorizontalPodAutoscaler, rc, hpa.Name)
	})
})
