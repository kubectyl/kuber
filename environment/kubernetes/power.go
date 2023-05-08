package kubernetes

import (
	"context"
	"fmt"
	"time"

	errors2 "emperror.dev/errors"
	"github.com/apex/log"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/kubectyl/kuber/config"
	"github.com/kubectyl/kuber/environment"
	"github.com/kubectyl/kuber/remote"
)

// OnBeforeStart run before the container starts and get the process
// configuration from the Panel. This is important since we use this to check
// configuration files as well as ensure we always have the latest version of
// a rocket available for server processes.
//
// This process will also confirm that the server environment exists and is in
// a bootable state. This ensures that unexpected container deletion while Kuber
// is running does not result in the server becoming un-bootable.
func (e *Environment) OnBeforeStart(ctx context.Context) error {
	// Always destroy and re-create the server container to ensure that synced data from the Panel is used.
	var zero int64 = 0
	policy := metav1.DeletePropagationForeground

	if err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Delete(ctx, e.Id, metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &policy}); err != nil {
		if !errors.IsNotFound(err) {
			return errors2.WrapIf(err, "environment/kubernetes: failed to remove pod during pre-boot")
		}
	}

	err := wait.Poll(time.Second, time.Second*10, func() (bool, error) {
		_, err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Get(context.TODO(), e.Id, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return true, nil
			}
			return true, err
		}
		return false, nil
	})
	if err != nil {
		return err
	}

	// The Create() function will check if the container exists in the first place, and if
	// so just silently return without an error. Otherwise, it will try to create the necessary
	// container and data storage directory.
	//
	// This won't actually run an installation process however, it is just here to ensure the
	// environment gets created properly if it is missing and the server is started. We're making
	// an assumption that all the files will still exist at this point.
	if err := e.Create(); err != nil {
		return err
	}

	return nil
}

// Start will start the server environment and begins piping output to the event
// listeners for the console. If a container does not exist, or needs to be
// rebuilt that will happen in the call to OnBeforeStart().
func (e *Environment) Start(ctx context.Context) error {
	sawError := false

	// If sawError is set to true there was an error somewhere in the pipeline that
	// got passed up, but we also want to ensure we set the server to be offline at
	// that point.
	defer func() {
		if sawError {
			// If we don't set it to stopping first, you'll trigger crash detection which
			// we don't want to do at this point since it'll just immediately try to do the
			// exact same action that lead to it crashing in the first place...
			e.SetState(environment.ProcessStoppingState)
			e.SetState(environment.ProcessOfflineState)
		}
	}()

	if c, err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Get(ctx, e.Id, metav1.GetOptions{}); err != nil {
		// Do nothing if the container is not found, we just don't want to continue
		// to the next block of code here. This check was inlined here to guard against
		// a nil-pointer when checking c.State below.
		if !errors.IsNotFound(err) {
			return errors2.WrapIf(err, "environment/kubernetes: failed to inspect pod")
		}
	} else {
		// If the server is running update our internal state and continue on with the attach.
		if running, _ := e.IsRunning(ctx); running && c.Status.Phase == v1.PodRunning {
			e.SetState(environment.ProcessRunningState)

			ctx, cancel := context.WithCancel(context.Background())
			go func(ctx context.Context) {
				err := wait.PollInfinite(time.Second, func() (bool, error) {
					if running, err := e.IsRunning(ctx); !running {
						cancel()
						return true, err
					}
					return false, err
				})
				if err != nil {
					e.SetState(environment.ProcessOfflineState)
				}
			}(ctx)

			return e.Attach(ctx)
		}
	}

	e.SetState(environment.ProcessStartingState)

	// Set this to true for now, we will set it to false once we reach the
	// end of this chain.
	sawError = true

	// Run the before start function and wait for it to finish. This will validate that the container
	// exists on the system, and rebuild the container if that is required for server booting to
	// occur.
	if err := e.OnBeforeStart(ctx); err != nil {
		return errors2.WithStackIf(err)
	}

	// If we cannot start & attach to the container in 30 seconds something has gone
	// quite sideways, and we should stop trying to avoid a hanging situation.
	actx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	err := wait.PollImmediate(time.Second, 10*time.Minute, func() (bool, error) {
		pod, err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Get(context.Background(), e.Id, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		switch pod.Status.Phase {
		case v1.PodPending:
			e.SetState(environment.ProcessStartingState)
		case v1.PodRunning:
			return true, nil
		case v1.PodFailed, v1.PodSucceeded:
			return false, fmt.Errorf("pod ran to completion")
		default:
			return false, fmt.Errorf("unknown pod status")
		}
		return false, nil
	})
	if err != nil {
		e.SetState(environment.ProcessOfflineState)

		return e.Terminate(context.Background())
	}

	// You must attach to the instance _before_ you start the container. If you do this
	// in the opposite order you'll enter a deadlock condition where we're attached to
	// the instance successfully, but the container has already stopped and you'll get
	// the entire program into a very confusing state.
	//
	// By explicitly attaching to the instance before we start it, we can immediately
	// react to errors/output stopping/etc. when starting.
	if err := e.Attach(actx); err != nil {
		return err
	}

	// No errors, good to continue through.
	sawError = false
	return nil
}

// Stop stops the container that the server is running in. This will allow up to
// 30 seconds to pass before the container is forcefully terminated if we are
// trying to stop it without using a command sent into the instance.
//
// You most likely want to be using WaitForStop() rather than this function,
// since this will return as soon as the command is sent, rather than waiting
// for the process to be completed stopped.
func (e *Environment) Stop(ctx context.Context) error {
	e.mu.RLock()
	s := e.meta.Stop
	e.mu.RUnlock()

	// A native "stop" as the Type field value will just skip over all of this
	// logic and end up only executing the container stop command (which may or
	// may not work as expected).
	if s.Type == "" || s.Type == remote.ProcessStopSignal {
		if s.Type == "" {
			log.WithField("pod_name", e.Id).Warn("no stop configuration detected for environment, using termination procedure")
		}

		return e.Terminate(ctx)
	}

	// If the process is already offline don't switch it back to stopping. Just leave it how
	// it is and continue through to the stop handling for the process.
	if e.st.Load() != environment.ProcessOfflineState {
		e.SetState(environment.ProcessStoppingState)
	}

	waitPoll := func(ctx context.Context) error {
		err := wait.PollUntilWithContext(ctx, time.Second, func(ctx context.Context) (done bool, err error) {
			running, err := e.IsRunning(ctx)
			if err != nil && !errors.IsNotFound(err) {
				return false, err
			}

			if !running {
				e.SetStream(nil)
				e.SetState(environment.ProcessOfflineState)

				return true, nil
			}

			return false, nil
		})
		if err != nil {
			return err
		}

		return nil
	}

	// Only attempt to send the stop command to the instance if we are actually attached to
	// the instance. If we are not for some reason, just send the container stop event.
	if e.IsAttached() &&
		s.Type == remote.ProcessStopCommand &&
		e.st.Load() != environment.ProcessOfflineState {
		e.SendCommand(s.Value)

		if err := waitPoll(ctx); err != nil {
			return err
		}
	}

	// Allow the stop action to run for however long it takes, similar to executing a command
	// and using a different logic pathway to wait for the container to stop successfully.
	//
	// Using a negative timeout here will allow the container to stop gracefully,
	// rather than forcefully terminating it, this value MUST be at least 1
	// second, otherwise it will be ignored.
	if err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Delete(ctx, e.Id, metav1.DeleteOptions{}); err != nil {
		// If the pod does not exist just mark the process as stopped and return without an error.
		if errors.IsNotFound(err) {
			e.SetStream(nil)
			e.SetState(environment.ProcessOfflineState)
			return nil
		}
		return errors2.Wrap(err, "environment/kubernetes: cannot stop pod")
	}

	if err := waitPoll(ctx); err != nil {
		return err
	}

	return nil
}

// WaitForStop attempts to gracefully stop a server using the defined stop
// command. If the server does not stop after seconds have passed, an error will
// be returned, or the instance will be terminated forcefully depending on the
// value of the second argument.
//
// Calls to Environment.Terminate() in this function use the context passed
// through since we don't want to prevent termination of the server instance
// just because the context.WithTimeout() has expired.
func (e *Environment) WaitForStop(ctx context.Context, duration time.Duration, terminate bool) error {
	tctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	// If the parent context is canceled, abort the timed context for termination.
	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-tctx.Done():
			// When the timed context is canceled, terminate this routine since we no longer
			// need to worry about the parent routine being canceled.
			break
		}
	}()

	doTermination := func(s string) error {
		e.log().WithField("step", s).WithField("duration", duration).Warn("container stop did not complete in time, terminating process...")
		return e.Terminate(ctx)
	}

	// We pass through the timed context for this stop action so that if one of the
	// internal kubernetes calls fails to ever finish before we've exhausted the time limit
	// the resources get cleaned up, and the exection is stopped.
	if err := e.Stop(tctx); err != nil {
		if terminate && errors2.Is(err, context.DeadlineExceeded) {
			return doTermination("stop")
		}
		return err
	}

	// Block the return of this function until the container as been marked as no
	// longer running. If this wait does not end by the time seconds have passed,
	// attempt to terminate the container, or return an error.
	err := wait.PollUntilWithContext(tctx, time.Second, func(context.Context) (bool, error) {
		_, err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Get(tctx, e.Id, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return true, nil
			}
			return false, nil
		}
		return false, nil
	})
	if terminate && err != nil {
		if !errors2.Is(err, context.DeadlineExceeded) {
			e.log().WithField("error", err).Warn("error while waiting for pod stop; terminating process")
		}
		return doTermination("wait")
	}

	return nil
}

// Terminate forcefully terminates the container using the signal provided.
func (e *Environment) Terminate(ctx context.Context) error {
	_, err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Get(ctx, e.Id, metav1.GetOptions{})
	if err != nil {
		// Treat missing containers as an okay error state, means it is obviously
		// already terminated at this point.
		if errors.IsNotFound(err) {
			return nil
		}
		return errors2.WithStack(err)
	}

	// We set it to stopping than offline to prevent crash detection from being triggered.
	e.SetState(environment.ProcessStoppingState)
	var zero int64 = 0
	policy := metav1.DeletePropagationForeground
	if err := e.client.CoreV1().Pods(config.Get().Cluster.Namespace).Delete(ctx, e.Id, metav1.DeleteOptions{
		GracePeriodSeconds: &zero,
		PropagationPolicy:  &policy,
	}); err != nil && !errors.IsNotFound(err) {
		return errors2.WithStack(err)
	}
	e.SetState(environment.ProcessOfflineState)

	return nil
}
