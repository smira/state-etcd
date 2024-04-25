// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package etcd_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cosi-project/runtime/pkg/safe"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/cosi-project/runtime/pkg/state/conformance"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc/metadata"
)

func TestWatchKindBug2(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t, goleak.IgnoreCurrent()) })

	withEtcd(t, func(s state.State) {
		//s = state.WrapCore(inmem.NewState("default"))

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		const N = 10000

		for i := range N {
			require.NoError(t, s.Create(ctx, conformance.NewPathResource("default", fmt.Sprintf("path-%d", i))))
		}

		watchCh := make(chan state.Event)

		for i := range N {
			require.NoError(t, s.Watch(ctx, conformance.NewPathResource("default", fmt.Sprintf("path-%d", i)).Metadata(), watchCh))
		}

		for range N {
			select {
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for event")
			case ev := <-watchCh:
				assert.Equal(t, state.Created, ev.Type)
			}
		}

		for i := range N {
			require.NoError(t, s.Destroy(ctx, conformance.NewPathResource("default", fmt.Sprintf("path-%d", i)).Metadata()))
		}

		for range N {
			select {
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for event")
			case ev := <-watchCh:
				assert.Equal(t, state.Destroyed, ev.Type, "ev: %v", ev.Resource)
			}
		}
	})
}

func TestWatchKindBug(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t, goleak.IgnoreCurrent()) })

	withEtcd(t, func(s state.State) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()

		const (
			initialCreated    = 100
			iterations        = 1000
			numObjects        = 1000
			watchEventTimeout = 10 * time.Second
		)

		for i := range initialCreated {
			require.NoError(t, s.Create(ctx, conformance.NewPathResource("default", fmt.Sprintf("path-%d", i))))
		}

		var watchChannels []chan state.Event

		for iteration := range iterations {
			t.Logf("iteration %d", iteration)

			dummuCh := make(chan state.Event)

			for j := range 20 {
				require.NoError(t, s.WatchKind(metadata.AppendToOutgoingContext(ctx, "bar", fmt.Sprintf("%d-%d", iteration, j)), conformance.NewPathResource(fmt.Sprintf("default-%d", j+iteration*20), "").Metadata(), dummuCh, state.WithBootstrapContents(true)))
			}

			watchCh := make(chan state.Event, numObjects)

			require.NoError(t, s.WatchKind(metadata.AppendToOutgoingContext(ctx, "foo", fmt.Sprintf("iter-%d", iteration)), conformance.NewPathResource("default", "").Metadata(), watchCh, state.WithBootstrapContents(true)))

			for range initialCreated {
				select {
				case <-time.After(watchEventTimeout):
					t.Fatal("timeout waiting for event")
				case ev := <-watchCh:
					assert.Equal(t, state.Created, ev.Type)
				}
			}

			select {
			case <-time.After(watchEventTimeout):
				t.Fatal("timeout waiting for event")
			case ev := <-watchCh:
				assert.Equal(t, state.Bootstrapped, ev.Type)
			}

			watchChannels = append(watchChannels, watchCh)

			for i := range numObjects {
				r := conformance.NewPathResource("default", fmt.Sprintf("o-%d", iteration*numObjects+i))

				for j := range 100 {
					r.Metadata().Labels().Set(fmt.Sprintf("label-%d", j), "prettybigvalueIwanttoputherejusttomakeitbig")
				}

				for j := range 100 {
					r.Metadata().Finalizers().Add(fmt.Sprintf("finalizer-%d", j))
				}

				require.NoError(t, s.Create(ctx, r))

				for j := range 100 {
					r.Metadata().Finalizers().Remove(fmt.Sprintf("finalizer-%d", j))
				}

				require.NoError(t, s.Update(ctx, r))
			}

			for i := range numObjects {
				require.NoError(t, s.Destroy(ctx, conformance.NewPathResource("default", fmt.Sprintf("o-%d", iteration*numObjects+i)).Metadata()))
			}

			for range numObjects {
				for _, watchCh := range watchChannels {
					select {
					case <-time.After(watchEventTimeout):
						t.Fatal("timeout waiting for event")
					case ev := <-watchCh:
						assert.Equal(t, state.Created, ev.Type)
					}

					select {
					case <-time.After(watchEventTimeout):
						t.Fatal("timeout waiting for event")
					case ev := <-watchCh:
						assert.Equal(t, state.Updated, ev.Type)
					}
				}
			}

			for range numObjects {
				for _, watchCh := range watchChannels {
					select {
					case <-time.After(watchEventTimeout):
						t.Fatal("timeout waiting for event")
					case ev := <-watchCh:
						assert.Equal(t, state.Destroyed, ev.Type)
					}
				}
			}
		}
	})
}

func TestWatchKindWithBootstrap(t *testing.T) {
	for _, test := range []struct {
		name                      string
		destroyIsTheLastOperation bool
	}{
		{
			name:                      "put is last",
			destroyIsTheLastOperation: false,
		},
		{
			name:                      "delete is last",
			destroyIsTheLastOperation: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

			withEtcd(t, func(s state.State) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				for i := range 3 {
					require.NoError(t, s.Create(ctx, conformance.NewPathResource("default", fmt.Sprintf("path-%d", i))))
				}

				if test.destroyIsTheLastOperation {
					require.NoError(t, s.Create(ctx, conformance.NewPathResource("default", "path-3")))
					require.NoError(t, s.Destroy(ctx, conformance.NewPathResource("default", "path-3").Metadata()))
				}

				watchCh := make(chan state.Event)

				require.NoError(t, s.WatchKind(ctx, conformance.NewPathResource("default", "").Metadata(), watchCh, state.WithBootstrapContents(true)))

				for i := range 3 {
					select {
					case <-time.After(time.Second):
						t.Fatal("timeout waiting for event")
					case ev := <-watchCh:
						assert.Equal(t, state.Created, ev.Type)
						assert.Equal(t, fmt.Sprintf("path-%d", i), ev.Resource.Metadata().ID())
						assert.IsType(t, &conformance.PathResource{}, ev.Resource)
					}
				}

				select {
				case <-time.After(time.Second):
					t.Fatal("timeout waiting for event")
				case ev := <-watchCh:
					assert.Equal(t, state.Bootstrapped, ev.Type)
				}

				require.NoError(t, s.Destroy(ctx, conformance.NewPathResource("default", "path-0").Metadata()))

				select {
				case <-time.After(time.Second):
					t.Fatal("timeout waiting for event")
				case ev := <-watchCh:
					assert.Equal(t, state.Destroyed, ev.Type, "event %s %s", ev.Type, ev.Resource)
					assert.Equal(t, "path-0", ev.Resource.Metadata().ID())
					assert.IsType(t, &conformance.PathResource{}, ev.Resource)
				}

				newR, err := safe.StateUpdateWithConflicts(ctx, s, conformance.NewPathResource("default", "path-1").Metadata(), func(r *conformance.PathResource) error {
					r.Metadata().Finalizers().Add("foo")

					return nil
				})
				require.NoError(t, err)

				select {
				case <-time.After(time.Second):
					t.Fatal("timeout waiting for event")
				case ev := <-watchCh:
					assert.Equal(t, state.Updated, ev.Type, "event %s %s", ev.Type, ev.Resource)
					assert.Equal(t, "path-1", ev.Resource.Metadata().ID())
					assert.Equal(t, newR.Metadata().Finalizers(), ev.Resource.Metadata().Finalizers())
					assert.Equal(t, newR.Metadata().Version(), ev.Resource.Metadata().Version())
					assert.IsType(t, &conformance.PathResource{}, ev.Resource)
				}
			})
		})
	}
}
