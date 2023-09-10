//! Helpers for windowed parallel execution of futures or tasks.
//! 
//! This provides helpers to spawn or join a set of futures or async tasks while limiting the number 
//! of parallel operations to a configurable window size to avoid overwhelming the executor or
//! target services.

#![feature(noop_waker)]

use std::{
    marker::Unpin,
    task::{Poll, Context},
    pin::Pin,
    boxed::Box,
};

use futures::{Future, FutureExt};
use log::debug;

/// Join a provided set of futures returning an unordered collection of results
///
/// This is provided for equivalence with [futures::future::join_all], you may prefer [exec_windowed_unordered] which
/// will create each future at the time of execution instead of upfront.
pub async fn join_windowed_unordered<F: Future>(window: usize, futures: impl Iterator<Item=F>) -> Vec<<F as Future>::Output> {
    exec_windowed_unordered(window, futures, |f| f).await
}

/// Join a provided set of futures returning an collection of results matching the order of the input iterator
/// 
/// This is provided for equivalence with [futures::future::join_all], you may prefer [exec_windowed_ordered] which
/// will create each future at the time of execution instead of upfront.
pub async fn join_windowed_ordered<F: Future>(window: usize, futures: impl Iterator<Item=F>) -> Vec<<F as Future>::Output> {
    // Execute windowing future with enumerated results
    exec_windowed_ordered(window, futures, |f| f).await
}

/// Execute an async closure for a provided set of inputs returning an unordered collection of results
pub async fn exec_windowed_unordered<I, R: Future>(window: usize, inputs: impl Iterator<Item=I>, f: impl Fn(I) -> R) -> Vec<<R as Future>::Output> {
    let w = FutureWindow::new(window, inputs, f);
    w.await
}

/// Execute an async closure for a provided set of inputs returning an collection of results matching the order of the input iterator
pub async fn exec_windowed_ordered<I, R: Future>(window: usize, inputs: impl Iterator<Item=I>, f: impl Fn(I) -> R) -> Vec<<R as Future>::Output> {
    // Execute windowing future with enumerated results
    let w = FutureWindow::new(window, inputs.enumerate(), |(n, i)| {
        let t = f(i);
        Box::pin(async move {
            let r = t.await;
            (n, r)
        })
    });
    let mut r = w.await;

    // Re-sort results and drop indexes
    r.sort_by_key(|(n, _r)| *n);
    r.drain(..).map(|(_n, r)| r).collect()
}

/// Resolve a collection of futures using a window of maximum parallel tasks
pub struct FutureWindow<I: Iterator, R: Future, F: Fn(<I as Iterator>::Item) -> R> {
    /// Parallelisation window size (maximum number of parallel tasks)
    window: usize,

    /// Iterator over inputs for parallel operation
    inputs: I,

    /// Closure to be executed per iterator item to create a pollable future
    f: F,

    /// Currently executing futures
    current: Vec<Pin<Box<R>>>,

    /// Completed executor results
    results: Vec<<R as Future>::Output>,

    /// Task counter
    count: usize,
}


impl <I: Iterator, R: Future, F: Fn(<I as Iterator>::Item) -> R> Unpin for FutureWindow<I, R, F> {}

impl <I: Iterator, R: Future, F: Fn(<I as Iterator>::Item) -> R> FutureWindow<I, R, F> {
    /// Create a new windowing future over an iterator of inputs with a function
    /// translating inputs into futures for execution.
    ///
    /// This will execute each future ensuring only n futures are executed in parallel,
    /// resolving into an **unsorted** array of results.
    pub fn new(n: usize, inputs: I, f: F) -> Self {
        Self {
            window: n,
            inputs,
            f,
            current: Vec::new(),
            results: Vec::new(),
            count: 0,
        }
    }

    fn update(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Vec<<R as Future>::Output>> {
        let mut pending_tasks = true;

        println!("poll!");

        // Ensure we're running n tasks
        while self.current.len() < self.window {
            match self.inputs.next() {
                // If we have remaining inputs, start tasks
                Some(v) => {
                    println!("new task {}", self.count);

                    let f = (self.f)(v);
                    self.current.push(Box::pin(f));
                    self.count += 1;

                    // Ensure we wake any time we add a new task
                    cx.waker().clone().wake();
                },
                // Otherwise, skip this and set flag indicating no new tasks are available
                None => {
                    println!("no pending tasks");
                    pending_tasks = false;
                    break;
                },
            }
        }

        // Poll for completion of current tasks
        let mut current: Vec<_> = self.current.drain(..).collect();
        for mut c in current.drain(..) {
            // Handle completion or re-stack for next future
            match c.poll_unpin(cx) {
                Poll::Ready(v) => {
                    println!("completed task");
                    // Store result and drop future
                    self.results.push(v);
                },
                Poll::Pending => {
                    // Keep tracking future
                    self.current.push(c);
                },
            }
        }

        // Complete when we have no pending tasks and the current list is empty
        if self.current.is_empty() && !pending_tasks {
            debug!("{} tasks complete", self.results.len());
            Poll::Ready(self.results.drain(..).collect())

        // Force wake if any tasks have completed but we still have some pending
        } else if self.current.len() < self.window && pending_tasks {
            cx.waker().clone().wake();
            Poll::Pending

        } else {
            Poll::Pending
        }
    }
}

/// [Future] implementation for [FutureWindow]
impl <I: Iterator, R: Future, F: Fn(<I as Iterator>::Item) -> R> Future for FutureWindow<I, R, F> {
    type Output = Vec<<R as Future>::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
       Self::update(&mut self.as_mut(), cx)
    }
}


#[cfg(test)]
mod tests {
    use std::task::Waker;

    use super::*;

    /// Poll-able helper that returns index after a configurable number of polls
    struct NPollMan {
        index: usize,
        polls: usize,
    }

    impl NPollMan {
        /// [NPollMan] that returns after one poll
        fn one_poll(index: usize) -> Self {
            Self{ index, polls: 1 }
        }

        /// [NPollMan] that returns after N polls
        fn n_poll(index: usize, polls: usize) -> Self {
            Self{ index, polls }
        }
    }

    impl Future for NPollMan {
        type Output = usize;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            // Check remaining number of polls
            match self.polls == 0 {
                // Return ready when completed
                true => Poll::Ready(self.index),
                // Otherwise decrement and return pending
                false => {
                    self.as_mut().polls -= 1;
                    cx.waker().clone().wake();

                    Poll::Pending
                }
            }
        }
    }

    async fn one_poll(index: usize) -> usize {
        let o = NPollMan::one_poll(index);
        o.await
    }

    #[test]
    fn test_window_internals() {
        let waker = Waker::noop();
        let mut ctx = Context::from_waker(&waker);

        let n = 2;

        // Create unordered windowing executor
        let mut w = FutureWindow::new(n, 0..5, |n| one_poll(n) );

        // Check we're not yet executing anything
        assert_eq!(w.current.len(), 0);
        assert_eq!(w.results.len(), 0);

        // First poll should queue up n tasks to run
        assert_eq!(w.poll_unpin(&mut ctx).is_pending(), true);
        assert_eq!(w.current.len(), n);
        assert_eq!(w.results.len(), 0);
        
        // Second poll should complete n tasks
        assert_eq!(w.poll_unpin(&mut ctx).is_pending(), true);
        assert_eq!(w.current.len(), 0);
        assert_eq!(w.results.len(), n);

        // Next poll should enqueue the next N tasks
        assert_eq!(w.poll_unpin(&mut ctx).is_pending(), true);
        assert_eq!(w.current.len(), n);
        assert_eq!(w.results.len(), n);

        // Next poll should complete the next N tasks
        assert_eq!(w.poll_unpin(&mut ctx).is_pending(), true);
        assert_eq!(w.current.len(), 0);
        assert_eq!(w.results.len(), 2 * n);

        // Next poll should enqueue the final task
        assert_eq!(w.poll_unpin(&mut ctx).is_pending(), true);
        assert_eq!(w.current.len(), 1);
        assert_eq!(w.results.len(), 2 * n);

        // Next poll should complete the final task, returning the result
        let r = w.poll_unpin(&mut ctx);
        assert_eq!(r.is_pending(), false);
        assert_eq!(w.current.len(), 0);
        assert_eq!(r, Poll::Ready(vec![0, 1, 2, 3, 4]));
    }

    async fn reverse_poll(index: usize, count: usize) -> usize {
        let o = NPollMan::n_poll(index, count + 1 - index);
        o.await
    }

    #[tokio::test]
    async fn test_window_ordered() {
        // Check unordered execution
        let w = exec_windowed_unordered(2, 0..5, |n| reverse_poll(n, 5) ).await;
        assert_ne!(w, vec![0, 1, 2, 3, 4]);

        // Check ordered execution
        let w = exec_windowed_ordered(2, 0..5, |n| reverse_poll(n, 5) ).await;
        assert_eq!(w, vec![0, 1, 2, 3, 4]);
    }
}
