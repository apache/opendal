// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use log::trace;

use crate::raw::glob_matcher::{Segment, compile_segments};
use crate::raw::oio;
use crate::raw::oio::List as _;
use crate::raw::*;
use crate::*;

/// Provide client-side glob filtering for `list` operations via *guided
/// traversal* (apache/opendal#6535).
///
/// `GlobLayer` advertises [`Capability::list_with_glob`] for every service
/// that supports `list`. When a caller passes `OpList::with_glob(pattern)`:
///
/// - If the underlying service advertises native glob support
///   (`native_capability.list_with_glob`), the args are forwarded unchanged.
/// - Otherwise the pattern is compiled into per-segment matchers and the
///   layer performs a series of *targeted, single-level* list calls,
///   pruning branches that cannot satisfy the next segment. Compared to
///   `list-then-filter`:
///   - literal prefix segments are walked without listing at all,
///   - non-recursive segments (`*.jpg`, `[ab].txt`, `{x,y}.csv`) are
///     resolved with one list call per matching directory,
///   - `**` segments fan out only over directories that could plausibly
///     contain a match.
///
/// Glob semantics follow fsspec / `pathlib`:
///
/// - `*` matches anything within a single path segment.
/// - `?` matches any single character within a segment.
/// - `**` matches zero or more path segments (recursive).
/// - `[abc]` and `[!abc]` are character classes.
/// - `{a,b}` is alternation.
///
/// # `limit` semantics under glob
///
/// When the caller sets `OpList::with_limit(N)` together with a glob pattern,
/// the layer interprets it as "stop after `N` *matched* entries" — i.e. it
/// behaves as a cap on the filtered result stream, not the backend page
/// size. This diverges from the field's usual paging-hint semantics; it is
/// the behavior most callers expect when combining `glob` and `limit`.
///
/// # Example
///
/// ```no_run
/// use opendal_core::layers::GlobLayer;
/// use opendal_core::services;
/// use opendal_core::Operator;
///
/// # async fn run() -> opendal_core::Result<()> {
/// let op = Operator::new(services::Memory::default())?
///     .layer(GlobLayer)
///     .finish();
///
/// let entries = op.list_with("/").glob("**/*.jpg").await?;
/// # let _ = entries;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
pub struct GlobLayer;

impl<A: Access> Layer<A> for GlobLayer {
    type LayeredAccess = GlobAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let info = inner.info();
        info.update_full_capability(|mut cap| {
            if cap.list {
                cap.list_with_glob = true;
            }
            cap
        });

        GlobAccessor {
            info,
            inner: Arc::new(inner),
        }
    }
}

/// Accessor produced by [`GlobLayer`].
pub struct GlobAccessor<A: Access> {
    info: Arc<AccessorInfo>,
    inner: Arc<A>,
}

impl<A: Access> Debug for GlobAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<A: Access> GlobAccessor<A> {
    async fn glob_list(&self, path: &str, args: OpList) -> Result<(RpList, GlobLister<A>)> {
        // No glob requested — forward unchanged. Lister still wraps `inner`
        // so it remains a single uniform return type.
        let Some(pattern) = args.glob() else {
            let (rp, lister) = self.inner.list(path, args).await?;
            return Ok((rp, GlobLister::Passthrough(lister)));
        };

        // Service advertises native glob — forward args verbatim, including
        // the glob field. The driver below is bypassed.
        if self.info.native_capability().list_with_glob {
            let (rp, lister) = self.inner.list(path, args).await?;
            return Ok((rp, GlobLister::Passthrough(lister)));
        }

        // `start_after` is undefined under client-side glob (entries come
        // from many list calls in arbitrary order). Reject up front rather
        // than silently misbehave.
        if args.start_after().is_some() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "list_with_glob does not support start_after on services without native glob",
            ));
        }

        let segments = compile_segments(pattern)?;
        if segments.is_empty() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "list_with_glob requires a non-empty glob pattern",
            ));
        }
        let pattern_owned = pattern.to_string();

        // Build the forwarded `OpList` template by cloning the caller's args
        // and stripping the fields the guided traversal manages itself:
        // `glob` (we resolve it segment-by-segment), `limit` (we enforce
        // post-filter against `emitted`), and `start_after` (rejected
        // above). Recursive listing is forced off because the lister drives
        // descent one segment at a time.
        //
        // Cloning rather than reassembling means future fields added to
        // `OpList` are forwarded automatically — fields we *don't* know
        // about default to whatever the caller set.
        let mut template = args.clone();
        template.clear_glob();
        template.clear_limit();
        template.clear_start_after();
        let template = template.with_recursive(false);

        // Seed the queue with one frame at the requested list root.
        let mut queue = VecDeque::new();
        let seed_path = ensure_trailing_slash(path);
        let mut visited = HashSet::new();
        visited.insert((seed_path.clone(), 0usize));
        queue.push_back(Frame {
            path: seed_path,
            idx: 0,
        });

        Ok((
            RpList::default(),
            GlobLister::Guided(Box::new(GuidedLister {
                acc: self.inner.clone(),
                segments: Arc::from(segments.into_boxed_slice()),
                template,
                queue,
                active: None,
                visited,
                emitted: 0,
                emitted_paths: HashSet::new(),
                limit: args.limit().unwrap_or(usize::MAX),
                pattern: pattern_owned,
            })),
        ))
    }
}

impl<A: Access> LayeredAccess for GlobAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = A::Writer;
    type Lister = GlobLister<A>;
    type Deleter = A::Deleter;
    type Copier = A::Copier;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        self.inner().create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    async fn copy(
        &self,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<(RpCopy, Self::Copier)> {
        self.inner.copy(from, to, args, opts).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner().delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.glob_list(path, args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        self.inner.presign(path, args).await
    }
}

/// Lister produced by [`GlobLayer`].
pub enum GlobLister<A: Access> {
    /// No glob requested, or the service has native glob support — entries
    /// come directly from the inner lister.
    Passthrough(A::Lister),
    /// Guided traversal driving zero or more single-level list calls.
    Guided(Box<GuidedLister<A>>),
}

impl<A: Access> oio::List for GlobLister<A> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        match self {
            GlobLister::Passthrough(l) => l.next().await,
            GlobLister::Guided(g) => g.next_entry().await,
        }
    }
}

/// State machine that walks the glob pattern segment-by-segment via guided
/// traversal. Each frame in the queue represents the obligation "starting at
/// `path`, consume `segments[idx..]`".
pub struct GuidedLister<A: Access> {
    acc: Arc<A>,
    segments: Arc<[Segment]>,
    /// Template `OpList` used for every downstream list call. Fields managed
    /// by the lister itself (`glob`, `recursive`, `limit`) are pre-stripped.
    template: OpList,
    queue: VecDeque<Frame>,
    /// Currently active list call: `(parent_path, idx_of_segment_being_matched, lister)`.
    /// `idx_of_segment_being_matched` is the index whose matcher is applied
    /// to each emitted entry's basename.
    active: Option<ActiveList<A>>,
    /// Set of `(path, idx)` frames already enqueued. `**` admits multiple
    /// consumption strategies that converge on the same `(path, idx)`
    /// state; without dedup the lister would re-list those subtrees and
    /// re-emit terminal matches. This is the standard NFA→DFA trick.
    visited: HashSet<(String, usize)>,
    /// Number of entries already emitted to the caller. Compared against
    /// `limit` to enforce the caller-supplied cap.
    emitted: usize,
    limit: usize,
    /// Set of paths already emitted to the caller. Belt-and-braces dedup
    /// for the case where two distinct `(path, idx)` frames both produce
    /// the same terminal entry via different segments (e.g. `**` zero-match
    /// vs explicit literal match).
    emitted_paths: HashSet<String>,
    /// Original pattern string, kept for error context only.
    pattern: String,
}

struct ActiveList<A: Access> {
    parent: String,
    /// Index of the segment whose matcher applies to entries from this list
    /// call. For literal segments this is unused (handled inline before the
    /// list call). For glob segments and `**` segments this is the current
    /// segment index.
    idx: usize,
    /// True if this active list was opened to handle a `**` frame. Each
    /// child dir is then enqueued at the *same* index (still on `**`).
    double_star: bool,
    lister: A::Lister,
}

struct Frame {
    path: String,
    idx: usize,
}

impl<A: Access> GuidedLister<A> {
    /// Enqueue a frame iff its `(path, idx)` has not been seen before.
    fn enqueue(&mut self, path: String, idx: usize) {
        if self.visited.insert((path.clone(), idx)) {
            self.queue.push_back(Frame { path, idx });
        }
    }

    /// Pull the next matching entry, driving the traversal as needed.
    async fn next_entry(&mut self) -> Result<Option<oio::Entry>> {
        if self.emitted >= self.limit {
            return Ok(None);
        }

        loop {
            // Drain the currently active list first.
            if let Some(active) = self.active.as_mut() {
                match active.lister.next().await? {
                    Some(entry) => {
                        if let Some(emit) = self.handle_active_entry(entry)? {
                            if self.emitted_paths.insert(emit.path().to_string()) {
                                self.emitted += 1;
                                return Ok(Some(emit));
                            }
                        }
                        continue;
                    }
                    None => {
                        self.active = None;
                        continue;
                    }
                }
            }

            // No active list — pop the next frame.
            let Some(frame) = self.queue.pop_front() else {
                return Ok(None);
            };

            if let Some(emit) = self.advance_frame(frame).await? {
                if self.emitted_paths.insert(emit.path().to_string()) {
                    self.emitted += 1;
                    return Ok(Some(emit));
                }
            }
        }
    }

    /// Drive a popped frame forward. Returns `Some(entry)` if the frame
    /// directly produces a terminal match (only possible for trailing
    /// literal segments via stat). Otherwise the frame either enqueues
    /// further work or opens an active list call.
    async fn advance_frame(&mut self, frame: Frame) -> Result<Option<oio::Entry>> {
        // Consume zero-cost literal segments inline. `compile_segments`
        // folds consecutive literals into a single `Literal` variant, so at
        // most one iteration of this loop runs per frame — but we keep it
        // as a loop to remain robust against future segment-classification
        // changes.
        let mut path = frame.path;
        let mut idx = frame.idx;

        while idx < self.segments.len() {
            match &self.segments[idx] {
                Segment::Literal(run) => {
                    let mut child = path.clone();
                    child.push_str(run);
                    if idx + 1 == self.segments.len() {
                        // Terminal literal run — confirm existence via
                        // stat. Missing entries simply yield no match.
                        return self.stat_terminal(child).await;
                    }
                    child.push('/');
                    path = child;
                    idx += 1;
                }
                Segment::Glob(_) | Segment::DoubleStar => break,
            }
        }

        if idx == self.segments.len() {
            // Pattern ran out on a pure-literal path — the constructed
            // `path` *is* the target. Stat it to recover the real entry
            // mode rather than emitting a synthetic DIR (the path may be a
            // file). NotFound yields no match.
            //
            // `path` ends in `/` from the literal-walk above; strip it for
            // stat since most backends key files without a trailing slash.
            let probe = path.trim_end_matches('/');
            return self.stat_terminal(probe.to_string()).await;
        }

        match &self.segments[idx] {
            Segment::Glob(_) => {
                let (_rp, lister) = self.acc.list(&path, self.template.clone()).await?;
                self.active = Some(ActiveList {
                    parent: path,
                    idx,
                    double_star: false,
                    lister,
                });
                Ok(None)
            }
            Segment::DoubleStar => {
                // `**` matches zero or more components. The "zero match at
                // this depth" case is handled by enqueuing a frame that
                // skips past the `**` and tries `segments[idx+1..]` against
                // this same directory's children at their own segment
                // level. Without this, patterns like `**/**/file.txt` would
                // miss `file.txt` at intermediate depths because the
                // `handle_active_entry` fast-path only matches files when
                // the *very next* segment is terminal — it can't bridge two
                // consecutive `**`s. The `visited` set avoids re-listing if
                // the skipped-past frame would converge with an existing
                // one.
                //
                // (Consecutive `**` are collapsed in `compile_segments`, so
                // `idx+1` is never another `DoubleStar`. The enqueue is
                // still useful when `segments[idx+1]` is a Glob/Literal
                // that should be tried against this dir's children.)
                let next_idx = idx + 1;
                if next_idx < self.segments.len() {
                    self.enqueue(path.clone(), next_idx);
                }
                // List this directory and treat each entry inside
                // `handle_active_entry` as: match against `segments[idx+1]`
                // (covers `**` matching zero levels at this point), and for
                // each child dir, enqueue another `**` frame back at the
                // same `idx` so traversal keeps descending.
                let (_rp, lister) = self.acc.list(&path, self.template.clone()).await?;
                self.active = Some(ActiveList {
                    parent: path,
                    idx,
                    double_star: true,
                    lister,
                });
                Ok(None)
            }
            Segment::Literal(_) => unreachable!("literals already consumed"),
        }
    }

    /// Process an entry produced by the currently active list call. Returns
    /// `Some(entry)` if it's a terminal match to emit, `None` otherwise
    /// (entry was either pruned or enqueued for further descent).
    fn handle_active_entry(&mut self, entry: oio::Entry) -> Result<Option<oio::Entry>> {
        let active = self.active.as_ref().expect("active guaranteed by caller");
        let parent = active.parent.as_str();
        let idx = active.idx;
        let double_star = active.double_star;

        let entry_path = entry.path();
        // Skip listings of the parent itself (some backends emit it).
        if entry_path == parent {
            return Ok(None);
        }

        // Compute the basename relative to parent. Strip parent prefix and
        // any leading slash, then trim a single trailing slash for dirs.
        let rel = entry_path
            .strip_prefix(parent)
            .unwrap_or(entry_path)
            .trim_start_matches('/');
        let (name, is_dir_suffix) = match rel.strip_suffix('/') {
            Some(n) => (n, true),
            None => (rel, false),
        };
        if name.is_empty() {
            return Ok(None);
        }
        if name.contains('/') {
            // Sub-paths can leak through if the backend doesn't fully honor
            // non-recursive listing. Skip them — the guided traversal only
            // operates on direct children. Trace-log so backend bugs are
            // observable rather than silently misclassified as "no match".
            trace!(
                target: "opendal::layers::glob",
                "guided traversal: skipping sub-path entry {entry_path:?} returned by non-recursive list of {parent:?}",
            );
            return Ok(None);
        }

        let is_dir = entry.mode().is_dir() || is_dir_suffix;
        // Use the backend-supplied entry path verbatim for descent. This
        // preserves whatever path convention the backend operates in
        // (leading slash or not) and avoids synthesizing a path the next
        // `acc.list()` call might reject (`build_abs_path` debug-asserts
        // that `path` does not start with `/`).
        let child_path = if is_dir {
            let p = entry_path.to_string();
            if p.ends_with('/') { p } else { format!("{p}/") }
        } else {
            entry_path.to_string()
        };

        if double_star {
            // We're inside a `**` frame. The frame's `idx` still points at
            // the `**` segment. Each child dir always becomes a new frame
            // back at the same `idx` so `**` can keep descending. We also
            // try to match `segments[idx+1]` against the entry's basename;
            // if that match would be terminal we emit immediately.
            if is_dir {
                self.enqueue(child_path.clone(), idx);
            }

            let next_idx = idx + 1;
            if next_idx >= self.segments.len() {
                // Trailing `**` — every entry matches.
                return Ok(Some(entry));
            }

            let matched = match &self.segments[next_idx] {
                Segment::Literal(lit) => lit == name,
                Segment::Glob(m) => m.matches(name),
                Segment::DoubleStar => true,
            };
            if !matched {
                return Ok(None);
            }

            if next_idx + 1 == self.segments.len() {
                // Terminal match against next segment.
                return Ok(Some(entry));
            }
            if is_dir {
                // Non-terminal — descend past both `**` and the matched
                // segment.
                self.enqueue(child_path, next_idx + 1);
            }
            Ok(None)
        } else {
            // Single-segment glob frame. The matcher at `idx` applies to
            // basenames; matches either become terminal (last segment) or
            // descent frames (more segments).
            let matched = match &self.segments[idx] {
                Segment::Glob(m) => m.matches(name),
                Segment::Literal(_) | Segment::DoubleStar => {
                    unreachable!("non-double-star active lists are always Glob")
                }
            };
            if !matched {
                return Ok(None);
            }

            if idx + 1 == self.segments.len() {
                // Terminal match — emit the entry as-is.
                return Ok(Some(entry));
            }
            if is_dir {
                self.enqueue(child_path, idx + 1);
            }
            Ok(None)
        }
    }

    // Note: `(path, idx)` frame dedup via `visited` collapses most overlap;
    // a path-level dedup at the emission site catches the remaining cases
    // where two distinct frames produce the same terminal entry inline.

    /// Probe a terminal literal segment. NotFound is silent (no match);
    /// other errors propagate.
    async fn stat_terminal(&self, path: String) -> Result<Option<oio::Entry>> {
        match self.acc.stat(&path, OpStat::new()).await {
            Ok(rp) => Ok(Some(oio::Entry::new(&path, rp.into_metadata()))),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.with_context("pattern", self.pattern.clone())),
        }
    }
}

fn ensure_trailing_slash(path: &str) -> String {
    // Inside the guided traversal we use the "no leading slash" path
    // convention that `build_abs_path` enforces on accessors. The user-
    // facing root path `/` therefore collapses to `""`, which every
    // accessor treats as "list the root".
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        String::new()
    } else if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::Operator;
    use crate::services;

    async fn seed(paths: &[&str]) -> Operator {
        let op = Operator::new(services::Memory::default())
            .unwrap()
            .layer(GlobLayer)
            .finish();
        for p in paths {
            op.write(p, "x").await.unwrap();
        }
        op
    }

    async fn glob(op: &Operator, pattern: &str) -> BTreeSet<String> {
        op.list_with("/")
            .glob(pattern)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.path().to_string())
            .collect()
    }

    #[tokio::test]
    async fn capability_advertised() {
        let op = Operator::new(services::Memory::default())
            .unwrap()
            .layer(GlobLayer)
            .finish();
        assert!(op.info().full_capability().list_with_glob);
        // Native memory has no glob support — layer is what enables it.
        assert!(!op.info().native_capability().list_with_glob);
    }

    #[tokio::test]
    async fn empty_pattern_rejected() {
        let op = seed(&["a.txt"]).await;
        let err = op.list_with("/").glob("").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[tokio::test]
    async fn simple_star_at_root() {
        let op = seed(&["a.jpg", "b.jpg", "c.png", "sub/d.jpg"]).await;
        let got = glob(&op, "*.jpg").await;
        assert_eq!(
            got,
            BTreeSet::from(["a.jpg".to_string(), "b.jpg".to_string()])
        );
    }

    #[tokio::test]
    async fn double_star_recursive() {
        let op = seed(&["a.jpg", "x/b.jpg", "x/y/c.jpg", "x/y/d.png"]).await;
        let got = glob(&op, "**/*.jpg").await;
        assert_eq!(
            got,
            BTreeSet::from([
                "a.jpg".to_string(),
                "x/b.jpg".to_string(),
                "x/y/c.jpg".to_string(),
            ])
        );
    }

    #[tokio::test]
    async fn literal_prefix() {
        let op = seed(&["a/x.jpg", "a/y.jpg", "b/x.jpg"]).await;
        let got = glob(&op, "a/*.jpg").await;
        assert_eq!(
            got,
            BTreeSet::from(["a/x.jpg".to_string(), "a/y.jpg".to_string()])
        );
    }

    /// Long leading literal prefix (`a/b/c/**/*.jpg`) is folded into a
    /// single Literal segment at compile time. Verify behaviour is
    /// unchanged.
    #[tokio::test]
    async fn long_literal_prefix_then_recursive_glob() {
        let op = seed(&[
            "a/b/c/x.jpg",
            "a/b/c/d/y.jpg",
            "a/b/c/d/e/z.jpg",
            "a/b/c/w.png",
            "a/b/other/q.jpg",
        ])
        .await;
        let got = glob(&op, "a/b/c/**/*.jpg").await;
        assert_eq!(
            got,
            BTreeSet::from([
                "a/b/c/x.jpg".to_string(),
                "a/b/c/d/y.jpg".to_string(),
                "a/b/c/d/e/z.jpg".to_string(),
            ])
        );
    }

    #[tokio::test]
    async fn double_star_middle() {
        let op = seed(&["a/b.txt", "a/x/b.txt", "a/x/y/b.txt", "a/x/y/c.txt"]).await;
        let got = glob(&op, "a/**/b.txt").await;
        assert_eq!(
            got,
            BTreeSet::from([
                "a/b.txt".to_string(),
                "a/x/b.txt".to_string(),
                "a/x/y/b.txt".to_string(),
            ])
        );
    }

    /// Regression: pattern `**/**/x.txt` against root-level and intermediate
    /// `x.txt` files. Before the consecutive-`**` collapse + the
    /// "enqueue (path, idx+1) on DS open" fix, files matched only by the
    /// zero-zero case were silently dropped (see review of
    /// `apache/opendal#6535`).
    #[tokio::test]
    async fn consecutive_double_stars_match_zero_at_every_depth() {
        let op = seed(&["x.txt", "a/x.txt", "a/b/x.txt", "a/b/c/x.txt", "y.txt"]).await;
        let got = glob(&op, "**/**/x.txt").await;
        assert_eq!(
            got,
            BTreeSet::from([
                "x.txt".to_string(),
                "a/x.txt".to_string(),
                "a/b/x.txt".to_string(),
                "a/b/c/x.txt".to_string(),
            ])
        );
    }

    /// `**` and `**/**` are semantically equivalent ("zero or more
    /// components"). Verify the collapse keeps results identical.
    #[tokio::test]
    async fn double_star_collapse_equivalent_to_single() {
        let op = seed(&["a.jpg", "x/b.jpg", "x/y/c.jpg"]).await;
        let single = glob(&op, "**/*.jpg").await;
        let collapsed = glob(&op, "**/**/*.jpg").await;
        let triple = glob(&op, "**/**/**/*.jpg").await;
        assert_eq!(single, collapsed);
        assert_eq!(single, triple);
    }

    /// `a/**/b/**/c` — multiple `**` separated by literals. Exercises both
    /// the DS-frame enqueue and the `visited` dedup across distinct paths.
    #[tokio::test]
    async fn multiple_double_stars_with_literal_separators() {
        let op = seed(&[
            "a/b/c",
            "a/x/b/c",
            "a/b/y/c",
            "a/x/b/y/c",
            "a/x/b/y/z/c",
            "a/different",
        ])
        .await;
        let got = glob(&op, "a/**/b/**/c").await;
        assert_eq!(
            got,
            BTreeSet::from([
                "a/b/c".to_string(),
                "a/x/b/c".to_string(),
                "a/b/y/c".to_string(),
                "a/x/b/y/c".to_string(),
                "a/x/b/y/z/c".to_string(),
            ])
        );
    }

    #[tokio::test]
    async fn alternation_and_class() {
        let op = seed(&["a.jpg", "b.png", "c.gif", "d.webp", "e.txt"]).await;
        let got = glob(&op, "*.{jpg,png,gif}").await;
        assert_eq!(
            got,
            BTreeSet::from([
                "a.jpg".to_string(),
                "b.png".to_string(),
                "c.gif".to_string(),
            ])
        );

        let got = glob(&op, "[ab].*").await;
        assert_eq!(
            got,
            BTreeSet::from(["a.jpg".to_string(), "b.png".to_string()])
        );
    }

    #[tokio::test]
    async fn literal_only_pattern_emits_via_stat() {
        let op = seed(&["a/b/c.txt"]).await;
        let got = glob(&op, "a/b/c.txt").await;
        assert_eq!(got, BTreeSet::from(["a/b/c.txt".to_string()]));

        // Missing terminal literal yields no match (not an error).
        let got = glob(&op, "a/b/missing.txt").await;
        assert!(got.is_empty());
    }

    /// `start_after` is rejected when combined with glob on a non-native
    /// service.
    #[tokio::test]
    async fn start_after_with_glob_rejected() {
        let op = seed(&["a.txt"]).await;
        let err = op
            .list_with("/")
            .glob("*.txt")
            .start_after("a.txt")
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unsupported);
    }

    /// `limit` is applied to *matched* entries, not raw backend page size.
    #[tokio::test]
    async fn limit_caps_filtered_results() {
        let op = seed(&["a.jpg", "b.jpg", "c.jpg", "d.png"]).await;
        let got: Vec<String> = op
            .list_with("/")
            .glob("*.jpg")
            .limit(2)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.path().to_string())
            .collect();
        assert_eq!(got.len(), 2);
        for p in &got {
            assert!(p.ends_with(".jpg"));
        }
    }

    /// Without a glob the layer should be transparent: the inner lister's
    /// output reaches the caller unmodified.
    #[tokio::test]
    async fn no_glob_is_passthrough() {
        let op = seed(&["a.txt", "b.txt"]).await;
        let got: BTreeSet<String> = op
            .list("/")
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.path().to_string())
            .collect();
        assert!(got.contains("a.txt"));
        assert!(got.contains("b.txt"));
    }
}
