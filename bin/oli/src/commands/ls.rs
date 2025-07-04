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

use anyhow::Result;
use futures::TryStreamExt;
use std::collections::BTreeMap;

use crate::config::Config;
use crate::make_tokio_runtime;
use crate::params::config::ConfigParams;

const TREE_LAST_ITEM: &str = "└── ";
const TREE_MIDDLE_ITEM: &str = "├── ";
const TREE_EMPTY_PREFIX: &str = "    ";
const TREE_VERTICAL_LINE: &str = "│   ";

#[derive(Debug, clap::Parser)]
#[command(name = "ls", about = "List object", disable_version_flag = true)]
pub struct LsCmd {
    #[command(flatten)]
    pub config_params: ConfigParams,
    /// In the form of `<profile>:/<path>`.
    #[arg()]
    pub target: String,
    /// List objects recursively.
    #[arg(short, long)]
    pub recursive: bool,
    /// List objects in a tree-like format.
    #[arg(short = 'T', long)]
    pub tree: bool,
}

/// A node in the tree representation of a directory.
///
/// It can be a directory or a file. A directory is a node with `is_dir` set to `true`.
struct TreeNode {
    is_dir: bool,
    children: BTreeMap<String, TreeNode>,
}

impl TreeNode {
    fn new() -> Self {
        Self {
            is_dir: false,
            children: BTreeMap::new(),
        }
    }
}

impl LsCmd {
    pub fn run(self) -> Result<()> {
        make_tokio_runtime(1).block_on(self.do_run())
    }

    async fn do_run(self) -> Result<()> {
        let cfg = Config::load(&self.config_params.config)?;

        let (op, path) = cfg.parse_location(&self.target)?;

        if self.tree {
            return self.run_tree(&op, &path).await;
        }

        if !self.recursive {
            let mut ds = op.lister(&path).await?;
            while let Some(de) = ds.try_next().await? {
                println!("{}", de.name());
            }
            return Ok(());
        }

        let mut ds = op.lister_with(&path).recursive(true).await?;
        while let Some(de) = ds.try_next().await? {
            println!("{}", de.path());
        }
        Ok(())
    }

    /// List objects in a tree-like format.
    ///
    /// This function fetches all entries recursively, builds a `TreeNode` structure representing
    /// the directory hierarchy, and then prints it using `print_tree`. The root of the tree
    /// is displayed as `.`.
    async fn run_tree(&self, op: &opendal::Operator, path: &str) -> Result<()> {
        let mut root = TreeNode::new();

        let mut ds = op.lister_with(path).recursive(true).await?;
        while let Some(de) = ds.try_next().await? {
            let p = de.path();
            let is_dir = p.ends_with('/');
            let rel_p = p.strip_prefix(path).unwrap_or(p);

            let mut current_node = &mut root;
            let components: Vec<&str> = rel_p.split('/').filter(|s| !s.is_empty()).collect();

            if let Some((last, elements)) = components.split_last() {
                for part in elements {
                    current_node = current_node
                        .children
                        .entry(part.to_string())
                        .or_insert_with(TreeNode::new);
                    current_node.is_dir = true;
                }
                let node = current_node
                    .children
                    .entry(last.to_string())
                    .or_insert_with(TreeNode::new);
                node.is_dir |= is_dir;
            }
        }

        println!(".");
        print_tree(&root, "");

        Ok(())
    }
}

/// Print the tree structure recursively.
///
/// This function iterates through the children of a `TreeNode`, printing each one with appropriate
/// connectors to form a tree-like structure. It determines whether a child is the last in the list
/// to use the correct connector (`└── ` for the last, `├── ` for others). It then recursively
/// calls itself for subdirectories, adjusting the prefix to maintain the tree alignment.
fn print_tree(node: &TreeNode, prefix: &str) {
    let mut it = node.children.iter().peekable();
    while let Some((name, child_node)) = it.next() {
        let is_last = it.peek().is_none();

        let connector = if is_last {
            TREE_LAST_ITEM
        } else {
            TREE_MIDDLE_ITEM
        };
        let display_name = if child_node.is_dir {
            format!("{name}/")
        } else {
            name.to_string()
        };
        println!("{prefix}{connector}{display_name}");

        if !child_node.children.is_empty() {
            let new_prefix = if is_last {
                TREE_EMPTY_PREFIX
            } else {
                TREE_VERTICAL_LINE
            };
            print_tree(child_node, &format!("{prefix}{new_prefix}"));
        }
    }
}
