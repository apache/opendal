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

//! Generate `website/data/capabilities.json` from core's `Capability` struct
//! and each service crate's native `Capability { ... }` initializer.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use proc_macro2::Span;
use serde::Serialize;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{
    Attribute, BinOp, Expr, ExprBinary, ExprCall, ExprCast, ExprField, ExprGroup, ExprIf, ExprLit,
    ExprMacro, ExprMethodCall, ExprParen, ExprPath, ExprStruct, ExprUnary, Item, ItemConst,
    ItemMod, Lit, Member, Meta, Stmt, Type, TypePath, UnOp,
};

use super::capability::{self, CapabilityField};
use super::parser::Services;

const SUMMARY_FIELDS: &[&str] = &[
    "stat",
    "read",
    "write",
    "create_dir",
    "delete",
    "copy",
    "compose",
    "restore",
    "rename",
    "list",
    "presign",
    "shared",
];

const GROUPS: &[CapabilityGroupDoc] = &[
    CapabilityGroupDoc {
        id: "summary",
        label: "Summary",
        description: "Core storage operations and concurrency support declared by each service.",
    },
    CapabilityGroupDoc {
        id: "stat",
        label: "Stat",
        description: "Metadata retrieval, conditional stat checks, and response header overrides.",
    },
    CapabilityGroupDoc {
        id: "read",
        label: "Read",
        description: "Object reads, suffix ranges, conditional reads, versioning, and header overrides.",
    },
    CapabilityGroupDoc {
        id: "write",
        label: "Write",
        description: "Single and multipart writes, append, metadata headers, conditional writes, and size limits.",
    },
    CapabilityGroupDoc {
        id: "list",
        label: "List",
        description: "Directory and prefix listing, pagination, native recursion, versions, deleted entries, and native glob.",
    },
    CapabilityGroupDoc {
        id: "delete",
        label: "Delete",
        description: "Object deletion, native recursive deletion, conditional deletes, versioning, and batch size limits.",
    },
    CapabilityGroupDoc {
        id: "copy",
        label: "Copy",
        description: "Server-side copy, segmented multipart copy, conditional copy, and metadata replacement.",
    },
    CapabilityGroupDoc {
        id: "compose",
        label: "Compose",
        description: "Server-side object composition from existing source objects.",
    },
    CapabilityGroupDoc {
        id: "presign",
        label: "Presign",
        description: "Presigned HTTP URL generation for client-side stat, read, write, and delete.",
    },
    CapabilityGroupDoc {
        id: "other",
        label: "Other",
        description: "Directory creation, rename, object restoration, and shared operator access.",
    },
];

#[derive(Debug, Serialize)]
pub struct CapabilitiesOutput {
    pub groups: &'static [CapabilityGroupDoc],
    pub fields: Vec<CapabilityFieldDoc>,
    pub services: Vec<ServiceCapabilityDoc>,
}

#[derive(Debug, Serialize)]
pub struct CapabilityGroupDoc {
    pub id: &'static str,
    pub label: &'static str,
    pub description: &'static str,
}

#[derive(Debug, Serialize)]
pub struct CapabilityFieldDoc {
    pub name: String,
    pub short: String,
    pub group: &'static str,
    pub kind: &'static str,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub summary: bool,
    pub doc: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum NativeBoolDoc {
    Supported(bool),
    Conditional(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LimitValueDoc {
    pub display: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServiceCapabilityDoc {
    pub name: String,
    pub scheme: String,
    pub native: BTreeMap<String, NativeBoolDoc>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub limits: BTreeMap<String, LimitValueDoc>,
}

#[derive(Debug, Clone)]
struct ConstDef {
    rel_path: String,
    line: usize,
    name: String,
    /// `None` = all targets, `Some(true)` = 64-bit only, `Some(false)` = non-64-bit only.
    is_64bit_only: Option<bool>,
    expr: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvalLimit {
    Exact(u64),
    UsizeMax,
}

#[derive(Debug, Default)]
struct RawLimit {
    v64: Option<EvalLimit>,
    v32: Option<EvalLimit>,
    platform_cond: Option<String>,
}

pub fn generate(workspace_dir: &Path, services: &Services) -> Result<()> {
    let output = extract_all(workspace_dir, services)?;

    let path = workspace_dir.join("website/data/capabilities.json");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(&output)?;
    fs::write(&path, format!("{json}\n"))
        .with_context(|| format!("failed to write {}", path.display()))?;
    log::info!(
        "generated {} with {} services and {} capability fields",
        path.display(),
        output.services.len(),
        output.fields.len()
    );
    Ok(())
}

pub fn extract_all(workspace_dir: &Path, services: &Services) -> Result<CapabilitiesOutput> {
    let raw_fields = capability::parse(&workspace_dir.join("core/core/src/types/capability.rs"))?;
    let fields: Vec<CapabilityFieldDoc> = raw_fields.iter().map(build_field_doc).collect();
    let field_types: HashMap<&str, bool> = raw_fields
        .iter()
        .map(|f| (f.name.as_str(), f.is_bool))
        .collect();

    let mut service_docs = Vec::with_capacity(services.len());
    for (name, service) in services {
        let doc =
            extract_service_capability(workspace_dir, name, &service.source_dir, &field_types)
                .with_context(|| format!("failed to extract capability for service `{name}`"))?;
        service_docs.push(doc);
    }
    service_docs.sort_by(|a, b| a.scheme.cmp(&b.scheme));

    Ok(CapabilitiesOutput {
        groups: GROUPS,
        fields,
        services: service_docs,
    })
}

fn build_field_doc(field: &CapabilityField) -> CapabilityFieldDoc {
    let mut group = "other";
    let mut short = field.name.clone();
    for (g, prefix) in [
        ("stat", "stat_"),
        ("read", "read_"),
        ("write", "write_"),
        ("list", "list_"),
        ("delete", "delete_"),
        ("copy", "copy_"),
        ("compose", "compose_"),
        ("presign", "presign_"),
    ] {
        if field.name == g {
            group = g;
            break;
        }
        if let Some(stripped) = field.name.strip_prefix(prefix) {
            group = g;
            short = stripped.to_string();
            break;
        }
    }

    let kind = if field.is_bool {
        "bool"
    } else if field.name == "delete_max_size" {
        "count"
    } else {
        "bytes"
    };

    // Strip trailing sentences that reference Rust-only `Type::method` APIs
    // (such as `Writer::copy_from`) so cross-language pages stay binding-neutral.
    let doc = sanitize_website_doc(&field.doc);

    CapabilityFieldDoc {
        summary: SUMMARY_FIELDS.contains(&field.name.as_str()),
        name: field.name.clone(),
        short,
        group,
        kind,
        doc,
    }
}

fn sanitize_website_doc(doc: &str) -> String {
    let kept: Vec<&str> = doc
        .split_inclusive(". ")
        .map(str::trim)
        .filter(|sentence| !sentence.contains("::"))
        .collect();
    kept.join(" ")
}

fn extract_service_capability(
    workspace_dir: &Path,
    name: &str,
    src_dir: &Path,
    field_types: &HashMap<&str, bool>,
) -> Result<ServiceCapabilityDoc> {
    let mut consts = Vec::new();
    collect_consts_in_dir(workspace_dir, src_dir, &mut consts)?;
    let (cap_rel_path, expr_struct) = find_service_capability_struct(workspace_dir, src_dir, name)?;

    parse_capability_literal(name, &cap_rel_path, &expr_struct, field_types, &consts)
}

fn parse_capability_literal(
    name: &str,
    rel_path: &str,
    expr_struct: &ExprStruct,
    field_types: &HashMap<&str, bool>,
    consts: &[ConstDef],
) -> Result<ServiceCapabilityDoc> {
    if let Some(rest) = &expr_struct.rest
        && !is_default_expr(rest)
    {
        bail!(
            "{}: unsupported Capability struct update base; expected `..Default::default()`",
            loc(rel_path, rest.span())
        );
    }

    let mut seen_64 = HashSet::new();
    let mut seen_32 = HashSet::new();
    let mut native = BTreeMap::new();
    let mut raw_limits: BTreeMap<String, RawLimit> = BTreeMap::new();

    for fv in &expr_struct.fields {
        let Member::Named(ident) = &fv.member else {
            bail!(
                "{}: tuple member in Capability struct is unsupported",
                loc(rel_path, fv.span())
            );
        };
        let field_name = ident.to_string();
        let Some(&is_bool) = field_types.get(field_name.as_str()) else {
            bail!(
                "{}: unknown Capability field `{field_name}`",
                loc(rel_path, ident.span())
            );
        };

        let (ptr_cfg, platform_cond) = parse_cfgs(&fv.attrs, rel_path)?;
        if ptr_cfg != Some(false) && !seen_64.insert(field_name.clone()) {
            bail!(
                "{}: duplicate Capability field `{field_name}`",
                loc(rel_path, ident.span())
            );
        }
        if ptr_cfg != Some(true) && !seen_32.insert(field_name.clone()) {
            bail!(
                "{}: duplicate Capability field `{field_name}`",
                loc(rel_path, ident.span())
            );
        }

        if is_bool {
            if ptr_cfg.is_some() {
                bail!(
                    "{}: pointer-width cfg on boolean capability `{field_name}` is unsupported",
                    loc(rel_path, fv.span())
                );
            }
            if let Some(doc) = eval_bool_field(&fv.expr, platform_cond, rel_path)? {
                native.insert(field_name, doc);
            }
        } else {
            if ptr_cfg.is_some() && platform_cond.is_some() {
                bail!(
                    "{}: combining platform and pointer-width #[cfg] on limit `{field_name}` is unsupported",
                    loc(rel_path, fv.span())
                );
            }
            let entry = raw_limits.entry(field_name).or_default();
            if ptr_cfg != Some(false) {
                entry.v64 = eval_limit_branch(&fv.expr, true, rel_path, consts)?;
            }
            if ptr_cfg != Some(true) {
                entry.v32 = eval_limit_branch(&fv.expr, false, rel_path, consts)?;
            }
            if let Some(cond) = platform_cond {
                entry.platform_cond = Some(cond);
            }
        }
    }

    let mut limits = BTreeMap::new();
    for (field_name, raw) in raw_limits {
        if let Some(limit) = build_limit_doc(&field_name, raw.v64, raw.v32, raw.platform_cond) {
            limits.insert(field_name, limit);
        }
    }

    if expr_struct.rest.is_none()
        && (seen_64.len() != field_types.len() || seen_32.len() != field_types.len())
    {
        bail!(
            "{}: Capability literal initializes {}/{} fields without `..Default::default()`",
            loc(rel_path, expr_struct.span()),
            seen_64.len().min(seen_32.len()),
            field_types.len()
        );
    }

    Ok(ServiceCapabilityDoc {
        name: name.to_string(),
        scheme: name.replace('_', "-"),
        native,
        limits,
    })
}

fn is_default_expr(expr: &Expr) -> bool {
    let Expr::Call(ExprCall { func, args, .. }) = strip_parens(expr) else {
        return false;
    };
    args.is_empty()
        && matches!(
            strip_parens(func),
            Expr::Path(ExprPath { path, .. })
                if path.segments.iter().map(|s| s.ident.to_string()).eq(["Default", "default"])
                    || path.segments.iter().map(|s| s.ident.to_string()).eq(["Capability", "default"])
        )
}

fn find_service_capability_struct(
    workspace_dir: &Path,
    src_dir: &Path,
    service_name: &str,
) -> Result<(String, ExprStruct)> {
    for rel in ["backend.rs", "core.rs"] {
        let file_path = src_dir.join(rel);
        if !file_path.is_file() {
            continue;
        }
        let rel_display = file_path
            .strip_prefix(workspace_dir)
            .unwrap_or(&file_path)
            .to_string_lossy()
            .to_string();
        let content = fs::read_to_string(&file_path)
            .with_context(|| format!("failed to read {rel_display}"))?;
        let ast =
            syn::parse_file(&content).with_context(|| format!("failed to parse {rel_display}"))?;

        let mut finder = CapabilityStructFinder { found: Vec::new() };
        finder.visit_file(&ast);

        if finder.found.len() > 1 {
            bail!(
                "{rel_display}: found {} Capability struct literals; expected 1",
                finder.found.len()
            );
        }
        if let Some(expr) = finder.found.into_iter().next() {
            return Ok((rel_display, expr));
        }
    }

    bail!(
        "no Capability struct literal found in {}/{{backend.rs,core.rs}} for `{service_name}`",
        src_dir.display()
    )
}

struct CapabilityStructFinder {
    found: Vec<ExprStruct>,
}

impl<'ast> Visit<'ast> for CapabilityStructFinder {
    fn visit_item(&mut self, item: &'ast Item) {
        if !has_cfg_test(item_attrs(item)) {
            visit::visit_item(self, item);
        }
    }

    fn visit_expr_struct(&mut self, node: &'ast ExprStruct) {
        if node
            .path
            .segments
            .last()
            .is_some_and(|seg| seg.ident == "Capability")
        {
            self.found.push(node.clone());
        }
        visit::visit_expr_struct(self, node);
    }
}

fn collect_consts_in_dir(workspace_dir: &Path, dir: &Path, out: &mut Vec<ConstDef>) -> Result<()> {
    let mut entries: Vec<_> = fs::read_dir(dir)?.collect::<std::io::Result<_>>()?;
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_consts_in_dir(workspace_dir, &path, out)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            let rel_display = path
                .strip_prefix(workspace_dir)
                .unwrap_or(&path)
                .to_string_lossy()
                .to_string();
            let ast = syn::parse_file(&fs::read_to_string(&path)?)
                .with_context(|| format!("failed to parse {rel_display}"))?;
            collect_consts_in_items(&ast.items, &rel_display, None, out)?;
        }
    }
    Ok(())
}

fn collect_consts_in_items(
    items: &[Item],
    rel_path: &str,
    inherited_ptr_cfg: Option<bool>,
    out: &mut Vec<ConstDef>,
) -> Result<()> {
    for item in items {
        let attrs = item_attrs(item);
        if has_cfg_test(attrs) {
            continue;
        }
        match item {
            Item::Mod(ItemMod {
                ident,
                content: Some((_, inner_items)),
                ..
            }) => {
                let (mod_ptr, mod_plat) = parse_cfgs(attrs, rel_path)?;
                if mod_plat.is_some() {
                    bail!(
                        "{}: unsupported platform #[cfg] on module `{ident}` containing constants",
                        loc(rel_path, ident.span())
                    );
                }
                let combined = merge_ptr_cfg(inherited_ptr_cfg, mod_ptr, rel_path, ident.span())?;
                collect_consts_in_items(inner_items, rel_path, combined, out)?;
            }
            Item::Const(ItemConst {
                attrs,
                ident,
                ty,
                expr,
                ..
            }) => {
                if !matches!(&**ty, Type::Path(TypePath { path, .. }) if path.is_ident("usize")) {
                    continue;
                }
                let (const_ptr, const_plat) = parse_cfgs(attrs, rel_path)?;
                if const_plat.is_some() {
                    bail!(
                        "{}: unsupported platform #[cfg] on usize constant `{ident}`",
                        loc(rel_path, ident.span())
                    );
                }
                let is_64bit_only =
                    merge_ptr_cfg(inherited_ptr_cfg, const_ptr, rel_path, ident.span())?;
                let name = ident.to_string();
                let line = ident.span().start().line;
                if let Some(existing) = out.iter().find(|c| {
                    c.name == name
                        && (c.is_64bit_only.is_none()
                            || is_64bit_only.is_none()
                            || c.is_64bit_only == is_64bit_only)
                }) {
                    bail!(
                        "{rel_path}:{line}: duplicate usize constant `{name}` (collides with {}:{})",
                        existing.rel_path,
                        existing.line
                    );
                }
                out.push(ConstDef {
                    rel_path: rel_path.to_string(),
                    line,
                    name,
                    is_64bit_only,
                    expr: (**expr).clone(),
                });
            }
            _ => {}
        }
    }
    Ok(())
}

fn merge_ptr_cfg(
    a: Option<bool>,
    b: Option<bool>,
    rel_path: &str,
    span: Span,
) -> Result<Option<bool>> {
    match (a, b) {
        (None, x) | (x, None) => Ok(x),
        (Some(x), Some(y)) if x == y => Ok(Some(x)),
        _ => bail!(
            "{}: contradictory pointer-width cfg attributes",
            loc(rel_path, span)
        ),
    }
}

fn eval_bool_field(
    expr: &Expr,
    attr_cond: Option<String>,
    rel_path: &str,
) -> Result<Option<NativeBoolDoc>> {
    let expr = strip_parens(expr);
    if let Expr::Lit(ExprLit {
        lit: Lit::Bool(b), ..
    }) = expr
    {
        if !b.value {
            return Ok(None);
        }
        return Ok(Some(match attr_cond {
            None => NativeBoolDoc::Supported(true),
            Some(cond) => NativeBoolDoc::Conditional(cond),
        }));
    }

    let expr_cond = describe_bool_condition(expr, rel_path)?;
    Ok(Some(NativeBoolDoc::Conditional(match attr_cond {
        None => expr_cond,
        Some(attr) => format!("{attr}; {expr_cond}"),
    })))
}

fn describe_bool_condition(expr: &Expr, rel_path: &str) -> Result<String> {
    let expr = strip_parens(expr);
    match expr {
        Expr::Binary(ExprBinary {
            left,
            op: BinOp::Or(_),
            right,
            ..
        }) => {
            let left = describe_bool_condition(left, rel_path)?;
            let right = describe_bool_condition(right, rel_path)?;
            if let Some(platform) = right.strip_suffix(" only") {
                Ok(format!("{left} or on {platform}"))
            } else {
                Ok(format!("{left} or {right}"))
            }
        }
        Expr::Macro(ExprMacro { mac, .. }) if mac.path.is_ident("cfg") => {
            let (ptr_cfg, plat_cond) =
                parse_cfg_tokens(&mac.tokens.to_string(), rel_path, mac.span())?;
            if let (None, Some(label)) = (ptr_cfg, plat_cond) {
                Ok(label.to_string())
            } else {
                bail!(
                    "{}: unsupported `cfg!({})` in capability boolean field",
                    loc(rel_path, mac.span()),
                    mac.tokens
                )
            }
        }
        Expr::MethodCall(ExprMethodCall {
            receiver,
            method,
            args,
            ..
        }) if args.is_empty() && (method == "is_some" || method == "is_none") => {
            let Some(ident) = extract_field_or_ident(receiver) else {
                bail!(
                    "{}: unsupported `.{method}()` receiver in capability field",
                    loc(rel_path, receiver.span())
                );
            };
            if method == "is_some" {
                Ok(format!("Requires `{ident}`"))
            } else if ident == "auth" {
                Ok("Anonymous only (no credentials configured)".to_string())
            } else {
                Ok(format!("When `{ident}` is not set"))
            }
        }
        Expr::Unary(ExprUnary {
            op: UnOp::Not(_),
            expr: inner,
            ..
        }) => {
            let Some(ident) = extract_field_or_ident(inner) else {
                bail!(
                    "{}: unsupported negated capability expression",
                    loc(rel_path, inner.span())
                );
            };
            if ident == "is_s3_express" {
                Ok("Standard S3 buckets (not S3 Express One Zone)".to_string())
            } else {
                Ok(format!("When `{ident}` is disabled"))
            }
        }
        Expr::Path(ExprPath { path, .. }) => {
            if let Some(ident) = path.get_ident().map(ToString::to_string) {
                if ident == "azblob_can_presign" {
                    return Ok(
                        "Requires `sas_token` or a host-style endpoint with `account_key` (and `account_name`), with `skip_signature` disabled"
                            .to_string(),
                    );
                }
                if ident == "gcs_can_restore" {
                    return Ok(
                        "Requires the default endpoint (`https://storage.googleapis.com`) with `skip_signature` and `allow_anonymous` disabled"
                            .to_string(),
                    );
                }
                if ident == "gcs_can_list_deleted" {
                    return Ok(
                        "Requires a bucket with a soft-delete policy at the default endpoint (`https://storage.googleapis.com`) with `skip_signature` and `allow_anonymous` disabled. Entries requiring a hierarchical namespace restore token are unsupported"
                            .to_string(),
                    );
                }
                if ident == "s3_write_can_multi" {
                    return Ok("When `checksum_algorithm` is not `md5`".to_string());
                }
                if let Some(rest) = ident.strip_prefix("has_") {
                    return Ok(format!("Requires `{rest}`"));
                }
            }
            bail!(
                "{}: unsupported capability boolean expression; add an explicit mapping in dev/src/generate/capabilities_docs.rs",
                loc(rel_path, expr.span())
            )
        }
        _ => bail!(
            "{}: unsupported capability boolean expression; add an explicit mapping in dev/src/generate/capabilities_docs.rs",
            loc(rel_path, expr.span())
        ),
    }
}

fn eval_limit_branch(
    expr: &Expr,
    is_64bit: bool,
    rel_path: &str,
    consts: &[ConstDef],
) -> Result<Option<EvalLimit>> {
    let expr = strip_parens(expr);
    match expr {
        Expr::Path(ExprPath { path, .. }) if path.is_ident("None") => Ok(None),
        Expr::Call(ExprCall { func, args, .. })
            if args.len() == 1
                && matches!(
                    strip_parens(func),
                    Expr::Path(ExprPath { path, .. })
                        if path.segments.last().is_some_and(|s| s.ident == "Some")
                ) =>
        {
            let inner = strip_parens(&args[0]);
            if matches!(
                inner,
                Expr::Path(ExprPath { path, .. })
                    if path.segments.iter().map(|s| s.ident.to_string()).eq(["usize", "MAX"])
            ) {
                return Ok(Some(EvalLimit::UsizeMax));
            }
            let raw = eval_u64_expr(inner, is_64bit, rel_path, consts, &mut HashSet::new())?;
            if !is_64bit && raw > u32::MAX as u64 {
                bail!(
                    "{}: limit value {raw} overflows 32-bit usize without target_pointer_width guard",
                    loc(rel_path, inner.span())
                );
            }
            Ok(Some(EvalLimit::Exact(raw)))
        }
        Expr::If(ExprIf {
            cond,
            then_branch,
            else_branch,
            ..
        }) => {
            let Expr::Macro(ExprMacro { mac, .. }) = strip_parens(cond) else {
                bail!(
                    "{}: unsupported conditional limit expression",
                    loc(rel_path, cond.span())
                );
            };
            if !mac.path.is_ident("cfg") {
                bail!(
                    "{}: unsupported macro in limit condition",
                    loc(rel_path, mac.span())
                );
            }
            let (Some(cfg_64), None) =
                parse_cfg_tokens(&mac.tokens.to_string(), rel_path, mac.span())?
            else {
                bail!(
                    "{}: unsupported `cfg!({})` in limit expression",
                    loc(rel_path, mac.span()),
                    mac.tokens
                );
            };
            if cfg_64 == is_64bit {
                let [Stmt::Expr(then_expr, None)] = then_branch.stmts.as_slice() else {
                    bail!(
                        "{}: expected single tail expression in `if` branch",
                        loc(rel_path, then_branch.span())
                    );
                };
                eval_limit_branch(then_expr, is_64bit, rel_path, consts)
            } else {
                let Some((_, else_expr)) = else_branch else {
                    bail!(
                        "{}: `if cfg!(...)` limit expression must have an `else` branch",
                        loc(rel_path, expr.span())
                    );
                };
                let Expr::Block(else_block) = strip_parens(else_expr) else {
                    return eval_limit_branch(else_expr, is_64bit, rel_path, consts);
                };
                let [Stmt::Expr(tail, None)] = else_block.block.stmts.as_slice() else {
                    bail!(
                        "{}: expected single tail expression in `else` branch",
                        loc(rel_path, else_block.span())
                    );
                };
                eval_limit_branch(tail, is_64bit, rel_path, consts)
            }
        }
        _ => bail!(
            "{}: unsupported limit expression",
            loc(rel_path, expr.span())
        ),
    }
}

fn build_limit_doc(
    field_name: &str,
    v64: Option<EvalLimit>,
    v32: Option<EvalLimit>,
    platform_cond: Option<String>,
) -> Option<LimitValueDoc> {
    let fmt = |v: EvalLimit| match v {
        EvalLimit::UsizeMax => "`usize::MAX`".to_string(),
        EvalLimit::Exact(raw) if field_name == "delete_max_size" => format_with_commas(raw),
        EvalLimit::Exact(raw) => format_bytes(raw),
    };

    let (display, ptr_note) = match (v64, v32) {
        (None, None) => return None,
        (Some(a), Some(b)) if a == b => (fmt(a), None),
        (Some(a), Some(b)) => (
            fmt(a),
            Some(format!("64-bit targets ({} on 32-bit)", fmt(b))),
        ),
        (Some(a), None) => (fmt(a), Some("64-bit targets (unset on 32-bit)".to_string())),
        (None, Some(b)) => (fmt(b), Some("32-bit targets (unset on 64-bit)".to_string())),
    };

    let condition = match (platform_cond, ptr_note) {
        (None, None) => None,
        (Some(p), None) => Some(p),
        (None, Some(n)) => Some(n),
        (Some(p), Some(n)) => Some(format!("{p}; {n}")),
    };

    Some(LimitValueDoc { display, condition })
}

fn eval_u64_expr(
    expr: &Expr,
    is_64bit: bool,
    rel_path: &str,
    consts: &[ConstDef],
    visiting: &mut HashSet<String>,
) -> Result<u64> {
    let expr = strip_parens(expr);
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Int(lit), ..
        }) => Ok(lit.base10_parse::<u64>()?),
        Expr::Cast(ExprCast {
            expr: inner, ty, ..
        }) => {
            if !matches!(&**ty, Type::Path(TypePath { path, .. }) if path.is_ident("usize")) {
                bail!(
                    "{}: unsupported cast in constant expression",
                    loc(rel_path, expr.span())
                );
            }
            eval_u64_expr(inner, is_64bit, rel_path, consts, visiting)
        }
        Expr::Path(ExprPath { path, .. }) => {
            let segments: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();
            if segments == ["usize", "MAX"] {
                return Ok(if is_64bit { u64::MAX } else { u32::MAX as u64 });
            }
            if segments == ["u64", "MAX"] {
                return Ok(u64::MAX);
            }
            if segments == ["i64", "MAX"] {
                return Ok(i64::MAX as u64);
            }
            let Some(name) = segments.last() else {
                bail!("{}: empty constant path", loc(rel_path, expr.span()));
            };
            let Some(def) = consts.iter().find(|c| {
                &c.name == name && c.is_64bit_only.is_none_or(|only64| only64 == is_64bit)
            }) else {
                bail!(
                    "{}: unknown usize constant `{}`",
                    loc(rel_path, expr.span()),
                    segments.join("::")
                );
            };
            if !visiting.insert(name.clone()) {
                bail!(
                    "{}:{}: cyclic usize constant `{name}`",
                    def.rel_path,
                    def.line
                );
            }
            let val = eval_u64_expr(&def.expr, is_64bit, &def.rel_path, consts, visiting)?;
            visiting.remove(name);
            Ok(val)
        }
        Expr::Binary(ExprBinary {
            left, op, right, ..
        }) => {
            let l = eval_u64_expr(left, is_64bit, rel_path, consts, visiting)?;
            let r = eval_u64_expr(right, is_64bit, rel_path, consts, visiting)?;
            match op {
                BinOp::Mul(_) => l.checked_mul(r).ok_or_else(|| {
                    anyhow!("{}: overflow in {l} * {r}", loc(rel_path, expr.span()))
                }),
                BinOp::Div(_) => l.checked_div(r).ok_or_else(|| {
                    anyhow!(
                        "{}: division by zero in {l} / {r}",
                        loc(rel_path, expr.span())
                    )
                }),
                BinOp::Add(_) => l.checked_add(r).ok_or_else(|| {
                    anyhow!("{}: overflow in {l} + {r}", loc(rel_path, expr.span()))
                }),
                BinOp::Sub(_) => l.checked_sub(r).ok_or_else(|| {
                    anyhow!("{}: underflow in {l} - {r}", loc(rel_path, expr.span()))
                }),
                BinOp::Shl(_) => l.checked_shl(r as u32).ok_or_else(|| {
                    anyhow!("{}: overflow in {l} << {r}", loc(rel_path, expr.span()))
                }),
                _ => bail!(
                    "{}: unsupported binary operator in integer expression",
                    loc(rel_path, expr.span())
                ),
            }
        }
        _ => bail!(
            "{}: unsupported integer expression",
            loc(rel_path, expr.span())
        ),
    }
}

fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    const TIB: u64 = 1024 * GIB;
    const PIB: u64 = 1024 * TIB;
    const EIB: u64 = 1024 * PIB;

    if bytes == 0 {
        return "0 B".to_string();
    }
    if bytes >= EIB && !bytes.is_multiple_of(EIB) {
        let rounded = (bytes as f64 / EIB as f64).round() as u64;
        return format!("~{rounded} EiB");
    }
    for (unit_val, unit_name) in [
        (EIB, "EiB"),
        (PIB, "PiB"),
        (TIB, "TiB"),
        (GIB, "GiB"),
        (MIB, "MiB"),
        (KIB, "KiB"),
    ] {
        if bytes >= unit_val && bytes.is_multiple_of(unit_val) {
            return format!("{} {unit_name}", format_with_commas(bytes / unit_val));
        }
    }
    for (unit_val, unit_name) in [
        (1_000_000_000_u64, "GB"),
        (1_000_000_u64, "MB"),
        (1_000_u64, "KB"),
    ] {
        if bytes >= unit_val && bytes.is_multiple_of(unit_val) {
            return format!("{} {unit_name}", format_with_commas(bytes / unit_val));
        }
    }
    format!("{} B", format_with_commas(bytes))
}

fn format_with_commas(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

fn strip_parens(mut expr: &Expr) -> &Expr {
    while let Expr::Paren(ExprParen { expr: inner, .. })
    | Expr::Group(ExprGroup { expr: inner, .. }) = expr
    {
        expr = inner;
    }
    expr
}

fn extract_field_or_ident(expr: &Expr) -> Option<String> {
    match strip_parens(expr) {
        Expr::Path(ExprPath { path, .. }) => path.get_ident().map(ToString::to_string),
        Expr::Field(ExprField {
            base,
            member: Member::Named(ident),
            ..
        }) => {
            extract_field_or_ident(base)?;
            Some(ident.to_string())
        }
        _ => None,
    }
}

fn item_attrs(item: &Item) -> &[Attribute] {
    match item {
        Item::Const(i) => &i.attrs,
        Item::Enum(i) => &i.attrs,
        Item::Fn(i) => &i.attrs,
        Item::Impl(i) => &i.attrs,
        Item::Mod(i) => &i.attrs,
        Item::Static(i) => &i.attrs,
        Item::Struct(i) => &i.attrs,
        Item::Trait(i) => &i.attrs,
        Item::Type(i) => &i.attrs,
        Item::Use(i) => &i.attrs,
        _ => &[],
    }
}

fn has_cfg_test(attrs: &[Attribute]) -> bool {
    fn meta_has_test(meta: &Meta) -> bool {
        match meta {
            Meta::Path(p) => p.is_ident("test"),
            Meta::List(list) if list.path.is_ident("all") => list
                .parse_args_with(
                    syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
                )
                .is_ok_and(|nested| nested.iter().any(meta_has_test)),
            _ => false,
        }
    }

    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg")
            && matches!(&attr.meta, Meta::List(list) if list.parse_args::<Meta>().is_ok_and(|m| meta_has_test(&m)))
    })
}

fn parse_cfgs(attrs: &[Attribute], rel_path: &str) -> Result<(Option<bool>, Option<String>)> {
    let mut ptr_cfg = None;
    let mut conds = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("cfg") {
            continue;
        }
        let Meta::List(list) = &attr.meta else {
            bail!("{}: malformed #[cfg] attribute", loc(rel_path, attr.span()));
        };
        let (next_ptr, plat) = parse_cfg_tokens(&list.tokens.to_string(), rel_path, attr.span())?;
        ptr_cfg = merge_ptr_cfg(ptr_cfg, next_ptr, rel_path, attr.span())?;
        if let Some(label) = plat {
            conds.push(label.to_string());
        }
    }
    Ok((ptr_cfg, (!conds.is_empty()).then(|| conds.join("; "))))
}

fn parse_cfg_tokens(
    tokens: &str,
    rel_path: &str,
    span: Span,
) -> Result<(Option<bool>, Option<&'static str>)> {
    match tokens.replace(char::is_whitespace, "").as_str() {
        "unix" => Ok((None, Some("Unix only"))),
        r#"target_os="linux""# => Ok((None, Some("Linux only"))),
        r#"target_pointer_width="64""# => Ok((Some(true), None)),
        r#"not(target_pointer_width="64")"# => Ok((Some(false), None)),
        _ => bail!(
            "{}: unsupported cfg predicate `cfg({})`",
            loc(rel_path, span),
            tokens.trim()
        ),
    }
}

fn loc(rel_path: &str, span: Span) -> String {
    format!("{rel_path}:{}", span.start().line)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::parser;
    use crate::workspace_dir;

    #[test]
    fn test_extract_all_services_capabilities() {
        let workspace = workspace_dir();
        let mut services = parser::Services::new();
        services.extend(
            parser::parse(&workspace.join("core/core/src/services").to_string_lossy()).unwrap(),
        );
        services.extend(parser::parse(&workspace.join("core/services").to_string_lossy()).unwrap());

        let out = extract_all(&workspace, &services).expect("all service capabilities must parse");
        assert_eq!(out.services.len(), services.len());
        assert!(out.fields.len() >= 96);
        for f in &out.fields {
            assert!(
                !f.doc.contains("::"),
                "field `{}` doc leaked Rust path syntax: {}",
                f.name,
                f.doc
            );
        }

        let s3 = out.services.iter().find(|s| s.scheme == "s3").unwrap();
        assert_eq!(s3.native.get("read"), Some(&NativeBoolDoc::Supported(true)));
        assert_eq!(
            s3.native.get("stat_with_version"),
            Some(&NativeBoolDoc::Conditional(
                "Standard S3 buckets (not S3 Express One Zone)".to_string()
            ))
        );
        assert_eq!(
            s3.native.get("write_can_copy_from"),
            Some(&NativeBoolDoc::Conditional(
                "When `checksum_algorithm` is not set".to_string()
            ))
        );
        assert!(!s3.native.contains_key("create_dir"));
        assert_eq!(
            s3.limits.get("write_multi_min_size"),
            Some(&LimitValueDoc {
                display: "5 MiB".to_string(),
                condition: None,
            })
        );
        assert_eq!(
            s3.limits.get("write_multi_max_size"),
            Some(&LimitValueDoc {
                display: "5 GiB".to_string(),
                condition: Some("64-bit targets (`usize::MAX` on 32-bit)".to_string()),
            })
        );
        assert_eq!(
            s3.limits.get("write_total_max_size"),
            Some(&LimitValueDoc {
                display: "50,000 GiB".to_string(),
                condition: Some("64-bit targets (unset on 32-bit)".to_string()),
            })
        );
        assert_eq!(
            s3.limits.get("delete_max_size").map(|l| l.display.as_str()),
            Some("1,000")
        );

        let fs_srv = out.services.iter().find(|s| s.scheme == "fs").unwrap();
        assert_eq!(
            fs_srv.native.get("write_can_append"),
            Some(&NativeBoolDoc::Conditional(
                "When `direct_io` is disabled".to_string()
            ))
        );
        assert_eq!(
            fs_srv.native.get("write_with_if_not_exists"),
            Some(&NativeBoolDoc::Supported(true))
        );
        assert_eq!(
            fs_srv.native.get("write_with_user_metadata"),
            Some(&NativeBoolDoc::Conditional("Unix only".to_string()))
        );
        assert_eq!(
            fs_srv.native.get("rename_with_if_not_exists"),
            Some(&NativeBoolDoc::Conditional("Linux only".to_string()))
        );

        let gcs = out.services.iter().find(|s| s.scheme == "gcs").unwrap();
        let gcs_restore = NativeBoolDoc::Conditional(
            "Requires the default endpoint (`https://storage.googleapis.com`) with `skip_signature` and `allow_anonymous` disabled"
                .to_string(),
        );
        for flag in [
            "restore",
            "restore_with_version",
            "restore_with_if_not_exists",
        ] {
            assert_eq!(gcs.native.get(flag), Some(&gcs_restore), "{flag}");
        }
        assert_eq!(
            gcs.native.get("list_with_deleted"),
            Some(&NativeBoolDoc::Conditional(
                "Requires a bucket with a soft-delete policy at the default endpoint (`https://storage.googleapis.com`) with `skip_signature` and `allow_anonymous` disabled. Entries requiring a hierarchical namespace restore token are unsupported"
                    .to_string(),
            ))
        );
        assert_eq!(
            gcs.limits.get("copy_multi_max_size"),
            Some(&LimitValueDoc {
                display: "~8 EiB".to_string(),
                condition: Some("64-bit targets (4,095 MiB on 32-bit)".to_string()),
            })
        );

        let onedrive = out
            .services
            .iter()
            .find(|s| s.scheme == "onedrive")
            .unwrap();
        assert_eq!(
            onedrive.limits.get("write_total_max_size"),
            Some(&LimitValueDoc {
                display: "250 GiB".to_string(),
                condition: Some("64-bit targets (unset on 32-bit)".to_string()),
            })
        );

        let d1 = out.services.iter().find(|s| s.scheme == "d1").unwrap();
        assert_eq!(
            d1.limits
                .get("write_total_max_size")
                .map(|l| l.display.as_str()),
            Some("1 MB")
        );

        let azblob = out.services.iter().find(|s| s.scheme == "azblob").unwrap();
        assert_eq!(
            azblob.native.get("presign"),
            Some(&NativeBoolDoc::Conditional(
                "Requires `sas_token` or a host-style endpoint with `account_key` (and `account_name`), with `skip_signature` disabled"
                    .to_string()
            ))
        );
    }

    #[test]
    fn test_rejects_non_default_struct_update() {
        let expr: ExprStruct =
            syn::parse_str("Capability {\n    read: true,\n    ..base_cap\n}").unwrap();
        let fields = HashMap::from([("read", true), ("write", true)]);
        let err = parse_capability_literal(
            "mock",
            "core/services/mock/src/backend.rs",
            &expr,
            &fields,
            &[],
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("core/services/mock/src/backend.rs:3"), "{msg}");
        assert!(msg.contains("..Default::default()"), "{msg}");
    }

    #[test]
    fn test_rejects_unrecognized_bool_expression() {
        let expr: ExprStruct =
            syn::parse_str("Capability {\n    read: can_read(),\n    ..Default::default()\n}")
                .unwrap();
        let fields = HashMap::from([("read", true)]);
        let err = parse_capability_literal(
            "mock",
            "core/services/mock/src/backend.rs",
            &expr,
            &fields,
            &[],
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("core/services/mock/src/backend.rs:2"), "{msg}");
        assert!(
            msg.contains("unsupported capability boolean expression"),
            "{msg}"
        );
    }

    #[test]
    fn test_rejects_duplicate_usize_constants() {
        let file = syn::parse_file("const LIMIT: usize = 10;\nconst LIMIT: usize = 20;\n").unwrap();
        let mut out = Vec::new();
        let err = collect_consts_in_items(
            &file.items,
            "core/services/mock/src/core.rs",
            None,
            &mut out,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("core/services/mock/src/core.rs:2"), "{msg}");
        assert!(msg.contains("duplicate usize constant `LIMIT`"), "{msg}");
    }

    #[test]
    fn test_merges_complementary_cfg_limit_fields() {
        let fields = HashMap::from([("write_multi_max_size", false)]);
        for snippet in [
            r#"Capability {
                #[cfg(target_pointer_width = "64")]
                write_multi_max_size: Some(5 * 1024 * 1024 * 1024),
                #[cfg(not(target_pointer_width = "64"))]
                write_multi_max_size: Some(usize::MAX),
                ..Default::default()
            }"#,
            r#"Capability {
                #[cfg(not(target_pointer_width = "64"))]
                write_multi_max_size: Some(usize::MAX),
                #[cfg(target_pointer_width = "64")]
                write_multi_max_size: Some(5 * 1024 * 1024 * 1024),
                ..Default::default()
            }"#,
        ] {
            let expr: ExprStruct = syn::parse_str(snippet).unwrap();
            let doc = parse_capability_literal(
                "mock",
                "core/services/mock/src/backend.rs",
                &expr,
                &fields,
                &[],
            )
            .unwrap();
            assert_eq!(
                doc.limits.get("write_multi_max_size"),
                Some(&LimitValueDoc {
                    display: "5 GiB".to_string(),
                    condition: Some("64-bit targets (`usize::MAX` on 32-bit)".to_string()),
                })
            );
        }
    }
}
