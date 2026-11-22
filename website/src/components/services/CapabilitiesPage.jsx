/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, { useEffect, useMemo, useState } from "react";
import Layout from "@theme/Layout";
import Link from "@docusaurus/Link";
import { useHistory, useLocation } from "@docusaurus/router";
import Breadcrumb from "../ui/Breadcrumb";
import Tabs from "../ui/Tabs";
import styles from "./styles.module.css";

function renderInlineCode(text) {
  if (!text) return null;
  const parts = text.split(/(`[^`]+`)/g);
  return parts.map((part, i) => {
    if (part.startsWith("`") && part.endsWith("`") && part.length > 1) {
      return <code key={i}>{part.slice(1, -1)}</code>;
    }
    return <span key={i}>{part}</span>;
  });
}

function stripInlineTicks(text) {
  return text ? text.replace(/`/g, "") : "";
}

export function resolveCapabilityCell(service, field) {
  if (!service) {
    return {
      status: field.kind === "bool" ? "unsupported" : "unset",
      supported: false,
    };
  }

  if (field.kind !== "bool") {
    const limit = service.limits && service.limits[field.name];
    if (limit) {
      return {
        status: "limit",
        supported: true,
        display: limit.display,
        condition: limit.condition || null,
      };
    }
    return { status: "unset", supported: false };
  }

  const nativeVal = service.native && service.native[field.name];
  if (nativeVal === true) {
    return { status: "native", supported: true };
  }
  if (typeof nativeVal === "string") {
    return {
      status: "conditional",
      supported: true,
      condition: nativeVal,
    };
  }

  return { status: "unsupported", supported: false };
}

function parseGroupParam(search, groups) {
  if (!search) return "summary";
  const params = new URLSearchParams(search);
  const group = params.get("group");
  if (group && groups.some((g) => g.id === group)) {
    return group;
  }
  return "summary";
}

export default function CapabilitiesPage({ data }) {
  const { groups, fields, services } = data;
  const location = useLocation();
  const history = useHistory();

  const [activeGroup, setActiveGroup] = useState("summary");
  const [query, setQuery] = useState("");
  const [requiredFields, setRequiredFields] = useState([]);

  useEffect(() => {
    setActiveGroup(parseGroupParam(location.search, groups));
  }, [location.search, groups]);

  function selectGroup(groupId) {
    setActiveGroup(groupId);
    const params = new URLSearchParams(location.search);
    if (groupId === "summary") {
      params.delete("group");
    } else {
      params.set("group", groupId);
    }
    const next = params.toString();
    history.replace({
      pathname: location.pathname,
      search: next ? `?${next}` : "",
    });
  }

  const fieldByName = useMemo(
    () => Object.fromEntries(fields.map((f) => [f.name, f])),
    [fields]
  );

  const visibleFields = useMemo(() => {
    if (activeGroup === "summary") {
      return fields.filter((f) => f.summary);
    }
    return fields.filter((f) => f.group === activeGroup);
  }, [fields, activeGroup]);

  const activeGroupMeta =
    groups.find((g) => g.id === activeGroup) || groups[0];

  const filteredServices = useMemo(() => {
    const q = query.trim().toLowerCase();
    return services.filter((service) => {
      if (q && !service.scheme.includes(q) && !service.name.includes(q)) {
        return false;
      }
      for (const reqName of requiredFields) {
        const field = fieldByName[reqName];
        if (!field) continue;
        if (!resolveCapabilityCell(service, field).supported) {
          return false;
        }
      }
      return true;
    });
  }, [services, query, requiredFields, fieldByName]);

  const supportCounts = useMemo(() => {
    const counts = {};
    for (const field of visibleFields) {
      let supported = 0;
      for (const service of services) {
        if (resolveCapabilityCell(service, field).supported) {
          supported += 1;
        }
      }
      counts[field.name] = supported;
    }
    return counts;
  }, [visibleFields, services]);

  const visibleConditions = useMemo(() => {
    const notes = [];
    for (const service of filteredServices) {
      for (const field of visibleFields) {
        const cell = resolveCapabilityCell(service, field);
        if (cell.condition) {
          notes.push({
            scheme: service.scheme,
            fieldName: field.name,
            display: cell.status === "limit" ? cell.display : null,
            condition: cell.condition,
          });
        }
      }
    }
    return notes;
  }, [filteredServices, visibleFields]);

  function toggleRequiredField(fieldName) {
    setRequiredFields((prev) =>
      prev.includes(fieldName)
        ? prev.filter((f) => f !== fieldName)
        : [...prev, fieldName]
    );
  }

  return (
    <Layout
      title="Service Capabilities | Services"
      description="Overview of native operations, conditional capabilities, and size limits declared across every OpenDAL storage service."
    >
      <div className={styles.matrixPage}>
        <Breadcrumb
          aria-label="Breadcrumb"
          rootLabel="Services"
          rootHref="/services"
          items={[{ label: "Capabilities" }]}
        />

        <header className={styles.header}>
          <h1 className={styles.title}>Service capabilities</h1>
          <p className={styles.subtitle}>
            {services.length} storage services · {fields.length} native
            capability fields declared across OpenDAL backends. Select a service
            for its configuration reference, or select any capability column to
            filter supporting backends.
          </p>
        </header>

        <div className={styles.matrixControls}>
          <input
            type="search"
            className={styles.matrixSearch}
            placeholder={`Filter ${services.length} services…`}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            aria-label="Filter services by name"
          />
        </div>

        <div className={styles.legendBar} aria-label="Capability legend">
          <span className={styles.legendItem}>
            <span className={styles.capNative} aria-hidden="true">
              ✓
            </span>
            <span>Supported natively</span>
          </span>
          <span className={styles.legendItem}>
            <span className={styles.capCondBadge} aria-hidden="true">
              Cond.
            </span>
            <span>Conditional (configuration or platform)</span>
          </span>
          <span className={styles.legendItem}>
            <span className={styles.capUnsup} aria-hidden="true">
              —
            </span>
            <span>Unsupported (or limit not declared)</span>
          </span>
        </div>

        {requiredFields.length > 0 && (
          <div
            className={styles.activeFilters}
            aria-label="Active capability filters"
          >
            <span className={styles.activeFiltersLabel}>Requiring:</span>
            {requiredFields.map((name) => (
              <button
                key={name}
                type="button"
                className={styles.filterChip}
                onClick={() => toggleRequiredField(name)}
                aria-label={`Remove required capability ${name}`}
              >
                <code>{name}</code>
                <span aria-hidden="true">×</span>
              </button>
            ))}
            <button
              type="button"
              className={styles.clearFiltersButton}
              onClick={() => setRequiredFields([])}
            >
              Clear all
            </button>
          </div>
        )}

        <Tabs
          items={groups}
          activeId={activeGroup}
          onChange={(g) => selectGroup(g.id)}
          aria-label="Capability operation groups"
          controlsId="capability-matrix-panel"
          id="capability-group-tabs"
        />

        <div
          role="tabpanel"
          id="capability-matrix-panel"
          aria-labelledby={`capability-group-tabs-tab-${activeGroup}`}
        >
          <div className={styles.matrixMetaRow}>
            <p className={styles.matrixGroupDesc}>
              {activeGroupMeta.description}
            </p>
            <span className={styles.matrixRowCount}>
              Showing {filteredServices.length} of {services.length} services
            </span>
          </div>

          {filteredServices.length === 0 ? (
            <div className={styles.emptyState}>
              <p>No service matches the current filter.</p>
              {(query || requiredFields.length > 0) && (
                <button
                  type="button"
                  className={styles.clearFiltersButton}
                  onClick={() => {
                    setQuery("");
                    setRequiredFields([]);
                  }}
                >
                  Reset filters
                </button>
              )}
            </div>
          ) : (
            <div className={styles.matrixWrap}>
              <table className={styles.matrixTable}>
                <thead>
                  <tr>
                    <th scope="col" className={styles.matrixStickyColHeader}>
                      Service
                    </th>
                    {visibleFields.map((field) => {
                      const label =
                        activeGroup === "summary" ? field.name : field.short;
                      const count = supportCounts[field.name] || 0;
                      const isRequired = requiredFields.includes(field.name);
                      const tooltip = `${field.name}: ${field.doc}`;

                      if (field.kind === "bool") {
                        return (
                          <th
                            key={field.name}
                            scope="col"
                            className={styles.matrixColHeader}
                          >
                            <button
                              type="button"
                              className={`${styles.colFilterButton} ${
                                isRequired ? styles.colFilterButtonActive : ""
                              }`}
                              aria-pressed={isRequired}
                              title={tooltip}
                              onClick={() => toggleRequiredField(field.name)}
                            >
                              <span className={styles.colName}>{label}</span>
                              <span className={styles.colCoverage}>
                                {count}/{services.length}
                              </span>
                            </button>
                          </th>
                        );
                      }

                      return (
                        <th
                          key={field.name}
                          scope="col"
                          className={styles.matrixColHeader}
                          title={tooltip}
                        >
                          <div className={styles.colStaticHeader}>
                            <span className={styles.colName}>{label}</span>
                            <span className={styles.colCoverage}>
                              {count} set
                            </span>
                          </div>
                        </th>
                      );
                    })}
                  </tr>
                </thead>
                <tbody>
                  {filteredServices.map((service) => (
                    <tr key={service.scheme} className={styles.matrixRow}>
                      <th scope="row" className={styles.matrixStickyCol}>
                        <Link
                          to={`/services/${service.scheme}#capabilities`}
                          className={styles.serviceLink}
                        >
                          {service.scheme}
                        </Link>
                      </th>
                      {visibleFields.map((field) => {
                        const cell = resolveCapabilityCell(service, field);

                        if (cell.status === "native") {
                          return (
                            <td key={field.name} className={styles.matrixCell}>
                              <span className={styles.capNative}>
                                <span aria-hidden="true">✓</span>
                                <span className={styles.srOnly}>Supported</span>
                              </span>
                            </td>
                          );
                        }

                        if (cell.status === "conditional") {
                          const plain = stripInlineTicks(cell.condition);
                          return (
                            <td key={field.name} className={styles.matrixCell}>
                              <span
                                className={styles.capCondBadge}
                                title={plain}
                              >
                                <span aria-hidden="true">Cond.</span>
                                <span className={styles.srOnly}>
                                  Conditional: {plain}
                                </span>
                              </span>
                            </td>
                          );
                        }

                        if (cell.status === "limit") {
                          const plainCond = cell.condition
                            ? stripInlineTicks(cell.condition)
                            : "";
                          return (
                            <td key={field.name} className={styles.matrixCell}>
                              <span
                                className={styles.capLimit}
                                title={
                                  plainCond
                                    ? `${cell.display} (${plainCond})`
                                    : cell.display
                                }
                              >
                                {cell.display}
                                {plainCond ? (
                                  <>
                                    <span aria-hidden="true">*</span>
                                    <span className={styles.srOnly}>
                                      {" "}
                                      ({plainCond})
                                    </span>
                                  </>
                                ) : null}
                              </span>
                            </td>
                          );
                        }

                        return (
                          <td key={field.name} className={styles.matrixCell}>
                            <span className={styles.capUnsup}>
                              <span aria-hidden="true">—</span>
                              <span className={styles.srOnly}>
                                {cell.status === "unset"
                                  ? "Not declared"
                                  : "Unsupported"}
                              </span>
                            </span>
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {visibleConditions.length > 0 && (
            <details className={styles.group} open>
              <summary className={styles.groupSummary}>
                Conditions and target notes in this view
                <span className={styles.groupCount}>
                  {visibleConditions.length}
                </span>
              </summary>
              <div className={styles.tableWrap}>
                <table className={styles.table}>
                  <thead className={styles.tableHead}>
                    <tr className={styles.tableHeaderRow}>
                      <th className={styles.tableHeaderCell} scope="col">
                        Service
                      </th>
                      <th className={styles.tableHeaderCell} scope="col">
                        Capability
                      </th>
                      <th className={styles.tableHeaderCell} scope="col">
                        Details
                      </th>
                    </tr>
                  </thead>
                  <tbody className={styles.tableBody}>
                    {visibleConditions.map((note) => (
                      <tr
                        key={`${note.scheme}-${note.fieldName}`}
                        className={styles.tableRow}
                      >
                        <td
                          className={`${styles.tableCell} ${styles.tableKeyCell}`}
                          data-label="Service"
                        >
                          <Link to={`/services/${note.scheme}#capabilities`}>
                            <code>{note.scheme}</code>
                          </Link>
                        </td>
                        <td
                          className={styles.tableCell}
                          data-label="Capability"
                        >
                          <code>{note.fieldName}</code>
                        </td>
                        <td
                          className={`${styles.tableCell} ${styles.desc}`}
                          data-label="Details"
                        >
                          {note.display ? (
                            <>
                              <code>{note.display}</code>:{" "}
                              {renderInlineCode(note.condition)}
                            </>
                          ) : (
                            renderInlineCode(note.condition)
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </details>
          )}

          <details className={styles.group}>
            <summary className={styles.groupSummary}>
              Field descriptions ({activeGroupMeta.label})
              <span className={styles.groupCount}>{visibleFields.length}</span>
            </summary>
            <div className={styles.tableWrap}>
              <table className={styles.table}>
                <thead className={styles.tableHead}>
                  <tr className={styles.tableHeaderRow}>
                    <th className={styles.tableHeaderCell} scope="col">
                      Field
                    </th>
                    <th className={styles.tableHeaderCell} scope="col">
                      Coverage
                    </th>
                    <th className={styles.tableHeaderCell} scope="col">
                      Description
                    </th>
                  </tr>
                </thead>
                <tbody className={styles.tableBody}>
                  {visibleFields.map((field) => (
                    <tr key={field.name} className={styles.tableRow}>
                      <td
                        className={`${styles.tableCell} ${styles.tableKeyCell}`}
                        data-label="Field"
                      >
                        <code>{field.name}</code>
                      </td>
                      <td className={styles.tableCell} data-label="Coverage">
                        {field.kind === "bool"
                          ? `${supportCounts[field.name] || 0} / ${services.length}`
                          : `${supportCounts[field.name] || 0} set`}
                      </td>
                      <td
                        className={`${styles.tableCell} ${styles.desc}`}
                        data-label="Description"
                      >
                        {renderInlineCode(field.doc)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </details>
        </div>

        <section className={styles.section}>
          <h2>About service capabilities</h2>
          <p className={styles.notesIntro}>
            This matrix summarizes the native capabilities declared by each
            storage service implementation in OpenDAL. Keep the following points
            in mind when choosing a backend or relying on optional operations:
          </p>
          <ul className={styles.notesList}>
            <li>
              <strong>Native declarations vs. client-side fallbacks:</strong>{" "}
              Columns reflect native backend support. For example,{" "}
              <code>list_with_recursive</code> indicates native prefix or
              recursive listing supported by the backend itself (which may still
              paginate across requests), rather than client-side
              directory-by-directory traversal. Depending on the operator setup
              and language binding, OpenDAL can also simulate operations such as
              suffix byte-range reads, nested directory traversal for recursive
              listing or deletion, and empty directory markers on top of
              primitive operations.
            </li>
            <li>
              <strong>Language binding surface:</strong> Each binding exposes
              its own idiomatic API and subset of options and capability checks.
              Check your language&apos;s guide in the documentation sidebar to
              confirm which options and inspection APIs are available.
            </li>
            <li>
              <strong>Compatible storage endpoints:</strong> Self-hosted or
              protocol-compatible servers (such as S3-compatible object stores
              or WebDAV servers) may support only a subset of the capabilities
              and limits declared for the protocol. Verify optional conditions
              and batch or multipart limits against your target endpoint.
            </li>
          </ul>
        </section>
      </div>
    </Layout>
  );
}
