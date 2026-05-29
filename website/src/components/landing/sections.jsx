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

import React from "react";
import Link from "@docusaurus/Link";
import { useBaseUrlUtils } from "@docusaurus/useBaseUrl";
import CodeTabs from "./CodeTabs";
import styles from "./styles.module.css";
import {
  REPO_URL,
  DOCS_URL,
  DISCORD_URL,
  heroStats,
  codeSamples,
  valueProps,
  capabilityGroups,
  usedBy,
  USERS_LIST_URL,
  serviceGroups,
  bindings,
  layers,
  layeredCode,
  principles,
} from "./data";

export function Hero() {
  return (
    <header className={styles.hero}>
      <div className={`${styles.heroGrid} odl-grid-bg`} aria-hidden="true" />
      <div className="odl-container">
        <div className={styles.heroInner}>
          <div>
            <span className="odl-eyebrow">
              Apache OpenDAL™ — Open Data Access Layer
            </span>
            <h1 className={styles.heroTitle}>
              One Layer,
              <br />
              <span className={styles.heroTitleAccent}>All Storage.</span>
            </h1>
            <p className={styles.heroLede}>
              One zero-cost Operator for object storage, file systems, databases
              and more — in the language you already ship.
            </p>
            <div className={styles.heroActions}>
              <Link
                className={`${styles.btn} ${styles.btnPrimary}`}
                to={DOCS_URL}
              >
                Get started <span className={styles.btnArrow}>→</span>
              </Link>
              <Link
                className={`${styles.btn} ${styles.btnSecondary}`}
                to={REPO_URL}
              >
                View on GitHub
              </Link>
            </div>
            <div className={styles.heroStats}>
              {heroStats.map((stat) => (
                <div className={styles.heroStat} key={stat.label}>
                  <span className={styles.heroStatValue}>{stat.value}</span>
                  <span className={styles.heroStatLabel}>{stat.label}</span>
                </div>
              ))}
            </div>
          </div>
          <div className={styles.heroAside}>
            <CodeTabs samples={codeSamples} title="quickstart" equalize />
          </div>
        </div>
      </div>
    </header>
  );
}

export function UsedBy() {
  const { withBaseUrl } = useBaseUrlUtils();
  return (
    <section className={styles.usedBy}>
      <div className="odl-container">
        <div className={styles.usedByHead}>
          <span className="odl-eyebrow">Used by</span>
          <Link className={styles.addLogo} to={USERS_LIST_URL}>
            + add your logo
          </Link>
        </div>
        <div className={styles.logoWall}>
          {usedBy.map((u) => (
            <Link
              key={u.name}
              className={styles.logoItem}
              to={u.href}
              title={u.name}
            >
              <img
                className={styles.logoMark}
                src={withBaseUrl(u.icon)}
                alt=""
                width="24"
                height="24"
                loading="lazy"
              />
              <span className={styles.logoName}>{u.name}</span>
            </Link>
          ))}
        </div>
      </div>
    </section>
  );
}

export function ValueProps() {
  return (
    <section className={styles.section}>
      <div className="odl-container">
        <div className={styles.sectionHead}>
          <span className="odl-eyebrow">Why OpenDAL</span>
          <h2 className={styles.sectionTitle}>
            A storage layer you can build production on.
          </h2>
          <p className={styles.sectionLede}>
            OpenDAL turns one vision — <em>One Layer, All Storage</em> — into a
            practical foundation for applications, libraries and data systems.
          </p>
        </div>
        <div className={`${styles.valueGrid} ${styles.reveal}`}>
          {valueProps.map((v) => (
            <article className={styles.valueCard} key={v.index}>
              <span className={styles.valueIndex}>{v.index}</span>
              <h3 className={styles.valueCardTitle}>{v.title}</h3>
              <p className={styles.valueCardBody}>{v.body}</p>
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}

export function Capabilities() {
  return (
    <section className={`${styles.section} ${styles.sectionSubtle}`}>
      <div className="odl-container">
        <div className={styles.sectionHead}>
          <span className="odl-eyebrow">Capabilities</span>
          <h2 className={styles.sectionTitle}>Configure once. Access anything.</h2>
          <p className={styles.sectionLede}>
            One Operator gives you a full toolkit for real-world data —
            streaming, concurrency, multipart uploads, conditionals and
            server-side moves — working the same way on every backend.
          </p>
        </div>
        <div className={styles.capabilityGrid}>
          {capabilityGroups.map((group) => (
            <div className={styles.capabilityGroup} key={group.title}>
              <h3 className={styles.capabilityGroupTitle}>{group.title}</h3>
              <ul className={styles.capabilityList}>
                {group.items.map((item) => (
                  <li className={styles.capabilityItem} key={item}>
                    {item}
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

export function Services() {
  const { withBaseUrl } = useBaseUrlUtils();
  return (
    <section className={styles.section}>
      <div className="odl-container">
        <div className={styles.sectionHead}>
          <span className="odl-eyebrow">Services</span>
          <h2 className={styles.sectionTitle}>
            50+ storage services, one interface.
          </h2>
          <p className={styles.sectionLede}>
            Enable only the backends your application needs. The Operator
            contract stays identical across every one.
          </p>
        </div>
        <div className={`${styles.serviceGroups} ${styles.reveal}`}>
          {serviceGroups.map((group) => (
            <div className={styles.serviceGroup} key={group.category}>
              <h3 className={styles.serviceGroupTitle}>{group.category}</h3>
              <div className={styles.serviceChips}>
                {group.services.map((s) => (
                  <span className={styles.serviceChip} key={s.name}>
                    <img
                      src={withBaseUrl(s.icon)}
                      alt=""
                      width="15"
                      height="15"
                      loading="lazy"
                    />
                    {s.name}
                  </span>
                ))}
              </div>
            </div>
          ))}
        </div>
        <div className={styles.servicesFoot}>
          <Link
            className={`${styles.btn} ${styles.btnSecondary}`}
            to={DOCS_URL}
          >
            Browse all services <span className={styles.btnArrow}>→</span>
          </Link>
        </div>
      </div>
    </section>
  );
}

export function Bindings() {
  const { withBaseUrl } = useBaseUrlUtils();
  return (
    <section className={`${styles.section} ${styles.sectionSubtle}`}>
      <div className="odl-container">
        <div className={styles.sectionHead}>
          <span className="odl-eyebrow">Bindings</span>
          <h2 className={styles.sectionTitle}>Ship it in your language.</h2>
          <p className={styles.sectionLede}>
            Each binding exposes the same OpenDAL service model while following
            its own ecosystem's conventions.
          </p>
        </div>
        <div className={`${styles.bindingGrid} ${styles.reveal}`}>
          {bindings.map((b) => (
            <div className={styles.bindingCard} key={b.name}>
              <img
                src={withBaseUrl(b.icon)}
                alt=""
                width="30"
                height="30"
                loading="lazy"
              />
              <span className={styles.bindingName}>{b.name}</span>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

export function Layers() {
  const { withBaseUrl } = useBaseUrlUtils();
  return (
    <section className={styles.section}>
      <div className="odl-container">
        <div className={styles.layersInner}>
          <div>
            <span className="odl-eyebrow">Layers</span>
            <h2 className={styles.sectionTitle}>
              Production behavior, composed — not coded.
            </h2>
            <p className={styles.sectionLede}>
              Stack cross-cutting concerns as reusable layers. The order is
              explicit and the core stays zero-cost.
            </p>
            <div style={{ marginTop: "var(--odl-space-6)" }}>
              <CodeTabs
                samples={[
                  {
                    id: "layered",
                    label: "Rust",
                    language: "rust",
                    code: layeredCode,
                  },
                ]}
                title="compose layers onto any operator"
              />
            </div>
          </div>
          <div className={`${styles.layerGrid} ${styles.reveal}`}>
            {layers.map((l) => (
              <div className={styles.layerItem} key={l.name}>
                <img
                  className={styles.layerIcon}
                  src={withBaseUrl(l.icon)}
                  alt=""
                  width="26"
                  height="26"
                  loading="lazy"
                />
                <div>
                  <p className={styles.layerName}>{l.name}</p>
                  <p className={styles.layerDesc}>{l.desc}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}

export function Community() {
  return (
    <section className={`${styles.section} ${styles.sectionSubtle}`}>
      <div className="odl-container">
        <div className={styles.sectionHead}>
          <span className="odl-eyebrow">Open the Apache Way</span>
          <h2 className={styles.sectionTitle}>
            Built by a community, for the commons.
          </h2>
          <p className={styles.sectionLede}>
            Apache OpenDAL™ graduated to a top-level project in 2024. Five
            principles guide every decision we make.
          </p>
        </div>
        <div className={styles.principleGrid}>
          {principles.map((p) => (
            <div className={styles.principle} key={p.title}>
              <h3 className={styles.principleTitle}>{p.title}</h3>
              <p className={styles.principleBody}>{p.body}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

export function FinalCta() {
  return (
    <section className={styles.section}>
      <div className="odl-container">
        <div className={styles.finalCta}>
          <div className={styles.finalGrid} aria-hidden="true" />
          <div className={`${styles.finalCtaInner} ${styles.finalCenter}`}>
            <span className={`odl-eyebrow ${styles.finalEyebrow}`}>
              Start building
            </span>
            <h2 className={styles.finalCtaTitle}>
              One layer for all your storage.
            </h2>
            <p className={styles.finalCtaLede}>
              Join the infrastructure builders, platform teams and application
              developers accessing data freely, painlessly and efficiently.
            </p>
            <div className={styles.finalCtaActions}>
              <Link
                className={`${styles.btn} ${styles.btnOnDark}`}
                to={DOCS_URL}
              >
                Get started <span className={styles.btnArrow}>→</span>
              </Link>
              <Link
                className={`${styles.btn} ${styles.btnOnDarkGhost}`}
                to={REPO_URL}
              >
                Star on GitHub
              </Link>
              <Link
                className={`${styles.btn} ${styles.btnOnDarkGhost}`}
                to={DISCORD_URL}
              >
                Join Discord
              </Link>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
