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

import React, { useEffect, useId, useRef, useState } from "react";
import CodeBlock from "@theme/CodeBlock";
import styles from "./styles.module.css";

// A framed "code window" with language tabs. Wraps Docusaurus' CodeBlock so the
// snippets get the same Prism highlighting as the docs.
//
// The header doubles as a terminal-style prompt: a sample's `install` command
// is shown there (tracking the active tab), so install lives in the title bar
// rather than a separate band. With `equalize`, the code area animates its
// height to fit the active snippet — no dead space for short snippets, and the
// left hero column never reflows (its width is locked and it is the taller
// column, so only the code window resizes).
export default function CodeTabs({ samples, title, equalize = false }) {
  const [active, setActive] = useState(0);
  const baseId = useId();
  const current = samples[active];
  const heading = current.install || title;

  const innerRef = useRef(null);
  const [bodyHeight, setBodyHeight] = useState(undefined);

  useEffect(() => {
    if (!equalize || !innerRef.current) return undefined;
    const measure = () => setBodyHeight(innerRef.current?.offsetHeight);
    measure();
    const ro =
      typeof ResizeObserver !== "undefined"
        ? new ResizeObserver(measure)
        : null;
    if (ro && innerRef.current) ro.observe(innerRef.current);
    return () => ro?.disconnect();
  }, [active, equalize]);

  function onKeyDown(event) {
    if (event.key !== "ArrowRight" && event.key !== "ArrowLeft") return;
    event.preventDefault();
    const dir = event.key === "ArrowRight" ? 1 : -1;
    setActive((i) => (i + dir + samples.length) % samples.length);
  }

  return (
    <div className={styles.codeWindow}>
      <div className={styles.windowBar}>
        <div className={styles.windowDots} aria-hidden="true">
          <span />
          <span />
          <span />
        </div>
        <span className={styles.windowTitle}>{heading}</span>
      </div>

      <div
        className={styles.codeTabs}
        role="tablist"
        aria-label="Choose a language"
        onKeyDown={onKeyDown}
      >
        {samples.map((sample, i) => (
          <button
            key={sample.id}
            type="button"
            role="tab"
            id={`${baseId}-tab-${sample.id}`}
            aria-selected={i === active}
            aria-controls={`${baseId}-panel`}
            tabIndex={i === active ? 0 : -1}
            className={`${styles.codeTab} ${
              i === active ? styles.codeTabActive : ""
            }`}
            onClick={() => setActive(i)}
          >
            {sample.label}
          </button>
        ))}
      </div>

      <div
        className={`${styles.codeBody} ${equalize ? styles.codeBodyAnimated : ""}`}
        style={
          equalize && bodyHeight != null ? { height: bodyHeight } : undefined
        }
        role="tabpanel"
        id={`${baseId}-panel`}
        aria-labelledby={`${baseId}-tab-${current.id}`}
      >
        <div ref={innerRef}>
          <CodeBlock language={current.language}>{current.code}</CodeBlock>
        </div>
      </div>
    </div>
  );
}
