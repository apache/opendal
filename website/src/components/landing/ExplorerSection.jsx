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
import CodeBlock from "@theme/CodeBlock";
import IconExternalLink from "@theme/Icon/ExternalLink";
import CodeWindow, {
  CodeWindowBody,
  CodeWindowTitleLink,
} from "../ui/CodeWindow";
import IconAction from "../ui/IconAction";
import styles from "./explorer.module.css";

function ChoiceItem({
  item,
  selected,
  previewId,
  onSelect,
  getTitle,
  getDescription,
  getDoc,
}) {
  const title = getTitle(item);
  const description = getDescription(item);
  const doc = getDoc(item);

  return (
    <li className={styles.choiceShell}>
      <button
        type="button"
        className={
          selected
            ? `${styles.choiceButton} ${styles.choiceButtonActive}`
            : styles.choiceButton
        }
        aria-pressed={selected}
        aria-controls={previewId}
        onClick={() => onSelect(item)}
      >
        <span className={styles.choiceText}>
          <span className={styles.choiceTitle}>{title}</span>
          <span className={styles.choiceBlurb}>{description}</span>
        </span>
      </button>
      <IconAction
        as={Link}
        size="md"
        className={styles.docAction}
        to={doc}
        target="_blank"
        rel="noreferrer"
        aria-label={`${title} documentation`}
      >
        <IconExternalLink />
      </IconAction>
    </li>
  );
}

export default function ExplorerSection({
  id,
  items,
  activeItem,
  onSelect,
  getKey,
  getTitle,
  getDescription,
  getDoc,
  getCode,
}) {
  const title = getTitle(activeItem);
  const doc = getDoc(activeItem);

  return (
    <div className={styles.explorer}>
      <ul className={styles.choiceNav}>
        {items.map((item) => {
          const key = getKey(item);
          return (
            <ChoiceItem
              key={key}
              item={item}
              selected={key === getKey(activeItem)}
              previewId={id}
              onSelect={onSelect}
              getTitle={getTitle}
              getDescription={getDescription}
              getDoc={getDoc}
            />
          );
        })}
      </ul>
      <div className={styles.previewPane} id={id}>
        <CodeWindow
          title={
            <CodeWindowTitleLink
              to={doc}
              target="_blank"
              rel="noreferrer"
              aria-label={`${title} documentation`}
            >
              {title}
            </CodeWindowTitleLink>
          }
        >
          <CodeWindowBody className={styles.previewCodeBody}>
            <div className={styles.previewCodeFade} key={getKey(activeItem)}>
              <CodeBlock language="rust">{getCode(activeItem)}</CodeBlock>
            </div>
          </CodeWindowBody>
        </CodeWindow>
      </div>
    </div>
  );
}
