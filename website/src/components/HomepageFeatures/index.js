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

import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';
import FeatureLanguages from './_feature_languages.mdx';
import FeatureServices from './_feature_services.mdx';
import FeatureLayers from './_feature_layers.mdx';
import MDXContent from '@theme/MDXContent';

const FeatureList = [
  {
    title: 'Languages',
    description: (
      <>
        <MDXContent>
          <FeatureLanguages />
        </MDXContent>
      </>
    ),
  },
  {
    title: 'Services',
    description: (
      <>
        <MDXContent>
          <FeatureServices />
        </MDXContent>
      </>
    ),
  },
  {
    title: 'Layers',
    description: (
      <>
        <MDXContent>
          <FeatureLayers />
        </MDXContent>
      </>
    ),
  },
];

function Feature({ Svg, title, description }) {
  return (
    <div className={clsx('col col--4')}>
      <div className="padding-horiz--md">
        <h3>{title}</h3>
        <div>{description}</div>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
