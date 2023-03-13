import React from 'react';
import clsx from 'clsx';
import styles from './styles.module.css';
import Link from "@docusaurus/Link";

const FeatureList = [
  {
    title: 'Access data freely',
    Svg: require('@site/static/img/undraw_the_world_is_mine.svg').default,
    description: (
      <>
        <div>Access different storage services in the same way</div>
        <div>Behavior tests for all services</div>
        <div>Cross language/project bindings (working on)</div>
      </>
    ),
  },
  {
    title: 'Access data painlessly',
    Svg: require('@site/static/img/undraw_i_can_fly.svg').default,
    description: (
      <>
        <div><b>100%</b> documents covered</div>
        <div>Powerful <Link href="https://docs.rs/opendal/latest/opendal/layers/index.html">Layers</Link></div>
        <div>Automatic <Link href="https://docs.rs/opendal/latest/opendal/layers/struct.RetryLayer.html">retry</Link> support</div>
        <div>Full observability: <Link href="https://docs.rs/opendal/latest/opendal/layers/struct.LoggingLayer.html">logging</Link>, <Link
          href="https://docs.rs/opendal/latest/opendal/layers/struct.TracingLayer.html">tracing</Link>, <Link href="https://docs.rs/opendal/latest/opendal/layers/struct.MetricsLayer.html">metrics</Link>.</div>
        <div><Link href="https://docs.rs/opendal/latest/opendal/layers/struct.ChaosLayer.html">Native chaos testing</Link></div>
      </>
    ),
  },
  {
    title: 'Access data efficiently',
    Svg: require('@site/static/img/undraw_outer_space.svg').default,
    description: (
      <>
        <div>Zero cost: Maps to API calls directly</div>
        <div>Best effort: Automatically selects best read/seek/next based on services</div>
        <div>Avoid extra calls: Reuses metadata when possible</div>
      </>
    ),
  },
];

function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} role="img" />
      </div>
      <div className="text--center padding-horiz--md">
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
