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
import Layout from "@theme/Layout";
import {
  Hero,
  UsedBy,
  ValueProps,
  Capabilities,
  Services,
  Bindings,
  FinalCta,
} from "../components/landing/sections";

export default function Home() {
  return (
    <Layout
      title="One Layer, All Storage"
      description="Apache OpenDAL™ is an Open Data Access Layer for Rust, Python, Node.js, Java, Go, and more. It keeps application storage code clean from simple file access to streaming, concurrent transfers, and multi-backend storage."
    >
      <main>
        <Hero />
        <ValueProps />
        <Capabilities />
        <UsedBy />
        <Bindings />
        <Services />
        <FinalCta />
      </main>
    </Layout>
  );
}
