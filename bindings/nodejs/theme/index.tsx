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

import { DefaultTheme, PageEvent, Reflection, Options, DefaultThemeRenderContext, Application, JSX } from 'typedoc'

export class FooterWithASFCopyright extends DefaultThemeRenderContext {
  constructor(theme: DefaultTheme, page: PageEvent<Reflection>, options: Options) {
    super(theme, page, options)

    this.footer = () => {
      return (
        <>
          <div class="tsd-generator">
            <p>
              Copyright © 2022-2024, The Apache Software Foundation Apache OpenDAL, OpenDAL, and Apache
              are either registered trademarks or trademarks of the Apache Software Foundation.
            </p>
          </div>
        </>
      ) as unknown as JSX.Element
    }
  }
}

export class FooterOverrideTheme extends DefaultTheme {
  private _contextCache?: FooterWithASFCopyright

  override getRenderContext(pageEvent: PageEvent<Reflection>): FooterWithASFCopyright {
    this._contextCache ||= new FooterWithASFCopyright(this, pageEvent, this.application.options)

    return this._contextCache
  }
}

export function load(app: Application) {
  app.renderer.defineTheme('opendal', FooterOverrideTheme)
}
