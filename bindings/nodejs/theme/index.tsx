import { DefaultTheme, PageEvent, Reflection, Options, DefaultThemeRenderContext, Application, JSX } from 'typedoc'

export class FooterWithASFCopyright extends DefaultThemeRenderContext {
  constructor(theme: DefaultTheme, page: PageEvent<Reflection>, options: Options) {
    super(theme, page, options)

    this.footer = () => {
      return (
        <>
          <div class="tsd-generator">
            <p>
              Copyright © 2022-2024, The Apache Software Foundation Apache OpenDAL, OpenDAL, Apache, Apache Incubator,
              the Apache feather, the Apache Incubator logo and the Apache OpenDAL project logo are either registered
              trademarks or trademarks of the Apache Software Foundation.
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
