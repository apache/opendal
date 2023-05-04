Apache OpenDAL (Incubating)
Copyright 2023 The Apache Software Foundation

This product includes software developed at
The Apache Software Foundation (http://www.apache.org/).

=======================================================================
Apache OpenDAL Subcomponents:

The Apache OpenDAL project contains subcomponents with separate copyright
notices and license terms. Your use of the source code for the these
subcomponents is subject to the terms and conditions of the following
licenses.
========================================================================

{{ range .Groups }}
========================================================================
{{ .LicenseID }} licenses
========================================================================
The following components are provided under the {{ .LicenseID }} License. See project link for details.
    {{ range .Deps }}
    https://crates.io/crates/{{ .Name }}/{{ .Version }} {{ .Version }} {{ .LicenseID }}
    {{- end }}
{{ end }}
