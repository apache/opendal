# S3 AWS Assume Role With Web Identity

This edge test case is for AWS s3 services that authed by `Assume Role With Web Identity`.

For setup, please configure bucket and OIDC correctly, for example:

```yaml
- uses: actions/github-script@v7
  id: id-token
  with:
    script: return await core.getIDToken("sts.amazonaws.com")
    result-encoding: string
- name: Write ID token to file
  run: echo "${{ steps.id-token.outputs.result }}" > core/tests/data/web_identity_token
```

And configure `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN`.

