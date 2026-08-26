# dbt-maxcompute v1.11.3b3 Release Notes

`1.11.3b3` is a Production Preview maintenance release for MaxFrame Python
models.

## Fixed

- Added support for Alibaba Cloud default credential provider chain
  authentication (`auth_type: chain`) in MaxFrame Python models.
- Prevented nested PyODPS option contexts from copying the credential
  provider's internal refresh lock, which previously failed before the
  MaxFrame job was submitted.
- Preserved dynamic credential lookup and automatic refresh. No static access
  key conversion or global monkey patch is used.

This resolves [GitHub issue #27](https://github.com/aliyun/dbt-maxcompute/issues/27).

## Upgrade

```bash
python -m pip install --upgrade "dbt-maxcompute[maxframe]==1.11.3b3"
```

After upgrading, profiles can use:

```yaml
auth_type: chain
```

The `sitecustomize.py` / `CredentialProviderAccount.__deepcopy__` workaround
described in issue #27 can then be removed.

## Validation

- PyODPS 0.12.6 and 0.13.0 nested option contexts
- MaxFrame 2.8.0.post0 native PyODPS option synchronization
- Real MaxCompute MaxFrame model execution with a chain-only profile
- Full unit, formatting, lint, type, and package checks
