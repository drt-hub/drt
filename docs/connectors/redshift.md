# Redshift Source

> Read from an Amazon Redshift cluster as a drt sync **source**. Redshift speaks
> the PostgreSQL wire protocol, with an explicit `schema`.

## Profile (`~/.drt/profiles.yml`)

```yaml
redshift_prod:
  type: redshift
  host: my-cluster.xxx.us-east-1.redshift.amazonaws.com
  port: 5439
  dbname: analytics
  user: analyst
  password_env: REDSHIFT_PASSWORD
  schema: public
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"redshift"` | — | Required |
| `host` | string | — | Cluster endpoint |
| `port` | int | `5439` | Redshift default port |
| `dbname` | string | — | Database name |
| `user` | string | — | Username |
| `password_env` | string \| null | null | Env var holding the password (preferred) |
| `password` | string \| null | null | Inline password (not recommended) |
| `schema` | string | `public` | Default schema for unqualified table names |

## Notes

- Requires the extra: `pip install drt-core[redshift]`.
- Prefer `password_env` over an inline `password` — it keeps secrets out of the
  profile file.
- Each sync's `model` SQL runs against the cluster; qualify tables as
  `schema.table` or rely on the profile's `schema`.

## References

- [Amazon Redshift documentation](https://docs.aws.amazon.com/redshift/)
