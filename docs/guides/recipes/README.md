# `rest_api` Recipes

Tested, copy-paste sync recipes for SaaS destinations using `drt`'s built-in [`rest_api`](../../connectors/rest-api.md) destination. Every recipe's YAML is validated and executed against a local HTTP server in `tests/unit/test_rest_api_recipes.py`.

## Available Recipes

- [PagerDuty (Events API v2)](pagerduty.md) — trigger or resolve alerts from warehouse conditions
- [Braze (`/users/track`)](braze.md) — sync user attributes and custom events
- [Customer.io (Track API v2 `batch`)](customer_io.md) — identify customers and send events
- [Iterable (`bulkUpdate` / `trackBulk`)](iterable.md) — sync user profiles and bulk events
