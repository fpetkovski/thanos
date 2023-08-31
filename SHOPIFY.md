# Shopify's Thanos Fork
This is Shopify's fork of [Thanos](https://github.com/thanos-io/thanos), a highly available Prometheus setup with long term storage capabilities. This fork is used to run Thanos in production at Shopify.

In this file we document the differences between this fork and the upstream Thanos project. It will only include changes that are not raised as pull requests to the upstream project.

## Differences to upstream Thanos

### Endpoint for force-flushing a tenant

In [#120](https://github.com/Shopify/thanos/pull/120) we exposed an endpoint which allows a tenant TSDB to be flushed fully from a receiver.

### Sharded Compactor
With [#115](https://github.com/Shopify/thanos/pull/115) we introduced a sharded compactor, which creates independent shards when compacting blocks. This allows us to run multiple compactor instances in parallel.

### Enable x Functions
x functions were [implemented as part of the Thanos promql engine](https://github.com/thanos-io/promql-engine/pull/216) and are enabled for our fork by default.

### Query Store Selector
In [#67](https://github.com/Shopify/thanos/pull/67) we added a store selector for queriers, which allows us to select a subset of stores to query from. This is useful for sharding.

### Excluded Upstream PRs
- https://github.com/thanos-io/thanos/pull/6299 (breaking changes to series sorting in stores)
- https://github.com/thanos-io/thanos/pull/6420 (see https://github.com/thanos-io/thanos/issues/6495)
