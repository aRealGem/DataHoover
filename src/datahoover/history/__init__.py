"""Staging-only historical backfills (ISC-060-HISTORY-001).

Nothing in this package writes to the DataHoover warehouse. Backfills land as
immutable raw provider bytes plus an append-only manifest in a separate staging
directory; ingesting staged data is a later, separate, reviewed step.
"""
