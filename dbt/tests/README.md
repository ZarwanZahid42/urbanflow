# Singular tests

This directory contains focused integrity rules that standard generic tests cannot express.
Three source tests enforce the authoritative Phase 7 aggregate composite keys. Seven downstream
tests verify intermediate row preservation, each mart aggregate business key, nonnegative count
measures, and the rule that daily financial-adjustment count cannot exceed trip count.

Generic source, staging, intermediate, and mart tests remain beside their YAML contracts. They
cover key nullability/uniqueness, relationships, resolved dimension attributes, and the governed
hourly time-of-day values. Phase 7 reconciliation, audits, partition gates, and idempotency are
not duplicated here.
