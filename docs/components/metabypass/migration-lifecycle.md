# SST migration lifetimes

SST disabled mode leaves SSTs in the index directory. Observe-only mode records
policy decisions and preserves file placement. Adaptive mode moves complete
immutable SSTs protected by a published recovery point. Its SSD budget counts
logical SST bytes and may be exceeded while new files await protection.

Promotion and demotion make the destination durable and publish routing before
reclaiming the old source. Reads already holding a physical file keep it until
they finish. A checkpoint candidate pins native file deletion while capturing
live files and retains its protection through publication or cancellation.
The GC worker removes retired recovery points after their replacement is
published.

See [SST tiering](sst-tiering.md) for configuration and metrics.
