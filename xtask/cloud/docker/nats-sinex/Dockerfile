# NATS 2.x with JetStream enabled, sinex-flavoured defaults.
# Image is intentionally thin — config lives in nats.conf next to this file.
FROM nats:2-alpine

COPY nats.conf /etc/nats.conf

# JetStream on; storage path matches the conf file.
CMD ["--config", "/etc/nats.conf", "--jetstream"]
