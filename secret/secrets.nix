let
  user     = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDwD8IB2eVfw6X7z9AqBBGjrqOIOCJ4tden1we7mCqOy sinity@sinnix-prime";
  prime    = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILrplBDI4Rrb1hyzqYO7f8/2pmFWupC7C2+hYkBAkOdF root@sinnix-prime";
  ethereal = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEsBMzW1MeF+qcxatMh4nvrQSl3jjAMQyMa+h7egmQyT root@sinnix-ethereal";
  recipients = [ user prime ethereal ];
in {
  "secret/sinex-local-db.age".publicKeys           = recipients;
  "secret/sinex-gateway-admin-token.age".publicKeys = recipients;
  "secret/sinex-remote-db.age".publicKeys           = recipients;
  "secret/sinex-remote-nats-ca.age".publicKeys      = recipients;
  "secret/sinex-remote-nats-cert.age".publicKeys    = recipients;
  "secret/sinex-remote-nats-key.age".publicKeys     = recipients;
}
