let
  operator = "ssh-ed25519 <operator-public-key> operator@example-host";
  deployment = "ssh-ed25519 <deployment-public-key> root@example-host";
  recipients = [ operator deployment ];
in {
  "secret/service-token.age".publicKeys = recipients;
  "secret/database-password.age".publicKeys = recipients;
  "secret/transport-ca.age".publicKeys = recipients;
}
