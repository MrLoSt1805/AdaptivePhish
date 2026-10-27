# Template only. Do not run without explicit AWS and billing authorization.
$ErrorActionPreference = 'Stop'
$ImageDigest = '<approved-private-ecr-image-at-sha256-digest>'
if ($ImageDigest -notmatch '@sha256:[0-9a-f]{64}$') {
    throw 'An immutable approved image digest is required.'
}
docker pull $ImageDigest
docker run -d --restart unless-stopped --name adaptivephish -p 127.0.0.1:8000:8000 $ImageDigest
