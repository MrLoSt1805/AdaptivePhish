# AWS EC2 deployment boundary

No AWS resources have been created by this repository. Running EC2, ECR,
load balancers, DNS, log storage, and data transfer can incur charges, so live
deployment requires explicit user authorization and account-specific choices.

After authorization, the intended path is:

1. Build and test the immutable container locally.
2. Push it to a private ECR repository using a versioned image digest.
3. Create or reuse a least-privilege EC2 role that can pull only that image.
4. Permit inbound HTTPS through a narrowly scoped security group; do not expose
   the Uvicorn development port directly to the internet.
5. Run the container with restart and health-check policies, place TLS at a
   reverse proxy or load balancer, and ship aggregate logs/metrics with a
   documented retention period.
6. Roll back by redeploying the preceding approved image digest.

Secrets belong in AWS Systems Manager Parameter Store or Secrets Manager, not
in user data, GitHub Actions, the image, or repository files. The model artifact
must be supplied from an approved private artifact store. The current project
has no DVC remote, so a clean-machine build cannot retrieve it automatically.
