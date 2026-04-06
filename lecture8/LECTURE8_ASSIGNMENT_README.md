# Lecture 8 Terraform Assignment – Baseline Infrastructure for Data Pipelines (Docker)

## Objective
This assignment deploys a baseline infrastructure for data pipelines using Terraform and Docker.

## Components
- MinIO as S3-compatible object storage
- PostgreSQL as pipeline metadata database

## Lecture 8 Concepts Used
- **Module**: reusable `docker_service` module
- **for_each equivalent logic** through multiple storage locations defined in variables and mapped in locals
- **Variables** for project and service configuration
- **Outputs** for connection info and storage names

## Storage Locations
- raw
- staged
- curated

## Run
```bash
cd lecture8/assignment/docker
terraform init
terraform validate
terraform apply