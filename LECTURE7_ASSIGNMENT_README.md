# Lecture 7 Terraform Assignment – Web Server + n8n (Docker)

## Objective
This project deploys a web server and n8n using Terraform with Docker.

## Services
- **Web server**: nginx serving a simple HTML page on port 8080
- **n8n**: workflow automation UI on port 5678

## Dependency Order
The web server is created first.

The n8n container uses:

`depends_on = [docker_container.webserver]`

This ensures that Terraform starts the web server before starting n8n.

## Files
- `assignment/docker/main.tf`

## Run
```bash
cd lecture7/assignment/docker
terraform init
terraform validate
terraform apply