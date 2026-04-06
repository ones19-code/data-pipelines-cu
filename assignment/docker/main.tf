terraform {
  required_version = ">= 1.0"

  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {}

resource "docker_image" "nginx" {
  name = "nginx:latest"
}

resource "docker_image" "n8n" {
  name = "n8nio/n8n:latest"
}

resource "docker_container" "webserver" {
  name  = "lecture7-webserver"
  image = docker_image.nginx.image_id

  ports {
    internal = 80
    external = 8080
  }

  upload {
    file = "/usr/share/nginx/html/index.html"
    content = <<-EOT
      <!DOCTYPE html>
      <html>
      <head>
        <title>Lecture 7 Assignment</title>
      </head>
      <body style="font-family: Arial; text-align: center; margin-top: 100px;">
        <h1>Web Server is Running</h1>
        <p>Terraform Docker Assignment - Lecture 7</p>
        <p>Name: Ons Talbi</p>
      </body>
      </html>
    EOT
  }
}

resource "docker_container" "n8n" {
  name  = "lecture7-n8n"
  image = docker_image.n8n.image_id

  ports {
    internal = 5678
    external = 5678
  }

  env = [
    "N8N_HOST=localhost",
    "N8N_PORT=5678",
    "N8N_PROTOCOL=http",
    "NODE_ENV=production"
  ]

  depends_on = [docker_container.webserver]
}

output "webserver_url" {
  value = "http://localhost:8080"
}

output "n8n_url" {
  value = "http://localhost:5678"
}
