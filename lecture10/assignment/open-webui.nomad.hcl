job "open-webui" {
  type = "service"

  group "web" {
    count = 1

    network {
      port "ui" {
        to     = 8080
        static = 3000
      }
    }

    task "open-webui-task" {
      driver = "docker"

      service {
        name     = "open-webui-svc"
        port     = "ui"
        provider = "nomad"
      }

      config {
        image = "ghcr.io/open-webui/open-webui:main"
        ports = ["ui"]
      }

      template {
        data = <<EOH
OLLAMA_BASE_URL=http://host.docker.internal:11434
WEBUI_SECRET_KEY=devkey
ENABLE_SIGNUP=True
EOH
        destination = "secrets/env.env"
        env         = true
      }

      resources {
        cpu    = 400
        memory = 1024
      }
    }
  }
}
